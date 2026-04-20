from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, is_dataclass
from typing import Any, Iterable

from vei.events.api import (
    ActorRef,
    CanonicalEvent,
    EventProvenance,
    ObjectRef,
    ProvenanceRecord,
    StateDelta,
    infer_domain,
)
from vei.structure.models import (
    DerivedCase,
    DerivedEntity,
    DerivedHypothesis,
    DerivedRelation,
    DerivedTimeline,
    StructureEvidence,
    StructureMetrics,
    StructureTruthComparison,
    StructureView,
)

_BOUNDARY_EXPORTS = (StructureTruthComparison, StructureView)


def build_structure_view_from_canonical_events(
    events: Iterable[CanonicalEvent],
    *,
    source_mode: str = "canonical",
) -> StructureView:
    ordered_events = sorted(events, key=lambda item: (int(item.ts_ms), item.event_id))
    entity_state: dict[str, dict[str, Any]] = {}
    relation_state: dict[tuple[str, str, str], dict[str, Any]] = {}
    case_state: dict[str, dict[str, Any]] = {}
    display_name_to_actor_ids: dict[str, set[str]] = defaultdict(set)
    actor_id_to_aliases: dict[str, set[str]] = defaultdict(set)
    case_event_times: dict[str, list[int]] = defaultdict(list)
    investigation_paths: list[str] = []

    for event in ordered_events:
        event_surfaces = _event_surfaces(event)
        actor_entity_ids = _actor_entity_ids(event, entity_state, event_surfaces)
        object_entity_ids = _object_entity_ids(event, entity_state, event_surfaces)
        case_key = _case_key(event)
        case_title = _case_title(event)

        if case_key is not None:
            case_entry = case_state.setdefault(
                case_key,
                {
                    "title": case_title,
                    "case_source": "explicit" if event.case_id else "inferred",
                    "confidence": 1.0 if event.case_id else 0.65,
                    "anchor_refs": set(),
                    "event_ids": [],
                    "entity_ids": set(),
                    "surfaces": set(),
                    "evidence_event_ids": set(),
                    "provenance_sources": set(),
                    "metadata": {
                        "explicit_case_id": event.case_id,
                        "case_key": case_key,
                    },
                },
            )
            if case_title and not case_entry["title"]:
                case_entry["title"] = case_title
            case_entry["anchor_refs"].add(case_key)
            case_entry["event_ids"].append(event.event_id)
            case_entry["surfaces"].update(event_surfaces)
            case_entry["evidence_event_ids"].add(event.event_id)
            case_entry["provenance_sources"].add(str(event.provenance.source_id or ""))
            case_event_times[case_key].append(int(event.ts_ms))

        for actor in _actor_refs(event):
            actor_id = actor.get("actor_id")
            display_name = actor.get("display_name")
            if actor_id and display_name:
                normalized = _normalize_label(display_name)
                if normalized:
                    display_name_to_actor_ids[normalized].add(actor_id)
                actor_id_to_aliases[actor_id].add(display_name)

        if case_key is not None:
            for entity_id in actor_entity_ids + object_entity_ids:
                case_state[case_key]["entity_ids"].add(entity_id)
                _record_relation(
                    relation_state,
                    relation_type="involved_in_case",
                    source_entity_id=entity_id,
                    target_entity_id=f"case:{case_key}",
                    event=event,
                    surfaces=event_surfaces,
                    metadata={"case_key": case_key},
                )

        if actor_entity_ids:
            for source_entity_id in actor_entity_ids:
                for target_entity_id in object_entity_ids:
                    _record_relation(
                        relation_state,
                        relation_type="acted_on",
                        source_entity_id=source_entity_id,
                        target_entity_id=target_entity_id,
                        event=event,
                        surfaces=event_surfaces,
                        metadata={"kind": event.kind},
                    )
            if len(actor_entity_ids) > 1:
                for source_entity_id in actor_entity_ids:
                    for target_entity_id in actor_entity_ids:
                        if source_entity_id == target_entity_id:
                            continue
                        _record_relation(
                            relation_state,
                            relation_type="co_present",
                            source_entity_id=source_entity_id,
                            target_entity_id=target_entity_id,
                            event=event,
                            surfaces=event_surfaces,
                            metadata={"kind": event.kind},
                        )

    hypotheses: list[DerivedHypothesis] = []
    for normalized_name, actor_ids in sorted(display_name_to_actor_ids.items()):
        if len(actor_ids) <= 1:
            continue
        candidate_entity_ids = sorted(f"actor:{item}" for item in actor_ids)
        aliases = sorted(
            {alias for actor_id in actor_ids for alias in actor_id_to_aliases[actor_id]}
        )
        evidence_event_ids = sorted(
            {
                event_id
                for entity_id in candidate_entity_ids
                for event_id in entity_state.get(entity_id, {}).get(
                    "evidence_event_ids", set()
                )
            }
        )
        hypothesis = DerivedHypothesis(
            hypothesis_id=f"hypothesis:{normalized_name}",
            title=f"Alias ambiguity for {aliases[0] if aliases else normalized_name}",
            summary=(
                "Multiple actor identifiers share the same normalized display name."
            ),
            confidence=0.6,
            candidate_entity_ids=candidate_entity_ids,
            evidence=StructureEvidence(
                event_ids=evidence_event_ids,
                surfaces=sorted(
                    {
                        surface
                        for entity_id in candidate_entity_ids
                        for surface in entity_state.get(entity_id, {}).get(
                            "surfaces", set()
                        )
                    }
                ),
                provenance_sources=sorted(
                    {
                        source
                        for entity_id in candidate_entity_ids
                        for source in entity_state.get(entity_id, {}).get(
                            "provenance_sources", set()
                        )
                        if source
                    }
                ),
            ),
            metadata={"normalized_name": normalized_name, "aliases": aliases},
        )
        hypotheses.append(hypothesis)
        investigation_paths.append(
            f"Review whether {', '.join(aliases[:2] or [normalized_name])} refer to the same person."
        )

    cases: list[DerivedCase] = []
    for case_key, payload in sorted(case_state.items()):
        case_id = f"case:{case_key}"
        title = payload["title"] or case_key
        surfaces = sorted(payload["surfaces"])
        event_ids = list(dict.fromkeys(payload["event_ids"]))
        confidence = float(payload["confidence"])
        if payload["case_source"] == "inferred" and len(surfaces) > 1:
            confidence = max(confidence, 0.75)
            investigation_paths.append(
                f"Inspect cross-surface cluster {title} for a hidden shared case."
            )
        cases.append(
            DerivedCase(
                case_id=case_id,
                title=title,
                case_source=payload["case_source"],
                confidence=confidence,
                anchor_refs=sorted(payload["anchor_refs"]),
                event_ids=event_ids,
                entity_ids=sorted(payload["entity_ids"]),
                surfaces=surfaces,
                evidence=StructureEvidence(
                    event_ids=sorted(payload["evidence_event_ids"]),
                    surfaces=surfaces,
                    provenance_sources=sorted(
                        source for source in payload["provenance_sources"] if source
                    ),
                ),
                metadata=payload["metadata"],
            )
        )

    entities = [
        DerivedEntity(
            entity_id=entity_id,
            entity_type=str(payload["entity_type"]),
            title=payload.get("title"),
            canonical_ref=payload.get("canonical_ref"),
            aliases=sorted(payload["aliases"]),
            confidence=float(payload["confidence"]),
            evidence=StructureEvidence(
                event_ids=sorted(payload["evidence_event_ids"]),
                surfaces=sorted(payload["surfaces"]),
                provenance_sources=sorted(
                    source for source in payload["provenance_sources"] if source
                ),
            ),
            metadata=dict(payload.get("metadata", {})),
        )
        for entity_id, payload in sorted(entity_state.items())
    ]

    relations = [
        DerivedRelation(
            relation_id=f"{relation_type}:{source_entity_id}:{target_entity_id}",
            relation_type=relation_type,
            source_entity_id=source_entity_id,
            target_entity_id=target_entity_id,
            confidence=float(payload["confidence"]),
            evidence=StructureEvidence(
                event_ids=sorted(payload["evidence_event_ids"]),
                surfaces=sorted(payload["surfaces"]),
                provenance_sources=sorted(
                    source for source in payload["provenance_sources"] if source
                ),
            ),
            metadata=dict(payload.get("metadata", {})),
        )
        for (relation_type, source_entity_id, target_entity_id), payload in sorted(
            relation_state.items()
        )
    ]

    timelines = [
        DerivedTimeline(
            timeline_id="timeline:all-events",
            label="Full event timeline",
            event_ids=[event.event_id for event in ordered_events],
            entity_ids=[],
            start_ts_ms=int(ordered_events[0].ts_ms) if ordered_events else 0,
            end_ts_ms=int(ordered_events[-1].ts_ms) if ordered_events else 0,
            metadata={"case_count": len(cases)},
        )
    ]
    for case in cases:
        timestamps = case_event_times.get(case.case_id.removeprefix("case:"), [])
        timelines.append(
            DerivedTimeline(
                timeline_id=f"timeline:{case.case_id}",
                label=case.title,
                case_id=case.case_id,
                event_ids=list(case.event_ids),
                entity_ids=list(case.entity_ids),
                start_ts_ms=min(timestamps) if timestamps else 0,
                end_ts_ms=max(timestamps) if timestamps else 0,
                metadata={"case_source": case.case_source},
            )
        )

    if not investigation_paths and cases:
        investigation_paths.append(
            f"Confirm whether {cases[0].title} has enough evidence for automated case linking."
        )

    return StructureView(
        source_mode=source_mode,
        total_event_count=len(ordered_events),
        entities=entities,
        cases=cases,
        relations=relations,
        timelines=timelines,
        hypotheses=hypotheses,
        suggested_investigations=list(dict.fromkeys(investigation_paths))[:8],
        metadata={
            "explicit_case_count": sum(
                1 for item in cases if item.case_source == "explicit"
            ),
            "inferred_case_count": sum(
                1 for item in cases if item.case_source == "inferred"
            ),
            "hypothesis_count": len(hypotheses),
        },
    )


def build_structure_view_from_world_state(state: Any) -> StructureView:
    canonical_events = _canonical_events_from_world_state(state)
    return build_structure_view_from_canonical_events(
        canonical_events,
        source_mode="world_state_event_log",
    )


def build_structure_view_from_state_payload(payload: dict[str, Any]) -> StructureView:
    return build_structure_view_from_world_state(payload)


def structure_signal_payload(
    comparison: StructureTruthComparison,
) -> dict[str, float]:
    metrics = comparison.metrics.model_dump(mode="json")
    payload: dict[str, float] = {}

    if comparison.truth_entity_refs:
        payload["entity_link_precision"] = float(metrics["entity_link_precision"])
        payload["entity_link_recall"] = float(metrics["entity_link_recall"])
        payload["entity_link_quality"] = float(metrics["entity_link_quality"])

    if comparison.truth_relation_refs:
        payload["relation_precision"] = float(metrics["relation_precision"])
        payload["relation_recall"] = float(metrics["relation_recall"])
        payload["relation_recovery"] = float(metrics["relation_recovery"])

    if comparison.truth_hidden_case_refs:
        payload["hidden_case_discovery"] = float(metrics["hidden_case_discovery"])

    if comparison.expected_ambiguity_refs:
        payload["action_choice_under_uncertainty"] = float(
            metrics["action_choice_under_uncertainty"]
        )

    if int(comparison.metadata.get("timeline_pair_count", 0)) > 0:
        payload["event_ordering"] = float(metrics["event_ordering"])

    return payload


def compare_structure_to_truth(
    structure_view: StructureView,
    state: Any,
) -> StructureTruthComparison:
    canonical_events = _canonical_events_from_world_state(state)
    truth_entity_refs = sorted(_truth_entity_refs(canonical_events))
    derived_entity_refs = sorted(
        item.canonical_ref for item in structure_view.entities if item.canonical_ref
    )
    matched_entity_refs = sorted(set(truth_entity_refs) & set(derived_entity_refs))

    truth_relation_refs = sorted(_truth_relation_refs(canonical_events))
    derived_relation_refs = sorted(
        _derived_relation_ref(item) for item in structure_view.relations
    )
    matched_relation_refs = sorted(
        set(truth_relation_refs) & set(derived_relation_refs)
    )

    truth_hidden_case_refs = sorted(_truth_hidden_case_refs(canonical_events))
    discovered_hidden_case_refs = sorted(
        item.case_id
        for item in structure_view.cases
        if item.case_source == "inferred" and len(item.surfaces) > 1
    )

    expected_ambiguity_refs = sorted(_expected_ambiguity_refs(canonical_events))
    satisfied_ambiguity_refs = sorted(
        item.metadata.get("normalized_name", "")
        for item in structure_view.hypotheses
        if item.status == "open"
    )

    entity_precision = _safe_ratio(len(matched_entity_refs), len(derived_entity_refs))
    entity_recall = _safe_ratio(len(matched_entity_refs), len(truth_entity_refs))
    relation_precision = _safe_ratio(
        len(matched_relation_refs), len(derived_relation_refs)
    )
    relation_recall = _safe_ratio(len(matched_relation_refs), len(truth_relation_refs))
    hidden_case_discovery = _safe_ratio(
        len(set(truth_hidden_case_refs) & set(discovered_hidden_case_refs)),
        len(truth_hidden_case_refs),
    )
    event_ordering, timeline_pair_count = _timeline_ordering_stats(
        structure_view,
        canonical_events,
    )
    ambiguity_score = _safe_ratio(
        len(set(expected_ambiguity_refs) & set(satisfied_ambiguity_refs)),
        len(expected_ambiguity_refs),
    )

    return StructureTruthComparison(
        source_mode=structure_view.source_mode,
        metrics=StructureMetrics(
            entity_link_precision=entity_precision,
            entity_link_recall=entity_recall,
            entity_link_quality=(entity_precision + entity_recall) / 2.0,
            hidden_case_discovery=hidden_case_discovery,
            relation_precision=relation_precision,
            relation_recall=relation_recall,
            relation_recovery=(relation_precision + relation_recall) / 2.0,
            event_ordering=event_ordering,
            action_choice_under_uncertainty=ambiguity_score,
        ),
        truth_entity_refs=truth_entity_refs,
        derived_entity_refs=derived_entity_refs,
        missing_entity_refs=sorted(set(truth_entity_refs) - set(derived_entity_refs)),
        extra_entity_refs=sorted(set(derived_entity_refs) - set(truth_entity_refs)),
        truth_relation_refs=truth_relation_refs,
        derived_relation_refs=derived_relation_refs,
        missing_relation_refs=sorted(
            set(truth_relation_refs) - set(derived_relation_refs)
        ),
        extra_relation_refs=sorted(
            set(derived_relation_refs) - set(truth_relation_refs)
        ),
        truth_hidden_case_refs=truth_hidden_case_refs,
        discovered_hidden_case_refs=discovered_hidden_case_refs,
        expected_ambiguity_refs=expected_ambiguity_refs,
        satisfied_ambiguity_refs=satisfied_ambiguity_refs,
        metadata={
            "entity_match_count": len(matched_entity_refs),
            "relation_match_count": len(matched_relation_refs),
            "canonical_event_count": len(canonical_events),
            "timeline_pair_count": timeline_pair_count,
        },
    )


def compare_structure_to_truth_from_state_payload(
    structure_view: StructureView,
    payload: dict[str, Any],
) -> StructureTruthComparison:
    return compare_structure_to_truth(structure_view, payload)


def _canonical_events_from_world_state(state: Any) -> list[CanonicalEvent]:
    events: list[CanonicalEvent] = []
    for raw_event in _state_list(state, "event_log"):
        payload = raw_event if isinstance(raw_event, dict) else _model_dump(raw_event)
        if not payload:
            continue
        events.append(_state_log_event_to_canonical(payload))
    if events:
        return events

    for index, raw_trace in enumerate(_state_list(state, "trace_entries"), start=1):
        if not isinstance(raw_trace, dict):
            continue
        if raw_trace.get("type") == "call":
            tool = str(raw_trace.get("tool", "trace.call"))
            payload = dict(raw_trace.get("args", {}))
            payload["target"] = tool.split(".", 1)[0]
            events.append(
                _state_log_event_to_canonical(
                    {
                        "event_id": f"trace-call-{index}",
                        "kind": tool,
                        "payload": payload,
                        "clock_ms": raw_trace.get("time_ms", 0),
                    }
                )
            )
        elif raw_trace.get("type") == "event":
            payload = dict(raw_trace.get("payload", {}))
            payload["target"] = raw_trace.get("target")
            events.append(
                _state_log_event_to_canonical(
                    {
                        "event_id": f"trace-event-{index}",
                        "kind": str(raw_trace.get("target", "trace.event")),
                        "payload": payload,
                        "clock_ms": raw_trace.get("time_ms", 0),
                    }
                )
            )
    return events


def _actor_entity_ids(
    event: CanonicalEvent,
    entity_state: dict[str, dict[str, Any]],
    event_surfaces: set[str],
) -> list[str]:
    entity_ids: list[str] = []
    for actor in _actor_refs(event):
        actor_id = actor.get("actor_id")
        if not actor_id:
            continue
        entity_id = f"actor:{actor_id}"
        entity_ids.append(entity_id)
        entity = entity_state.setdefault(
            entity_id,
            {
                "entity_type": "actor",
                "title": actor.get("display_name") or actor_id,
                "canonical_ref": entity_id,
                "aliases": set(),
                "confidence": 1.0,
                "evidence_event_ids": set(),
                "surfaces": set(),
                "provenance_sources": set(),
                "metadata": {"role": actor.get("role") or ""},
            },
        )
        if actor.get("display_name"):
            entity["aliases"].add(actor["display_name"])
        entity["evidence_event_ids"].add(event.event_id)
        entity["surfaces"].update(event_surfaces)
        source_id = str(event.provenance.source_id or "")
        if source_id:
            entity["provenance_sources"].add(source_id)
    return entity_ids


def _object_entity_ids(
    event: CanonicalEvent,
    entity_state: dict[str, dict[str, Any]],
    event_surfaces: set[str],
) -> list[str]:
    entity_ids: list[str] = []
    for object_ref in event.object_refs:
        object_id = str(object_ref.object_id or "")
        if not object_id:
            continue
        entity_id = f"object:{object_id}"
        entity_ids.append(entity_id)
        entity = entity_state.setdefault(
            entity_id,
            {
                "entity_type": object_ref.kind or "object",
                "title": object_ref.label or object_id,
                "canonical_ref": entity_id,
                "aliases": set(),
                "confidence": 1.0,
                "evidence_event_ids": set(),
                "surfaces": set(),
                "provenance_sources": set(),
                "metadata": {"domain": object_ref.domain or event.domain.value},
            },
        )
        if object_ref.label:
            entity["aliases"].add(object_ref.label)
        entity["evidence_event_ids"].add(event.event_id)
        entity["surfaces"].update(event_surfaces)
        source_id = str(event.provenance.source_id or "")
        if source_id:
            entity["provenance_sources"].add(source_id)

    payload = _event_payload(event)
    for key, kind in (
        ("thread_id", "thread"),
        ("thread_ref", "thread"),
        ("conversation_anchor", "thread"),
        ("ticket_id", "ticket"),
        ("doc_id", "document"),
        ("document_id", "document"),
        ("channel", "channel"),
        ("deal_id", "deal"),
    ):
        value = _coerce_scalar(payload.get(key))
        if not value:
            continue
        entity_id = f"object:{kind}:{value}"
        entity_ids.append(entity_id)
        entity = entity_state.setdefault(
            entity_id,
            {
                "entity_type": kind,
                "title": value,
                "canonical_ref": entity_id,
                "aliases": set(),
                "confidence": 0.8,
                "evidence_event_ids": set(),
                "surfaces": set(),
                "provenance_sources": set(),
                "metadata": {"derived_from": key},
            },
        )
        entity["evidence_event_ids"].add(event.event_id)
        entity["surfaces"].update(event_surfaces)
        source_id = str(event.provenance.source_id or "")
        if source_id:
            entity["provenance_sources"].add(source_id)

    return sorted(set(entity_ids))


def _record_relation(
    relation_state: dict[tuple[str, str, str], dict[str, Any]],
    *,
    relation_type: str,
    source_entity_id: str,
    target_entity_id: str,
    event: CanonicalEvent,
    surfaces: set[str],
    metadata: dict[str, Any],
) -> None:
    key = (relation_type, source_entity_id, target_entity_id)
    relation = relation_state.setdefault(
        key,
        {
            "confidence": 0.75,
            "evidence_event_ids": set(),
            "surfaces": set(),
            "provenance_sources": set(),
            "metadata": {},
        },
    )
    relation["evidence_event_ids"].add(event.event_id)
    relation["surfaces"].update(surfaces)
    source_id = str(event.provenance.source_id or "")
    if source_id:
        relation["provenance_sources"].add(source_id)
    relation["metadata"].update(metadata)


def _case_key(event: CanonicalEvent) -> str | None:
    if event.case_id:
        return str(event.case_id)
    payload = _event_payload(event)
    for key in (
        "case_id",
        "conversation_anchor",
        "thread_ref",
        "thread_id",
        "ticket_id",
        "doc_id",
        "document_id",
        "deal_id",
    ):
        value = _coerce_scalar(payload.get(key))
        if value:
            return value
    subject = _coerce_scalar(payload.get("subject"))
    if subject:
        return f"subject:{_normalize_label(subject)}"
    if event.object_refs:
        return str(event.object_refs[0].object_id)
    return None


def _case_title(event: CanonicalEvent) -> str:
    payload = _event_payload(event)
    for key in ("subject", "title", "label", "snippet"):
        value = _coerce_scalar(payload.get(key))
        if value:
            return value
    if event.case_id:
        return str(event.case_id)
    if event.object_refs:
        return str(event.object_refs[0].label or event.object_refs[0].object_id)
    return event.kind


def _event_surfaces(event: CanonicalEvent) -> set[str]:
    surfaces = {event.domain.value}
    payload = _event_payload(event)
    for key in ("surface", "target", "tool"):
        value = _coerce_scalar(payload.get(key))
        if value:
            surfaces.add(value.split(".", 1)[0])
    for object_ref in event.object_refs:
        if object_ref.kind:
            surfaces.add(str(object_ref.kind))
    return {item for item in surfaces if item}


def _actor_refs(event: CanonicalEvent) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    if event.actor_ref is not None and event.actor_ref.actor_id:
        refs.append(event.actor_ref.model_dump(mode="json"))
    for participant in event.participants:
        if participant.actor_id:
            refs.append(participant.model_dump(mode="json"))

    payload = _event_payload(event)
    actor_id = _coerce_scalar(
        payload.get("actor_id")
        or payload.get("actor")
        or payload.get("user_id")
        or payload.get("sender")
        or payload.get("assignee")
    )
    if actor_id:
        refs.append(
            {
                "actor_id": actor_id,
                "display_name": _coerce_scalar(payload.get("actor_name")) or actor_id,
                "role": _coerce_scalar(payload.get("role")) or "",
            }
        )
    unique: dict[str, dict[str, str]] = {}
    for item in refs:
        unique[item.get("actor_id", "")] = item
    return [item for key, item in unique.items() if key]


def _truth_entity_refs(events: list[CanonicalEvent]) -> set[str]:
    refs: set[str] = set()
    for event in events:
        for actor in _actor_refs(event):
            actor_id = actor.get("actor_id")
            if actor_id:
                refs.add(f"actor:{actor_id}")
        for object_ref in event.object_refs:
            if object_ref.object_id:
                refs.add(f"object:{object_ref.object_id}")
    return refs


def _truth_relation_refs(events: list[CanonicalEvent]) -> set[str]:
    refs: set[str] = set()
    for event in events:
        actor_ids = [
            item.get("actor_id") for item in _actor_refs(event) if item.get("actor_id")
        ]
        object_ids = [item.object_id for item in event.object_refs if item.object_id]
        case_key = _case_key(event)
        for actor_id in actor_ids:
            for object_id in object_ids:
                refs.add(f"acted_on:actor:{actor_id}->object:{object_id}")
            if case_key:
                refs.add(f"involved_in_case:actor:{actor_id}->case:{case_key}")
        if case_key:
            for object_id in object_ids:
                refs.add(f"involved_in_case:object:{object_id}->case:{case_key}")
        for source_actor_id in actor_ids:
            for target_actor_id in actor_ids:
                if source_actor_id == target_actor_id:
                    continue
                refs.add(f"co_present:actor:{source_actor_id}->actor:{target_actor_id}")
    return refs


def _truth_hidden_case_refs(events: list[CanonicalEvent]) -> set[str]:
    grouped_surfaces: dict[str, set[str]] = defaultdict(set)
    grouped_counts: dict[str, int] = defaultdict(int)
    for event in events:
        if event.case_id:
            continue
        case_key = _case_key(event)
        if not case_key:
            continue
        grouped_surfaces[case_key].update(_event_surfaces(event))
        grouped_counts[case_key] += 1
    return {
        f"case:{case_key}"
        for case_key, count in grouped_counts.items()
        if count > 1 and len(grouped_surfaces[case_key]) > 1
    }


def _expected_ambiguity_refs(events: list[CanonicalEvent]) -> set[str]:
    names: dict[str, set[str]] = defaultdict(set)
    for event in events:
        for actor in _actor_refs(event):
            actor_id = actor.get("actor_id")
            display_name = actor.get("display_name")
            normalized = _normalize_label(display_name)
            if actor_id and normalized:
                names[normalized].add(actor_id)
    return {name for name, actor_ids in names.items() if len(actor_ids) > 1}


def _derived_relation_ref(relation: DerivedRelation) -> str:
    return f"{relation.relation_type}:{relation.source_entity_id}->{relation.target_entity_id}"


def _timeline_ordering_score(
    structure_view: StructureView,
    canonical_events: list[CanonicalEvent],
) -> float:
    score, _checked = _timeline_ordering_stats(structure_view, canonical_events)
    return score


def _timeline_ordering_stats(
    structure_view: StructureView,
    canonical_events: list[CanonicalEvent],
) -> tuple[float, int]:
    order_index = {
        event.event_id: index for index, event in enumerate(canonical_events)
    }
    checked = 0
    ordered = 0
    for timeline in structure_view.timelines:
        if len(timeline.event_ids) < 2:
            continue
        for previous, current in zip(timeline.event_ids, timeline.event_ids[1:]):
            previous_index = order_index.get(previous)
            current_index = order_index.get(current)
            if previous_index is None or current_index is None:
                continue
            checked += 1
            if previous_index <= current_index:
                ordered += 1
    return _safe_ratio(ordered, checked), checked


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 1.0
    return round(float(numerator) / float(denominator), 4)


def _coerce_scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    return ""


def _normalize_label(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(
        "".join(char.lower() if char.isalnum() else " " for char in value).split()
    )


def _model_dump(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if is_dataclass(value):
        return asdict(value)
    return {}


def _event_payload(event: CanonicalEvent) -> dict[str, Any]:
    payload = dict(event.delta.data) if event.delta is not None else {}
    args = payload.get("args")
    if isinstance(args, dict):
        for key, value in args.items():
            payload.setdefault(key, value)
    return payload


def _state_field(state: Any, field_name: str, default: Any) -> Any:
    if isinstance(state, dict):
        return state.get(field_name, default)
    return getattr(state, field_name, default)


def _state_list(state: Any, field_name: str) -> list[Any]:
    value = _state_field(state, field_name, [])
    return value if isinstance(value, list) else []


def _state_log_event_to_canonical(raw: dict[str, Any]) -> CanonicalEvent:
    kind = str(raw.get("kind", ""))
    payload = dict(raw.get("payload", {}))
    domain = infer_domain(kind, payload)
    actor_ref = _payload_actor_ref(payload)
    object_refs = _payload_object_refs(payload, domain=domain)
    explicit_case_id = _coerce_scalar(payload.get("case_id")) or None
    return CanonicalEvent(
        event_id=str(raw.get("event_id", "")),
        case_id=explicit_case_id,
        ts_ms=int(raw.get("clock_ms", 0)),
        domain=domain,
        kind=f"{domain.value}.{kind}" if "." not in kind else kind,
        actor_ref=actor_ref,
        object_refs=object_refs,
        provenance=ProvenanceRecord(origin=EventProvenance.IMPORTED),
        delta=StateDelta(
            domain=domain,
            delta_schema_version=0,
            data=payload,
        ),
    )


def _payload_actor_ref(payload: dict[str, Any]) -> ActorRef | None:
    actor_id = _coerce_scalar(
        payload.get("actor_id")
        or payload.get("actor")
        or payload.get("user_id")
        or payload.get("sender")
        or payload.get("assignee")
    )
    if not actor_id:
        return None
    return ActorRef(
        actor_id=actor_id,
        display_name=_coerce_scalar(payload.get("actor_name")) or actor_id,
        role=_coerce_scalar(payload.get("role")),
    )


def _payload_object_refs(
    payload: dict[str, Any],
    *,
    domain: Any,
) -> list[ObjectRef]:
    refs: list[ObjectRef] = []
    for key, kind in (
        ("thread_id", "thread"),
        ("thread_ref", "thread"),
        ("conversation_anchor", "thread"),
        ("ticket_id", "ticket"),
        ("doc_id", "document"),
        ("document_id", "document"),
        ("channel", "channel"),
        ("deal_id", "deal"),
    ):
        value = _coerce_scalar(payload.get(key))
        if not value:
            continue
        refs.append(
            ObjectRef(
                object_id=f"{kind}:{value}",
                domain=str(getattr(domain, "value", domain) or ""),
                kind=kind,
                label=value,
            )
        )
    unique: dict[str, ObjectRef] = {}
    for ref in refs:
        unique[ref.object_id] = ref
    return list(unique.values())


__all__ = [
    "DerivedCase",
    "DerivedEntity",
    "DerivedHypothesis",
    "DerivedRelation",
    "DerivedTimeline",
    "StructureTruthComparison",
    "StructureView",
    "build_structure_view_from_canonical_events",
    "build_structure_view_from_state_payload",
    "build_structure_view_from_world_state",
    "compare_structure_to_truth",
    "compare_structure_to_truth_from_state_payload",
    "structure_signal_payload",
]
