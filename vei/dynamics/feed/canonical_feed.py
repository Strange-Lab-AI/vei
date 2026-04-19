"""Canonical training-data feed.

Reads CanonicalEvent streams from one or many tenants and emits training
samples of the form:
    (graph_slice, recent_events, candidate_action, next_events,
     state_delta, business_heads)

Text bodies are NOT the primitive — only structured fields plus optional
text_handle with tenant opt-in.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from vei.events.api import CanonicalEvent


class TrainingSample(BaseModel):
    """One training sample for the dynamics model."""

    sample_id: str = ""
    tenant_id: str = ""
    graph_slice: Dict[str, Any] = Field(default_factory=dict)
    recent_events: List[CanonicalEvent] = Field(default_factory=list)
    candidate_action: Dict[str, Any] = Field(default_factory=dict)
    next_events: List[CanonicalEvent] = Field(default_factory=list)
    state_delta: Dict[str, Any] = Field(default_factory=dict)
    business_heads: Dict[str, float] = Field(default_factory=dict)
    provenance: Dict[str, Any] = Field(default_factory=dict)


def build_samples_from_events(
    events: List[CanonicalEvent],
    *,
    window_size: int = 10,
    horizon: int = 5,
    tenant_id: str = "",
) -> List[TrainingSample]:
    """Build training samples from a sequence of canonical events.

    Slides a window of ``window_size`` events, uses the next ``horizon``
    events as the target.
    """
    samples: List[TrainingSample] = []
    for i in range(len(events) - window_size - horizon + 1):
        recent = events[i : i + window_size]
        future = events[i + window_size : i + window_size + horizon]
        branch_event = recent[-1]
        sample = TrainingSample(
            sample_id=f"{tenant_id}_{i:06d}",
            tenant_id=tenant_id,
            graph_slice=_graph_slice(recent),
            recent_events=recent,
            candidate_action=_candidate_action(branch_event),
            next_events=future,
            state_delta=_state_delta(recent, future),
            business_heads=_business_heads(branch_event, future),
            provenance={
                "tenant_id": tenant_id,
                "window_start": i,
                "window_end": i + window_size - 1,
                "horizon": horizon,
                "branch_event_id": branch_event.event_id,
                "source": "canonical_events",
            },
        )
        samples.append(sample)
    return samples


def emit_feed(
    events_by_tenant: Dict[str, List[CanonicalEvent]],
    *,
    output_path: Path,
    split: str = "train",
    window_size: int = 10,
    horizon: int = 5,
) -> int:
    """Write a schema-versioned training feed to disk as JSONL."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with output_path.open("w", encoding="utf-8") as fh:
        for tenant_id, events in events_by_tenant.items():
            samples = build_samples_from_events(
                events,
                window_size=window_size,
                horizon=horizon,
                tenant_id=tenant_id,
            )
            for sample in samples:
                fh.write(sample.model_dump_json() + "\n")
                total += 1
    return total


def _graph_slice(events: List[CanonicalEvent]) -> Dict[str, Any]:
    if not events:
        return {}

    actor_counts = Counter()
    case_counts = Counter()
    domain_counts = Counter()
    surface_counts = Counter()
    object_ids: set[str] = set()
    policy_tags: set[str] = set()

    for event in events:
        for actor_id in _actor_ids(event):
            actor_counts[actor_id] += 1
        if event.case_id:
            case_counts[str(event.case_id)] += 1
        domain_counts[str(event.domain.value)] += 1
        surface_counts[_surface_name(event)] += 1
        for object_ref in event.object_refs:
            if object_ref.object_id:
                object_ids.add(object_ref.object_id)
        for tag in event.policy_tags:
            if tag:
                policy_tags.add(tag)

    return {
        "event_count": len(events),
        "case_ids": sorted(case_counts),
        "surface_counts": dict(surface_counts),
        "domain_counts": dict(domain_counts),
        "top_actor_ids": [actor_id for actor_id, _ in actor_counts.most_common(5)],
        "object_ids": sorted(object_ids),
        "policy_tags": sorted(policy_tags),
        "start_ts_ms": events[0].ts_ms,
        "end_ts_ms": events[-1].ts_ms,
    }


def _candidate_action(event: CanonicalEvent) -> Dict[str, Any]:
    return {
        "event_id": event.event_id,
        "case_id": str(event.case_id or ""),
        "kind": event.kind,
        "domain": event.domain.value,
        "surface": _surface_name(event),
        "actor_id": event.actor_ref.actor_id if event.actor_ref is not None else "",
        "participant_ids": _actor_ids(event),
        "object_ids": [ref.object_id for ref in event.object_refs if ref.object_id],
        "internal_external": event.internal_external.value,
        "policy_tags": list(event.policy_tags),
        "text_handle": (
            event.text_handle.model_dump(mode="json")
            if event.text_handle is not None
            else {}
        ),
    }


def _state_delta(
    recent_events: List[CanonicalEvent],
    next_events: List[CanonicalEvent],
) -> Dict[str, Any]:
    recent_external = sum(1 for event in recent_events if _is_external(event))
    future_external = sum(1 for event in next_events if _is_external(event))
    recent_actors = {
        actor_id for event in recent_events for actor_id in _actor_ids(event)
    }
    future_actors = {
        actor_id for event in next_events for actor_id in _actor_ids(event)
    }
    recent_cases = {str(event.case_id) for event in recent_events if event.case_id}
    future_cases = {str(event.case_id) for event in next_events if event.case_id}

    return {
        "recent_event_count": len(recent_events),
        "next_event_count": len(next_events),
        "recent_external_count": recent_external,
        "future_external_count": future_external,
        "future_new_actor_count": len(future_actors - recent_actors),
        "future_repeated_actor_count": len(future_actors & recent_actors),
        "future_new_case_count": len(future_cases - recent_cases),
        "future_same_case_count": len(future_cases & recent_cases),
        "future_kind_counts": dict(Counter(event.kind for event in next_events)),
    }


def _business_heads(
    branch_event: CanonicalEvent,
    next_events: List[CanonicalEvent],
) -> Dict[str, float]:
    future_count = float(len(next_events))
    if future_count <= 0:
        return {
            "future_event_count": 0.0,
            "external_share": 0.0,
            "same_case_share": 0.0,
            "escalation_signal": 0.0,
            "approval_signal": 0.0,
        }

    external_count = sum(1 for event in next_events if _is_external(event))
    same_case_count = sum(
        1
        for event in next_events
        if branch_event.case_id and event.case_id == branch_event.case_id
    )
    escalation_count = sum(1 for event in next_events if _flagged(event, "escalat"))
    approval_count = sum(1 for event in next_events if _flagged(event, "approv"))

    return {
        "future_event_count": future_count,
        "external_share": round(external_count / future_count, 6),
        "same_case_share": round(same_case_count / future_count, 6),
        "escalation_signal": round(escalation_count / future_count, 6),
        "approval_signal": round(approval_count / future_count, 6),
    }


def _surface_name(event: CanonicalEvent) -> str:
    delta = event.delta.data if event.delta is not None else {}
    surface = str(delta.get("surface") or "").strip().lower()
    if surface:
        return surface
    if event.object_refs:
        fallback = str(event.object_refs[0].kind or "").strip().lower()
        if fallback:
            return fallback
    return str(event.domain.value)


def _actor_ids(event: CanonicalEvent) -> List[str]:
    actor_ids: list[str] = []
    if event.actor_ref is not None and event.actor_ref.actor_id:
        actor_ids.append(event.actor_ref.actor_id)
    for participant in event.participants:
        if not participant.actor_id:
            continue
        if participant.actor_id in actor_ids:
            continue
        actor_ids.append(participant.actor_id)
    return actor_ids


def _is_external(event: CanonicalEvent) -> bool:
    return event.internal_external.value == "external"


def _flagged(event: CanonicalEvent, needle: str) -> bool:
    lowered = needle.lower()
    texts = [event.kind]
    texts.extend(event.policy_tags)
    if event.delta is not None:
        texts.extend(
            str(event.delta.data.get(key) or "")
            for key in ("subject", "snippet", "surface", "thread_ref")
        )
    haystack = " ".join(texts).lower()
    return lowered in haystack
