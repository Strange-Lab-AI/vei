"""Explicit projections between the canonical spine and derived event views.

CanonicalEvent is the authoritative event envelope.
World state events and run timeline events are derived views that keep only the
fields those layers can display or replay.
"""

from __future__ import annotations

from typing import Any

from vei.run.api import RunTimelineEvent
from vei.world.api import Event as WorldStateEvent

from .legacy import _infer_domain
from .models import (
    ActorRef,
    CanonicalEvent,
    EventDomain,
    EventProvenance,
    ObjectRef,
    ProvenanceRecord,
    StateDelta,
)


def world_state_event_to_canonical_event(
    event: WorldStateEvent,
    *,
    tenant_id: str = "",
    case_id: str | None = None,
) -> CanonicalEvent:
    payload = dict(event.payload)
    domain = _infer_domain(event.kind, payload)
    actor_id = _first_non_empty(
        payload.get("actor_id"),
        payload.get("actor"),
        payload.get("user_id"),
        payload.get("sender"),
    )
    actor_ref = ActorRef(actor_id=actor_id, tenant_id=tenant_id) if actor_id else None
    return CanonicalEvent(
        event_id=event.event_id,
        tenant_id=tenant_id,
        case_id=case_id or _case_id_from_payload(payload),
        ts_ms=int(event.clock_ms),
        domain=domain,
        kind=_canonical_kind(domain, event.kind),
        actor_ref=actor_ref,
        object_refs=_object_refs_from_payload(payload),
        provenance=ProvenanceRecord(
            origin=EventProvenance.DERIVED,
            source_id="world.state.Event",
        ),
        delta=StateDelta(
            domain=domain,
            delta_schema_version=0,
            data=payload,
        ),
    )


def canonical_event_to_world_state_event(
    event: CanonicalEvent,
    *,
    index: int = 0,
) -> WorldStateEvent:
    payload = dict(event.delta.data) if event.delta is not None else {}
    if event.case_id and "case_id" not in payload:
        payload["case_id"] = event.case_id
    if event.actor_ref and event.actor_ref.actor_id and "actor_id" not in payload:
        payload["actor_id"] = event.actor_ref.actor_id
    if event.object_refs and "object_refs" not in payload:
        payload["object_refs"] = [
            item.model_dump(mode="json") for item in event.object_refs
        ]
    payload.setdefault("_canonical_event_id", event.event_id)
    payload.setdefault("_canonical_domain", event.domain.value)
    return WorldStateEvent.create(
        index=index,
        kind=event.kind,
        payload=payload,
        clock_ms=int(event.ts_ms),
        event_id=event.event_id,
    )


def run_timeline_event_to_canonical_event(
    event: RunTimelineEvent,
    *,
    tenant_id: str = "",
    case_id: str | None = None,
) -> CanonicalEvent:
    payload: dict[str, Any] = dict(event.payload)
    payload.setdefault("timeline_kind", event.kind)
    payload.setdefault("label", event.label)
    if event.status:
        payload.setdefault("status", event.status)
    if event.tool:
        payload.setdefault("tool", event.tool)
    if event.resolved_tool:
        payload.setdefault("resolved_tool", event.resolved_tool)
    if event.branch:
        payload.setdefault("branch", event.branch)
    if event.snapshot_id is not None:
        payload.setdefault("snapshot_id", event.snapshot_id)

    domain = _domain_for_run_timeline_event(event, payload)
    actor_ref = (
        ActorRef(actor_id=event.runner, tenant_id=tenant_id) if event.runner else None
    )
    object_refs = [
        ObjectRef(object_id=object_ref, domain=domain.value, kind="timeline_ref")
        for object_ref in event.object_refs
    ]
    return CanonicalEvent(
        event_id=f"run-{event.index}-{event.kind}-{event.time_ms}",
        tenant_id=tenant_id,
        case_id=case_id,
        ts_ms=int(event.time_ms),
        domain=domain,
        kind=_canonical_kind(domain, event.kind),
        actor_ref=actor_ref,
        object_refs=object_refs,
        provenance=ProvenanceRecord(
            origin=EventProvenance.DERIVED,
            source_id="run.timeline",
        ),
        delta=StateDelta(
            domain=domain,
            delta_schema_version=0,
            data=payload,
        ),
    )


def canonical_event_to_run_timeline_event(event: CanonicalEvent) -> RunTimelineEvent:
    payload = dict(event.delta.data) if event.delta is not None else {}
    if event.case_id:
        payload.setdefault("case_id", event.case_id)
    return RunTimelineEvent(
        index=0,
        kind="trace_event",
        label=event.kind,
        channel=_timeline_channel_for_domain(event.domain),
        time_ms=int(event.ts_ms),
        runner=event.actor_ref.actor_id if event.actor_ref else None,
        object_refs=[item.object_id for item in event.object_refs],
        payload=payload,
    )


def _canonical_kind(domain: EventDomain, raw_kind: str) -> str:
    if "." in raw_kind:
        return raw_kind
    return f"{domain.value}.{raw_kind}"


def _case_id_from_payload(payload: dict[str, Any]) -> str | None:
    return _first_non_empty(
        payload.get("case_id"),
        payload.get("thread_id"),
        payload.get("ticket_id"),
        payload.get("deal_id"),
    )


def _object_refs_from_payload(payload: dict[str, Any]) -> list[ObjectRef]:
    object_refs: list[ObjectRef] = []
    for key, kind in (
        ("thread_id", "thread"),
        ("ticket_id", "ticket"),
        ("deal_id", "deal"),
        ("document_id", "document"),
        ("record_id", "record"),
    ):
        value = _first_non_empty(payload.get(key))
        if not value:
            continue
        object_refs.append(ObjectRef(object_id=value, kind=kind))
    return object_refs


def _domain_for_run_timeline_event(
    event: RunTimelineEvent,
    payload: dict[str, Any],
) -> EventDomain:
    if event.graph_domain:
        try:
            return EventDomain(event.graph_domain)
        except ValueError:
            pass
    target = ""
    if event.tool:
        target = event.tool.split(".", 1)[0]
    elif event.channel:
        target = event.channel.lower()
    return _infer_domain(event.kind, {**payload, "target": target})


def _timeline_channel_for_domain(domain: EventDomain) -> str:
    if domain == EventDomain.COMM_GRAPH:
        return "Communication"
    if domain == EventDomain.WORK_GRAPH:
        return "Work"
    if domain == EventDomain.DOC_GRAPH:
        return "Documents"
    if domain == EventDomain.REVENUE_GRAPH:
        return "Revenue"
    if domain == EventDomain.GOVERNANCE:
        return "Governance"
    return "World"


def _first_non_empty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""
