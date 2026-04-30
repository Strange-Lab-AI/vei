"""Canonical event builders for agent identity/session evidence."""

from __future__ import annotations

from typing import Any

from .api import build_event, emit_event
from .context import EventContext, merge_event_context
from .links import EventLink, merge_event_links
from .models import ActorRef, CanonicalEvent, EventDomain, EventProvenance


def build_identity_event(
    *,
    kind: str,
    event_id: str | None = None,
    tenant_id: str = "",
    case_id: str | None = None,
    ts_ms: int = 0,
    actor_ref: ActorRef | None = None,
    source_id: str = "",
    provenance_origin: EventProvenance = EventProvenance.SIMULATED,
    detail: dict[str, Any] | None = None,
    link_refs: list[str] | None = None,
    links: list[EventLink | dict[str, Any]] | None = None,
    context: EventContext | dict[str, Any] | None = None,
) -> CanonicalEvent:
    payload = dict(detail or {})
    payload = merge_event_links(payload, links=links, link_refs=link_refs)
    payload = merge_event_context(payload, context)
    return build_event(
        event_id=event_id,
        domain=EventDomain.IDENTITY_GRAPH,
        kind=kind,
        tenant_id=tenant_id,
        case_id=case_id,
        ts_ms=ts_ms,
        actor_ref=actor_ref,
        provenance_origin=provenance_origin,
        provenance_source_id=source_id,
        delta_data=payload,
    ).with_hash()


def emit_agent_session_opened(**kwargs: Any) -> CanonicalEvent:
    return emit_event(
        build_identity_event(kind="identity.agent_session.opened", **kwargs)
    )


def emit_agent_session_closed(**kwargs: Any) -> CanonicalEvent:
    return emit_event(
        build_identity_event(kind="identity.agent_session.closed", **kwargs)
    )


def emit_agent_identity_resolved(**kwargs: Any) -> CanonicalEvent:
    return emit_event(
        build_identity_event(kind="identity.agent_identity.resolved", **kwargs)
    )
