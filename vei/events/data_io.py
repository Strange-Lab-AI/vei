"""Canonical event builders for data/object reads and writes."""

from __future__ import annotations

from typing import Any

from .api import build_event, emit_event
from .context import EventContext, merge_event_context
from .links import EventLink, merge_event_links
from .models import (
    ActorRef,
    CanonicalEvent,
    EventDomain,
    EventProvenance,
    ObjectRef,
    TextHandle,
)


def build_data_io_event(
    *,
    kind: str,
    event_id: str | None = None,
    tenant_id: str = "",
    case_id: str | None = None,
    ts_ms: int = 0,
    actor_ref: ActorRef | None = None,
    object_refs: list[ObjectRef] | None = None,
    source_id: str = "",
    source_granularity: str = "per_call",
    provenance_origin: EventProvenance = EventProvenance.SIMULATED,
    detail: dict[str, Any] | None = None,
    body_text: str | None = None,
    link_refs: list[str] | None = None,
    links: list[EventLink | dict[str, Any]] | None = None,
    context: EventContext | dict[str, Any] | None = None,
) -> CanonicalEvent:
    payload = dict(detail or {})
    payload.setdefault("source_granularity", source_granularity)
    payload = merge_event_links(payload, links=links, link_refs=link_refs)
    payload = merge_event_context(payload, context)
    handle = (
        TextHandle.from_text(body_text, store_uri=f"payload://{source_id}/body")
        if body_text is not None
        else None
    )
    return build_event(
        event_id=event_id,
        domain=EventDomain.DATA_GRAPH,
        kind=kind,
        tenant_id=tenant_id,
        case_id=case_id,
        ts_ms=ts_ms,
        actor_ref=actor_ref,
        object_refs=object_refs or [],
        provenance_origin=provenance_origin,
        provenance_source_id=source_id,
        text_handle=handle,
        delta_data=payload,
    ).with_hash()


def emit_data_asset_read(**kwargs: Any) -> CanonicalEvent:
    return emit_event(build_data_io_event(kind="data.asset.read", **kwargs))


def emit_data_object_read(**kwargs: Any) -> CanonicalEvent:
    return emit_event(build_data_io_event(kind="data.object.read", **kwargs))


def emit_data_object_written(**kwargs: Any) -> CanonicalEvent:
    return emit_event(build_data_io_event(kind="data.object.written", **kwargs))
