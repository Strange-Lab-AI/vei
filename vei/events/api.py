"""Public API for vei.events.

Other modules should import from here:
    from vei.events.api import CanonicalEvent, EventDomain, emit_event
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .legacy import _infer_domain as _legacy_infer_domain
from .context import EventContext, ExecutionPrincipal
from .links import EventLink, link_event_ids, typed_event_links
from .models import (
    CaseRef,
    ObjectRef,
    TextHandle,
    ActorRef,
    CanonicalEvent,
    EventDomain,
    EventProvenance,
    InternalExternal,
    ProvenanceRecord,
    StateDelta,
)
from .object_refs import extract_object_refs, parse_object_refs
from .store import CanonicalEventSink, CanonicalEventStore, WorkspaceEventStore

_BOUNDARY_EXPORTS = (
    ActorRef,
    CanonicalEvent,
    EventDomain,
    EventProvenance,
    InternalExternal,
    ProvenanceRecord,
    StateDelta,
)


def infer_domain(kind: str, payload: dict) -> EventDomain:
    """Public domain inference used by ingest.

    Delegates to the same lookup the legacy adapter uses, so the mapping
    stays in one place.
    """
    return _legacy_infer_domain(kind, payload)


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level spine (in-process collector; replaced by ingest layer later)
# ---------------------------------------------------------------------------

_spine: List[CanonicalEvent] = []


def emit_event(
    event: CanonicalEvent,
    *,
    sink: Optional[CanonicalEventSink] = None,
) -> CanonicalEvent:
    """Append a canonical event to the compatibility spine and optional sink."""
    hashed = event.with_hash()
    _spine.append(hashed)
    if sink is not None:
        sink.append(hashed)
    return hashed


def drain_spine() -> List[CanonicalEvent]:
    """Return and clear all collected events (test/debug helper)."""
    events = list(_spine)
    _spine.clear()
    return events


def spine_snapshot() -> List[CanonicalEvent]:
    """Return a snapshot without clearing."""
    return list(_spine)


def build_event(
    *,
    event_id: Optional[str] = None,
    domain: EventDomain | str,
    kind: str,
    tenant_id: str = "",
    case_id: Optional[str] = None,
    ts_ms: int = 0,
    actor_ref: Optional[ActorRef] = None,
    participants: Optional[List[ActorRef]] = None,
    object_refs: Optional[List[ObjectRef]] = None,
    internal_external: InternalExternal | str = InternalExternal.UNKNOWN,
    provenance_origin: EventProvenance | str = EventProvenance.SIMULATED,
    provenance_source_id: str = "",
    text_handle: Optional[TextHandle] = None,
    policy_tags: Optional[List[str]] = None,
    delta: Optional[StateDelta] = None,
    delta_data: Optional[Dict[str, Any]] = None,
) -> CanonicalEvent:
    """Convenience builder for ``CanonicalEvent`` with sensible defaults."""
    if isinstance(domain, str):
        domain = EventDomain(domain)
    if isinstance(internal_external, str):
        internal_external = InternalExternal(internal_external)
    if isinstance(provenance_origin, str):
        provenance_origin = EventProvenance(provenance_origin)
    if delta is None and delta_data is not None:
        delta = StateDelta(domain=domain, delta_schema_version=0, data=delta_data)
    return CanonicalEvent(
        event_id=event_id or CanonicalEvent().event_id,
        domain=domain,
        kind=kind,
        tenant_id=tenant_id,
        case_id=case_id,
        ts_ms=ts_ms,
        actor_ref=actor_ref,
        participants=participants or [],
        object_refs=object_refs or [],
        internal_external=internal_external,
        provenance=ProvenanceRecord(
            origin=provenance_origin,
            source_id=provenance_source_id,
        ),
        text_handle=text_handle,
        policy_tags=policy_tags or [],
        delta=delta,
    )


# Public builders are imported lazily after build_event/emit_event are defined so
# implementation modules can use this API without circular initialization issues.
from .artifacts import emit_artifact_created, emit_incident_flagged  # noqa: E402
from .data_io import (  # noqa: E402
    emit_data_asset_read,
    emit_data_object_read,
    emit_data_object_written,
)
from .governance import (  # noqa: E402
    emit_approval_denied,
    emit_approval_granted,
    emit_approval_requested,
    emit_policy_decision,
)
from .identity import (  # noqa: E402
    emit_agent_identity_resolved,
    emit_agent_session_closed,
    emit_agent_session_opened,
)
from .llm_calls import (  # noqa: E402
    build_llm_call_event,
    build_llm_usage_observed,
    emit_llm_call_completed,
    emit_llm_call_failed,
    emit_llm_call_started,
    emit_llm_usage_observed,
)
from .tool_calls import (  # noqa: E402
    build_tool_call_event,
    emit_tool_completed,
    emit_tool_failed,
    emit_tool_requested,
    stable_event_id,
)

__all__ = [
    "ActorRef",
    "CanonicalEvent",
    "CanonicalEventSink",
    "CanonicalEventStore",
    "CaseRef",
    "EventDomain",
    "EventContext",
    "EventLink",
    "EventProvenance",
    "ExecutionPrincipal",
    "InternalExternal",
    "ObjectRef",
    "parse_object_refs",
    "ProvenanceRecord",
    "StateDelta",
    "TextHandle",
    "WorkspaceEventStore",
    "build_event",
    "build_llm_call_event",
    "build_llm_usage_observed",
    "build_tool_call_event",
    "drain_spine",
    "emit_agent_identity_resolved",
    "emit_agent_session_closed",
    "emit_agent_session_opened",
    "emit_artifact_created",
    "emit_approval_denied",
    "emit_approval_granted",
    "emit_approval_requested",
    "emit_data_asset_read",
    "emit_data_object_read",
    "emit_data_object_written",
    "emit_event",
    "emit_incident_flagged",
    "emit_llm_call_completed",
    "emit_llm_call_failed",
    "emit_llm_call_started",
    "emit_llm_usage_observed",
    "emit_policy_decision",
    "emit_tool_completed",
    "emit_tool_failed",
    "emit_tool_requested",
    "extract_object_refs",
    "infer_domain",
    "link_event_ids",
    "spine_snapshot",
    "stable_event_id",
    "typed_event_links",
]
