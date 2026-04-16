"""Public API for vei.events.

Other modules should import from here:
    from vei.events.api import CanonicalEvent, EventDomain, emit_event
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .legacy import _infer_domain as _legacy_infer_domain
from .models import (
    ActorRef,
    CanonicalEvent,
    CaseRef,
    EventDomain,
    EventProvenance,
    InternalExternal,
    ObjectRef,
    ProvenanceRecord,
    StateDelta,
    TextHandle,
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


def emit_event(event: CanonicalEvent) -> CanonicalEvent:
    """Append a canonical event to the in-process spine and return it hashed."""
    hashed = event.with_hash()
    _spine.append(hashed)
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


__all__ = [
    "ActorRef",
    "CanonicalEvent",
    "CaseRef",
    "EventDomain",
    "EventProvenance",
    "InternalExternal",
    "ObjectRef",
    "ProvenanceRecord",
    "StateDelta",
    "TextHandle",
    "build_event",
    "drain_spine",
    "emit_event",
    "infer_domain",
    "spine_snapshot",
]
