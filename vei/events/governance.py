"""Governance event helpers.

Provides typed builders for control-plane events that share the canonical
spine: approvals, holds, denials, connector safety state changes, and
receipts.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .api import build_event, emit_event
from .context import EventContext, merge_event_context
from .links import EventLink, merge_event_links
from .models import (
    ActorRef,
    CanonicalEvent,
    EventDomain,
    EventProvenance,
    InternalExternal,
    ObjectRef,
)


def emit_approval_requested(
    *,
    tenant_id: str = "",
    ts_ms: int = 0,
    actor_ref: Optional[ActorRef] = None,
    object_refs: Optional[List[ObjectRef]] = None,
    case_id: Optional[str] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> CanonicalEvent:
    return emit_event(
        build_event(
            domain=EventDomain.GOVERNANCE,
            kind="governance.approval.requested",
            tenant_id=tenant_id,
            ts_ms=ts_ms,
            case_id=case_id,
            actor_ref=actor_ref,
            object_refs=object_refs,
            internal_external=InternalExternal.INTERNAL,
            provenance_origin=EventProvenance.SIMULATED,
            delta_data=detail or {},
        )
    )


def emit_approval_granted(
    *,
    tenant_id: str = "",
    ts_ms: int = 0,
    actor_ref: Optional[ActorRef] = None,
    object_refs: Optional[List[ObjectRef]] = None,
    case_id: Optional[str] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> CanonicalEvent:
    return emit_event(
        build_event(
            domain=EventDomain.GOVERNANCE,
            kind="governance.approval.granted",
            tenant_id=tenant_id,
            ts_ms=ts_ms,
            case_id=case_id,
            actor_ref=actor_ref,
            object_refs=object_refs,
            internal_external=InternalExternal.INTERNAL,
            provenance_origin=EventProvenance.SIMULATED,
            delta_data=detail or {},
        )
    )


def emit_approval_denied(
    *,
    tenant_id: str = "",
    ts_ms: int = 0,
    actor_ref: Optional[ActorRef] = None,
    object_refs: Optional[List[ObjectRef]] = None,
    case_id: Optional[str] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> CanonicalEvent:
    return emit_event(
        build_event(
            domain=EventDomain.GOVERNANCE,
            kind="governance.approval.denied",
            tenant_id=tenant_id,
            ts_ms=ts_ms,
            case_id=case_id,
            actor_ref=actor_ref,
            object_refs=object_refs,
            internal_external=InternalExternal.INTERNAL,
            provenance_origin=EventProvenance.SIMULATED,
            delta_data=detail or {},
        )
    )


def emit_hold_applied(
    *,
    tenant_id: str = "",
    ts_ms: int = 0,
    actor_ref: Optional[ActorRef] = None,
    object_refs: Optional[List[ObjectRef]] = None,
    case_id: Optional[str] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> CanonicalEvent:
    return emit_event(
        build_event(
            domain=EventDomain.GOVERNANCE,
            kind="governance.hold.applied",
            tenant_id=tenant_id,
            ts_ms=ts_ms,
            case_id=case_id,
            actor_ref=actor_ref,
            object_refs=object_refs,
            internal_external=InternalExternal.INTERNAL,
            provenance_origin=EventProvenance.SIMULATED,
            delta_data=detail or {},
        )
    )


def emit_hold_released(
    *,
    tenant_id: str = "",
    ts_ms: int = 0,
    actor_ref: Optional[ActorRef] = None,
    object_refs: Optional[List[ObjectRef]] = None,
    case_id: Optional[str] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> CanonicalEvent:
    return emit_event(
        build_event(
            domain=EventDomain.GOVERNANCE,
            kind="governance.hold.released",
            tenant_id=tenant_id,
            ts_ms=ts_ms,
            case_id=case_id,
            actor_ref=actor_ref,
            object_refs=object_refs,
            internal_external=InternalExternal.INTERNAL,
            provenance_origin=EventProvenance.SIMULATED,
            delta_data=detail or {},
        )
    )


def emit_surface_denied(
    *,
    tenant_id: str = "",
    ts_ms: int = 0,
    actor_ref: Optional[ActorRef] = None,
    object_refs: Optional[List[ObjectRef]] = None,
    case_id: Optional[str] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> CanonicalEvent:
    return emit_event(
        build_event(
            domain=EventDomain.GOVERNANCE,
            kind="governance.surface.denied",
            tenant_id=tenant_id,
            ts_ms=ts_ms,
            case_id=case_id,
            actor_ref=actor_ref,
            object_refs=object_refs,
            internal_external=InternalExternal.INTERNAL,
            provenance_origin=EventProvenance.SIMULATED,
            delta_data=detail or {},
        )
    )


def emit_connector_safety_changed(
    *,
    tenant_id: str = "",
    ts_ms: int = 0,
    service: str = "",
    old_state: str = "",
    new_state: str = "",
    detail: Optional[Dict[str, Any]] = None,
) -> CanonicalEvent:
    return emit_event(
        build_event(
            domain=EventDomain.GOVERNANCE,
            kind="governance.connector.safety_state_changed",
            tenant_id=tenant_id,
            ts_ms=ts_ms,
            internal_external=InternalExternal.INTERNAL,
            provenance_origin=EventProvenance.SIMULATED,
            delta_data={
                "service": service,
                "old_state": old_state,
                "new_state": new_state,
                **(detail or {}),
            },
        )
    )


def emit_receipt_recorded(
    *,
    tenant_id: str = "",
    ts_ms: int = 0,
    service: str = "",
    operation: str = "",
    policy_action: str = "",
    ok: bool = True,
    detail: Optional[Dict[str, Any]] = None,
) -> CanonicalEvent:
    return emit_event(
        build_event(
            domain=EventDomain.GOVERNANCE,
            kind="governance.receipt.recorded",
            tenant_id=tenant_id,
            ts_ms=ts_ms,
            internal_external=InternalExternal.INTERNAL,
            provenance_origin=EventProvenance.SIMULATED,
            delta_data={
                "service": service,
                "operation": operation,
                "policy_action": policy_action,
                "ok": ok,
                **(detail or {}),
            },
        )
    )


def emit_policy_decision(
    *,
    tenant_id: str = "",
    ts_ms: int = 0,
    actor_ref: Optional[ActorRef] = None,
    object_refs: Optional[List[ObjectRef]] = None,
    case_id: Optional[str] = None,
    decision: str = "",
    policy_code: str = "",
    reason: str = "",
    detail: Optional[Dict[str, Any]] = None,
    link_refs: Optional[List[str]] = None,
    links: Optional[List[EventLink | Dict[str, Any]]] = None,
    context: EventContext | Dict[str, Any] | None = None,
) -> CanonicalEvent:
    payload = {
        "decision": decision,
        "policy_code": policy_code,
        "reason": reason,
        **(detail or {}),
    }
    payload = merge_event_links(payload, links=links, link_refs=link_refs)
    payload = merge_event_context(payload, context)
    return emit_event(
        build_event(
            domain=EventDomain.GOVERNANCE,
            kind="governance.policy.decision",
            tenant_id=tenant_id,
            ts_ms=ts_ms,
            case_id=case_id,
            actor_ref=actor_ref,
            object_refs=object_refs,
            internal_external=InternalExternal.INTERNAL,
            provenance_origin=EventProvenance.SIMULATED,
            delta_data=payload,
        )
    )
