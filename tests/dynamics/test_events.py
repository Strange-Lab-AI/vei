"""Tests for vei.events — CanonicalEvent envelope and governance emitters."""

from __future__ import annotations

from vei.events.api import (
    CanonicalEvent,
    EventDomain,
    build_event,
    drain_spine,
    emit_event,
    spine_snapshot,
)
from vei.events.governance import (
    emit_approval_denied,
    emit_approval_granted,
    emit_approval_requested,
    emit_hold_applied,
    emit_hold_released,
    emit_receipt_recorded,
    emit_surface_denied,
    emit_connector_safety_changed,
)
from vei.events.legacy import legacy_event_to_canonical
from vei.events.models import (
    ActorRef,
    ObjectRef,
    TextHandle,
)


class TestCanonicalEvent:
    def test_schema_version_is_1(self) -> None:
        event = CanonicalEvent()
        assert event.schema_version == 1

    def test_hash_deterministic(self) -> None:
        e = build_event(
            domain=EventDomain.COMM_GRAPH,
            kind="comm_graph.mail.sent",
            ts_ms=1000,
        )
        h1 = e.compute_hash()
        h2 = e.compute_hash()
        assert h1 == h2
        assert len(h1) == 64

    def test_with_hash_populates_field(self) -> None:
        e = build_event(domain=EventDomain.GOVERNANCE, kind="test")
        assert e.hash == ""
        hashed = e.with_hash()
        assert hashed.hash != ""
        assert hashed.hash == hashed.compute_hash()

    def test_text_handle_from_text(self) -> None:
        handle = TextHandle.from_text("hello world")
        assert handle.byte_length == 11
        assert len(handle.content_hash) == 64

    def test_build_event_convenience(self) -> None:
        e = build_event(
            domain="comm_graph",
            kind="comm_graph.mail.sent",
            tenant_id="acme",
            ts_ms=5000,
            actor_ref=ActorRef(actor_id="alice"),
            delta_data={"thread_id": "t1"},
        )
        assert e.domain == EventDomain.COMM_GRAPH
        assert e.tenant_id == "acme"
        assert e.delta is not None
        assert e.delta.delta_schema_version == 0

    def test_roundtrip_json(self) -> None:
        e = build_event(
            domain=EventDomain.WORK_GRAPH,
            kind="work_graph.ticket.created",
            object_refs=[ObjectRef(object_id="TKT-1", domain="work_graph")],
        )
        dumped = e.model_dump_json()
        restored = CanonicalEvent.model_validate_json(dumped)
        assert restored.kind == e.kind
        assert len(restored.object_refs) == 1


class TestSpine:
    def test_emit_and_drain(self) -> None:
        drain_spine()
        e = build_event(domain=EventDomain.INTERNAL, kind="test.ping")
        emitted = emit_event(e)
        assert emitted.hash != ""
        assert len(spine_snapshot()) == 1
        events = drain_spine()
        assert len(events) == 1
        assert len(spine_snapshot()) == 0


class TestGovernanceEvents:
    def test_all_governance_kinds_emit(self) -> None:
        drain_spine()
        emit_approval_requested(tenant_id="t")
        emit_approval_granted(tenant_id="t")
        emit_approval_denied(tenant_id="t")
        emit_hold_applied(tenant_id="t")
        emit_hold_released(tenant_id="t")
        emit_surface_denied(tenant_id="t")
        emit_connector_safety_changed(tenant_id="t", service="slack")
        emit_receipt_recorded(tenant_id="t", service="mail", operation="send")
        events = drain_spine()
        assert len(events) == 8
        kinds = {e.kind for e in events}
        assert "governance.approval.requested" in kinds
        assert "governance.approval.granted" in kinds
        assert "governance.approval.denied" in kinds
        assert "governance.hold.applied" in kinds
        assert "governance.hold.released" in kinds
        assert "governance.surface.denied" in kinds
        assert "governance.connector.safety_state_changed" in kinds
        assert "governance.receipt.recorded" in kinds
        for e in events:
            assert e.domain == EventDomain.GOVERNANCE


class TestLegacyAdapter:
    def test_convert_legacy_event(self) -> None:
        raw = {
            "index": 0,
            "event_id": "evt-001",
            "kind": "mail_delivered",
            "payload": {"target": "mail", "subject": "hello"},
            "clock_ms": 1000,
        }
        canonical = legacy_event_to_canonical(raw, tenant_id="enron")
        assert canonical.schema_version == 1
        assert canonical.event_id == "evt-001"
        assert canonical.tenant_id == "enron"
        assert canonical.domain == EventDomain.COMM_GRAPH
        assert canonical.delta is not None
        assert canonical.delta.delta_schema_version == 0

    def test_convert_legacy_slack_event(self) -> None:
        raw = {
            "event_id": "evt-002",
            "kind": "slack_message",
            "payload": {"target": "slack", "text": "hey"},
            "clock_ms": 2000,
        }
        canonical = legacy_event_to_canonical(raw)
        assert canonical.domain == EventDomain.COMM_GRAPH

    def test_convert_legacy_ticket_event(self) -> None:
        raw = {
            "event_id": "evt-003",
            "kind": "ticket_created",
            "payload": {"target": "tickets", "title": "bug"},
            "clock_ms": 3000,
        }
        canonical = legacy_event_to_canonical(raw)
        assert canonical.domain == EventDomain.WORK_GRAPH
