from __future__ import annotations

import json
from pathlib import Path

from vei.dynamics.feed.canonical_feed import build_samples_from_events, emit_feed
from vei.events.api import (
    ActorRef,
    CanonicalEvent,
    EventDomain,
    EventProvenance,
    InternalExternal,
    ProvenanceRecord,
    StateDelta,
)


def test_build_samples_from_events_populates_decision_state_fields() -> None:
    events = [
        _event(
            "evt-1",
            ts_ms=1_000,
            kind="mail.message",
            surface="mail",
            actor_id="maya@acme.example.com",
            case_id="case:ACME-101",
            internal_external="internal",
        ),
        _event(
            "evt-2",
            ts_ms=2_000,
            kind="tickets.comment",
            surface="tickets",
            actor_id="legal@acme.example.com",
            case_id="case:ACME-101",
            internal_external="internal",
        ),
        _event(
            "evt-3",
            ts_ms=3_000,
            kind="mail.forward",
            surface="mail",
            actor_id="maya@acme.example.com",
            case_id="case:ACME-101",
            internal_external="external",
            snippet="Escalate to outside counsel for approval.",
        ),
        _event(
            "evt-4",
            ts_ms=4_000,
            kind="tickets.status_change",
            surface="tickets",
            actor_id="ops@acme.example.com",
            case_id="case:ACME-101",
            internal_external="internal",
        ),
    ]

    samples = build_samples_from_events(
        events,
        window_size=3,
        horizon=1,
        tenant_id="acme.example.com",
    )

    assert len(samples) == 1
    sample = samples[0]
    assert sample.graph_slice["event_count"] == 3
    assert sample.graph_slice["surface_counts"]["mail"] == 2
    assert sample.candidate_action["event_id"] == "evt-3"
    assert sample.candidate_action["surface"] == "mail"
    assert sample.state_delta["next_event_count"] == 1
    assert sample.state_delta["future_kind_counts"]["tickets.status_change"] == 1
    assert sample.business_heads["future_event_count"] == 1.0
    assert sample.business_heads["same_case_share"] == 1.0
    assert sample.provenance["branch_event_id"] == "evt-3"


def test_emit_feed_writes_enriched_training_rows(tmp_path: Path) -> None:
    output_path = tmp_path / "feed.jsonl"
    total = emit_feed(
        {
            "acme.example.com": [
                _event("evt-1", ts_ms=1_000, kind="mail.message", surface="mail"),
                _event("evt-2", ts_ms=2_000, kind="mail.reply", surface="mail"),
                _event("evt-3", ts_ms=3_000, kind="tickets.comment", surface="tickets"),
            ]
        },
        output_path=output_path,
        window_size=2,
        horizon=1,
    )

    assert total == 1
    row = json.loads(output_path.read_text(encoding="utf-8").splitlines()[0])
    assert row["graph_slice"]["event_count"] == 2
    assert row["candidate_action"]["kind"] == "mail.reply"
    assert row["state_delta"]["next_event_count"] == 1


def _event(
    event_id: str,
    *,
    ts_ms: int,
    kind: str,
    surface: str,
    actor_id: str = "maya@acme.example.com",
    case_id: str = "",
    internal_external: str = "internal",
    snippet: str = "",
) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=event_id,
        tenant_id="acme.example.com",
        case_id=case_id or None,
        ts_ms=ts_ms,
        domain=(
            EventDomain.WORK_GRAPH if surface == "tickets" else EventDomain.COMM_GRAPH
        ),
        kind=kind,
        actor_ref=ActorRef(actor_id=actor_id, tenant_id="acme.example.com"),
        internal_external=InternalExternal(internal_external),
        provenance=ProvenanceRecord(
            origin=EventProvenance.IMPORTED, source_id="fixture"
        ),
        delta=StateDelta(
            domain=(
                EventDomain.WORK_GRAPH
                if surface == "tickets"
                else EventDomain.COMM_GRAPH
            ),
            data={"surface": surface, "snippet": snippet},
        ),
    ).with_hash()
