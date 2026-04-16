"""Tests for vei.ingest — pipeline, case resolver, and local stores."""

from __future__ import annotations

from pathlib import Path


from vei.events.api import build_event
from vei.events.models import ActorRef, CanonicalEvent, EventDomain, ObjectRef
from vei.ingest.api import CaseAssignment, IngestPipeline
from vei.ingest.cases.resolver import DefaultCaseResolver
from vei.ingest.normalize.pipeline import StreamingNormalizer
from vei.ingest.raw.postgres_log import _decode_cursor, _encode_cursor
from vei.ingest.raw.jsonl_log import JsonlRawLog


class TestJsonlRawLog:
    def test_append_and_iter(self, tmp_path: Path) -> None:
        log = JsonlRawLog(tmp_path, tenant_id="acme")
        rid1 = log.append({"kind": "mail", "payload": {"subject": "hi"}})
        log.append({"kind": "slack", "payload": {"text": "hey"}})
        all_records = list(log.iter_since(""))
        assert len(all_records) == 2
        after_first = list(log.iter_since(rid1))
        assert len(after_first) == 1
        assert after_first[0]["kind"] == "slack"


class TestStreamingNormalizer:
    def test_normalize_produces_canonical_event(self) -> None:
        norm = StreamingNormalizer(tenant_id="acme")
        events = norm.normalize(
            {"kind": "mail_sent", "payload": {"target": "mail"}, "clock_ms": 5000}
        )
        assert len(events) == 1
        assert events[0].domain == EventDomain.COMM_GRAPH
        assert events[0].tenant_id == "acme"
        assert events[0].ts_ms == 5000


class TestDefaultCaseResolver:
    def test_links_events_by_participant(self) -> None:
        resolver = DefaultCaseResolver(time_window_ms=100_000)
        e1 = build_event(
            domain=EventDomain.COMM_GRAPH,
            kind="mail.sent",
            ts_ms=1000,
            actor_ref=ActorRef(actor_id="alice"),
        )
        e2 = build_event(
            domain=EventDomain.COMM_GRAPH,
            kind="mail.reply",
            ts_ms=2000,
            actor_ref=ActorRef(actor_id="alice"),
        )
        assignments = resolver.resolve([e1])
        assert len(assignments) == 1
        assignments2 = resolver.resolve([e2])
        assert len(assignments2) == 0  # linked to existing case

    def test_links_events_by_object_ref(self) -> None:
        resolver = DefaultCaseResolver()
        e1 = build_event(
            domain=EventDomain.WORK_GRAPH,
            kind="ticket.created",
            ts_ms=1000,
            object_refs=[ObjectRef(object_id="TKT-1", domain="work_graph")],
        )
        e2 = build_event(
            domain=EventDomain.COMM_GRAPH,
            kind="mail.sent",
            ts_ms=50000,
            object_refs=[ObjectRef(object_id="TKT-1", domain="work_graph")],
        )
        a1 = resolver.resolve([e1])
        a2 = resolver.resolve([e2])
        assert len(a1) == 1
        assert len(a2) == 0  # linked via object ref

    def test_creates_separate_cases_for_unrelated(self) -> None:
        resolver = DefaultCaseResolver(time_window_ms=1000)
        e1 = build_event(
            domain=EventDomain.COMM_GRAPH,
            kind="mail.sent",
            ts_ms=1000,
            actor_ref=ActorRef(actor_id="alice"),
        )
        e2 = build_event(
            domain=EventDomain.COMM_GRAPH,
            kind="mail.sent",
            ts_ms=100000,
            actor_ref=ActorRef(actor_id="bob"),
        )
        a1 = resolver.resolve([e1])
        a2 = resolver.resolve([e2])
        assert len(a1) == 1
        assert len(a2) == 1
        assert a1[0].case_id != a2[0].case_id


class TestCaseAssignment:
    def test_frozen_shape(self) -> None:
        ca = CaseAssignment(
            case_id="c1",
            event_ids=["e1", "e2"],
            participants=["alice"],
            linked_object_refs=["work_graph:TKT-1"],
            surfaces=["comm_graph", "work_graph"],
            start_ts=1000,
            end_ts=5000,
        )
        dumped = ca.model_dump()
        assert dumped["case_id"] == "c1"
        assert len(dumped["surfaces"]) == 2

    def test_roundtrip_json(self) -> None:
        ca = CaseAssignment(case_id="c2", event_ids=["e1"])
        restored = CaseAssignment.model_validate_json(ca.model_dump_json())
        assert restored.case_id == "c2"


class TestIngestPipeline:
    def test_end_to_end(self, tmp_path: Path) -> None:
        raw_log = JsonlRawLog(tmp_path, tenant_id="demo")
        normalizer = StreamingNormalizer(tenant_id="demo")
        resolver = DefaultCaseResolver()

        class SimpleMaterializer:
            def __init__(self) -> None:
                self.events: list[CanonicalEvent] = []

            def apply(self, events: list[CanonicalEvent]) -> int:
                self.events.extend(events)
                return len(events)

        mat = SimpleMaterializer()
        pipeline = IngestPipeline(
            raw_log=raw_log,
            normalizer=normalizer,
            case_resolver=resolver,
            materializer=mat,
        )
        applied = pipeline.ingest(
            [
                {
                    "kind": "mail",
                    "payload": {"target": "mail", "subject": "hello"},
                    "clock_ms": 1000,
                },
                {
                    "kind": "slack",
                    "payload": {"target": "slack", "text": "hi"},
                    "clock_ms": 2000,
                },
            ]
        )
        assert applied == 2
        assert len(mat.events) == 2
        assert all(event.case_id for event in mat.events)


class TestPostgresRawLogCursor:
    def test_roundtrip_cursor(self) -> None:
        from datetime import datetime, timezone

        created_at = datetime(2026, 4, 16, 12, 30, tzinfo=timezone.utc)
        cursor = _encode_cursor(created_at, "rec-123")

        decoded = _decode_cursor(cursor)

        assert decoded == (created_at, "rec-123")

    def test_decode_cursor_rejects_legacy_ids(self) -> None:
        assert _decode_cursor("legacy-record-id") is None
