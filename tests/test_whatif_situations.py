from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from vei.whatif.cases import build_case_summaries
from vei.whatif.corpus import build_thread_summaries, choose_branch_event
from vei.whatif.models import (
    WhatIfArtifactFlags,
    WhatIfEvent,
    WhatIfWorld,
    WhatIfWorldSummary,
)
from vei.whatif.situations import (
    build_situation_context,
    build_situation_graph,
    recommend_branch_thread,
)


def test_build_situation_graph_merges_transitively_across_surfaces() -> None:
    events = [
        _event(
            event_id="mail-1",
            thread_id="mail:legal-thread",
            surface="mail",
            actor_id="maya@acme.example.com",
            target_id="legal@acme.example.com",
            timestamp_ms=1_720_000_000_000,
            subject="Acme legal review",
            snippet="Need legal review for Acme renewal.",
            case_id="case:LEGAL-7",
        ),
        _event(
            event_id="ticket-1",
            thread_id="jira:LEGAL-7",
            surface="tickets",
            actor_id="legal@acme.example.com",
            timestamp_ms=1_720_000_200_000,
            event_type="assignment",
            subject="Acme legal review",
            snippet="Track Acme renewal review work.",
            case_id="case:LEGAL-7",
        ),
        _event(
            event_id="doc-1",
            thread_id="docs:doc-1",
            surface="docs",
            actor_id="maya@acme.example.com",
            timestamp_ms=1_720_000_400_000,
            subject="Acme renewal checklist",
            snippet="Legal review checklist for Acme renewal.",
        ),
    ]

    world = _world(events)

    assert world.situation_graph is not None
    assert len(world.situation_graph.clusters) == 1
    cluster = world.situation_graph.clusters[0]
    assert set(cluster.thread_ids) == {
        "docs:doc-1",
        "jira:LEGAL-7",
        "mail:legal-thread",
    }
    assert {link.link_type for link in world.situation_graph.links} >= {
        "token",
        "actor_time",
    }


def test_build_situation_graph_links_docs_and_crm_by_text_and_time() -> None:
    events = [
        _event(
            event_id="doc-1",
            thread_id="docs:doc-1",
            surface="docs",
            actor_id="owner.docs@acme.example.com",
            timestamp_ms=1_720_000_000_000,
            subject="Acme renewal plan",
            snippet="Acme renewal approval notes.",
        ),
        _event(
            event_id="crm-1",
            thread_id="crm:deal-1",
            surface="crm",
            actor_id="owner.sales@acme.example.com",
            timestamp_ms=1_720_000_100_000,
            subject="Acme renewal",
            snippet="Renewal plan for Acme account.",
        ),
    ]

    world = _world(events)

    assert world.situation_graph is not None
    assert len(world.situation_graph.links) == 1
    assert world.situation_graph.links[0].link_type == "text_time"
    assert len(world.situation_graph.clusters) == 1


def test_build_situation_graph_skips_time_only_and_weak_text_only_pairs() -> None:
    events = [
        _event(
            event_id="mail-1",
            thread_id="mail:thread-1",
            surface="mail",
            actor_id="maya@acme.example.com",
            timestamp_ms=1_720_000_000_000,
            subject="Status update",
            snippet="General customer update.",
        ),
        _event(
            event_id="ticket-1",
            thread_id="jira:OPS-1",
            surface="tickets",
            actor_id="ops@acme.example.com",
            timestamp_ms=1_720_000_100_000,
            subject="Open issue",
            snippet="Follow up later.",
        ),
        _event(
            event_id="doc-1",
            thread_id="docs:doc-1",
            surface="docs",
            actor_id="docs@acme.example.com",
            timestamp_ms=1_720_000_200_000,
            subject="Customer notes",
            snippet="General notes only.",
        ),
        _event(
            event_id="crm-1",
            thread_id="crm:deal-1",
            surface="crm",
            actor_id="sales@acme.example.com",
            timestamp_ms=1_720_000_300_000,
            subject="Customer status",
            snippet="Only status context.",
        ),
    ]

    world = _world(events)

    assert world.situation_graph is not None
    assert world.situation_graph.links == []
    assert world.situation_graph.clusters == []


def test_recommend_branch_thread_prefers_multi_surface_cluster() -> None:
    events = [
        _event(
            event_id="isolated-1",
            thread_id="mail:isolated",
            surface="mail",
            actor_id="solo@acme.example.com",
            timestamp_ms=1_720_000_000_000,
            subject="Isolated follow-up",
            snippet="Internal status only.",
        ),
        _event(
            event_id="cluster-mail-1",
            thread_id="mail:clustered",
            surface="mail",
            actor_id="maya@acme.example.com",
            target_id="outside@buyer.example.com",
            timestamp_ms=1_720_000_100_000,
            subject="Acme renewal review",
            snippet="Need approval before external send.",
        ),
        _event(
            event_id="cluster-doc-1",
            thread_id="docs:doc-1",
            surface="docs",
            actor_id="maya@acme.example.com",
            timestamp_ms=1_720_000_200_000,
            subject="Acme renewal plan",
            snippet="Renewal draft and approval notes.",
        ),
        _event(
            event_id="cluster-crm-1",
            thread_id="crm:deal-1",
            surface="crm",
            actor_id="sales@acme.example.com",
            timestamp_ms=1_720_000_300_000,
            subject="Acme renewal",
            snippet="Renewal plan for Acme account.",
        ),
    ]

    world = _world(events)

    recommended = recommend_branch_thread(world)

    assert recommended.thread_id == "mail:clustered"


def test_build_situation_context_only_includes_pre_branch_threads() -> None:
    events = [
        _event(
            event_id="mail-1",
            thread_id="mail:thread-1",
            surface="mail",
            actor_id="maya@acme.example.com",
            timestamp_ms=1_720_000_000_000,
            subject="Acme renewal review",
            snippet="Start renewal review.",
        ),
        _event(
            event_id="doc-1",
            thread_id="docs:doc-1",
            surface="docs",
            actor_id="maya@acme.example.com",
            timestamp_ms=1_720_500_000_000,
            subject="Acme renewal review",
            snippet="Renewal review checklist.",
        ),
    ]

    world = _world(events)

    context = build_situation_context(
        world,
        branch_thread_id="mail:thread-1",
        branch_timestamp_ms=1_720_000_100_000,
    )

    assert context is None


def test_choose_branch_event_stays_thread_local() -> None:
    events = [
        _event(
            event_id="evt-1",
            thread_id="mail:thread-1",
            surface="mail",
            actor_id="maya@acme.example.com",
            timestamp_ms=1_720_000_000_000,
            subject="Approval thread",
            snippet="Opening note.",
        ),
        _event(
            event_id="evt-2",
            thread_id="mail:thread-1",
            surface="mail",
            actor_id="legal@acme.example.com",
            timestamp_ms=1_720_000_100_000,
            event_type="assignment",
            subject="Approval thread",
            snippet="Need legal assignment.",
        ),
        _event(
            event_id="evt-3",
            thread_id="mail:thread-1",
            surface="mail",
            actor_id="maya@acme.example.com",
            timestamp_ms=1_720_000_200_000,
            event_type="reply",
            subject="Approval thread",
            snippet="Following up.",
        ),
    ]

    branch_event = choose_branch_event(events, requested_event_id=None)

    assert branch_event.event_id == "evt-2"


def _world(events: list[WhatIfEvent]) -> WhatIfWorld:
    ordered_events = sorted(events, key=lambda item: (item.timestamp_ms, item.event_id))
    threads = build_thread_summaries(
        ordered_events, organization_domain="acme.example.com"
    )
    cases = build_case_summaries(ordered_events)
    situation_graph = build_situation_graph(
        threads=threads,
        cases=cases,
        events=ordered_events,
    )
    summary = WhatIfWorldSummary(
        source="company_history",
        organization_name="Acme Cloud",
        organization_domain="acme.example.com",
        event_count=len(ordered_events),
        thread_count=len(threads),
        actor_count=len(
            {
                actor_id
                for event in ordered_events
                for actor_id in {event.actor_id, event.target_id}
                if actor_id
            }
        ),
        first_timestamp=ordered_events[0].timestamp if ordered_events else "",
        last_timestamp=ordered_events[-1].timestamp if ordered_events else "",
    )
    return WhatIfWorld(
        source="company_history",
        source_dir=Path("/tmp/situations"),
        summary=summary,
        threads=threads,
        cases=cases,
        events=ordered_events,
        situation_graph=situation_graph,
    )


def _event(
    *,
    event_id: str,
    thread_id: str,
    surface: str,
    actor_id: str,
    timestamp_ms: int,
    subject: str,
    snippet: str,
    target_id: str = "",
    case_id: str = "",
    event_type: str = "message",
) -> WhatIfEvent:
    return WhatIfEvent(
        event_id=event_id,
        timestamp=_timestamp(timestamp_ms),
        timestamp_ms=timestamp_ms,
        actor_id=actor_id,
        target_id=target_id,
        event_type=event_type,
        thread_id=thread_id,
        case_id=case_id,
        surface=surface,
        subject=subject,
        snippet=snippet,
        flags=WhatIfArtifactFlags(
            subject=subject,
            norm_subject=subject.lower(),
            to_recipients=[target_id] if target_id else [],
            to_count=1 if target_id else 0,
        ),
    )


def _timestamp(timestamp_ms: int) -> str:
    return (
        datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
