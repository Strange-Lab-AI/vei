from __future__ import annotations

import pytest

from vei.router.core import Router
from vei.world.scenario import CalendarEvent, Document, Scenario, Ticket

pytestmark = pytest.mark.integration


def _make_scenario() -> Scenario:
    return Scenario(
        slack_initial_message="Welcome",
        documents={
            "DOC-1": Document(
                doc_id="DOC-1", title="Policy", body="Budget rules", tags=["policy"]
            ),
        },
        calendar_events=[
            CalendarEvent(
                event_id="EVT-1",
                title="Approval Sync",
                start_ms=20_000,
                end_ms=21_000,
                attendees=["sam@example.com"],
            )
        ],
        tickets={
            "TCK-1": Ticket(
                ticket_id="TCK-1",
                title="Laptop approval",
                status="open",
                assignee="sam",
                description="Review purchase request",
                history=[{"status": "open"}],
            )
        },
    )


def test_docs_calendar_tickets_tools_available() -> None:
    router = Router(seed=123, artifacts_dir=None, scenario=_make_scenario())

    docs = router.call_and_step("docs.list", {})
    assert docs and docs[0]["doc_id"] == "DOC-1"

    search = router.call_and_step("docs.search", {"query": "budget"})
    assert search and search[0]["doc_id"] == "DOC-1"

    created = router.call_and_step(
        "docs.create", {"title": "Minutes", "body": "Meeting notes"}
    )
    doc_id = created["doc_id"]
    router.call_and_step("docs.update", {"doc_id": doc_id, "title": "Minutes v2"})
    updated = router.docs.read(doc_id)
    assert updated["title"] == "Minutes v2"

    events = router.call_and_step("calendar.list_events", {})
    assert events and events[0]["event_id"] == "EVT-1"
    ack = router.call_and_step(
        "calendar.accept", {"event_id": "EVT-1", "attendee": "sam@example.com"}
    )
    assert ack["status"] == "accepted"

    new_event = router.call_and_step(
        "calendar.create_event",
        {
            "title": "Follow-up",
            "start_ms": 30_000,
            "end_ms": 31_000,
            "attendees": ["sam@example.com"],
        },
    )
    assert new_event["event_id"].startswith("EVT-")

    tickets = router.call_and_step("tickets.list", {})
    assert tickets and tickets[0]["ticket_id"] == "TCK-1"
    router.call_and_step(
        "tickets.transition", {"ticket_id": "TCK-1", "status": "closed"}
    )
    closed = router.call_and_step("tickets.get", {"ticket_id": "TCK-1"})
    assert closed["status"] == "closed"

    obs = router.observe(focus_hint="docs")
    action_names = [item["tool"] for item in obs.action_menu]
    assert "docs.list" in action_names

    pending = router.pending()
    assert set(["mail", "slack", "docs", "calendar", "tickets", "total"]) <= set(
        pending.keys()
    )
