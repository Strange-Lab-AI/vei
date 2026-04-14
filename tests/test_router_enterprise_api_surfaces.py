from __future__ import annotations

import pytest

from vei.router.core import MCPError, Router
from vei.world.scenarios import scenario_multi_channel

pytestmark = pytest.mark.integration


def _router() -> Router:
    router = Router(seed=2026, artifacts_dir=None, scenario=scenario_multi_channel())
    router._fault_overrides.update(  # type: ignore[attr-defined]
        {
            "tickets.create": 0.0,
            "tickets.transition": 0.0,
            "docs.create": 0.0,
            "mail.compose": 0.0,
        }
    )
    return router


def test_docs_support_metadata_and_cursor_pagination() -> None:
    router = _router()
    router.call_and_step(
        "docs.create",
        {
            "title": "Approval Handbook",
            "body": "Policy and escalation details.",
            "tags": ["policy"],
            "owner": "amy@macrocompute.example",
            "status": "DRAFT",
        },
    )
    router.call_and_step(
        "docs.create",
        {
            "title": "Q1 Plan",
            "body": "Execution plan and approvals.",
            "tags": ["plan"],
            "owner": "ops@macrocompute.example",
            "status": "ACTIVE",
        },
    )

    paged = router.call_and_step(
        "docs.list",
        {
            "limit": 1,
            "sort_by": "created_ms",
            "sort_dir": "asc",
        },
    )
    assert paged["count"] == 1
    assert paged["total"] >= 2
    assert paged["next_cursor"]
    first_doc = paged["documents"][0]
    assert "status" in first_doc
    assert "version" in first_doc

    update = router.call_and_step(
        "docs.update",
        {"doc_id": first_doc["doc_id"], "status": "ACTIVE"},
    )
    assert update["status"] == "ACTIVE"
    assert int(update["version"]) >= 2


def test_calendar_lifecycle_update_cancel_and_filtering() -> None:
    router = _router()
    created = router.call_and_step(
        "calendar.create_event",
        {
            "title": "Finance Review",
            "start_ms": 50_000,
            "end_ms": 60_000,
            "attendees": ["sam@example.com"],
            "organizer": "ops@example.com",
        },
    )
    event_id = created["event_id"]
    router.call_and_step(
        "calendar.update_event",
        {"event_id": event_id, "location": "Room 42", "status": "CONFIRMED"},
    )
    canceled = router.call_and_step(
        "calendar.cancel_event", {"event_id": event_id, "reason": "reschedule"}
    )
    assert canceled["status"] == "CANCELED"
    listing = router.call_and_step(
        "calendar.list_events",
        {"status": "CANCELED", "limit": 5},
    )
    assert any(evt["event_id"] == event_id for evt in listing["events"])
    with pytest.raises(MCPError):
        router.call_and_step(
            "calendar.accept", {"event_id": event_id, "attendee": "sam@example.com"}
        )


def test_tickets_support_priority_comments_and_transition_guards() -> None:
    router = _router()
    created = router.call_and_step(
        "tickets.create",
        {
            "title": "Escalation Follow-Up",
            "description": "Track approvals and dependencies.",
            "assignee": "ops.agent",
            "priority": "P1",
            "labels": ["approval", "finance"],
        },
    )
    ticket_id = created["ticket_id"]

    router.call_and_step(
        "tickets.transition", {"ticket_id": ticket_id, "status": "in_progress"}
    )
    router.call_and_step(
        "tickets.add_comment",
        {
            "ticket_id": ticket_id,
            "body": "Waiting on legal sign-off.",
            "author": "ops.agent",
        },
    )
    router.call_and_step(
        "tickets.transition", {"ticket_id": ticket_id, "status": "resolved"}
    )
    router.call_and_step(
        "tickets.transition", {"ticket_id": ticket_id, "status": "closed"}
    )

    paged = router.call_and_step(
        "tickets.list",
        {"priority": "P1", "limit": 2, "sort_by": "updated_ms", "sort_dir": "desc"},
    )
    assert paged["count"] >= 1
    assert any(row["ticket_id"] == ticket_id for row in paged["tickets"])

    with pytest.raises(MCPError):
        router.call_and_step(
            "tickets.transition", {"ticket_id": ticket_id, "status": "blocked"}
        )


def test_okta_and_servicedesk_support_enterprise_listing_and_state_changes() -> None:
    router = _router()
    users = router.call_and_step("okta.list_users", {"limit": 1})
    assert users["count"] == 1
    assert users["total"] >= 1

    suspended = router.call_and_step(
        "okta.suspend_user",
        {"user_id": "USR-2001", "reason": "investigation"},
    )
    assert suspended["status"] == "SUSPENDED"
    unsuspended = router.call_and_step("okta.unsuspend_user", {"user_id": "USR-2001"})
    assert unsuspended["status"] == "ACTIVE"

    assigned = router.call_and_step(
        "okta.assign_group", {"user_id": "USR-2001", "group_id": "GRP-procurement"}
    )
    assert assigned["group_id"] == "GRP-procurement"
    unassigned = router.call_and_step(
        "okta.unassign_group", {"user_id": "USR-2001", "group_id": "GRP-procurement"}
    )
    assert unassigned["group_id"] == "GRP-procurement"

    requests = router.call_and_step(
        "servicedesk.list_requests",
        {"status": "PENDING_APPROVAL", "limit": 5},
    )
    assert requests["count"] >= 1
    assert requests["total"] >= requests["count"]


def test_erp_and_db_support_procure_to_pay_and_cursor_queries() -> None:
    router = _router()
    po = router.call_and_step(
        "erp.create_po",
        {
            "vendor": "MacroCompute",
            "currency": "USD",
            "lines": [
                {
                    "item_id": "LAPTOP-15",
                    "desc": "laptops",
                    "qty": 2,
                    "unit_price": 1000,
                }
            ],
        },
    )
    po_id = po["id"]
    receipt = router.call_and_step(
        "erp.receive_goods",
        {"po_id": po_id, "lines": [{"item_id": "LAPTOP-15", "qty": 2}]},
    )
    invoice = router.call_and_step(
        "erp.submit_invoice",
        {
            "vendor": "MacroCompute",
            "po_id": po_id,
            "lines": [{"item_id": "LAPTOP-15", "qty": 2, "unit_price": 1000}],
        },
    )
    invoice_id = invoice["id"]
    match = router.call_and_step(
        "erp.match_three_way",
        {"po_id": po_id, "invoice_id": invoice_id, "receipt_id": receipt["id"]},
    )
    assert match["status"] == "MATCH"
    paid = router.call_and_step(
        "erp.post_payment", {"invoice_id": invoice_id, "amount": 2000}
    )
    assert paid["status"] in {"PAID", "PARTIALLY_PAID"}

    pos = router.call_and_step("erp.list_pos", {"status": "INVOICED", "limit": 5})
    assert pos["total"] >= 1

    router.call_and_step(
        "db.upsert",
        {
            "table": "approval_audit",
            "row": {
                "id": "APR-Z1",
                "entity_type": "po",
                "entity_id": po_id,
                "status": "PENDING",
            },
        },
    )
    router.call_and_step(
        "db.upsert",
        {
            "table": "approval_audit",
            "row": {
                "id": "APR-Z2",
                "entity_type": "po",
                "entity_id": po_id,
                "status": "APPROVED",
            },
        },
    )
    page1 = router.call_and_step(
        "db.query",
        {"table": "approval_audit", "limit": 1, "sort_by": "id"},
    )
    assert page1["count"] == 1
    assert page1["next_cursor"] is not None
    page2 = router.call_and_step(
        "db.query",
        {
            "table": "approval_audit",
            "limit": 1,
            "cursor": page1["next_cursor"],
            "sort_by": "id",
        },
    )
    assert page2["count"] == 1
