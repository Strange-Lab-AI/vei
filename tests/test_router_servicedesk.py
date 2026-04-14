from __future__ import annotations

import pytest

from vei.router.core import Router
from vei.world.scenarios import scenario_multi_channel

pytestmark = pytest.mark.integration


def _router() -> Router:
    return Router(seed=1234, artifacts_dir=None, scenario=scenario_multi_channel())


def test_servicedesk_tools_registered():
    router = _router()
    results = router.search_tools("servicedesk")
    names = {row["name"] for row in results["results"]}
    assert "servicedesk.list_incidents" in names
    assert "servicedesk.update_request" in names


def test_servicedesk_request_update():
    router = _router()
    router.call_and_step(
        "servicedesk.update_request",
        {
            "request_id": "REQ-8801",
            "status": "APPROVED",
            "approval_stage": "security",
            "approval_status": "APPROVED",
            "comment": "Okta check complete",
        },
    )
    details = router.call_and_step(
        "servicedesk.get_request", {"request_id": "REQ-8801"}
    )
    assert details["status"] == "APPROVED"
    assert any(
        approval.get("status") == "APPROVED"
        for approval in details.get("approvals", [])
    )
    assert details.get("comments")


def test_servicedesk_listing_supports_pagination_and_query():
    router = _router()
    incidents = router.call_and_step(
        "servicedesk.list_incidents",
        {"query": "procurement", "limit": 1},
    )
    assert incidents["count"] == 1
    assert incidents["total"] >= incidents["count"]
    assert incidents["incidents"][0]["id"].startswith("INC-")
