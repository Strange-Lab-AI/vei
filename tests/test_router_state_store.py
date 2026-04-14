from __future__ import annotations

import pytest

from vei.router.core import Router

pytestmark = pytest.mark.integration


def test_router_records_tool_calls() -> None:
    router = Router(seed=123, artifacts_dir=None)
    pre_head = router.state_store.head

    router.call_and_step("browser.read", {})

    assert router.state_store.head > pre_head
    state = router.state_store.materialised_state()
    assert state.get("tool_calls")
    assert state["tool_calls"][-1]["tool"] == "browser.read"


def test_router_records_mail_delivery() -> None:
    router = Router(seed=123, artifacts_dir=None)
    router.call_and_step(
        "mail.compose",
        {
            "to": "sales@example",
            "subj": "Quote",
            "body_text": "Need pricing",
        },
    )
    router.tick(20000)
    state = router.state_store.materialised_state()
    deliveries = state.get("deliveries", {})
    assert deliveries.get("mail", 0) >= 1


def test_router_state_snapshot_includes_receipts() -> None:
    router = Router(seed=99, artifacts_dir=None)
    router.call_and_step("browser.read", {})
    router.tick(1000)
    snapshot = router.state_snapshot(tool_tail=5)
    assert snapshot["head"] >= 0
    assert snapshot["tool_tail"], "expected recent tool calls"
    assert snapshot["receipts"], "expected receipts in snapshot"
    assert isinstance(snapshot.get("monitor_findings"), list)
    # Ensure registry exposes new tool metadata
    spec = router.registry.get("vei.state")
    assert spec is not None
    assert "state" in spec.description.lower()
