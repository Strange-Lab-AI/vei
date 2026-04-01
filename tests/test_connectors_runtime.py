from __future__ import annotations

import pytest

from vei.connectors import adapters as connector_adapters
from vei.router.core import Router
from vei.router.errors import MCPError


def test_connector_runtime_records_receipts_in_state_snapshot() -> None:
    router = Router(seed=123, artifacts_dir=None, connector_mode="sim")
    result = router.call_and_step(
        "slack.send_message",
        {"channel": "#procurement", "text": "Request approval budget $2200"},
    )
    assert "ts" in result

    snapshot = router.state_snapshot(include_state=False, tool_tail=5)
    connectors = snapshot.get("connectors", {})
    assert connectors.get("mode") == "sim"
    assert connectors.get("last_receipt"), "expected connector receipt in snapshot"


def test_live_mode_requires_approval_for_safe_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VEI_LIVE_ALLOW_WRITE_SAFE", raising=False)
    monkeypatch.delenv("VEI_LIVE_ALLOW_WRITE_RISKY", raising=False)
    router = Router(seed=321, artifacts_dir=None, connector_mode="live")

    with pytest.raises(MCPError) as exc:
        router.call_and_step(
            "mail.compose",
            {"to": "sales@example.com", "subj": "Quote", "body_text": "Need quote"},
        )
    assert exc.value.code == "policy.approval_required"


def test_live_mode_blocks_risky_writes_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VEI_LIVE_ALLOW_WRITE_SAFE", "1")
    monkeypatch.delenv("VEI_LIVE_ALLOW_WRITE_RISKY", raising=False)
    router = Router(seed=456, artifacts_dir=None, connector_mode="live")
    # Pre-create ticket in default world to transition.
    created = router.call_and_step(
        "tickets.create",
        {"title": "Close me", "description": "generated"},
    )
    with pytest.raises(MCPError) as exc:
        router.call_and_step(
            "tickets.transition",
            {"ticket_id": created["ticket_id"], "status": "closed"},
        )
    assert exc.value.code == "policy.denied"


def test_live_mode_safe_write_allowed_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VEI_LIVE_ALLOW_WRITE_SAFE", "1")
    router = Router(seed=654, artifacts_dir=None, connector_mode="live")
    result = router.call_and_step(
        "mail.compose",
        {"to": "sales@example.com", "subj": "Quote", "body_text": "Need quote"},
    )
    assert result["id"].startswith("m")


def test_live_slack_backend_forwards_and_keeps_local_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_live_http(
        url: str, *, token: str, body: dict[str, object]
    ) -> dict[str, object]:
        calls.append((url, body))
        if url.endswith("/conversations.list"):
            return {
                "ok": True,
                "channels": [{"id": "C-PROC", "name": "procurement"}],
            }
        if url.endswith("/chat.postMessage"):
            return {"ok": True, "ts": "1710301000.000100"}
        if url.endswith("/reactions.add"):
            return {"ok": True}
        raise AssertionError(url)

    monkeypatch.setenv("VEI_LIVE_ALLOW_WRITE_SAFE", "1")
    monkeypatch.setenv("VEI_LIVE_SLACK_TOKEN", "xoxb-live-token")
    monkeypatch.setattr(
        connector_adapters,
        "_perform_live_http_json",
        fake_live_http,
    )

    router = Router(seed=654, artifacts_dir=None, connector_mode="live")
    result = router.call_and_step(
        "slack.send_message",
        {"channel": "#procurement", "text": "Live path says the PO is approved."},
    )
    react = router.call_and_step(
        "slack.react",
        {"channel": "#procurement", "ts": result["ts"], "emoji": "eyes"},
    )

    assert result["ts"] == "1710301000.000100"
    assert react["ok"] is True
    assert any(url.endswith("/chat.postMessage") for url, _ in calls)
    local_message = router.slack.channels["#procurement"]["messages"][-1]
    assert local_message["text"] == "Live path says the PO is approved."
    assert local_message["ts"] == result["ts"]
    assert local_message["reactions"] == [
        {"name": "eyes", "count": 1, "users": ["agent"]}
    ]
    receipt = router.state_snapshot(include_state=False, tool_tail=3)["connectors"][
        "last_receipt"
    ]
    assert receipt["response_payload"]["live_backend"] == "slack_http"


def test_live_slack_backend_fails_when_local_state_cannot_mirror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_live_http(
        url: str, *, token: str, body: dict[str, object]
    ) -> dict[str, object]:
        if url.endswith("/chat.postMessage"):
            return {"ok": True, "ts": "1710301000.000100"}
        raise AssertionError(url)

    monkeypatch.setenv("VEI_LIVE_ALLOW_WRITE_SAFE", "1")
    monkeypatch.setenv("VEI_LIVE_SLACK_TOKEN", "xoxb-live-token")
    monkeypatch.setattr(
        connector_adapters,
        "_perform_live_http_json",
        fake_live_http,
    )

    router = Router(seed=654, artifacts_dir=None, connector_mode="live")
    with pytest.raises(MCPError) as exc:
        router.call_and_step(
            "slack.send_message",
            {"channel": "C123LIVE", "text": "Hello from the live channel id path."},
        )

    assert exc.value.code == "slack.mirror_sync_failed"
    receipt = router.state_snapshot(include_state=False, tool_tail=3)["connectors"][
        "last_receipt"
    ]
    assert receipt["ok"] is False
    assert receipt["response_payload"]["mirror_sync_failed"] is True
    assert receipt["response_payload"]["message"]["text"] == (
        "Hello from the live channel id path."
    )


def test_live_http_json_rejects_non_http_urls() -> None:
    with pytest.raises(RuntimeError, match="http or https"):
        connector_adapters._perform_live_http_json(
            "file:///tmp/not-allowed",
            token="xoxb-live-token",
            body={"text": "hello"},
        )


def test_db_connector_tools_support_query_and_upsert() -> None:
    router = Router(seed=777, artifacts_dir=None, connector_mode="sim")

    tables = router.call_and_step("db.list_tables", {})
    assert isinstance(tables, list)
    assert any(item["table"] == "approval_audit" for item in tables)

    upsert = router.call_and_step(
        "db.upsert",
        {
            "table": "approval_audit",
            "row": {
                "id": "APR-NEW-1",
                "entity_type": "purchase_order",
                "entity_id": "PO-2001",
                "status": "REQUESTED",
            },
        },
    )
    assert upsert["ok"] is True
    assert upsert["id"] == "APR-NEW-1"

    query = router.call_and_step(
        "db.query",
        {
            "table": "approval_audit",
            "filters": {"entity_id": {"eq": "PO-2001"}},
            "limit": 5,
        },
    )
    assert query["count"] >= 1
    assert any(row["id"] == "APR-NEW-1" for row in query["rows"])


def test_salesforce_alias_executes_against_crm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VEI_CRM_ALIAS_PACKS", "salesforce")
    router = Router(seed=880, artifacts_dir=None, connector_mode="sim")

    created = router.call_and_step(
        "salesforce.opportunity.create",
        {"name": "Renewal FY27", "amount": 125000, "stage": "Qualification"},
    )
    assert created["id"].startswith("D-")

    deals = router.call_and_step("crm.list_deals", {})
    assert any(deal["id"] == created["id"] for deal in deals)

    payload = router.act_and_observe(
        "salesforce.activity.log",
        {"kind": "note", "note": "Approval context added"},
    )
    assert payload["observation"]["focus"] == "crm"


def test_enterprise_services_are_managed_by_connector_runtime() -> None:
    router = Router(seed=991, artifacts_dir=None, connector_mode="sim")

    router.call_and_step(
        "erp.create_po",
        {
            "vendor": "MacroCompute",
            "currency": "USD",
            "lines": [{"item_id": "LAPTOP-15", "qty": 1, "unit_price": 1200}],
        },
    )
    receipt = router.state_snapshot(include_state=False, tool_tail=3)["connectors"][
        "last_receipt"
    ]
    assert receipt["service"] == "erp"
    assert receipt["operation"] == "create_po"

    router.call_and_step("okta.list_users", {"limit": 1})
    receipt = router.state_snapshot(include_state=False, tool_tail=3)["connectors"][
        "last_receipt"
    ]
    assert receipt["service"] == "okta"
    assert receipt["operation"] == "list_users"

    router.call_and_step("servicedesk.list_requests", {"limit": 1})
    receipt = router.state_snapshot(include_state=False, tool_tail=3)["connectors"][
        "last_receipt"
    ]
    assert receipt["service"] == "servicedesk"
    assert receipt["operation"] == "list_requests"


def test_live_mode_requires_approval_for_okta_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VEI_LIVE_ALLOW_WRITE_SAFE", raising=False)
    monkeypatch.delenv("VEI_LIVE_ALLOW_WRITE_RISKY", raising=False)
    router = Router(seed=992, artifacts_dir=None, connector_mode="live")

    with pytest.raises(MCPError) as exc:
        router.call_and_step(
            "okta.assign_group",
            {"user_id": "USR-9001", "group_id": "GRP-it"},
        )
    assert exc.value.code == "policy.approval_required"
