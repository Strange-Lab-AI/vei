from __future__ import annotations

from pathlib import Path

from vei.router.core import Router
from vei.world.api import (
    create_world_session,
    get_catalog_scenario,
    list_catalog_scenario_manifest,
)


def test_oauth_containment_tools_are_registered_and_mutable() -> None:
    router = Router(
        seed=42042,
        artifacts_dir=None,
        scenario=get_catalog_scenario("oauth_app_containment"),
    )

    google_tools = {
        item["name"] for item in router.search_tools("google_admin")["results"]
    }
    siem_tools = {item["name"] for item in router.search_tools("siem")["results"]}

    assert "google_admin.list_oauth_apps" in google_tools
    assert "google_admin.suspend_oauth_app" in google_tools
    assert "siem.preserve_evidence" in siem_tools
    assert "siem.update_case" in siem_tools

    app = router.call_and_step("google_admin.get_oauth_app", {"app_id": "OAUTH-9001"})
    security_channel = router.call_and_step(
        "slack.open_channel", {"channel": "#security-incident"}
    )
    assert app["risk_level"] == "critical"
    assert security_channel["messages"][0]["text"].startswith("Contain the suspicious")

    preserved = router.call_and_step(
        "google_admin.preserve_oauth_evidence",
        {"app_id": "OAUTH-9001", "note": "snapshot before containment"},
    )
    suspended = router.call_and_step(
        "google_admin.suspend_oauth_app",
        {"app_id": "OAUTH-9001", "reason": "containment"},
    )
    case_update = router.call_and_step(
        "siem.update_case",
        {
            "case_id": "CASE-0001",
            "customer_notification_required": True,
            "note": "Customer notification required due to restricted scope access.",
        },
    )

    assert preserved["evidence_hold"] is True
    assert suspended["status"] == "SUSPENDED"
    assert case_update["customer_notification_required"] is True


def test_checkout_control_planes_cover_monitoring_incident_and_flags() -> None:
    router = Router(
        seed=42042,
        artifacts_dir=None,
        scenario=get_catalog_scenario("checkout_spike_mitigation"),
    )

    services = router.call_and_step("datadog.list_services", {"status": "degraded"})
    incidents = router.call_and_step(
        "pagerduty.list_incidents", {"status": "triggered"}
    )
    flags = router.call_and_step(
        "feature_flags.list_flags", {"service": "checkout-api"}
    )

    ack = router.call_and_step(
        "pagerduty.ack_incident",
        {"incident_id": "PD-9001", "assignee": "commerce-ic"},
    )
    rollout = router.call_and_step(
        "feature_flags.update_rollout",
        {
            "flag_key": "checkout_v2",
            "rollout_pct": 15,
            "reason": "contain blast radius",
        },
    )
    kill_switch = router.call_and_step(
        "feature_flags.set_flag",
        {
            "flag_key": "checkout_kill_switch",
            "enabled": True,
            "reason": "temporary mitigation during incident",
        },
    )

    assert services["total"] == 1
    assert incidents["total"] == 1
    assert flags["total"] >= 2
    assert ack["status"] == "acknowledged"
    assert rollout["rollout_pct"] == 15
    assert kill_switch["enabled"] is True


def test_checkout_mixed_stack_includes_spreadsheet_docs_and_crm() -> None:
    router = Router(
        seed=42042,
        artifacts_dir=None,
        scenario=get_catalog_scenario("checkout_spike_mitigation"),
    )

    workbooks = router.call_and_step("spreadsheet.list_workbooks", {})
    updated_row = router.call_and_step(
        "spreadsheet.upsert_row",
        {
            "workbook_id": "WB-CHK-1",
            "sheet_id": "sheet-impact",
            "match_field": "metric",
            "match_value": "estimated_revenue_loss_usd",
            "table_id": "tbl-impact",
            "row": {
                "metric": "estimated_revenue_loss_usd",
                "value": 128000,
                "notes": "Impact updated while canary rollback is active.",
            },
        },
    )
    doc = router.call_and_step(
        "docs.update",
        {
            "doc_id": "RUN-CHK-1",
            "body": "Revenue impact has been quantified and customer guidance updated.",
        },
    )
    activity = router.call_and_step(
        "crm.log_activity",
        {
            "kind": "note",
            "deal_id": "D-812",
            "note": "Estimated revenue loss quantified for incident follow-through.",
        },
    )
    war_room = router.call_and_step(
        "slack.open_channel", {"channel": "#commerce-war-room"}
    )

    assert workbooks["total"] == 1
    assert updated_row["row_count"] >= 2
    assert doc["doc_id"] == "RUN-CHK-1"
    assert activity["ok"] is True
    assert war_room["messages"][0]["text"].startswith("Checkout conversion is dropping")


def test_onboarding_surfaces_cover_hris_jira_and_salesforce_aliases() -> None:
    router = Router(
        seed=42042,
        artifacts_dir=None,
        scenario=get_catalog_scenario("acquired_sales_onboarding"),
    )

    employees = router.call_and_step(
        "hris.list_employees", {"cohort": "acquired-sales-wave-1"}
    )
    resolved = router.call_and_step(
        "hris.resolve_identity",
        {
            "employee_id": "EMP-2201",
            "corporate_email": "jordan.sellers@example.com",
            "note": "Merged acquired identity into corporate domain.",
        },
    )
    issues = router.call_and_step("jira.list_issues", {"limit": 5})
    docs = router.call_and_step("docs.list", {"query": "Cutover", "include_body": True})
    transfer = router.call_and_step(
        "salesforce.opportunity.transfer_owner",
        {"id": "D-100", "owner": "maya.rex@example.com"},
    )
    deals = router.call_and_step("salesforce.opportunity.list", {})
    cutover_channel = router.call_and_step(
        "slack.open_channel", {"channel": "#sales-cutover"}
    )

    assert employees["total"] == 2
    assert resolved["identity_conflict"] is False
    assert issues["issues"][0]["issue_id"] == "JRA-204"
    assert docs["documents"][0]["doc_id"] == "CUTOVER-2201"
    assert transfer["owner"] == "maya.rex@example.com"
    assert deals and deals[0]["owner"] == "maya.rex@example.com"
    assert cutover_channel["messages"][0]["text"].startswith("Wave 1 acquired-sales")


def test_world_session_snapshot_restores_control_plane_state(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("VEI_STATE_DIR", str(tmp_path / "state"))
    session = create_world_session(
        seed=42042,
        scenario=get_catalog_scenario("oauth_app_containment"),
    )

    baseline = session.snapshot("baseline")
    session.call_tool(
        "google_admin.suspend_oauth_app",
        {"app_id": "OAUTH-9001", "reason": "temporary containment"},
    )
    session.call_tool(
        "siem.update_case",
        {"case_id": "CASE-0001", "status": "CONTAINED", "owner": "sec-manager"},
    )

    session.restore(baseline.snapshot_id)

    app = session.call_tool("google_admin.get_oauth_app", {"app_id": "OAUTH-9001"})
    case = session.call_tool("siem.get_case", {"case_id": "CASE-0001"})

    assert app["status"] == "ACTIVE"
    assert case["status"] == "OPEN"
    assert case["owner"] == "ir.oncall@example.com"


def test_acceptance_scenarios_appear_in_manifest_catalog() -> None:
    manifests = {item.name: item for item in list_catalog_scenario_manifest()}

    assert "oauth_app_containment" in manifests
    assert "acquired_sales_onboarding" in manifests
    assert "checkout_spike_mitigation" in manifests
    assert "google_admin" in manifests["oauth_app_containment"].tool_families
    assert "hris" in manifests["acquired_sales_onboarding"].tool_families
    assert "datadog" in manifests["checkout_spike_mitigation"].tool_families
    assert "spreadsheet" in manifests["checkout_spike_mitigation"].tool_families
