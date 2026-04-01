from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from vei.run.api import (
    get_run_capability_graphs,
    get_run_surface_state,
    launch_workspace_run,
)
from vei.ui.api import create_ui_app
from vei.verticals import (
    build_vertical_blueprint_asset,
    get_vertical_pack_manifest,
    list_vertical_pack_names,
)
from vei.workspace.api import create_workspace_from_template, preview_workspace_scenario


@pytest.mark.parametrize(
    ("vertical_name", "expected_domain", "expected_intent"),
    [
        (
            "real_estate_management",
            "property_graph",
            "property_graph.assign_vendor",
        ),
        (
            "digital_marketing_agency",
            "campaign_graph",
            "campaign_graph.approve_creative",
        ),
        (
            "storage_solutions",
            "inventory_graph",
            "inventory_graph.allocate_capacity",
        ),
        (
            "b2b_saas",
            "revenue_graph",
            "crm.update_deal_stage",
        ),
        (
            "service_ops",
            "ops_graph",
            "ops_graph.assign_dispatch",
        ),
    ],
)
def test_vertical_workspace_runs_and_exposes_domain_graphs(
    tmp_path: Path,
    vertical_name: str,
    expected_domain: str,
    expected_intent: str,
) -> None:
    root = tmp_path / vertical_name
    manifest = get_vertical_pack_manifest(vertical_name)

    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref=vertical_name,
    )
    preview = preview_workspace_scenario(root)
    workflow_manifest = launch_workspace_run(root, runner="workflow")
    scripted_manifest = launch_workspace_run(root, runner="scripted")
    graphs = get_run_capability_graphs(root, workflow_manifest.run_id)

    assert preview["compiled_blueprint"]["metadata"]["scenario_materialization"] == (
        "capability_graphs"
    )
    assert (
        preview["scenario"]["metadata"]["builder_environment"]["vertical"]
        == vertical_name
    )
    assert workflow_manifest.success is True
    assert workflow_manifest.contract.ok is True
    assert (
        workflow_manifest.contract.success_assertions_passed
        == workflow_manifest.contract.success_assertion_count
    )
    assert scripted_manifest.success is False
    assert scripted_manifest.contract.issue_count > 0
    assert expected_domain in graphs["available_domains"]
    assert graphs[expected_domain]

    timeline_path = root / "runs" / workflow_manifest.run_id / "timeline.json"
    payload = timeline_path.read_text(encoding="utf-8")
    assert expected_intent in payload
    assert manifest.company_name in preview["compiled_blueprint"]["title"]


@pytest.mark.parametrize(
    ("vertical_name", "expected_domain"),
    [
        ("real_estate_management", "property_graph"),
        ("digital_marketing_agency", "campaign_graph"),
        ("storage_solutions", "inventory_graph"),
        ("b2b_saas", "revenue_graph"),
        ("service_ops", "ops_graph"),
    ],
)
def test_vertical_workspace_ui_serves_vertical_graphs(
    tmp_path: Path,
    vertical_name: str,
    expected_domain: str,
) -> None:
    root = tmp_path / f"{vertical_name}-ui"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref=vertical_name,
    )
    manifest = launch_workspace_run(root, runner="workflow")

    client = TestClient(create_ui_app(root))
    graphs_response = client.get(f"/api/runs/{manifest.run_id}/graphs")
    workspace_response = client.get("/api/workspace")

    assert graphs_response.status_code == 200
    assert expected_domain in graphs_response.json()["available_domains"]
    assert graphs_response.json()[expected_domain]
    assert workspace_response.status_code == 200
    assert workspace_response.json()["manifest"]["source_kind"] == "vertical"


@pytest.mark.parametrize(
    "vertical_name",
    [
        "real_estate_management",
        "digital_marketing_agency",
        "storage_solutions",
        "b2b_saas",
        "service_ops",
    ],
)
def test_vertical_packs_seed_dense_company_context(vertical_name: str) -> None:
    asset = build_vertical_blueprint_asset(vertical_name)
    graphs = asset.capability_graphs
    assert graphs is not None
    assert graphs.comm_graph is not None
    assert graphs.doc_graph is not None
    assert graphs.work_graph is not None

    slack_channels = graphs.comm_graph.slack_channels
    slack_messages = sum(len(channel.messages) for channel in slack_channels)
    work_item_count = len(graphs.work_graph.tickets) + len(
        graphs.work_graph.service_requests
    )

    assert len(slack_channels) >= 5
    assert slack_messages >= 12
    assert len(graphs.comm_graph.mail_threads) >= 3
    assert len(graphs.doc_graph.documents) >= 4
    assert work_item_count >= 8


@pytest.mark.parametrize(
    "vertical_name",
    [
        "real_estate_management",
        "digital_marketing_agency",
        "storage_solutions",
        "b2b_saas",
        "service_ops",
    ],
)
def test_vertical_runs_expose_living_surface_state(
    tmp_path: Path,
    vertical_name: str,
) -> None:
    root = tmp_path / f"{vertical_name}-surfaces"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref=vertical_name,
    )
    manifest = launch_workspace_run(root, runner="workflow")
    surfaces = get_run_surface_state(root, manifest.run_id)

    panel_map = {panel.surface: panel for panel in surfaces.panels}

    assert surfaces.company_name
    assert surfaces.current_tension
    assert set(panel_map) == {
        "slack",
        "mail",
        "tickets",
        "docs",
        "approvals",
        "vertical_heartbeat",
    }
    assert all(panel.items for panel in panel_map.values())


@pytest.mark.parametrize(
    ("vertical_name", "runtime_component", "runtime_family"),
    [
        ("real_estate_management", "property_ops", "property"),
        ("digital_marketing_agency", "campaign_ops", "campaign"),
        ("storage_solutions", "inventory_ops", "inventory"),
        ("b2b_saas", "crm", "revenue"),
        ("service_ops", "service_ops", "service_ops"),
    ],
)
def test_vertical_pack_registry_exposes_runtime_metadata(
    vertical_name: str,
    runtime_component: str | None,
    runtime_family: str,
) -> None:
    manifest = get_vertical_pack_manifest(vertical_name)

    assert manifest.runtime_component == runtime_component
    assert manifest.runtime_family == runtime_family
    assert vertical_name in list_vertical_pack_names()
