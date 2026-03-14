from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from vei.run.api import get_run_capability_graphs, launch_workspace_run
from vei.ui.api import create_ui_app
from vei.verticals import get_vertical_pack_manifest
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
