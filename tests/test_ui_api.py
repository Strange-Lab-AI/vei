from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from vei.imports.api import get_import_package_example_path
from vei.run.api import launch_workspace_run
from vei.ui import api as ui_api
from vei.workspace.api import (
    create_workspace_from_template,
    generate_workspace_scenarios_from_import,
    import_workspace,
)


class _ImmediateThread:
    def __init__(self, *, target=None, daemon=None):
        self._target = target

    def start(self) -> None:
        if self._target is not None:
            self._target()


def test_ui_api_serves_workspace_and_run_details(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    manifest = launch_workspace_run(root, runner="workflow")

    client = TestClient(ui_api.create_ui_app(root))

    workspace_response = client.get("/api/workspace")
    assert workspace_response.status_code == 200
    assert workspace_response.json()["manifest"]["name"]

    runs_response = client.get("/api/runs")
    assert runs_response.status_code == 200
    assert runs_response.json()[0]["run_id"] == manifest.run_id

    timeline_response = client.get(f"/api/runs/{manifest.run_id}/timeline")
    assert timeline_response.status_code == 200
    assert any(item["kind"] == "workflow_step" for item in timeline_response.json())

    contract_response = client.get(f"/api/runs/{manifest.run_id}/contract")
    assert contract_response.status_code == 200
    assert contract_response.json()["ok"] is True

    receipts_response = client.get(f"/api/runs/{manifest.run_id}/receipts")
    assert receipts_response.status_code == 200
    assert isinstance(receipts_response.json(), list)

    orientation_response = client.get(f"/api/runs/{manifest.run_id}/orientation")
    assert orientation_response.status_code == 200
    assert orientation_response.json()["organization_name"] == "MacroCompute"

    timeline_path = root / "runs" / manifest.run_id / "timeline.json"
    timeline_path.unlink()
    with client.stream("GET", f"/api/runs/{manifest.run_id}/stream") as response:
        body = "".join(response.iter_text())
    assert response.status_code == 200
    assert "workflow_step" in body


def test_ui_api_start_run_returns_generated_run_id(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )

    monkeypatch.setattr(ui_api, "Thread", _ImmediateThread)
    client = TestClient(ui_api.create_ui_app(root))

    response = client.post("/api/runs", json={"runner": "workflow"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["run_id"].startswith("run_")

    run_response = client.get(f"/api/runs/{payload['run_id']}")
    assert run_response.status_code == 200
    assert run_response.json()["status"] == "ok"


def test_ui_api_rejects_invalid_runner_before_worker_starts(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    client = TestClient(ui_api.create_ui_app(root))

    response = client.post("/api/runs", json={"runner": "invalid-runner"})
    assert response.status_code == 400
    assert response.json()["detail"] == "runner must be workflow, scripted, bc, or llm"
    runs_response = client.get("/api/runs")
    assert runs_response.json() == []


def test_ui_api_serves_import_diagnostics_and_provenance(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    import_workspace(
        root=root,
        package_path=get_import_package_example_path("macrocompute_identity_export"),
    )
    generate_workspace_scenarios_from_import(root)
    manifest = launch_workspace_run(
        root,
        runner="workflow",
        scenario_name="oversharing_remediation",
    )

    client = TestClient(ui_api.create_ui_app(root))

    summary_response = client.get("/api/imports/summary")
    assert summary_response.status_code == 200
    assert summary_response.json()["package_name"] == "macrocompute_identity_export"

    normalization_response = client.get("/api/imports/normalization")
    assert normalization_response.status_code == 200
    assert normalization_response.json()["normalized_counts"]["identity_users"] == 2

    scenarios_response = client.get("/api/imports/scenarios")
    assert scenarios_response.status_code == 200
    assert any(
        item["name"] == "oversharing_remediation" for item in scenarios_response.json()
    )

    provenance_response = client.get(
        "/api/imports/provenance", params={"object_ref": "drive_share:GDRIVE-2201"}
    )
    assert provenance_response.status_code == 200
    assert provenance_response.json()[0]["origin"] == "imported"

    timeline_response = client.get(f"/api/runs/{manifest.run_id}/timeline")
    assert timeline_response.status_code == 200
    assert any(
        "drive_share:GDRIVE-2201" in item.get("object_refs", [])
        for item in timeline_response.json()
    )
