from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from vei.run.api import launch_workspace_run
from vei.ui import api as ui_api
from vei.workspace.api import create_workspace_from_template


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
