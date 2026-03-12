from __future__ import annotations

import pytest
from pathlib import Path

from vei.imports.api import get_import_package_example_path
from vei.run import api as run_api
from vei.run.api import (
    generate_run_id,
    get_run_capability_graphs,
    get_run_orientation,
    launch_workspace_run,
    list_run_snapshots,
    load_run_events_for_run,
    load_run_timeline,
)
from vei.workspace.api import (
    generate_workspace_scenarios_from_import,
    import_workspace,
)
from vei.workspace.api import create_workspace_from_template


def test_workspace_run_launches_and_writes_timeline(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )

    manifest = launch_workspace_run(root, runner="workflow")
    timeline = load_run_timeline(root / "runs" / manifest.run_id / "timeline.json")
    events = load_run_events_for_run(root, manifest.run_id)
    snapshots = list_run_snapshots(root, manifest.run_id)
    orientation = get_run_orientation(root, manifest.run_id)
    graphs = get_run_capability_graphs(root, manifest.run_id)

    assert manifest.status == "ok"
    assert manifest.success is True
    assert manifest.contract.ok is True
    assert manifest.artifacts.timeline_path == f"runs/{manifest.run_id}/timeline.json"
    assert manifest.artifacts.events_path == f"runs/{manifest.run_id}/events.jsonl"
    assert events[0].kind == "run_started"
    assert events[-1].kind == "run_completed"
    assert any(event.kind == "workflow_step" for event in timeline)
    assert any(event.kind == "snapshot" for event in timeline)
    assert snapshots
    assert orientation["organization_name"] == "MacroCompute"
    assert graphs["identity_graph"]["policies"][0]["policy_id"] == "POL-WAVE2"


def test_run_ids_are_unique_and_existing_run_id_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )

    assert generate_run_id() != generate_run_id()

    manifest = launch_workspace_run(root, runner="workflow", run_id="fixed-run")
    assert manifest.run_id == "fixed-run"

    try:
        launch_workspace_run(root, runner="workflow", run_id="fixed-run")
    except ValueError as exc:
        assert "run_id already exists" in str(exc)
    else:
        raise AssertionError("expected duplicate run_id to be rejected")


def test_imported_workspace_runs_generated_scenarios(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    import_workspace(
        root=root,
        package_path=get_import_package_example_path("macrocompute_identity_export"),
    )
    generate_workspace_scenarios_from_import(root)

    workflow_manifest = launch_workspace_run(
        root,
        runner="workflow",
        scenario_name="oversharing_remediation",
        run_id="oversharing-workflow",
    )
    scripted_manifest = launch_workspace_run(
        root,
        runner="scripted",
        scenario_name="oversharing_remediation",
        run_id="oversharing-scripted",
    )
    timeline = load_run_timeline(
        root / "runs" / workflow_manifest.run_id / "timeline.json"
    )

    assert workflow_manifest.status == "ok"
    assert workflow_manifest.contract.ok is True
    assert scripted_manifest.status == "ok"
    assert any(event.object_refs for event in timeline)
    assert any(
        "drive_share:GDRIVE-2201" in event.object_refs
        for event in timeline
        if event.object_refs
    )


def test_workspace_run_failure_persists_error_manifest_and_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )

    def _boom(spec):
        raise RuntimeError("simulated benchmark failure")

    monkeypatch.setattr(run_api, "run_benchmark_case", _boom)

    with pytest.raises(RuntimeError, match="simulated benchmark failure"):
        launch_workspace_run(root, runner="workflow", run_id="failing-run")

    manifest = run_api.load_run_manifest(
        root / "runs" / "failing-run" / "run_manifest.json"
    )
    events = run_api.load_run_events_for_run(root, "failing-run")
    runs = list(run_api.list_run_manifests(root))

    assert manifest.status == "error"
    assert manifest.error == "simulated benchmark failure"
    assert [item.kind for item in events] == ["run_started", "run_failed"]
    assert any(item.run_id == "failing-run" and item.status == "error" for item in runs)


def test_bc_runner_requires_model_path(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )

    with pytest.raises(ValueError, match="bc runner requires bc_model_path"):
        launch_workspace_run(root, runner="bc")
