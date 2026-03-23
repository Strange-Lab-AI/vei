from __future__ import annotations

from pathlib import Path


from vei.synthesis.api import synthesize_runbook
from vei.workspace.api import create_workspace_from_template
from vei.run.api import launch_workspace_run


def test_runbook_from_completed_run(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    manifest = launch_workspace_run(root, runner="workflow")

    runbook = synthesize_runbook(root, manifest.run_id)

    assert runbook.scenario_name
    assert runbook.total_steps >= 0
    assert isinstance(runbook.steps, list)
    for step in runbook.steps:
        assert step.index > 0
        assert step.domain


def test_runbook_with_no_events(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    manifest = launch_workspace_run(root, runner="workflow")

    events_path = root / "runs" / manifest.run_id / "events.jsonl"
    if events_path.exists():
        events_path.write_text("", encoding="utf-8")

    runbook = synthesize_runbook(root, manifest.run_id)
    assert runbook.total_steps == 0
    assert len(runbook.steps) == 0
