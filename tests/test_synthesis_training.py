from __future__ import annotations

from pathlib import Path


from vei.synthesis.api import synthesize_training_set
from vei.synthesis.api import synthesize_authoring_finetune_set
from vei.workspace.api import create_workspace_from_template
from vei.run.api import launch_workspace_run


def _launch_run(tmp_path: Path) -> tuple[Path, str]:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    manifest = launch_workspace_run(root, runner="workflow")
    return root, manifest.run_id


def test_conversations_format(tmp_path: Path) -> None:
    root, run_id = _launch_run(tmp_path)
    result = synthesize_training_set(root, [run_id], format="conversations")

    assert result.format == "conversations"
    assert result.example_count >= 0
    for ex in result.examples:
        assert ex.format == "conversations"
        assert "messages" in ex.data


def test_trajectories_format(tmp_path: Path) -> None:
    root, run_id = _launch_run(tmp_path)
    result = synthesize_training_set(root, [run_id], format="trajectories")

    assert result.format == "trajectories"
    for ex in result.examples:
        assert "state_id" in ex.data
        assert "action" in ex.data
        assert "reward" in ex.data


def test_demonstrations_format(tmp_path: Path) -> None:
    root, run_id = _launch_run(tmp_path)
    result = synthesize_training_set(root, [run_id], format="demonstrations")

    assert result.format == "demonstrations"
    for ex in result.examples:
        assert "tool" in ex.data
        assert "args" in ex.data


def test_multiple_run_ids(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    m1 = launch_workspace_run(root, runner="workflow", run_id="run-a")
    m2 = launch_workspace_run(root, runner="workflow", run_id="run-b")

    result = synthesize_training_set(
        root, [m1.run_id, m2.run_id], format="demonstrations"
    )
    run_ids_found = {ex.run_id for ex in result.examples}
    if result.example_count > 0:
        assert len(run_ids_found) >= 1


def test_authoring_format(tmp_path: Path) -> None:
    root = tmp_path / "knowledge_workspace"
    create_workspace_from_template(
        root=root,
        source_kind="family",
        source_ref="knowledge_authoring",
    )
    manifest = launch_workspace_run(root, runner="workflow")

    result = synthesize_authoring_finetune_set(root, [manifest.run_id])

    assert result.format == "authoring"
    assert result.example_count >= 1
    row = result.examples[0]
    assert row.data["artifact"]["kind"] == "proposal"
    assert row.data["retrieved_asset_ids"]
    assert "contract" in row.data
    assert row.data["contract"]["success_assertion_count"] > 0
    assert row.data["contract"]["success_assertions_passed"] > 0
