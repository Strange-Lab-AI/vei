from __future__ import annotations

import json
from pathlib import Path

import pytest

from vei.run.api import launch_workspace_run, verify_run_replay
from vei.workspace.api import create_workspace_from_template


def _workspace(root: Path) -> Path:
    root.parent.mkdir(parents=True, exist_ok=True)
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
        overwrite=True,
    )
    return root


def test_scripted_runs_are_byte_identical_for_same_seed(tmp_path: Path) -> None:
    root_a = _workspace(tmp_path / "left" / "shared_workspace")
    root_b = _workspace(tmp_path / "right" / "shared_workspace")

    launch_workspace_run(
        root_a,
        runner="scripted",
        run_id="determinism_run",
        branch="determinism.branch",
        seed=42042,
        max_steps=8,
    )
    launch_workspace_run(
        root_b,
        runner="scripted",
        run_id="determinism_run",
        branch="determinism.branch",
        seed=42042,
        max_steps=8,
    )

    events_a = (root_a / "runs" / "determinism_run" / "events.jsonl").read_bytes()
    events_b = (root_b / "runs" / "determinism_run" / "events.jsonl").read_bytes()
    assert events_a == events_b

    replay = verify_run_replay(root_a, "determinism_run")
    assert replay["ok"] is True
    assert replay["state_match"] is True
    assert replay["event_snapshot_match"] is True
    assert replay["latest_snapshot_match"] is True
    assert replay["verified_reproducibility"]["seed"] == 42042
    assert replay["verified_reproducibility"]["blueprint_hash"]


def test_replay_verify_fails_when_event_log_is_invalid(tmp_path: Path) -> None:
    root = _workspace(tmp_path / "broken_events" / "workspace")
    launch_workspace_run(
        root,
        runner="scripted",
        run_id="determinism_run",
        branch="determinism.branch",
        seed=42042,
        max_steps=8,
    )

    events_path = root / "runs" / "determinism_run" / "events.jsonl"
    payload = json.loads(events_path.read_text(encoding="utf-8").splitlines()[0])
    payload["kind"] = "invalid_kind"
    events_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="run events are invalid"):
        verify_run_replay(root, "determinism_run")


def test_replay_verify_fails_when_event_log_misses_latest_snapshot(
    tmp_path: Path,
) -> None:
    root = _workspace(tmp_path / "stale_events" / "workspace")
    launch_workspace_run(
        root,
        runner="scripted",
        run_id="determinism_run",
        branch="determinism.branch",
        seed=42042,
        max_steps=8,
    )

    events_path = root / "runs" / "determinism_run" / "events.jsonl"
    lines = events_path.read_text(encoding="utf-8").splitlines()
    while lines:
        payload = json.loads(lines[-1])
        lines.pop()
        if payload.get("kind") == "snapshot":
            break
    events_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    replay = verify_run_replay(root, "determinism_run")
    assert replay["ok"] is False
    assert replay["latest_snapshot_match"] is False
