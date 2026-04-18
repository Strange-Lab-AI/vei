from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from vei.whatif._source_locator import resolve_whatif_rosetta_dir


def _write_rosetta_fixture(root: Path, *, marker: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "event_id": f"{marker}-metadata",
                    "timestamp": "2001-01-01T00:00:00Z",
                    "actor_id": "actor@enron.com",
                    "target_id": "target@enron.com",
                    "event_type": "message",
                    "thread_task_id": "thr-1",
                    "artifacts": "{}",
                }
            ]
        ),
        root / "enron_rosetta_events_metadata.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist([{"event_id": f"{marker}-metadata", "content": marker}]),
        root / "enron_rosetta_events_content.parquet",
    )


def test_resolve_whatif_rosetta_dir_prefers_repo_default_before_env_and_workspace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo_rosetta = tmp_path / "repo_rosetta"
    env_rosetta = tmp_path / "env_rosetta"
    workspace_root = tmp_path / "workspace"
    workspace_rosetta = workspace_root / "rosetta"
    _write_rosetta_fixture(repo_rosetta, marker="repo")
    _write_rosetta_fixture(env_rosetta, marker="env")
    _write_rosetta_fixture(workspace_rosetta, marker="workspace")

    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(env_rosetta))
    monkeypatch.setattr(
        "vei.whatif._source_locator._repo_default_rosetta_dir",
        lambda: repo_rosetta,
    )

    assert resolve_whatif_rosetta_dir(workspace_root) == repo_rosetta.resolve()


def test_resolve_whatif_rosetta_dir_falls_back_to_env_then_workspace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo_rosetta = tmp_path / "missing_repo_rosetta"
    env_rosetta = tmp_path / "env_rosetta"
    workspace_root = tmp_path / "workspace"
    workspace_rosetta = workspace_root / "rosetta"
    _write_rosetta_fixture(env_rosetta, marker="env")
    _write_rosetta_fixture(workspace_rosetta, marker="workspace")

    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(env_rosetta))
    monkeypatch.setattr(
        "vei.whatif._source_locator._repo_default_rosetta_dir",
        lambda: repo_rosetta,
    )

    assert resolve_whatif_rosetta_dir(workspace_root) == env_rosetta.resolve()

    monkeypatch.delenv("VEI_WHATIF_ROSETTA_DIR", raising=False)
    assert resolve_whatif_rosetta_dir(workspace_root) == workspace_rosetta.resolve()
