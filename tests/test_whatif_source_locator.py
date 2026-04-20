from __future__ import annotations

import shutil
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


def test_resolve_whatif_rosetta_dir_prefers_env_before_cache_and_repo_sample(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo_rosetta = tmp_path / "repo_rosetta"
    cached_rosetta = tmp_path / "cached_rosetta"
    env_rosetta = tmp_path / "env_rosetta"
    workspace_root = tmp_path / "workspace"
    workspace_rosetta = workspace_root / "rosetta"
    _write_rosetta_fixture(repo_rosetta, marker="repo")
    _write_rosetta_fixture(cached_rosetta, marker="cached")
    _write_rosetta_fixture(env_rosetta, marker="env")
    _write_rosetta_fixture(workspace_rosetta, marker="workspace")

    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(env_rosetta))
    monkeypatch.setattr(
        "vei.whatif._source_locator._repo_default_rosetta_dir",
        lambda: repo_rosetta,
    )
    monkeypatch.setattr(
        "vei.whatif._source_locator.resolve_cached_full_enron_rosetta_dir",
        lambda: cached_rosetta,
    )

    assert resolve_whatif_rosetta_dir(workspace_root) == env_rosetta.resolve()


def test_resolve_whatif_rosetta_dir_prefers_cache_before_repo_sample_and_workspace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo_rosetta = tmp_path / "repo_rosetta"
    cached_rosetta = tmp_path / "cached_rosetta"
    workspace_root = tmp_path / "workspace"
    workspace_rosetta = workspace_root / "rosetta"
    _write_rosetta_fixture(repo_rosetta, marker="repo")
    _write_rosetta_fixture(cached_rosetta, marker="cached")
    _write_rosetta_fixture(workspace_rosetta, marker="workspace")

    monkeypatch.setattr(
        "vei.whatif._source_locator._repo_default_rosetta_dir",
        lambda: repo_rosetta,
    )
    monkeypatch.setattr(
        "vei.whatif._source_locator.resolve_cached_full_enron_rosetta_dir",
        lambda: cached_rosetta,
    )

    assert resolve_whatif_rosetta_dir(workspace_root) == cached_rosetta.resolve()


def test_resolve_whatif_rosetta_dir_falls_back_to_repo_sample_then_workspace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo_rosetta = tmp_path / "repo_rosetta"
    workspace_root = tmp_path / "workspace"
    workspace_rosetta = workspace_root / "rosetta"
    _write_rosetta_fixture(repo_rosetta, marker="repo")
    _write_rosetta_fixture(workspace_rosetta, marker="workspace")

    monkeypatch.setattr(
        "vei.whatif._source_locator._repo_default_rosetta_dir",
        lambda: repo_rosetta,
    )
    monkeypatch.setattr(
        "vei.whatif._source_locator.resolve_cached_full_enron_rosetta_dir",
        lambda: None,
    )

    assert resolve_whatif_rosetta_dir(workspace_root) == repo_rosetta.resolve()

    shutil.rmtree(repo_rosetta)
    assert resolve_whatif_rosetta_dir(workspace_root) == workspace_rosetta.resolve()
