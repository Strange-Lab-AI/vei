from __future__ import annotations

import json
from pathlib import Path


def test_enron_large_files_are_not_lfs_tracked_in_repo_tip() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    gitattributes = (repo_root / ".gitattributes").read_text(encoding="utf-8")

    assert "filter=lfs" not in gitattributes
    assert "release assets" in gitattributes.lower()


def test_repo_ships_release_manifest_and_sample_rosetta_contract() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    release_manifest_path = repo_root / "data" / "enron" / "full_dataset_release.json"
    rosetta_root = repo_root / "data" / "enron" / "rosetta"

    payload = json.loads(release_manifest_path.read_text(encoding="utf-8"))
    assert payload["asset_name"].endswith(".tar.gz")
    assert payload["release_tag"]
    assert payload["rosetta_relpath"] == "rosetta"
    assert payload["source_relpath"] == "source"
    assert payload["raw_relpath"] == "raw"

    assert (rosetta_root / "enron_rosetta_events_metadata.parquet").exists()
    assert (rosetta_root / "enron_rosetta_events_content.parquet").exists()
    assert (rosetta_root / "enron_talk_graph_edges.parquet").exists()
    assert (rosetta_root / "enron_work_graph_transitions.parquet").exists()
    assert (rosetta_root / "enron_work_graph_edges.parquet").exists()
    marker_payload = json.loads(
        (rosetta_root / "enron_rosetta_dataset.json").read_text(encoding="utf-8")
    )
    assert marker_payload["dataset_kind"] == "sample"


def test_default_workflows_avoid_lfs_and_manual_workflow_fetches_full_dataset() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    ci_workflow = (repo_root / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    nightly_workflow = (
        repo_root / ".github" / "workflows" / "nightly_release.yml"
    ).read_text(encoding="utf-8")
    manual_workflow = (
        repo_root / ".github" / "workflows" / "full_enron_manual.yml"
    ).read_text(encoding="utf-8")

    assert "lfs: true" not in ci_workflow
    assert "lfs: true" not in nightly_workflow
    assert "workflow_dispatch:" in manual_workflow
    assert "scripts/fetch_enron_full_dataset.py" in manual_workflow
