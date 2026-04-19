from __future__ import annotations

import json
from pathlib import Path

from vei.context.api import canonical_history_paths, load_canonical_history_bundle
from vei.whatif.filenames import (
    EPISODE_MANIFEST_FILE,
    HEURISTIC_FORECAST_FILE,
    REFERENCE_FORECAST_FILE,
    WORKSPACE_DIRECTORY,
)

try:
    from scripts.build_service_ops_example_bundles import validate_bundle
except ModuleNotFoundError:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    from build_service_ops_example_bundles import validate_bundle


EXAMPLES_ROOT = Path(__file__).resolve().parents[1] / "docs" / "examples"


def _bundle_roots() -> list[Path]:
    return sorted(
        path
        for path in EXAMPLES_ROOT.glob("clearwater-*")
        if path.is_dir() and (path / "workspace" / "episode_manifest.json").exists()
    )


def test_all_repo_owned_service_ops_bundles_validate_cleanly() -> None:
    bundle_roots = _bundle_roots()
    assert bundle_roots
    for bundle_root in bundle_roots:
        assert validate_bundle(bundle_root) == [], bundle_root


def test_all_repo_owned_service_ops_bundles_use_reference_forecast_and_sidecars() -> (
    None
):
    for bundle_root in _bundle_roots():
        experiment_payload = json.loads(
            (bundle_root / "whatif_experiment_result.json").read_text(encoding="utf-8")
        )
        forecast_path = str(
            (
                (experiment_payload.get("artifacts") or {}).get("forecast_json_path")
                or ""
            )
        ).strip()
        assert forecast_path == REFERENCE_FORECAST_FILE, bundle_root
        assert (bundle_root / REFERENCE_FORECAST_FILE).exists(), bundle_root
        assert not (bundle_root / HEURISTIC_FORECAST_FILE).exists(), bundle_root

        history_paths = canonical_history_paths(
            bundle_root / WORKSPACE_DIRECTORY / "context_snapshot.json"
        )
        assert history_paths.events_path.exists(), history_paths.events_path
        assert history_paths.index_path.exists(), history_paths.index_path


def test_all_repo_owned_service_ops_bundles_have_rich_prior_timelines() -> None:
    for bundle_root in _bundle_roots():
        workspace_root = bundle_root / WORKSPACE_DIRECTORY
        manifest_payload = json.loads(
            (workspace_root / EPISODE_MANIFEST_FILE).read_text(encoding="utf-8")
        )
        branch_event_id = str(manifest_payload.get("branch_event_id") or "").strip()
        branch_timestamp = str(manifest_payload.get("branch_timestamp") or "").strip()
        history_bundle = load_canonical_history_bundle(
            workspace_root / "context_snapshot.json"
        )

        assert history_bundle is not None, bundle_root
        prior_rows = [
            row
            for row in history_bundle.index.rows
            if row.event_id != branch_event_id
            and (not branch_timestamp or row.timestamp <= branch_timestamp)
        ]
        assert len(prior_rows) >= 30, bundle_root

        source_families = {
            str(row.metadata.get("source_family") or "").strip().lower()
            or str(row.provider or "").strip().lower()
            or str(row.surface or "").strip().lower()
            for row in prior_rows
            if (
                str(row.metadata.get("source_family") or "").strip()
                or str(row.provider or "").strip()
                or str(row.surface or "").strip()
            )
        }
        assert len(source_families) >= 3, (bundle_root, sorted(source_families))
