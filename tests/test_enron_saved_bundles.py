from __future__ import annotations

import json
from pathlib import Path

from vei.whatif.artifact_validation import validate_packaged_example_bundle
from vei.whatif.filenames import STUDIO_SAVED_FORECAST_FILES

EXAMPLES_ROOT = Path(__file__).resolve().parents[1] / "docs" / "examples"


def _bundle_roots() -> list[Path]:
    return sorted(
        path
        for path in EXAMPLES_ROOT.glob("enron-*")
        if path.is_dir() and (path / "workspace" / "episode_manifest.json").exists()
    )


def test_all_repo_owned_enron_bundles_validate_cleanly() -> None:
    bundle_roots = _bundle_roots()
    assert bundle_roots
    for bundle_root in bundle_roots:
        assert validate_packaged_example_bundle(bundle_root) == [], bundle_root


def test_all_repo_owned_enron_bundles_have_saved_forecast_and_ranked_comparison() -> (
    None
):
    for bundle_root in _bundle_roots():
        assert (bundle_root / "README.md").exists(), bundle_root
        assert (bundle_root / "whatif_experiment_result.json").exists(), bundle_root
        assert (
            bundle_root / "whatif_business_state_comparison.json"
        ).exists(), bundle_root
        comparison_payload = json.loads(
            (bundle_root / "whatif_business_state_comparison.json").read_text(
                encoding="utf-8"
            )
        )
        assert len(comparison_payload.get("candidates", [])) >= 3
        assert any(
            (bundle_root / filename).exists()
            for filename in STUDIO_SAVED_FORECAST_FILES
        ), bundle_root
