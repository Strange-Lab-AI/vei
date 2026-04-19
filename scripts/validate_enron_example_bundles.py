from __future__ import annotations

import argparse
import json
from pathlib import Path

from vei.context.api import canonical_history_paths, load_canonical_history_bundle
from vei.whatif.artifact_validation import validate_packaged_example_bundle
from vei.whatif.filenames import (
    EXPERIMENT_RESULT_FILE,
    HEURISTIC_FORECAST_FILE,
    EPISODE_MANIFEST_FILE,
    REFERENCE_FORECAST_FILE,
    WORKSPACE_DIRECTORY,
)

DEFAULT_ROOT = Path("docs/examples")
REQUIRED_DEMO_FILES = (
    "enron_story_overview.md",
    "enron_story_manifest.json",
    "enron_exports_preview.json",
    "enron_presentation_manifest.json",
    "enron_presentation_guide.md",
)


def _bundle_roots(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.glob("enron-*")
        if path.is_dir() and (path / "workspace" / "episode_manifest.json").exists()
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate every repo-owned Enron saved example bundle."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="Examples root that contains the tracked enron-* bundles.",
    )
    args = parser.parse_args()
    bundle_roots = _bundle_roots(args.root.resolve())
    issues_found = False
    for bundle_root in bundle_roots:
        issues = validate_packaged_example_bundle(bundle_root)
        issues.extend(_enron_bundle_issues(bundle_root))
        if issues:
            issues_found = True
            print(f"failed: {bundle_root}")
            for issue in issues:
                print(f"- {issue}")
            continue
        print(f"ok: {bundle_root}")
    return 1 if issues_found else 0


def _enron_bundle_issues(bundle_root: Path) -> list[str]:
    issues: list[str] = []
    experiment_path = bundle_root / EXPERIMENT_RESULT_FILE
    payload = json.loads(experiment_path.read_text(encoding="utf-8"))
    forecast_path = str(
        ((payload.get("artifacts") or {}).get("forecast_json_path") or "")
    ).strip()
    if forecast_path != REFERENCE_FORECAST_FILE:
        issues.append(
            f"expected {REFERENCE_FORECAST_FILE} as the saved forecast, got {forecast_path!r}"
        )

    reference_path = bundle_root / REFERENCE_FORECAST_FILE
    if not reference_path.exists():
        issues.append(f"missing bundle artifact: {reference_path}")

    heuristic_path = bundle_root / HEURISTIC_FORECAST_FILE
    if heuristic_path.exists():
        issues.append(
            f"heuristic baseline should stay debug-only and out of the saved Enron bundle: {heuristic_path}"
        )

    workspace_snapshot = bundle_root / WORKSPACE_DIRECTORY / "context_snapshot.json"
    history_paths = canonical_history_paths(workspace_snapshot)
    if not history_paths.events_path.exists():
        issues.append(f"missing bundle artifact: {history_paths.events_path}")
    if not history_paths.index_path.exists():
        issues.append(f"missing bundle artifact: {history_paths.index_path}")
    for filename in REQUIRED_DEMO_FILES:
        path = bundle_root / filename
        if not path.exists():
            issues.append(f"missing bundle artifact: {path}")
    issues.extend(_bundle_history_issues(bundle_root))
    return issues


def _bundle_history_issues(bundle_root: Path) -> list[str]:
    issues: list[str] = []
    workspace_root = bundle_root / WORKSPACE_DIRECTORY
    manifest_payload = json.loads(
        (workspace_root / EPISODE_MANIFEST_FILE).read_text(encoding="utf-8")
    )
    branch_event_id = str(manifest_payload.get("branch_event_id") or "").strip()
    branch_timestamp = str(manifest_payload.get("branch_timestamp") or "").strip()
    history_bundle = load_canonical_history_bundle(
        workspace_root / "context_snapshot.json"
    )
    if history_bundle is None:
        return ["missing canonical history bundle"]

    prior_rows = [
        row
        for row in history_bundle.index.rows
        if row.event_id != branch_event_id
        and (not branch_timestamp or row.timestamp <= branch_timestamp)
    ]
    if len(prior_rows) < 30:
        issues.append(
            f"expected at least 30 prior canonical events, found {len(prior_rows)}"
        )

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
    if len(source_families) < 3:
        issues.append(
            "expected at least 3 source families or domains in the saved timeline, "
            f"found {sorted(source_families)}"
        )
    return issues


if __name__ == "__main__":
    raise SystemExit(main())
