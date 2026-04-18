from __future__ import annotations

import argparse
from pathlib import Path

from vei.whatif.artifact_validation import validate_packaged_example_bundle

DEFAULT_ROOT = Path("docs/examples")


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
        if issues:
            issues_found = True
            print(f"failed: {bundle_root}")
            for issue in issues:
                print(f"- {issue}")
            continue
        print(f"ok: {bundle_root}")
    return 1 if issues_found else 0


if __name__ == "__main__":
    raise SystemExit(main())
