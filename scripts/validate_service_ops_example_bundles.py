from __future__ import annotations

import argparse
from pathlib import Path

try:
    from scripts.build_service_ops_example_bundles import validate_bundle
except ModuleNotFoundError:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from build_service_ops_example_bundles import validate_bundle


DEFAULT_ROOT = Path("docs/examples")


def _bundle_roots(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.glob("clearwater-*")
        if path.is_dir() and (path / "workspace" / "episode_manifest.json").exists()
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate every repo-owned synthetic Clearwater saved example bundle."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="Examples root that contains the tracked clearwater-* bundles.",
    )
    args = parser.parse_args()

    issues_found = False
    for bundle_root in _bundle_roots(args.root.resolve()):
        issues = validate_bundle(bundle_root)
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
