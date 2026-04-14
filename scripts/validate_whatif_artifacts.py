from __future__ import annotations

import argparse
import sys
from pathlib import Path

from vei.whatif.artifact_validation import (
    detect_validation_mode,
    validate_artifact_tree,
    validate_packaged_example_bundle,
    validate_saved_workspace,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate saved what-if workspaces and packaged artifact bundles."
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Workspace root, packaged example root, or artifact tree root.",
    )
    parser.add_argument(
        "--mode",
        choices=("workspace", "bundle", "tree"),
        default=None,
        help="Validation mode to run.",
    )
    parser.add_argument(
        "--allow-relative-workspace-root",
        action="store_true",
        help="Allow packaged examples that store workspace_root as 'workspace'.",
    )
    args = parser.parse_args()

    target = args.path.expanduser().resolve()
    mode = args.mode or detect_validation_mode(target)
    if mode == "workspace":
        issues = validate_saved_workspace(
            target,
            allow_relative_workspace_root=args.allow_relative_workspace_root,
        )
    elif mode == "bundle":
        issues = validate_packaged_example_bundle(target)
    else:
        issues = validate_artifact_tree(target)

    if not issues:
        print(f"ok: {target}")
        return 0

    print(f"validation failed for {target}")
    for issue in issues:
        print(f"- {issue}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
