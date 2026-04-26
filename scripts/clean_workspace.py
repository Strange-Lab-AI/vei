from __future__ import annotations

import argparse
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

_ROOT_DIRS = (
    ".artifacts",
    ".hypothesis",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".pyright",
    ".cache",
    "build",
    "dist",
    "htmlcov",
    "output",
    "pyvei.egg-info",
    "vei.egg-info",
)
_ROOT_FILES = (".coverage",)
_ROOT_GLOBS = (".coverage.*",)
_PRESERVED_VEI_OUT_DIRS = ("datasets", "world_model_current")
_PRESERVED_LLM_LIVE_DIRS = ("latest",)
_SKIP_RECURSIVE_DIRS = {".git", ".venv"}


@dataclass(frozen=True)
class CleanupReport:
    removed: tuple[Path, ...]
    preserved: tuple[Path, ...]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove local generated artifacts while keeping useful long-lived outputs.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Workspace root to clean. Defaults to the current directory.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be removed without deleting anything.",
    )
    parser.add_argument(
        "--hard",
        action="store_true",
        help=(
            "Also prune old generated _vei_out runs. Keeps datasets, "
            "world_model_current, and llm_live/latest."
        ),
    )
    return parser.parse_args()


def _iter_existing(paths: Iterable[Path]) -> list[Path]:
    return [path for path in paths if path.exists()]


def _delete_path(path: Path, *, dry_run: bool) -> None:
    if dry_run:
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
        return
    path.unlink(missing_ok=True)


def _clean_vei_out(root: Path, *, dry_run: bool) -> CleanupReport:
    vei_out = root / "_vei_out"
    if not vei_out.exists():
        return CleanupReport(removed=(), preserved=())

    removed: list[Path] = []
    preserved: list[Path] = []

    for child in sorted(vei_out.iterdir()):
        if child.name in _PRESERVED_VEI_OUT_DIRS:
            preserved.append(child)
            continue
        if child.name != "llm_live":
            removed.append(child)
            _delete_path(child, dry_run=dry_run)
            continue

        for llm_child in sorted(child.iterdir()):
            if llm_child.name in _PRESERVED_LLM_LIVE_DIRS:
                preserved.append(llm_child)
                continue
            removed.append(llm_child)
            _delete_path(llm_child, dry_run=dry_run)

        if not dry_run and child.exists() and not any(child.iterdir()):
            child.rmdir()
            removed.append(child)

    if not dry_run and vei_out.exists() and not any(vei_out.iterdir()):
        vei_out.rmdir()
        removed.append(vei_out)

    return CleanupReport(
        removed=tuple(removed),
        preserved=tuple(preserved),
    )


def _clean_root_outputs(root: Path, *, dry_run: bool) -> tuple[Path, ...]:
    candidates = [root / name for name in _ROOT_DIRS]
    candidates.extend(root / name for name in _ROOT_FILES)
    for pattern in _ROOT_GLOBS:
        candidates.extend(root.glob(pattern))

    removed: list[Path] = []
    for path in sorted(_iter_existing(candidates)):
        removed.append(path)
        _delete_path(path, dry_run=dry_run)
    return tuple(removed)


def _clean_recursive_caches(root: Path, *, dry_run: bool) -> tuple[Path, ...]:
    removed: list[Path] = []

    for current_root, dirnames, filenames in os.walk(root, topdown=True):
        dirnames[:] = sorted(
            name for name in dirnames if name not in _SKIP_RECURSIVE_DIRS
        )
        current = Path(current_root)

        if "__pycache__" in dirnames:
            cache_dir = current / "__pycache__"
            removed.append(cache_dir)
            _delete_path(cache_dir, dry_run=dry_run)
            dirnames[:] = [name for name in dirnames if name != "__pycache__"]

        for filename in sorted(filenames):
            if not (filename.endswith((".pyc", ".pyo")) or filename == ".DS_Store"):
                continue
            path = current / filename
            removed.append(path)
            _delete_path(path, dry_run=dry_run)

    return tuple(removed)


def clean_workspace(root: Path, *, dry_run: bool, hard: bool = False) -> CleanupReport:
    root = root.resolve()
    root_removed = list(_clean_root_outputs(root, dry_run=dry_run))
    vei_out_report = (
        _clean_vei_out(root, dry_run=dry_run)
        if hard
        else CleanupReport(removed=(), preserved=())
    )
    recursive_removed = list(_clean_recursive_caches(root, dry_run=dry_run))
    removed = tuple(root_removed + list(vei_out_report.removed) + recursive_removed)
    preserved = vei_out_report.preserved
    return CleanupReport(removed=removed, preserved=preserved)


def _print_report(report: CleanupReport, *, root: Path, dry_run: bool) -> None:
    action = "Would remove" if dry_run else "Removed"
    if report.removed:
        print(f"{action} {len(report.removed)} paths under {root}:")
        for path in report.removed:
            print(f"  - {path.relative_to(root)}")
    else:
        print(f"Nothing to remove under {root}.")

    if report.preserved:
        print("Kept useful local outputs:")
        for path in report.preserved:
            print(f"  - {path.relative_to(root)}")


def main() -> None:
    args = _parse_args()
    root = args.root.resolve()
    report = clean_workspace(root, dry_run=args.dry_run, hard=args.hard)
    _print_report(report, root=root, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
