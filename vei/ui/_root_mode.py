from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from vei.whatif import load_branch_point_benchmark_build_result
from vei.workspace import WORKSPACE_MANIFEST
from vei.workspace.api import show_workspace
from vei.workspace.models import WorkspaceManifest, WorkspaceSummary


def root_has_workspace(root: Path) -> bool:
    return (root / WORKSPACE_MANIFEST).exists()


def root_has_benchmark_artifacts(root: Path) -> bool:
    return any(
        path.exists()
        for path in (
            root / "branch_point_benchmark_build.json",
            root / "judge_result.json",
        )
    )


def load_ui_workspace_summary(root: Path) -> WorkspaceSummary | None:
    if root_has_workspace(root):
        return show_workspace(root)
    if not root_has_benchmark_artifacts(root):
        return None

    title = _benchmark_title(root)
    description = _benchmark_description(root)
    manifest = WorkspaceManifest(
        name=_slug(root.name or "benchmark-audit"),
        title=title,
        description=description,
        created_at=_iso_now(),
        source_kind="compiled_blueprint",
        source_ref=str(root),
        metadata={
            "ui_mode": "benchmark_audit",
            "benchmark_root": str(root),
        },
    )
    return WorkspaceSummary(
        manifest=manifest,
        compiled_scenarios=[],
        run_count=0,
        latest_run_id=None,
        imports=None,
    )


def _benchmark_title(root: Path) -> str:
    try:
        build = load_branch_point_benchmark_build_result(root)
    except Exception:  # noqa: BLE001
        return _humanize(root.name or "benchmark audit")
    return _humanize(build.label or build.heldout_pack_id or root.name)


def _benchmark_description(root: Path) -> str:
    try:
        build = load_branch_point_benchmark_build_result(root)
    except Exception:  # noqa: BLE001
        return "Saved benchmark audit bundle."
    pack_label = _humanize(build.heldout_pack_id or "benchmark")
    return f"Saved audit bundle for {pack_label}."


def _humanize(value: str) -> str:
    cleaned = " ".join(part for part in value.replace("-", "_").split("_") if part)
    return cleaned.strip().title() or "Benchmark Audit"


def _iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _slug(value: str) -> str:
    cleaned = [
        char.lower() if char.isalnum() else "-" for char in value.strip().lower()
    ]
    slug = "".join(cleaned).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug or "benchmark-audit"
