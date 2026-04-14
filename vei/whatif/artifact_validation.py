from __future__ import annotations

import json
from pathlib import Path
from typing import Any

CANONICAL_CONTEXT_FILE = "context_snapshot.json"
CANONICAL_MANIFEST_FILE = "episode_manifest.json"
CANONICAL_PUBLIC_CONTEXT_FILE = "whatif_public_context.json"
LEGACY_MANIFEST_FILES = ("whatif_episode_manifest.json",)


def validate_saved_workspace(
    workspace_root: str | Path,
    *,
    allow_relative_workspace_root: bool = False,
) -> list[str]:
    resolved_workspace = Path(workspace_root).expanduser().resolve()
    issues: list[str] = []

    manifest_path = resolved_workspace / CANONICAL_MANIFEST_FILE
    snapshot_path = resolved_workspace / CANONICAL_CONTEXT_FILE
    legacy_manifest_paths = [
        resolved_workspace / name
        for name in LEGACY_MANIFEST_FILES
        if (resolved_workspace / name).exists()
    ]
    if not manifest_path.exists():
        if legacy_manifest_paths:
            legacy_names = ", ".join(str(path.name) for path in legacy_manifest_paths)
            issues.append(
                f"legacy workspace manifest present in {resolved_workspace}: "
                f"{legacy_names}; expected {CANONICAL_MANIFEST_FILE}"
            )
        else:
            issues.append(f"missing workspace manifest: {manifest_path}")
        return issues
    if not snapshot_path.exists():
        issues.append(f"missing workspace snapshot: {snapshot_path}")
    for legacy_manifest_path in legacy_manifest_paths:
        issues.append(
            f"legacy workspace manifest present alongside canonical manifest: "
            f"{legacy_manifest_path}"
        )

    manifest = _read_json(manifest_path)
    expected_workspace_root = (
        "workspace" if allow_relative_workspace_root else str(resolved_workspace)
    )
    actual_workspace_root = str(manifest.get("workspace_root", "") or "").strip()
    if actual_workspace_root != expected_workspace_root:
        issues.append(
            f"workspace_root mismatch in {manifest_path}: "
            f"expected {expected_workspace_root!r}, got {actual_workspace_root!r}"
        )
    return issues


def validate_packaged_example_bundle(root: str | Path) -> list[str]:
    bundle_root = Path(root).expanduser().resolve()
    issues = validate_saved_workspace(
        bundle_root / "workspace",
        allow_relative_workspace_root=True,
    )
    experiment_path = bundle_root / "whatif_experiment_result.json"
    if experiment_path.exists():
        payload = _read_json(experiment_path)
        materialization = payload.get("materialization")
        if isinstance(materialization, dict):
            _check_path_value(
                issues,
                path=experiment_path,
                key="manifest_path",
                actual=materialization.get("manifest_path"),
                expected="workspace/episode_manifest.json",
            )
            _check_path_value(
                issues,
                path=experiment_path,
                key="context_snapshot_path",
                actual=materialization.get("context_snapshot_path"),
                expected="workspace/context_snapshot.json",
            )
            _check_path_value(
                issues,
                path=experiment_path,
                key="workspace_root",
                actual=materialization.get("workspace_root"),
                expected="workspace",
            )
    for relative_path in (
        "whatif_experiment_result.json",
        "whatif_ejepa_result.json",
        "whatif_ejepa_proxy_result.json",
        "workspace/episode_manifest.json",
    ):
        candidate = bundle_root / relative_path
        if not candidate.exists():
            continue
        text = candidate.read_text(encoding="utf-8")
        if "/Users/" in text:
            issues.append(f"unscrubbed absolute path in {candidate}")
    return issues


def validate_artifact_tree(root: str | Path) -> list[str]:
    resolved_root = Path(root).expanduser().resolve()
    issues: list[str] = []
    seen_workspaces: set[Path] = set()
    for manifest_path in resolved_root.rglob(CANONICAL_MANIFEST_FILE):
        workspace_root = manifest_path.parent
        seen_workspaces.add(workspace_root)
        issues.extend(validate_saved_workspace(workspace_root))
    for legacy_name in LEGACY_MANIFEST_FILES:
        for manifest_path in resolved_root.rglob(legacy_name):
            workspace_root = manifest_path.parent
            if workspace_root in seen_workspaces:
                continue
            issues.extend(validate_saved_workspace(workspace_root))
    return issues


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _check_path_value(
    issues: list[str],
    *,
    path: Path,
    key: str,
    actual: Any,
    expected: str,
) -> None:
    actual_text = str(actual or "").strip()
    if actual_text != expected:
        issues.append(
            f"{key} mismatch in {path}: expected {expected!r}, got {actual_text!r}"
        )
