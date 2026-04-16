from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from vei.context.models import ContextSnapshot

from ._constants import (
    BUSINESS_STATE_COMPARISON_FILE,
    BUSINESS_STATE_COMPARISON_OVERVIEW_FILE,
    CONTEXT_SNAPSHOT_FILE as CANONICAL_CONTEXT_FILE,
    EJEPA_PROXY_RESULT_FILE,
    EJEPA_RESULT_FILE,
    EPISODE_MANIFEST_FILE as CANONICAL_MANIFEST_FILE,
    EXPERIMENT_OVERVIEW_FILE,
    EXPERIMENT_RESULT_FILE,
    LLM_RESULT_FILE,
    PUBLIC_CONTEXT_FILE as CANONICAL_PUBLIC_CONTEXT_FILE,
    SCRUBBED_PATH_PLACEHOLDER,
    STUDIO_SAVED_FORECAST_FILES,
    WORKSPACE_DIRECTORY,
)
from .models import WhatIfEpisodeManifest, WhatIfPublicContext


def detect_validation_mode(path: str | Path) -> str:
    resolved = Path(path).expanduser().resolve()
    if (resolved / CANONICAL_MANIFEST_FILE).exists():
        return "workspace"
    if (resolved / WORKSPACE_DIRECTORY / CANONICAL_MANIFEST_FILE).exists():
        return "bundle"
    return "tree"


def validate_saved_workspace(
    workspace_root: str | Path,
    *,
    allow_relative_workspace_root: bool = False,
) -> list[str]:
    resolved_workspace = Path(workspace_root).expanduser().resolve()
    issues: list[str] = []

    manifest_path = resolved_workspace / CANONICAL_MANIFEST_FILE
    snapshot_path = resolved_workspace / CANONICAL_CONTEXT_FILE
    public_context_path = resolved_workspace / CANONICAL_PUBLIC_CONTEXT_FILE
    unexpected_manifest_paths = _unexpected_manifest_paths(resolved_workspace)
    if not manifest_path.exists():
        if unexpected_manifest_paths:
            manifest_names = ", ".join(
                str(path.name) for path in unexpected_manifest_paths
            )
            issues.append(
                f"unexpected workspace manifest present in {resolved_workspace}: "
                f"{manifest_names}; expected {CANONICAL_MANIFEST_FILE}"
            )
        else:
            issues.append(f"missing workspace manifest: {manifest_path}")
        return issues
    manifest: WhatIfEpisodeManifest | None = None
    try:
        manifest = WhatIfEpisodeManifest.model_validate_json(
            manifest_path.read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        issues.append(f"invalid workspace manifest {manifest_path}: {exc}")

    if not snapshot_path.exists():
        issues.append(f"missing workspace snapshot: {snapshot_path}")
    else:
        try:
            ContextSnapshot.model_validate_json(
                snapshot_path.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError, ValidationError) as exc:
            issues.append(f"invalid workspace snapshot {snapshot_path}: {exc}")
    loaded_public_context: WhatIfPublicContext | None = None
    if not public_context_path.exists():
        issues.append(f"missing workspace public context: {public_context_path}")
    else:
        try:
            loaded_public_context = WhatIfPublicContext.model_validate_json(
                public_context_path.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError, ValidationError) as exc:
            issues.append(
                f"invalid workspace public context {public_context_path}: {exc}"
            )

    for unexpected_manifest_path in unexpected_manifest_paths:
        issues.append(
            f"unexpected workspace manifest present alongside canonical manifest: "
            f"{unexpected_manifest_path}"
        )

    if manifest is None:
        return issues

    expected_workspace_root = (
        "workspace" if allow_relative_workspace_root else str(resolved_workspace)
    )
    actual_workspace_root = str(manifest.workspace_root).strip()
    if actual_workspace_root != expected_workspace_root:
        issues.append(
            f"workspace_root mismatch in {manifest_path}: "
            f"expected {expected_workspace_root!r}, got {actual_workspace_root!r}"
        )
    if manifest.public_context is not None and loaded_public_context is not None:
        manifest_public_context = manifest.public_context.model_dump(mode="json")
        sidecar_public_context = loaded_public_context.model_dump(mode="json")
        if manifest_public_context != sidecar_public_context:
            issues.append(
                f"public context mismatch between {manifest_path} and "
                f"{public_context_path}"
            )
    baseline_dataset_path = resolved_workspace / manifest.baseline_dataset_path
    if not baseline_dataset_path.exists():
        issues.append(
            f"missing baseline dataset referenced by {manifest_path}: "
            f"{baseline_dataset_path}"
        )
    return issues


def validate_packaged_example_bundle(root: str | Path) -> list[str]:
    bundle_root = Path(root).expanduser().resolve()
    issues = validate_saved_workspace(
        bundle_root / WORKSPACE_DIRECTORY,
        allow_relative_workspace_root=True,
    )
    _validate_required_bundle_files(issues, bundle_root)

    experiment_path = bundle_root / EXPERIMENT_RESULT_FILE
    if experiment_path.exists():
        payload = _read_json(experiment_path)
        materialization = payload.get("materialization")
        if isinstance(materialization, dict):
            _check_path_value(
                issues,
                path=experiment_path,
                key="manifest_path",
                actual=materialization.get("manifest_path"),
                expected=f"{WORKSPACE_DIRECTORY}/{CANONICAL_MANIFEST_FILE}",
            )
            _check_path_value(
                issues,
                path=experiment_path,
                key="context_snapshot_path",
                actual=materialization.get("context_snapshot_path"),
                expected=f"{WORKSPACE_DIRECTORY}/{CANONICAL_CONTEXT_FILE}",
            )
            _check_path_value(
                issues,
                path=experiment_path,
                key="workspace_root",
                actual=materialization.get("workspace_root"),
                expected=WORKSPACE_DIRECTORY,
            )
        artifacts = payload.get("artifacts")
        if isinstance(artifacts, dict):
            _check_path_value(
                issues,
                path=experiment_path,
                key="result_json_path",
                actual=artifacts.get("result_json_path"),
                expected=EXPERIMENT_RESULT_FILE,
            )
            _check_path_value(
                issues,
                path=experiment_path,
                key="overview_markdown_path",
                actual=artifacts.get("overview_markdown_path"),
                expected=EXPERIMENT_OVERVIEW_FILE,
            )
            _check_optional_path_value(
                issues,
                bundle_root=bundle_root,
                path=experiment_path,
                key="llm_json_path",
                actual=artifacts.get("llm_json_path"),
                expected=LLM_RESULT_FILE,
            )
            _check_optional_forecast_path_value(
                issues,
                bundle_root=bundle_root,
                path=experiment_path,
                key="forecast_json_path",
                actual=artifacts.get("forecast_json_path"),
            )
    _validate_optional_business_state_files(issues, bundle_root)

    for relative_path in _scrubbed_bundle_paths(bundle_root):
        candidate = bundle_root / relative_path
        if not candidate.exists():
            continue
        text = candidate.read_text(encoding="utf-8")
        if "/Users/" in text:
            issues.append(f"unscrubbed absolute path in {candidate}")
        if relative_path in {EJEPA_RESULT_FILE, EJEPA_PROXY_RESULT_FILE}:
            if SCRUBBED_PATH_PLACEHOLDER not in text:
                issues.append(f"missing scrubbed-path placeholder in {candidate}")
    return issues


def validate_artifact_tree(root: str | Path) -> list[str]:
    resolved_root = Path(root).expanduser().resolve()
    issues: list[str] = []
    workspace_roots = {
        manifest_path.parent
        for manifest_path in resolved_root.rglob(CANONICAL_MANIFEST_FILE)
    }
    workspace_roots.update(
        manifest_path.parent
        for manifest_path in resolved_root.rglob("*episode_manifest.json")
        if manifest_path.name != CANONICAL_MANIFEST_FILE
    )
    for workspace_root in sorted(workspace_roots):
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


def _check_optional_path_value(
    issues: list[str],
    *,
    bundle_root: Path,
    path: Path,
    key: str,
    actual: Any,
    expected: str,
) -> None:
    actual_text = str(actual or "").strip()
    if not actual_text:
        return
    _check_path_value(
        issues,
        path=path,
        key=key,
        actual=actual_text,
        expected=expected,
    )
    candidate = bundle_root / expected
    if not candidate.exists():
        issues.append(f"missing bundle artifact: {candidate}")


def _check_optional_forecast_path_value(
    issues: list[str],
    *,
    bundle_root: Path,
    path: Path,
    key: str,
    actual: Any,
) -> None:
    actual_text = str(actual or "").strip()
    if not actual_text:
        return
    if actual_text not in STUDIO_SAVED_FORECAST_FILES:
        expected = " or ".join(filename for filename in STUDIO_SAVED_FORECAST_FILES)
        issues.append(
            f"{key} mismatch in {path}: expected {expected!r}, got {actual_text!r}"
        )
        return
    candidate = bundle_root / actual_text
    if not candidate.exists():
        issues.append(f"missing bundle artifact: {candidate}")


def _unexpected_manifest_paths(workspace_root: Path) -> list[Path]:
    return sorted(
        candidate
        for candidate in workspace_root.glob("*episode_manifest.json")
        if candidate.name != CANONICAL_MANIFEST_FILE
    )


def _validate_required_bundle_files(issues: list[str], bundle_root: Path) -> None:
    for relative_path in (
        EXPERIMENT_RESULT_FILE,
        EXPERIMENT_OVERVIEW_FILE,
        f"{WORKSPACE_DIRECTORY}/{CANONICAL_CONTEXT_FILE}",
        f"{WORKSPACE_DIRECTORY}/{CANONICAL_MANIFEST_FILE}",
        f"{WORKSPACE_DIRECTORY}/{CANONICAL_PUBLIC_CONTEXT_FILE}",
    ):
        candidate = bundle_root / relative_path
        if not candidate.exists():
            issues.append(f"missing bundle artifact: {candidate}")


def _validate_optional_business_state_files(
    issues: list[str],
    bundle_root: Path,
) -> None:
    comparison_json = bundle_root / BUSINESS_STATE_COMPARISON_FILE
    comparison_md = bundle_root / BUSINESS_STATE_COMPARISON_OVERVIEW_FILE
    if not comparison_json.exists() and not comparison_md.exists():
        return
    if not comparison_json.exists():
        issues.append(f"missing bundle artifact: {comparison_json}")
        return
    if not comparison_md.exists():
        issues.append(f"missing bundle artifact: {comparison_md}")
    payload = _read_json(comparison_json)
    if not isinstance(payload.get("candidates"), list):
        issues.append(f"missing candidates in {comparison_json}")


def _scrubbed_bundle_paths(bundle_root: Path) -> list[str]:
    candidates = [
        EXPERIMENT_RESULT_FILE,
        EXPERIMENT_OVERVIEW_FILE,
        LLM_RESULT_FILE,
        EJEPA_RESULT_FILE,
        EJEPA_PROXY_RESULT_FILE,
        BUSINESS_STATE_COMPARISON_FILE,
        BUSINESS_STATE_COMPARISON_OVERVIEW_FILE,
        f"{WORKSPACE_DIRECTORY}/{CANONICAL_MANIFEST_FILE}",
    ]
    return [
        relative_path
        for relative_path in candidates
        if (bundle_root / relative_path).exists()
    ]
