from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from vei.run.api import list_run_manifests


def load_saved_governor_payload(workspace_root: Path) -> dict[str, Any]:
    twin_path = workspace_root / "twin_manifest.json"
    fallback: dict[str, Any] = {}
    if twin_path.exists():
        try:
            data = json.loads(twin_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            data = {}
        fallback = dict(data.get("metadata", {}).get("governor", {}) or {})
    if isinstance(fallback, dict) and (
        "config" in fallback
        or "agents" in fallback
        or "pending_approvals" in fallback
        or "pending_demo_steps" in fallback
    ):
        return fallback

    completed_governor: dict[str, Any] | None = None
    for manifest in list_run_manifests(workspace_root):
        if manifest.runner != "external":
            continue
        governor = manifest.metadata.get("governor", {})
        if not isinstance(governor, dict):
            continue
        if manifest.status == "running":
            return dict(governor)
        if completed_governor is None and manifest.status == "completed":
            completed_governor = dict(governor)
    return completed_governor if completed_governor is not None else fallback


def load_saved_workforce_payload(workspace_root: Path) -> dict[str, Any]:
    completed_workforce: dict[str, Any] | None = None
    for manifest in list_run_manifests(workspace_root):
        if manifest.runner != "external":
            continue
        workforce = manifest.metadata.get("workforce", {})
        if not isinstance(workforce, dict):
            continue
        if manifest.status == "running":
            return dict(workforce)
        if completed_workforce is None and manifest.status == "completed":
            completed_workforce = dict(workforce)
    return completed_workforce or {}


__all__ = ["load_saved_governor_payload", "load_saved_workforce_payload"]
