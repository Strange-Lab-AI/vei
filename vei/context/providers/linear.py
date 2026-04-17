from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from vei.context.models import ContextProviderConfig, ContextSourceResult

from .base import iso_now


class LinearContextProvider:
    name = "linear"

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult:
        root = _resolve_path(config.base_url)
        if root is None:
            raise ValueError(
                "linear provider requires base_url pointing to a local export"
            )
        payload = _load_linear_export(root)
        return ContextSourceResult(
            provider="linear",
            captured_at=iso_now(),
            status="ok" if payload["issues"] or payload["cycles"] else "empty",
            record_counts={
                "cycles": len(payload["cycles"]),
                "issues": len(payload["issues"]),
                "projects": len(payload["projects"]),
            },
            data=payload,
        )


def capture_from_export(export_path: str | Path) -> ContextSourceResult:
    return LinearContextProvider().capture(
        ContextProviderConfig(provider="linear", base_url=str(export_path))
    )


def _resolve_path(raw: str | None) -> Path | None:
    if not raw:
        return None
    path = Path(raw).expanduser().resolve()
    if not path.exists():
        return None
    return path


def _load_linear_export(root: Path) -> dict[str, Any]:
    if root.is_file():
        raw = json.loads(root.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            return {
                "cycles": list(raw.get("cycles") or []),
                "issues": list(raw.get("issues") or []),
                "projects": list(raw.get("projects") or []),
            }
        if isinstance(raw, list):
            return {"cycles": [], "issues": raw, "projects": []}
    cycles: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    projects: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        name = path.stem.lower()
        if isinstance(raw, dict):
            if "cycles" in raw:
                cycles.extend(list(raw.get("cycles") or []))
            if "issues" in raw:
                issues.extend(list(raw.get("issues") or []))
            if "projects" in raw:
                projects.extend(list(raw.get("projects") or []))
            if "nodes" in raw and isinstance(raw["nodes"], list):
                if "cycle" in name:
                    cycles.extend(list(raw["nodes"]))
                elif "project" in name:
                    projects.extend(list(raw["nodes"]))
                else:
                    issues.extend(list(raw["nodes"]))
            continue
        if not isinstance(raw, list):
            continue
        if "cycle" in name:
            cycles.extend(raw)
        elif "project" in name:
            projects.extend(raw)
        else:
            issues.extend(raw)
    return {"cycles": cycles, "issues": issues, "projects": projects}
