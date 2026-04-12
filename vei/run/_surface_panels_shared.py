from __future__ import annotations

from typing import Any, Dict

from .models import (
    LivingSurfaceItem,
    LivingSurfacePanel,
    SurfacePanelKind,
    SurfacePanelStatus,
)


def build_panel(
    *,
    surface: str,
    kind: SurfacePanelKind,
    title: str,
    accent: str,
    headline: str,
    items: list[LivingSurfaceItem],
    fallback_status: SurfacePanelStatus | str | None = None,
    policy: dict[str, Any] | None = None,
) -> LivingSurfacePanel | None:
    if not items:
        return None
    return LivingSurfacePanel(
        surface=surface,
        kind=kind,
        title=title,
        accent=accent,
        status=aggregate_status(items, fallback=fallback_status or "ok"),
        headline=headline,
        items=items,
        highlight_refs=[
            item.highlight_ref for item in items if item.highlight_ref is not None
        ],
        policy=dict(policy or {}),
    )


def aggregate_status(
    items: list[LivingSurfaceItem],
    *,
    fallback: SurfacePanelStatus | str,
) -> SurfacePanelStatus:
    levels = [str(item.status or "").lower() for item in items]
    if any(
        level in {"critical", "pending_vendor", "stale", "launch_risk"}
        for level in levels
    ):
        return "critical"
    if any(
        level in {"warning", "pending", "pending_approval", "review"}
        for level in levels
    ):
        return "warning"
    if any(
        level in {"attention", "open", "in_progress", "scheduled", "draft"}
        for level in levels
    ):
        return "attention"
    if fallback in ("ok", "attention", "warning", "critical"):
        return fallback  # type: ignore[return-value]
    return "ok"


def ticket_sort_rank(status: str) -> int:
    normalized = status.lower()
    if normalized in {"open", "pending"}:
        return 0
    if normalized in {"in_progress", "review"}:
        return 1
    if normalized in {"scheduled", "ready"}:
        return 2
    return 3


def approval_sort_rank(status: str) -> int:
    normalized = status.lower()
    if normalized in {"pending_approval", "pending"}:
        return 0
    if normalized in {"in_progress", "review"}:
        return 1
    if normalized in {"approved", "complete"}:
        return 2
    return 3


def dict_records(payload: Dict[str, Any], key: str) -> Dict[str, Dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, dict):
        return {}
    return {
        str(record_key): item
        for record_key, item in value.items()
        if isinstance(item, dict)
    }


def dict_list(payload: Dict[str, Any], key: str) -> list[Dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def compact_badges(values: list[str]) -> list[str]:
    return [value for value in values if value]


def truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return f"{value[: limit - 1].rstrip()}\u2026"


def slack_ts(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
