from __future__ import annotations

import importlib
import json
from collections import Counter
from pathlib import Path
from typing import Any


def load_run_timeline(
    *,
    workspace_root: Path,
    run_id: str,
    fallback_run_dir: Path,
) -> tuple[list[dict[str, Any]], bool]:
    public_events = _load_run_timeline_from_vei_api(workspace_root, run_id)
    if public_events is not None:
        return public_events, True
    return _load_events_from_path(fallback_run_dir / "events.jsonl"), False


def load_run_snapshots(
    *,
    workspace_root: Path,
    run_id: str,
    fallback_run_dir: Path,
) -> tuple[list[dict[str, Any]], bool]:
    public_snapshots = _load_run_snapshots_from_vei_api(workspace_root, run_id)
    if public_snapshots:
        return public_snapshots, True
    return _load_snapshots_from_path(fallback_run_dir), False


def load_run_surface_summary(
    *,
    workspace_root: Path,
    run_id: str,
) -> tuple[dict[str, object], bool]:
    surface_state = _load_run_surface_state_from_vei_api(workspace_root, run_id)
    if surface_state is None:
        return empty_surface_summary(), False
    return summarize_surface_state(surface_state), True


def load_snapshot_diff_summary(
    *,
    workspace_root: Path,
    run_id: str,
    snapshot_from: int,
    snapshot_to: int,
) -> dict[str, object] | None:
    diff_payload = _load_snapshot_diff_from_vei_api(
        workspace_root=workspace_root,
        run_id=run_id,
        snapshot_from=snapshot_from,
        snapshot_to=snapshot_to,
    )
    if diff_payload is None:
        return None
    return summarize_snapshot_diff(diff_payload)


def summarize_surface_state(surface_state: object) -> dict[str, object]:
    payload = _as_json_dict(surface_state)
    if not payload:
        return empty_surface_summary()

    panels = payload.get("panels", [])
    if not isinstance(panels, list):
        panels = []

    status_counts = Counter(
        str(panel.get("status", "ok"))
        for panel in panels
        if isinstance(panel, dict) and panel.get("status")
    )
    item_count = 0
    panel_titles: list[str] = []
    for panel in panels:
        if not isinstance(panel, dict):
            continue
        items = panel.get("items", [])
        if isinstance(items, list):
            item_count += len(items)
        title = str(panel.get("title", "")).strip()
        if title:
            panel_titles.append(title)

    return {
        "company_name": str(payload.get("company_name", "")).strip(),
        "vertical_name": str(payload.get("vertical_name", "")).strip(),
        "current_tension": str(payload.get("current_tension", "")).strip(),
        "panel_count": len(panels),
        "item_count": item_count,
        "ok_count": status_counts.get("ok", 0),
        "attention_count": status_counts.get("attention", 0),
        "warning_count": status_counts.get("warning", 0),
        "critical_count": status_counts.get("critical", 0),
        "panel_titles": panel_titles[:6],
    }


def summarize_snapshot_diff(
    diff_payload: dict[str, Any],
    *,
    max_highlights: int = 6,
) -> dict[str, object]:
    added = diff_payload.get("added", {})
    removed = diff_payload.get("removed", {})
    changed = diff_payload.get("changed", {})

    added = added if isinstance(added, dict) else {}
    removed = removed if isinstance(removed, dict) else {}
    changed = changed if isinstance(changed, dict) else {}

    highlights: list[str] = []
    for key in sorted(changed):
        if len(highlights) == max_highlights:
            break
        change = changed[key]
        if not isinstance(change, dict):
            continue
        highlights.append(
            f"{key}: {_format_compact_value(change.get('from'))} -> "
            f"{_format_compact_value(change.get('to'))}"
        )

    for key in sorted(added):
        if len(highlights) == max_highlights:
            break
        highlights.append(f"{key}: added {_format_compact_value(added[key])}")

    for key in sorted(removed):
        if len(highlights) == max_highlights:
            break
        highlights.append(f"{key}: removed {_format_compact_value(removed[key])}")

    return {
        "added_count": len(added),
        "removed_count": len(removed),
        "changed_count": len(changed),
        "top_changes": highlights,
    }


def empty_surface_summary() -> dict[str, object]:
    return {
        "company_name": "",
        "vertical_name": "",
        "current_tension": "",
        "panel_count": 0,
        "item_count": 0,
        "ok_count": 0,
        "attention_count": 0,
        "warning_count": 0,
        "critical_count": 0,
        "panel_titles": [],
    }


def _load_run_timeline_from_vei_api(
    workspace_root: Path,
    run_id: str,
) -> list[dict[str, Any]] | None:
    load_run_events = _import_vei_callable(
        ("vei.run.api", "load_run_events_for_run"),
        ("vei.sdk", "load_run_events_entry"),
        ("vei.sdk.api", "load_run_events_entry"),
    )
    if load_run_events is None:
        return None

    try:
        events = load_run_events(workspace_root, run_id)
    except Exception:
        return None
    return [_as_json_dict(event) for event in events]


def _load_run_snapshots_from_vei_api(
    workspace_root: Path,
    run_id: str,
) -> list[dict[str, Any]] | None:
    list_run_snapshots = _import_vei_callable(
        ("vei.run.api", "list_run_snapshots"),
        ("vei.sdk", "list_run_snapshots_entry"),
        ("vei.sdk.api", "list_run_snapshots_entry"),
    )
    if list_run_snapshots is None:
        return None
    load_run_snapshot_payload = _import_vei_callable(
        ("vei.run.api", "load_run_snapshot_payload"),
    )

    try:
        refs = list_run_snapshots(workspace_root, run_id)
    except Exception:
        return None
    if not refs:
        return []

    snapshots: list[dict[str, Any]] = []
    for ref in refs:
        try:
            payload = _load_snapshot_payload_from_ref(
                workspace_root=workspace_root,
                run_id=run_id,
                ref=ref,
                load_run_snapshot_payload=load_run_snapshot_payload,
            )
        except Exception:
            return None
        if not isinstance(payload, dict):
            continue
        snapshot = dict(payload)
        snapshot.setdefault("snapshot_id", int(ref.snapshot_id))
        snapshot.setdefault("branch", str(ref.branch))
        snapshot.setdefault("label", ref.label)
        snapshot.setdefault("clock_ms", int(snapshot.get("clock_ms", ref.time_ms) or ref.time_ms))
        snapshots.append(snapshot)

    snapshots.sort(key=lambda item: int(item.get("clock_ms", item.get("time_ms", 0)) or 0))
    return snapshots


def _load_run_surface_state_from_vei_api(
    workspace_root: Path,
    run_id: str,
) -> object | None:
    get_run_surface_state = _import_vei_callable(
        ("vei.run.api", "get_run_surface_state"),
    )
    if get_run_surface_state is None:
        return None

    try:
        return get_run_surface_state(workspace_root, run_id)
    except Exception:
        return None


def _load_snapshot_diff_from_vei_api(
    *,
    workspace_root: Path,
    run_id: str,
    snapshot_from: int,
    snapshot_to: int,
) -> dict[str, Any] | None:
    diff_run_snapshots = _import_vei_callable(
        ("vei.run.api", "diff_run_snapshots"),
        ("vei.sdk", "diff_run_snapshots_entry"),
        ("vei.sdk.api", "diff_run_snapshots_entry"),
    )
    if diff_run_snapshots is None:
        return None

    try:
        diff_payload = diff_run_snapshots(
            workspace_root,
            run_id,
            int(snapshot_from),
            int(snapshot_to),
        )
    except TypeError:
        try:
            diff_payload = diff_run_snapshots(
                workspace_root,
                run_id,
                snapshot_from=int(snapshot_from),
                snapshot_to=int(snapshot_to),
            )
        except Exception:
            return None
    except Exception:
        return None
    return diff_payload if isinstance(diff_payload, dict) else None


def _import_vei_callable(*candidates: tuple[str, str]) -> Any | None:
    for module_name, attribute_name in candidates:
        try:
            module = importlib.import_module(module_name)
            candidate = getattr(module, attribute_name)
        except (ImportError, AttributeError):
            continue
        if callable(candidate):
            return candidate
    return None


def _load_snapshot_payload_from_ref(
    *,
    workspace_root: Path,
    run_id: str,
    ref: object,
    load_run_snapshot_payload: Any | None,
) -> dict[str, Any]:
    snapshot_id = int(ref.snapshot_id)  # type: ignore[attr-defined]
    if callable(load_run_snapshot_payload):
        payload = load_run_snapshot_payload(workspace_root, run_id, snapshot_id)
        return payload if isinstance(payload, dict) else {}

    ref_path = getattr(ref, "path", None)
    if ref_path is None:
        raise ValueError("snapshot ref path not available")
    snapshot_path = Path(str(ref_path))
    if not snapshot_path.is_absolute():
        snapshot_path = workspace_root / snapshot_path
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _load_events_from_path(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            records.append(json.loads(line))
    return records


def _load_snapshots_from_path(run_dir: Path) -> list[dict[str, Any]]:
    snapshot_paths = sorted((run_dir / "state").rglob("snapshots/*.json"))
    snapshots: list[dict[str, Any]] = []
    for index, path in enumerate(snapshot_paths, start=1):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            continue
        snapshot = dict(payload)
        snapshot.setdefault("snapshot_id", int(snapshot.get("index", index) or index))
        snapshot.setdefault("label", snapshot.get("label"))
        snapshots.append(snapshot)

    snapshots.sort(key=lambda item: int(item.get("clock_ms", item.get("time_ms", 0)) or 0))
    return snapshots


def _as_json_dict(value: object) -> dict[str, Any]:
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        payload = model_dump(mode="json")
        return payload if isinstance(payload, dict) else {}
    if isinstance(value, dict):
        return value
    return {}


def _format_compact_value(value: object, *, limit: int = 72) -> str:
    if isinstance(value, str):
        text = value
    elif isinstance(value, (int, float, bool)) or value is None:
        text = json.dumps(value)
    else:
        text = json.dumps(value, sort_keys=True)

    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."
