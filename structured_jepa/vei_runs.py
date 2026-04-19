from __future__ import annotations

import json
import logging
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

from .storage import PreparedDataset, finalize_processed_dataset
from .utils import make_split_map, normalize_token
from .vei_support import (
    empty_surface_summary,
    load_run_snapshots,
    load_run_surface_summary,
    load_run_timeline,
)
from .vei_surface_features import summarize_snapshot_surface_features

logger = logging.getLogger(__name__)


def prepare_vei_runs_dataset(
    *,
    workspace_root: str | Path,
    output_dir: str | Path,
    run_ids: list[str] | None = None,
    seed: int = 7,
    train_fraction: float = 0.7,
    val_fraction: float = 0.15,
) -> PreparedDataset:
    workspace = Path(workspace_root).expanduser().resolve()
    runs_root = workspace / "runs"
    if not runs_root.exists():
        raise ValueError(f"VEI runs directory not found: {runs_root}")

    selected_run_ids = run_ids or sorted(path.name for path in runs_root.iterdir() if path.is_dir())
    logger.info(
        "vei_runs_preparation_start",
        extra={"workspace": str(workspace), "run_count": len(selected_run_ids)},
    )
    rows: list[dict[str, object]] = []
    used_public_snapshot_api = False
    used_public_timeline_api = False
    used_public_surface_api = False
    metadata_columns = [
        "meta__runner",
        "meta__scenario_name",
        "meta__branch",
        "meta__snapshot_id",
        "meta__snapshot_label",
        "meta__mission_name",
        "meta__objective_variant",
        "meta__scorecard_overall_score",
        "meta__branch_label_count",
        "meta__surface_company_name",
        "meta__surface_vertical_name",
        "meta__surface_current_tension",
        "meta__surface_panel_titles",
        "meta__surface_panel_count",
        "meta__surface_item_count",
        "meta__surface_ok_count",
        "meta__surface_attention_count",
        "meta__surface_warning_count",
        "meta__surface_critical_count",
    ]

    for run_id in selected_run_ids:
        run_dir = runs_root / run_id
        manifest_path = run_dir / "run_manifest.json"
        if not manifest_path.exists():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        timeline, timeline_used_public_api = load_run_timeline(
            workspace_root=workspace,
            run_id=run_id,
            fallback_run_dir=run_dir,
        )
        snapshots, snapshots_used_public_api = load_run_snapshots(
            workspace_root=workspace,
            run_id=run_id,
            fallback_run_dir=run_dir,
        )
        if not snapshots:
            continue
        surface_summary, surface_used_public_api = load_run_surface_summary(
            workspace_root=workspace,
            run_id=run_id,
        )
        if not surface_summary:
            surface_summary = empty_surface_summary()
        used_public_timeline_api = used_public_timeline_api or timeline_used_public_api
        used_public_snapshot_api = used_public_snapshot_api or snapshots_used_public_api
        used_public_surface_api = used_public_surface_api or surface_used_public_api

        mission_state = _load_mission_state(run_dir)
        snapshot_times = [
            int(snapshot.get("clock_ms", snapshot.get("time_ms", 0)) or 0) for snapshot in snapshots
        ]

        for index, snapshot in enumerate(snapshots):
            snapshot_time = snapshot_times[index]
            next_time = (
                snapshot_times[index + 1] if index + 1 < len(snapshot_times) else snapshot_time
            )
            interval_events = [
                event
                for event in timeline
                if snapshot_time < int(event.get("time_ms", 0) or 0) <= next_time
            ]
            aligned_action = _last_action_event(interval_events)

            state_payload = snapshot.get("data", {})
            graphs_summary, orientation_summary = _summarize_snapshot_state(state_payload)
            pending_summary = _summarize_pending_events(state_payload)
            interval_summary = _summarize_interval_events(interval_events)
            surface_numeric_summary, surface_categorical_summary = (
                summarize_snapshot_surface_features(state_payload)
            )

            row: dict[str, object] = {
                "episode_id": run_id,
                "step_idx": index,
                "timestamp": str(snapshot_time),
                "delta_t_s": 0.0
                if index == 0
                else max(0.0, (snapshot_time - snapshot_times[index - 1]) / 1000.0),
                "done": index == len(snapshots) - 1,
                "split": "train",
                "action_name": _action_name(aligned_action),
                "meta__runner": str(manifest.get("runner", "")),
                "meta__scenario_name": str(manifest.get("scenario_name", "")),
                "meta__branch": str(manifest.get("branch", "")),
                "meta__snapshot_id": int(snapshot.get("snapshot_id", index + 1) or index + 1),
                "meta__snapshot_label": str(snapshot.get("label") or ""),
                "meta__mission_name": str(mission_state.get("mission_name", "")),
                "meta__objective_variant": str(mission_state.get("objective_variant", "")),
                "meta__scorecard_overall_score": float(
                    mission_state.get("scorecard_overall_score", 0.0)
                ),
                "meta__branch_label_count": int(mission_state.get("branch_label_count", 0)),
                "meta__surface_company_name": _surface_text(surface_summary, "company_name"),
                "meta__surface_vertical_name": _surface_text(surface_summary, "vertical_name"),
                "meta__surface_current_tension": _surface_text(surface_summary, "current_tension"),
                "meta__surface_panel_titles": _surface_panel_titles(surface_summary),
                "meta__surface_panel_count": _surface_count(surface_summary, "panel_count"),
                "meta__surface_item_count": _surface_count(surface_summary, "item_count"),
                "meta__surface_ok_count": _surface_count(surface_summary, "ok_count"),
                "meta__surface_attention_count": _surface_count(surface_summary, "attention_count"),
                "meta__surface_warning_count": _surface_count(surface_summary, "warning_count"),
                "meta__surface_critical_count": _surface_count(surface_summary, "critical_count"),
                "clock_time_ms": float(snapshot_time),
            }
            row.update(pending_summary)
            row.update(graphs_summary)
            row.update(orientation_summary)
            row.update(interval_summary)
            row.update(surface_numeric_summary)
            row.update(surface_categorical_summary)
            row.update(_action_features(aligned_action))
            rows.append(row)

    if not rows:
        raise ValueError("no VEI runs produced usable snapshot rows")

    steps = pd.DataFrame(rows)
    split_map = make_split_map(
        steps["episode_id"].tolist(),
        seed=seed,
        train_fraction=train_fraction,
        val_fraction=val_fraction,
    )
    steps["split"] = steps["episode_id"].map(split_map)

    observation_numeric_columns = [
        "delta_t_s",
        "clock_time_ms",
        *sorted(column for column in steps.columns if column.startswith("pending_total__")),
        *sorted(column for column in steps.columns if column.startswith("graph_count__")),
        *sorted(column for column in steps.columns if column.startswith("graph_status__")),
        *sorted(column for column in steps.columns if column.startswith("orientation_count__")),
        *sorted(column for column in steps.columns if column.startswith("surface_panel_count__")),
        *sorted(column for column in steps.columns if column.startswith("surface_item_count__")),
        *sorted(column for column in steps.columns if column.startswith("surface_status__")),
        *sorted(column for column in steps.columns if column.startswith("surface_signal__")),
        *sorted(column for column in steps.columns if column.startswith("interval_event_count__")),
        *sorted(
            column for column in steps.columns if column.startswith("interval_object_ref_count__")
        ),
    ]
    observation_categorical_columns = [
        "interval_last_resolved_tool",
        "interval_last_graph_domain",
        "interval_last_graph_action",
        "surface_primary_panel",
        "surface_primary_status",
    ]
    action_numeric_columns = [
        "action_object_ref_count_total",
        "action_interval_event_count",
    ]
    action_categorical_columns = [
        "action_tool",
        "action_graph_domain",
        "action_graph_action",
    ]

    logger.info(
        "vei_runs_preparation_complete",
        extra={"rows": len(steps), "episodes": steps["episode_id"].nunique()},
    )
    return finalize_processed_dataset(
        raw_steps=steps,
        output_dir=output_dir,
        dataset_kind="vei_runs",
        observation_numeric_columns=observation_numeric_columns,
        observation_categorical_columns=observation_categorical_columns,
        action_numeric_columns=action_numeric_columns,
        action_categorical_columns=action_categorical_columns,
        auxiliary_numeric_targets=[],
        metadata_columns=metadata_columns,
        notes={
            "run_count": steps["episode_id"].nunique(),
            "workspace_root": str(workspace),
            "vei_public_snapshot_api_used": used_public_snapshot_api,
            "vei_public_timeline_api_used": used_public_timeline_api,
            "vei_public_surface_api_used": used_public_surface_api,
        },
    )


def _load_mission_state(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "mission_state.json"
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    mission = payload.get("mission", {}) if isinstance(payload, dict) else {}
    scorecard = payload.get("scorecard", {}) if isinstance(payload, dict) else {}
    return {
        "mission_name": mission.get("mission_name", ""),
        "objective_variant": payload.get("objective_variant", ""),
        "scorecard_overall_score": scorecard.get("overall_score", 0),
        "branch_label_count": len(mission.get("branch_labels", []))
        if isinstance(mission, dict)
        else 0,
    }


def _surface_text(surface_summary: dict[str, object], key: str) -> str:
    value = surface_summary.get(key, "")
    if value is None:
        return ""
    return str(value).strip()


def _surface_panel_titles(surface_summary: dict[str, object]) -> str:
    value = surface_summary.get("panel_titles", [])
    if not isinstance(value, list):
        return ""
    return ", ".join(str(title) for title in value if title is not None)


def _surface_count(surface_summary: dict[str, object], key: str) -> int:
    value = surface_summary.get(key, 0)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _last_action_event(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidates = [
        event
        for event in events
        if event.get("tool") or event.get("resolved_tool") or event.get("graph_action")
    ]
    return candidates[-1] if candidates else None


def _action_name(event: dict[str, Any] | None) -> str:
    if not event:
        return "__none__"
    return normalize_token(event.get("resolved_tool") or event.get("tool") or event.get("label"))


def _summarize_pending_events(state_payload: object) -> dict[str, float]:
    if not isinstance(state_payload, dict):
        return {"pending_total__all": 0.0}
    pending = state_payload.get("pending_events", [])
    if not isinstance(pending, list):
        return {"pending_total__all": 0.0}
    counts = Counter(
        str(item.get("target", "unknown")) for item in pending if isinstance(item, dict)
    )
    summary = {"pending_total__all": float(len(pending))}
    for key, count in counts.items():
        summary[f"pending_total__{key}"] = float(count)
    return summary


def _summarize_interval_events(events: list[dict[str, Any]]) -> dict[str, object]:
    kind_counts = Counter(str(event.get("kind", "unknown")) for event in events)
    object_ref_counts: Counter[str] = Counter()
    for event in events:
        for ref in event.get("object_refs", []) or []:
            prefix = str(ref).split(":", 1)[0]
            object_ref_counts[prefix] += 1
    last_event = _last_action_event(events)
    summary: dict[str, object] = {
        "interval_event_count__all": float(len(events)),
        "interval_object_ref_count__all": float(sum(object_ref_counts.values())),
        "interval_last_resolved_tool": normalize_token(
            None
            if last_event is None
            else last_event.get("resolved_tool") or last_event.get("tool")
        ),
        "interval_last_graph_domain": normalize_token(
            None if last_event is None else last_event.get("graph_domain")
        ),
        "interval_last_graph_action": normalize_token(
            None if last_event is None else last_event.get("graph_action")
        ),
    }
    for key, count in kind_counts.items():
        summary[f"interval_event_count__{key}"] = float(count)
    for key, count in object_ref_counts.items():
        summary[f"interval_object_ref_count__{key}"] = float(count)
    return summary


def _action_features(event: dict[str, Any] | None) -> dict[str, object]:
    if not event:
        return {
            "action_tool": "__none__",
            "action_graph_domain": "__none__",
            "action_graph_action": "__none__",
            "action_object_ref_count_total": 0.0,
            "action_interval_event_count": 0.0,
        }
    object_refs = event.get("object_refs", []) or []
    return {
        "action_tool": normalize_token(event.get("resolved_tool") or event.get("tool")),
        "action_graph_domain": normalize_token(event.get("graph_domain")),
        "action_graph_action": normalize_token(event.get("graph_action")),
        "action_object_ref_count_total": float(len(object_refs)),
        "action_interval_event_count": 1.0,
    }


def _summarize_snapshot_state(state_payload: object) -> tuple[dict[str, float], dict[str, float]]:
    graphs = _try_build_vei_graphs(state_payload)
    orientation = _try_build_vei_orientation(state_payload)
    if graphs is None:
        graphs = _fallback_graphs_summary(state_payload)
    if orientation is None:
        orientation = _fallback_orientation_summary(state_payload)
    return graphs, orientation


def _try_build_vei_graphs(state_payload: object) -> dict[str, float] | None:
    try:
        from vei.capability_graph.api import build_runtime_capability_graphs
        from vei.world.models import WorldState
    except Exception:
        return None

    try:
        state = WorldState.model_validate(state_payload)
        graphs = build_runtime_capability_graphs(state).model_dump(mode="json")
    except Exception:
        return None

    summary: dict[str, float] = {}
    for domain, payload in graphs.items():
        if not domain.endswith("_graph") or not isinstance(payload, dict):
            continue
        for key, value in payload.items():
            if isinstance(value, list):
                summary[f"graph_count__{domain}__{key}"] = float(len(value))
                status_counts = Counter(
                    normalize_token(item.get("status")).lower()
                    for item in value
                    if isinstance(item, dict) and item.get("status") is not None
                )
                for status, count in status_counts.items():
                    summary[f"graph_status__{domain}__{status}"] = float(count)
            elif isinstance(value, (int, float)):
                summary[f"graph_count__{domain}__{key}"] = float(value)
    return summary


def _try_build_vei_orientation(state_payload: object) -> dict[str, float] | None:
    try:
        from vei.orientation.api import build_world_orientation
        from vei.world.models import WorldState
    except Exception:
        return None

    try:
        state = WorldState.model_validate(state_payload)
        orientation = build_world_orientation(state).model_dump(mode="json")
    except Exception:
        return None

    summary: dict[str, float] = {}
    key_objects = orientation.get("key_objects", [])
    if not isinstance(key_objects, list):
        return summary
    for item in key_objects:
        if not isinstance(item, dict):
            continue
        domain = normalize_token(item.get("domain")).lower()
        kind = normalize_token(item.get("kind")).lower()
        status = normalize_token(item.get("status")).lower()
        summary[f"orientation_count__domain__{domain}"] = (
            summary.get(f"orientation_count__domain__{domain}", 0.0) + 1.0
        )
        summary[f"orientation_count__kind__{kind}"] = (
            summary.get(f"orientation_count__kind__{kind}", 0.0) + 1.0
        )
        summary[f"orientation_count__status__{status}"] = (
            summary.get(f"orientation_count__status__{status}", 0.0) + 1.0
        )
    return summary


def _fallback_graphs_summary(state_payload: object) -> dict[str, float]:
    if not isinstance(state_payload, dict):
        return {}
    components = state_payload.get("components", {})
    if not isinstance(components, dict):
        return {}
    summary: dict[str, float] = {}
    for component_name, payload in components.items():
        if not isinstance(payload, dict):
            continue
        for key, value in payload.items():
            if isinstance(value, dict):
                summary[f"graph_count__{component_name}__{key}"] = float(len(value))
                status_counts = Counter(
                    normalize_token(item.get("status")).lower()
                    for item in value.values()
                    if isinstance(item, dict) and item.get("status") is not None
                )
                for status, count in status_counts.items():
                    summary[f"graph_status__{component_name}__{status}"] = float(count)
    return summary


def _fallback_orientation_summary(state_payload: object) -> dict[str, float]:
    if not isinstance(state_payload, dict):
        return {}
    components = state_payload.get("components", {})
    if not isinstance(components, dict):
        return {}
    summary: dict[str, float] = {}
    for component_name, payload in components.items():
        if not isinstance(payload, dict):
            continue
        for key, value in payload.items():
            if isinstance(value, dict):
                summary[f"orientation_count__domain__{component_name}"] = summary.get(
                    f"orientation_count__domain__{component_name}", 0.0
                ) + float(len(value))
                summary[f"orientation_count__kind__{key}"] = summary.get(
                    f"orientation_count__kind__{key}", 0.0
                ) + float(len(value))
    return summary
