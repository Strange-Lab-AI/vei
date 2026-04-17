from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from vei.blueprint import get_facade_plugin, list_runtime_facade_plugins
from vei.orientation.api import build_world_orientation
from vei.verticals import get_vertical_pack_manifest
from vei.world.models import WorldState
from vei.workspace.api import load_workspace

from ._surface_panels_business import build_workforce_panel
from .models import LivingSurfacePanel, LivingSurfaceState, RunManifest, RunSnapshotRef


def build_surface_state(
    *,
    workspace_root: Path,
    run_id: str,
    state: WorldState,
    run_manifest: RunManifest,
    snapshots: list[RunSnapshotRef],
) -> LivingSurfaceState:
    workspace = load_workspace(workspace_root)
    orientation = build_world_orientation(state)
    metadata = _scenario_metadata(state)
    vertical_name = _vertical_name(metadata, workspace)
    vertical_runtime_family = _vertical_runtime_family(vertical_name)
    company_name = orientation.organization_name or workspace.title or workspace.name
    current_tension = _current_tension(metadata, state, orientation)
    panel_context = {
        "run_manifest": run_manifest,
        "state": state,
        "vertical_name": vertical_name,
        "vertical_runtime_family": vertical_runtime_family,
        "workspace": workspace,
    }

    panels: list[LivingSurfacePanel] = []
    seen_surfaces: set[str] = set()
    for plugin in list_runtime_facade_plugins():
        builder = plugin.studio_panel_builder
        if builder is None:
            continue
        panel = builder(state.components, panel_context)
        if panel is not None and panel.surface == "knowledge":
            continue
        if panel is None or panel.surface in seen_surfaces:
            continue
        panels.append(panel)
        seen_surfaces.add(panel.surface)

    workforce_panel = build_workforce_panel(state.components.get("workforce", {}))
    if workforce_panel is not None and workforce_panel.surface not in seen_surfaces:
        panels.append(workforce_panel)

    return LivingSurfaceState(
        company_name=company_name,
        vertical_name=vertical_name,
        run_id=run_id,
        branch=run_manifest.branch or state.branch,
        snapshot_id=(snapshots[-1].snapshot_id if snapshots else 0),
        seed=run_manifest.seed,
        run_identity=_run_identity(run_manifest),
        current_tension=current_tension,
        panels=panels,
    )


def _scenario_metadata(state: WorldState) -> Dict[str, Any]:
    metadata = state.scenario.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _builder_env(metadata: Dict[str, Any]) -> Dict[str, Any]:
    env = metadata.get("builder_environment")
    return env if isinstance(env, dict) else {}


def _builder_graphs_meta(metadata: Dict[str, Any]) -> Dict[str, Any]:
    graphs = metadata.get("builder_capability_graphs")
    if isinstance(graphs, dict):
        inner = graphs.get("metadata")
        return inner if isinstance(inner, dict) else graphs
    return {}


def _vertical_name(metadata: Dict[str, Any], workspace: Any) -> str:
    for source in (_builder_env(metadata), _builder_graphs_meta(metadata)):
        vertical = source.get("vertical")
        if isinstance(vertical, str) and vertical:
            return vertical
    if isinstance(getattr(workspace, "source_ref", None), str) and workspace.source_ref:
        return workspace.source_ref
    return "workspace"


def _current_tension(
    metadata: Dict[str, Any], state: WorldState, orientation: Any
) -> str:
    for source in (_builder_env(metadata), _builder_graphs_meta(metadata)):
        brief = source.get("scenario_brief")
        if isinstance(brief, str) and brief:
            return brief
    description = state.scenario.get("description")
    if isinstance(description, str) and description:
        return description
    return orientation.summary


def _vertical_runtime_family(vertical_name: str) -> str:
    try:
        manifest = get_vertical_pack_manifest(vertical_name)
    except KeyError:
        return ""
    return str(manifest.runtime_family or "").strip().lower()


def _run_identity(run_manifest: RunManifest) -> str:
    reproducibility = run_manifest.metadata.get("reproducibility")
    if isinstance(reproducibility, dict):
        record_id = reproducibility.get("record_id")
        if isinstance(record_id, str) and record_id:
            return record_id
    return run_manifest.run_id


def build_facade_panel(
    facade_name: str,
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> LivingSurfacePanel | None:
    plugin = get_facade_plugin(facade_name)
    builder = plugin.studio_panel_builder
    if builder is None:
        return None
    return builder(components, context)
