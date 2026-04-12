from __future__ import annotations

from importlib import import_module

__all__ = [
    "RunArtifactIndex",
    "RunContractSummary",
    "RunManifest",
    "RunSnapshotRef",
    "RunTimelineEvent",
    "LivingSurfaceItem",
    "LivingSurfacePanel",
    "LivingSurfaceState",
    "build_facade_panel",
    "build_reproducibility_record",
    "build_run_timeline",
    "evaluate_run_workspace_contract",
    "generate_run_id",
    "get_run_capability_graphs",
    "get_run_orientation",
    "get_run_surface_state",
    "launch_workspace_run",
    "append_run_event",
    "append_run_events",
    "load_run_events",
    "load_run_events_for_run",
    "load_run_contract_evaluation",
    "list_run_manifests",
    "list_run_snapshots",
    "load_run_manifest",
    "load_run_timeline",
    "merge_reproducibility_metadata",
    "normalize_runner",
    "write_run_manifest",
    "write_run_events",
    "write_run_timeline",
]


def __getattr__(name: str):
    if name in {
        "build_facade_panel",
        "build_run_timeline",
        "evaluate_run_workspace_contract",
        "generate_run_id",
        "get_run_capability_graphs",
        "get_run_orientation",
        "get_run_surface_state",
        "launch_workspace_run",
        "load_run_events_for_run",
        "load_run_contract_evaluation",
        "list_run_manifests",
        "list_run_snapshots",
        "load_run_manifest",
        "load_run_timeline",
        "normalize_runner",
        "write_run_manifest",
        "write_run_timeline",
    }:
        module = import_module("vei.run.api")
        return getattr(module, name)
    if name in {
        "append_run_event",
        "append_run_events",
        "load_run_events",
        "write_run_events",
    }:
        module = import_module("vei.run.events")
        return getattr(module, name)
    if name in {
        "build_reproducibility_record",
        "merge_reproducibility_metadata",
    }:
        module = import_module("vei.run.reproducibility")
        return getattr(module, name)
    if name in {
        "LivingSurfaceItem",
        "LivingSurfacePanel",
        "LivingSurfaceState",
        "RunArtifactIndex",
        "RunContractSummary",
        "RunManifest",
        "RunSnapshotRef",
        "RunTimelineEvent",
    }:
        module = import_module("vei.run.models")
        return getattr(module, name)
    raise AttributeError(name)
