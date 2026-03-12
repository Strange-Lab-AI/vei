from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


WorkspaceSourceKind = Literal[
    "example",
    "family",
    "scenario",
    "grounding_bundle",
    "blueprint_asset",
    "compiled_blueprint",
]

WorkspaceRunStatus = Literal["queued", "running", "ok", "error"]


class WorkspaceScenarioSpec(BaseModel):
    name: str
    title: str
    description: str
    scenario_name: Optional[str] = None
    workflow_name: Optional[str] = None
    workflow_variant: Optional[str] = None
    workflow_parameters: Dict[str, Any] = Field(default_factory=dict)
    contract_path: Optional[str] = None
    inspection_focus: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WorkspaceRunEntry(BaseModel):
    run_id: str
    scenario_name: str
    runner: str
    status: WorkspaceRunStatus
    manifest_path: str
    started_at: str
    completed_at: Optional[str] = None
    success: Optional[bool] = None
    branch: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WorkspaceManifest(BaseModel):
    version: Literal["1"] = "1"
    name: str
    title: str
    description: str
    created_at: str
    source_kind: WorkspaceSourceKind
    source_ref: Optional[str] = None
    blueprint_asset_path: str = "sources/blueprint_asset.json"
    grounding_bundle_path: Optional[str] = None
    compiled_root: str = "compiled"
    scenarios_dir: str = "scenarios"
    contracts_dir: str = "contracts"
    runs_dir: str = "runs"
    runs_index_path: str = "runs/index.json"
    active_scenario: str = "default"
    scenarios: List[WorkspaceScenarioSpec] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WorkspaceCompileRecord(BaseModel):
    scenario_name: str
    compiled_blueprint_path: str
    contract_path: str
    scenario_seed_path: str
    contract_bootstrapped: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WorkspaceSummary(BaseModel):
    manifest: WorkspaceManifest
    compiled_scenarios: List[WorkspaceCompileRecord] = Field(default_factory=list)
    run_count: int = 0
    latest_run_id: Optional[str] = None
