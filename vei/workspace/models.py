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
    "import_package",
]

WorkspaceRunStatus = Literal["queued", "running", "ok", "error"]


class WorkspaceSourceConfig(BaseModel):
    source_id: str
    connector: str
    config_path: str
    connector_mode: Literal["live"] = "live"
    created_at: str
    updated_at: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WorkspaceSourceSyncRecord(BaseModel):
    source_id: str
    connector: str
    synced_at: str
    status: Literal["ok", "error"] = "ok"
    package_path: str
    message: Optional[str] = None
    record_counts: Dict[str, int] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


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
    hidden_faults: Dict[str, Any] = Field(default_factory=dict)
    actor_hints: List[str] = Field(default_factory=list)
    contract_overrides: Dict[str, Any] = Field(default_factory=dict)
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
    imports_dir: str = "imports"
    import_package_path: Optional[str] = None
    normalization_report_path: Optional[str] = None
    provenance_path: Optional[str] = None
    redaction_report_path: Optional[str] = None
    generated_scenarios_path: Optional[str] = None
    source_registry_path: Optional[str] = "imports/source_registry.json"
    source_sync_history_path: Optional[str] = "imports/source_sync_history.json"
    compiled_root: str = "compiled"
    scenarios_dir: str = "scenarios"
    contracts_dir: str = "contracts"
    runs_dir: str = "runs"
    runs_index_path: str = "runs/index.json"
    active_scenario: str = "default"
    scenarios: List[WorkspaceScenarioSpec] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WorkspaceImportSummary(BaseModel):
    package_name: str
    source_count: int = 0
    connected_source_count: int = 0
    source_sync_count: int = 0
    issue_count: int = 0
    warning_count: int = 0
    error_count: int = 0
    provenance_count: int = 0
    generated_scenario_count: int = 0
    normalized_counts: Dict[str, int] = Field(default_factory=dict)
    origin_counts: Dict[str, int] = Field(default_factory=dict)


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
    imports: Optional[WorkspaceImportSummary] = None
