from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, Field

from vei.orchestrators.api import (
    OrchestratorConfig,
    OrchestratorSnapshot,
    OrchestratorSyncHealth,
)
from vei.orchestrators.models import ActivityItemBase

TwinArchetype = Literal[
    "b2b_saas",
    "digital_marketing_agency",
    "real_estate_management",
    "storage_solutions",
    "service_ops",
]
GatewaySurfaceName = Literal["slack", "jira", "graph", "salesforce"]
TwinRuntimeStatusValue = Literal["running", "completed", "error"]
TwinDensityLevel = Literal["small", "medium", "large"]
TwinCrisisLevel = Literal["calm", "escalated", "adversarial"]
TwinRedactionMode = Literal["preserve", "mask"]
TwinSyntheticExpansionStrength = Literal["light", "medium", "strong"]
TwinNamedTeamExpansion = Literal["minimal", "standard", "expanded"]
TwinIncludedSurface = Literal[
    "slack",
    "mail",
    "tickets",
    "docs",
    "identity",
    "crm",
    "calendar",
    "approvals",
    "vertical",
]
TwinServiceName = Literal["gateway", "studio"]
TwinServiceState = Literal["running", "stopped", "error"]


class ContextMoldConfig(BaseModel):
    archetype: TwinArchetype = "b2b_saas"
    expansion_level: Literal["light", "medium"] = "medium"
    density_level: TwinDensityLevel = "medium"
    named_team_expansion: TwinNamedTeamExpansion = "standard"
    crisis_family: str | None = None
    included_surfaces: list[TwinIncludedSurface] = Field(default_factory=list)
    redaction_mode: TwinRedactionMode = "preserve"
    synthetic_expansion_strength: TwinSyntheticExpansionStrength = "medium"
    scenario_variant: str | None = None
    contract_variant: str | None = None


class CompatibilitySurfaceSpec(BaseModel):
    name: GatewaySurfaceName
    title: str
    base_path: str
    auth_style: Literal["bearer"] = "bearer"


class TwinGatewayConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 3020
    auth_token: str
    surfaces: list[CompatibilitySurfaceSpec] = Field(default_factory=list)
    ui_command: str | None = None


class ExternalAgentIdentity(BaseModel):
    agent_id: str | None = None
    name: str | None = None
    role: str | None = None
    team: str | None = None
    source: str | None = None


class CustomerTwinBundle(BaseModel):
    version: Literal["1"] = "1"
    workspace_root: Path
    workspace_name: str
    organization_name: str
    organization_domain: str = ""
    mold: ContextMoldConfig
    context_snapshot_path: str
    blueprint_asset_path: str
    gateway: TwinGatewayConfig
    summary: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class TwinRuntimeStatus(BaseModel):
    run_id: str
    branch_name: str
    status: TwinRuntimeStatusValue = "running"
    started_at: str
    completed_at: str | None = None
    latest_snapshot_id: int | None = None
    latest_contract_ok: bool | None = None
    contract_issue_count: int = 0
    request_count: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class TwinTemplateSpec(BaseModel):
    organization_name: str
    organization_domain: str = ""
    source_snapshot_path: str | None = None
    source_workspace_roots: list[str] = Field(default_factory=list)
    archetypes: list[TwinArchetype] = Field(default_factory=list)
    density_levels: list[TwinDensityLevel] = Field(default_factory=list)
    crisis_levels: list[TwinCrisisLevel] = Field(default_factory=list)
    seeds: list[int] = Field(default_factory=list)


class TwinVariantSpec(BaseModel):
    variant_id: str
    workspace_root: Path
    organization_name: str
    organization_domain: str = ""
    archetype: TwinArchetype
    density_level: TwinDensityLevel = "medium"
    crisis_level: TwinCrisisLevel = "escalated"
    seed: int = 42042
    mold: ContextMoldConfig
    scenario_variant: str | None = None
    contract_variant: str | None = None


class TwinMatrixBundle(BaseModel):
    version: Literal["1"] = "1"
    output_root: Path
    template: TwinTemplateSpec
    variants: list[TwinVariantSpec] = Field(default_factory=list)
    generated_at: str = ""


class WorkspaceGovernorStatus(BaseModel):
    governor: dict[str, Any] = Field(default_factory=dict)
    workforce: dict[str, Any] = Field(default_factory=dict)
    manifest: dict[str, Any] = Field(default_factory=dict)
    runtime: dict[str, Any] = Field(default_factory=dict)
    active_run: str | None = None
    twin_status: str = "stopped"
    request_count: int = 0
    services_ready: bool = False
    active_agents: list[dict[str, Any]] = Field(default_factory=list)
    activity: list[dict[str, Any]] = Field(default_factory=list)
    outcome: dict[str, Any] = Field(default_factory=dict)
    orchestrator: dict[str, Any] | None = None
    orchestrator_sync: dict[str, Any] | None = None
    exercise: dict[str, Any] = Field(default_factory=dict)


class TwinLaunchSnippet(BaseModel):
    name: str
    title: str
    language: str = "bash"
    content: str


class TwinServiceRecord(BaseModel):
    name: TwinServiceName
    host: str
    port: int
    url: str
    pid: int | None = None
    state: TwinServiceState = "stopped"
    log_path: str | None = None


class TwinLaunchManifest(BaseModel):
    version: Literal["1"] = "1"
    workspace_root: Path
    workspace_name: str
    organization_name: str
    organization_domain: str = ""
    archetype: TwinArchetype
    crisis_name: str
    studio_url: str
    control_room_url: str = Field(
        validation_alias=AliasChoices("control_room_url", "pilot_console_url")
    )
    gateway_url: str
    gateway_status_url: str
    bearer_token: str
    supported_surfaces: list[CompatibilitySurfaceSpec] = Field(default_factory=list)
    recommended_first_move: str = Field(
        default="",
        validation_alias=AliasChoices(
            "recommended_first_move",
            "recommended_first_exercise",
        ),
    )
    sample_client_path: str
    snippets: list[TwinLaunchSnippet] = Field(default_factory=list)
    orchestrator: OrchestratorConfig | None = None


class TwinLaunchRuntime(BaseModel):
    version: Literal["1"] = "1"
    workspace_root: Path
    services: list[TwinServiceRecord] = Field(default_factory=list)
    started_at: str = ""
    updated_at: str = ""


class TwinActivityItem(ActivityItemBase):
    channel: str
    tool: str | None = None
    timestamp: str | None = None
    source_label: str | None = None
    agent_role: str | None = None
    agent_team: str | None = None
    agent_source: str | None = None


class TwinOutcomeSummary(BaseModel):
    status: str
    contract_ok: bool | None = None
    issue_count: int = 0
    summary: str
    latest_tool: str | None = None
    current_tension: str = ""
    affected_surfaces: list[str] = Field(default_factory=list)
    vei_action_count: int = 0
    downstream_response_count: int = 0
    governance_active: bool = False
    direction: Literal["improving", "stable", "declining", "unknown"] = "unknown"


class TwinLaunchStatus(BaseModel):
    manifest: TwinLaunchManifest
    runtime: TwinLaunchRuntime
    active_run: str | None = None
    twin_status: str = "stopped"
    request_count: int = 0
    services_ready: bool = False
    active_agents: list[ExternalAgentIdentity] = Field(default_factory=list)
    activity: list[TwinActivityItem] = Field(default_factory=list)
    outcome: TwinOutcomeSummary
    orchestrator: OrchestratorSnapshot | None = None
    orchestrator_sync: OrchestratorSyncHealth | None = None
