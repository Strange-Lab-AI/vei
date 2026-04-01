from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

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
