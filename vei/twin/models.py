from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field


TwinArchetype = Literal[
    "b2b_saas",
    "digital_marketing_agency",
    "real_estate_management",
    "storage_solutions",
]
GatewaySurfaceName = Literal["slack", "jira", "graph", "salesforce"]
TwinRuntimeStatusValue = Literal["running", "completed", "error"]


class ContextMoldConfig(BaseModel):
    archetype: TwinArchetype = "b2b_saas"
    expansion_level: Literal["light", "medium"] = "medium"
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
