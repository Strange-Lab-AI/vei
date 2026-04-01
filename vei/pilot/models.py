from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from vei.twin.models import (
    CompatibilitySurfaceSpec,
    ExternalAgentIdentity,
    TwinArchetype,
)

PilotServiceName = Literal["gateway", "studio"]
PilotServiceState = Literal["running", "stopped", "error"]


class PilotSnippet(BaseModel):
    name: str
    title: str
    language: str = "bash"
    content: str


class PilotServiceRecord(BaseModel):
    name: PilotServiceName
    host: str
    port: int
    url: str
    pid: int | None = None
    state: PilotServiceState = "stopped"
    log_path: str | None = None


class PilotManifest(BaseModel):
    version: Literal["1"] = "1"
    workspace_root: Path
    workspace_name: str
    organization_name: str
    organization_domain: str = ""
    archetype: TwinArchetype
    crisis_name: str
    studio_url: str
    pilot_console_url: str
    gateway_url: str
    gateway_status_url: str
    bearer_token: str
    supported_surfaces: list[CompatibilitySurfaceSpec] = Field(default_factory=list)
    recommended_first_exercise: str
    sample_client_path: str
    snippets: list[PilotSnippet] = Field(default_factory=list)


class PilotRuntime(BaseModel):
    version: Literal["1"] = "1"
    workspace_root: Path
    services: list[PilotServiceRecord] = Field(default_factory=list)
    started_at: str = ""
    updated_at: str = ""


class PilotActivityItem(BaseModel):
    label: str
    channel: str
    tool: str | None = None
    status: str | None = None
    object_refs: list[str] = Field(default_factory=list)
    agent_name: str | None = None
    agent_role: str | None = None
    agent_team: str | None = None
    agent_source: str | None = None


class PilotOutcomeSummary(BaseModel):
    status: str
    contract_ok: bool | None = None
    issue_count: int = 0
    summary: str
    latest_tool: str | None = None
    current_tension: str = ""
    affected_surfaces: list[str] = Field(default_factory=list)


class PilotStatus(BaseModel):
    manifest: PilotManifest
    runtime: PilotRuntime
    active_run: str | None = None
    twin_status: str = "stopped"
    request_count: int = 0
    services_ready: bool = False
    active_agents: list[ExternalAgentIdentity] = Field(default_factory=list)
    activity: list[PilotActivityItem] = Field(default_factory=list)
    outcome: PilotOutcomeSummary
