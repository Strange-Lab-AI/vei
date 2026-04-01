from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

MirrorConnectorMode = Literal["sim", "live"]
MirrorAgentMode = Literal["proxy", "ingest", "demo"]
MirrorAgentStatus = Literal["registered", "active", "idle", "error"]
MirrorHandleMode = Literal["dispatch", "inject", "record_only", "denied"]


class MirrorWorkspaceConfig(BaseModel):
    connector_mode: MirrorConnectorMode = "sim"
    demo_mode: bool = False
    autoplay: bool = False
    demo_interval_ms: int = 1500
    hero_world: str | None = None

    @model_validator(mode="after")
    def validate_demo_connector_mode(self) -> "MirrorWorkspaceConfig":
        if self.demo_mode and self.connector_mode != "sim":
            raise ValueError("mirror demo mode requires connector_mode='sim'")
        return self


class MirrorAgentSpec(BaseModel):
    agent_id: str
    name: str
    mode: MirrorAgentMode = "ingest"
    role: str | None = None
    team: str | None = None
    allowed_surfaces: list[str] = Field(default_factory=list)
    policy_profile: str | None = None
    status: MirrorAgentStatus = "registered"
    last_seen_at: str | None = None
    source: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    denied_count: int = 0
    last_action: str | None = None


class MirrorIngestEvent(BaseModel):
    event_id: str | None = None
    agent_id: str
    external_tool: str
    resolved_tool: str | None = None
    focus_hint: str | None = None
    target: str | None = None
    args: dict[str, Any] = Field(default_factory=dict)
    payload: dict[str, Any] = Field(default_factory=dict)
    label: str | None = None
    source_mode: MirrorAgentMode = "ingest"


class MirrorEventResult(BaseModel):
    ok: bool = True
    handled_by: MirrorHandleMode
    agent_id: str
    remaining_demo_steps: int = 0
    result: dict[str, Any] = Field(default_factory=dict)


class MirrorRecentEvent(BaseModel):
    event_id: str | None = None
    agent_id: str
    tool: str
    handled_by: MirrorHandleMode
    label: str | None = None
    timestamp: str


class MirrorRuntimeSnapshot(BaseModel):
    config: MirrorWorkspaceConfig
    agents: list[MirrorAgentSpec] = Field(default_factory=list)
    event_count: int = 0
    pending_demo_steps: int = 0
    last_event_at: str | None = None
    autoplay_running: bool = False
    recent_events: list[MirrorRecentEvent] = Field(default_factory=list)
