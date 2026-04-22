from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

GovernorConnectorMode = Literal["sim", "live"]
GovernorAgentMode = Literal["proxy", "ingest", "demo"]
GovernorAgentStatus = Literal["registered", "active", "idle", "error"]
GovernorHandleMode = Literal[
    "dispatch",
    "inject",
    "record_only",
    "denied",
    "pending_approval",
]
GovernorPolicyProfileId = Literal["observer", "operator", "approver", "admin"]
GovernorOperationClass = Literal["read", "write_safe", "write_risky"]
GovernorApprovalStatus = Literal[
    "pending",
    "approved",
    "rejected",
    "executed",
    "failed",
]
GovernorConnectorAvailability = Literal["healthy", "degraded"]
GovernorConnectorWriteCapability = Literal["interactive", "read_only", "unsupported"]
GovernorActionDecision = Literal["allow", "deny", "approval_required"]


class GovernorApprovalRule(BaseModel):
    surface: str | None = None
    resolved_tools: list[str] = Field(default_factory=list)
    operation_classes: list[GovernorOperationClass] = Field(default_factory=list)
    reason_code: str = "mirror.approval_required"
    reason: str

    @model_validator(mode="after")
    def validate_rule_scope(self) -> "GovernorApprovalRule":
        if self.surface or self.resolved_tools:
            return self
        raise ValueError(
            "approval rule needs at least one matcher: surface or resolved_tools"
        )


class GovernorPolicyProfile(BaseModel):
    profile_id: GovernorPolicyProfileId
    label: str
    description: str
    can_approve: bool = False
    read_access: bool = True
    safe_write_access: Literal["allow", "deny"] = "deny"
    risky_write_access: Literal["allow", "deny", "require_approval"] = "deny"


class GovernorWorkspaceConfig(BaseModel):
    connector_mode: GovernorConnectorMode = "sim"
    demo_mode: bool = False
    autoplay: bool = False
    demo_interval_ms: int = 1500
    hero_world: str | None = None
    approval_rules: list[GovernorApprovalRule] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_demo_connector_mode(self) -> "GovernorWorkspaceConfig":
        if self.demo_mode and self.connector_mode != "sim":
            raise ValueError("governor demo mode requires connector_mode='sim'")
        return self


class GovernorAgentSpec(BaseModel):
    agent_id: str
    name: str
    mode: GovernorAgentMode = "ingest"
    role: str | None = None
    team: str | None = None
    allowed_surfaces: list[str] = Field(default_factory=list)
    policy_profile_id: GovernorPolicyProfileId = "admin"
    resolved_policy_profile: GovernorPolicyProfile | None = None
    status: GovernorAgentStatus = "registered"
    last_seen_at: str | None = None
    source: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    denied_count: int = 0
    throttled_count: int = 0
    last_action: str | None = None

    @model_validator(mode="before")
    @classmethod
    def upgrade_legacy_policy_profile(
        cls,
        value: Any,
    ) -> Any:
        if not isinstance(value, dict):
            return value
        if value.get("policy_profile_id") is not None:
            return value
        legacy = str(value.get("policy_profile") or "").strip().lower()
        if not legacy:
            return {**value, "policy_profile_id": "admin"}
        mapped = {
            "observe": "observer",
            "observer": "observer",
            "write_safe": "operator",
            "operator": "operator",
            "approver": "approver",
            "full": "admin",
            "admin": "admin",
            "dispatch_safe": "operator",
            "billing_safe": "operator",
        }.get(legacy, "admin")
        return {**value, "policy_profile_id": mapped}


class GovernorIngestEvent(BaseModel):
    event_id: str | None = None
    agent_id: str
    external_tool: str
    resolved_tool: str | None = None
    focus_hint: str | None = None
    target: str | None = None
    args: dict[str, Any] = Field(default_factory=dict)
    payload: dict[str, Any] = Field(default_factory=dict)
    label: str | None = None
    source_mode: GovernorAgentMode = "ingest"


class GovernorEventResult(BaseModel):
    ok: bool = True
    handled_by: GovernorHandleMode
    agent_id: str
    remaining_demo_steps: int = 0
    result: dict[str, Any] = Field(default_factory=dict)


class GovernorRecentEvent(BaseModel):
    event_id: str | None = None
    agent_id: str
    tool: str
    handled_by: GovernorHandleMode
    resolved_tool: str | None = None
    surface: str | None = None
    label: str | None = None
    reason_code: str | None = None
    reason: str | None = None
    timestamp: str


class GovernorPendingApproval(BaseModel):
    approval_id: str
    agent_id: str
    surface: str
    resolved_tool: str
    operation_class: GovernorOperationClass
    args: dict[str, Any] = Field(default_factory=dict)
    reason_code: str
    reason: str
    status: GovernorApprovalStatus = "pending"
    created_at: str
    resolved_by: str | None = None
    resolved_at: str | None = None
    execution_result: dict[str, Any] = Field(default_factory=dict)
    external_tool: str | None = None
    focus_hint: str | None = None
    target: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    source_mode: GovernorAgentMode = "ingest"


class GovernorConnectorStatus(BaseModel):
    surface: str
    source_mode: GovernorConnectorMode
    availability: GovernorConnectorAvailability
    write_capability: GovernorConnectorWriteCapability
    reason: str | None = None
    last_checked_at: str | None = None


class GovernorActionPlan(BaseModel):
    action: Literal["dispatch", "inject"]
    surface: str
    resolved_tool: str
    operation_class: GovernorOperationClass
    decision: GovernorActionDecision = "allow"
    reason_code: str | None = None
    reason: str | None = None


class GovernorRuntimeSnapshot(BaseModel):
    config: GovernorWorkspaceConfig
    agents: list[GovernorAgentSpec] = Field(default_factory=list)
    policy_profiles: list[GovernorPolicyProfile] = Field(default_factory=list)
    event_count: int = 0
    denied_event_count: int = 0
    throttled_event_count: int = 0
    pending_demo_steps: int = 0
    last_event_at: str | None = None
    autoplay_running: bool = False
    pending_approvals: list[GovernorPendingApproval] = Field(default_factory=list)
    connector_status: list[GovernorConnectorStatus] = Field(default_factory=list)
    recent_events: list[GovernorRecentEvent] = Field(default_factory=list)
