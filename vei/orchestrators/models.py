from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

OrchestratorProviderId = Literal["paperclip"]
OrchestratorIntegrationMode = Literal["proxy", "ingest", "observe"]
OrchestratorSyncStatusValue = Literal["disabled", "healthy", "stale", "error"]
OrchestratorCommandAction = Literal[
    "pause",
    "resume",
    "comment_task",
    "approve",
    "reject",
    "request_revision",
]


class OrchestratorConfig(BaseModel):
    provider: OrchestratorProviderId
    base_url: str
    company_id: str
    api_key_env: str = "PAPERCLIP_API_KEY"


class OrchestratorSyncCapabilities(BaseModel):
    can_pause_agents: bool = False
    can_resume_agents: bool = False
    can_comment_on_tasks: bool = False
    can_manage_approvals: bool = False
    routeable_surfaces: list[str] = Field(default_factory=list)


class OrchestratorBudgetSummary(BaseModel):
    monthly_budget_cents: int | None = None
    monthly_spend_cents: int | None = None
    utilization_ratio: float | None = None


class OrchestratorSummary(BaseModel):
    provider: str
    company_id: str
    company_name: str
    company_status: str | None = None
    description: str | None = None
    agent_counts: dict[str, int] = Field(default_factory=dict)
    task_counts: dict[str, int] = Field(default_factory=dict)
    stale_task_count: int = 0


class OrchestratorAgent(BaseModel):
    provider: str
    agent_id: str
    external_agent_id: str
    name: str
    role: str | None = None
    title: str | None = None
    team: str | None = None
    status: str | None = None
    reports_to: str | None = None
    integration_mode: OrchestratorIntegrationMode = "observe"
    allowed_surfaces: list[str] = Field(default_factory=list)
    policy_profile_id: str | None = None
    monthly_budget_cents: int | None = None
    monthly_spend_cents: int | None = None
    task_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class OrchestratorComment(BaseModel):
    provider: str
    comment_id: str
    body: str
    created_at: str | None = None
    author_agent_id: str | None = None
    author_name: str | None = None
    author_user_id: str | None = None


class OrchestratorTask(BaseModel):
    provider: str
    task_id: str
    external_task_id: str
    title: str
    identifier: str | None = None
    status: str | None = None
    assignee_agent_id: str | None = None
    priority: str | None = None
    project_name: str | None = None
    goal_name: str | None = None
    summary: str | None = None
    linked_approval_ids: list[str] = Field(default_factory=list)
    latest_comment_preview: str | None = None
    comments: list[OrchestratorComment] = Field(default_factory=list)


class OrchestratorApproval(BaseModel):
    provider: str
    approval_id: str
    external_approval_id: str
    approval_type: str
    status: str | None = None
    requested_by_agent_id: str | None = None
    requested_by_name: str | None = None
    decision_note: str | None = None
    created_at: str | None = None
    summary: str | None = None
    task_ids: list[str] = Field(default_factory=list)
    comments: list[OrchestratorComment] = Field(default_factory=list)


class OrchestratorActivityItem(BaseModel):
    provider: str
    label: str
    action: str | None = None
    created_at: str | None = None
    agent_id: str | None = None
    agent_name: str | None = None
    task_id: str | None = None
    status: str | None = None
    detail: str | None = None
    object_refs: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class OrchestratorSnapshot(BaseModel):
    provider: str
    company_id: str
    fetched_at: str
    summary: OrchestratorSummary
    budget: OrchestratorBudgetSummary = Field(default_factory=OrchestratorBudgetSummary)
    capabilities: OrchestratorSyncCapabilities = Field(
        default_factory=OrchestratorSyncCapabilities
    )
    agents: list[OrchestratorAgent] = Field(default_factory=list)
    tasks: list[OrchestratorTask] = Field(default_factory=list)
    approvals: list[OrchestratorApproval] = Field(default_factory=list)
    recent_activity: list[OrchestratorActivityItem] = Field(default_factory=list)


class OrchestratorSyncHealth(BaseModel):
    provider: str | None = None
    status: OrchestratorSyncStatusValue = "disabled"
    last_attempt_at: str | None = None
    last_success_at: str | None = None
    cache_used: bool = False
    synced_agent_count: int = 0
    message: str | None = None
    last_error: str | None = None


class OrchestratorCommandResult(BaseModel):
    ok: bool = True
    provider: str
    action: OrchestratorCommandAction
    agent_id: str | None = None
    external_agent_id: str | None = None
    task_id: str | None = None
    external_task_id: str | None = None
    approval_id: str | None = None
    external_approval_id: str | None = None
    comment_id: str | None = None
    message: str | None = None
