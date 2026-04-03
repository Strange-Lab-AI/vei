from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from vei.orchestrators.api import OrchestratorSnapshot, OrchestratorSyncHealth

WorkforceCommandAction = Literal[
    "sync",
    "pause",
    "resume",
    "comment_task",
    "approve",
    "reject",
    "request_revision",
]


class WorkforceCommandRecord(BaseModel):
    provider: str
    action: WorkforceCommandAction
    created_at: str = ""
    message: str | None = None
    agent_id: str | None = None
    external_agent_id: str | None = None
    task_id: str | None = None
    external_task_id: str | None = None
    approval_id: str | None = None
    external_approval_id: str | None = None
    comment_id: str | None = None
    decision_note: str | None = None


class WorkforceControlSummary(BaseModel):
    provider: str | None = None
    company_name: str | None = None
    sync_status: str = "disabled"
    observed_agent_count: int = 0
    governable_agent_count: int = 0
    steerable_agent_count: int = 0
    active_agent_count: int = 0
    task_count: int = 0
    pending_approval_count: int = 0
    routeable_surface_count: int = 0
    latest_activity_at: str | None = None


class WorkforceState(BaseModel):
    version: Literal["1"] = "1"
    updated_at: str = ""
    summary: WorkforceControlSummary = Field(default_factory=WorkforceControlSummary)
    sync: OrchestratorSyncHealth | None = None
    snapshot: OrchestratorSnapshot | None = None
    commands: list[WorkforceCommandRecord] = Field(default_factory=list)
