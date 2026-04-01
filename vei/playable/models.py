from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from vei.capability_graph.models import CapabilityGraphActionInput

MoveTier = Literal["recommended", "available", "risky"]
MoveAvailability = Literal["recommended", "available", "risky", "blocked"]
MissionStatus = Literal["ready", "running", "completed"]


class PlayableMissionMoveSpec(BaseModel):
    move_id: str
    title: str
    summary: str
    tier: MoveTier = "recommended"
    graph_action: CapabilityGraphActionInput
    consequence_preview: str
    step_index: int | None = None
    step_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PlayableMissionSpec(BaseModel):
    vertical_name: str
    mission_name: str
    title: str
    briefing: str
    why_it_matters: str
    failure_impact: str
    scenario_variant: str
    default_objective: str
    supported_objectives: list[str] = Field(default_factory=list)
    branch_labels: list[str] = Field(default_factory=list)
    hero: bool = False
    action_budget: int = 6
    turn_budget: int = 8
    countdown_ms: int = 180000
    primary_domain: str
    manual_moves: list[PlayableMissionMoveSpec] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)


class MissionCatalog(BaseModel):
    version: Literal["1"] = "1"
    hero_world: str
    included_worlds: list[str] = Field(default_factory=list)
    missions: list[PlayableMissionSpec] = Field(default_factory=list)


class MissionMoveState(BaseModel):
    move_id: str
    title: str
    summary: str
    availability: MoveAvailability
    consequence_preview: str
    graph_action: CapabilityGraphActionInput
    blocked_reason: str | None = None
    executed: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class MissionScorecard(BaseModel):
    overall_score: int = 0
    mission_success: bool | None = None
    deadline_pressure: str = "stable"
    business_risk: str = "moderate"
    artifact_hygiene: str = "incomplete"
    policy_correctness: str = "drifting"
    move_count: int = 0
    action_budget_remaining: int = 0
    summary: str = ""
    contract_issue_count: int = 0
    success_assertions_passed: int = 0
    success_assertions_total: int = 0


class PlayerMove(BaseModel):
    run_id: str
    move_id: str


class PlayerMoveResult(BaseModel):
    move_id: str
    title: str
    branch_label: str
    summary: str
    graph_intent: str
    resolved_tool: str
    object_refs: list[str] = Field(default_factory=list)
    time_ms: int = 0
    payload: dict[str, Any] = Field(default_factory=dict)


class MissionExportBundle(BaseModel):
    name: Literal["rl", "eval", "agent_ops"]
    title: str
    summary: str
    payload: dict[str, Any] = Field(default_factory=dict)


class MissionSessionState(BaseModel):
    version: Literal["1"] = "1"
    run_id: str
    workspace_root: Path
    world_name: str
    mission: PlayableMissionSpec
    objective_variant: str
    status: MissionStatus = "ready"
    branch_name: str
    turn_index: int = 0
    action_budget_remaining: int = 0
    last_snapshot_id: int | None = None
    selected_run_id: str | None = None
    baseline_run_id: str | None = None
    comparison_run_id: str | None = None
    executed_moves: list[PlayerMoveResult] = Field(default_factory=list)
    available_moves: list[MissionMoveState] = Field(default_factory=list)
    scorecard: MissionScorecard = Field(default_factory=MissionScorecard)
    exports: list[MissionExportBundle] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class PlayableShowcaseWorldResult(BaseModel):
    vertical_name: str
    world_name: str
    workspace_root: Path
    mission_name: str
    objective_variant: str
    human_run_id: str
    baseline_run_id: str | None = None
    comparison_run_id: str | None = None
    fidelity_status: str = "ok"
    ui_command: str


class PlayableShowcaseResult(BaseModel):
    version: Literal["1"] = "1"
    run_id: str
    root: Path
    hero_world: str
    worlds: list[PlayableShowcaseWorldResult] = Field(default_factory=list)
    result_path: Path
    overview_path: Path
