from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_validator


class ActorSpec(BaseModel):
    actor_id: str
    role: str
    email: Optional[str] = None
    slack: Optional[str] = None


class ConstraintSpec(BaseModel):
    name: str
    description: str
    required: bool = True


class ApprovalSpec(BaseModel):
    stage: str
    approver: str
    required: bool = True
    evidence: Optional[str] = None


class AssertionSpec(BaseModel):
    kind: Literal[
        "result_contains",
        "result_not_contains",
        "result_equals",
        "observation_contains",
        "observation_not_contains",
        "pending_max",
        "state_contains",
        "state_not_contains",
        "state_equals",
        "state_count_equals",
        "state_count_max",
        "state_exists",
        "time_max_ms",
    ]
    field: Optional[str] = None
    contains: Optional[str] = None
    equals: Any = None
    focus: Optional[str] = None
    max_value: Optional[int] = None
    description: Optional[str] = None

    @model_validator(mode="after")
    def _validate_shape(self) -> "AssertionSpec":
        if self.kind in {
            "result_contains",
            "result_not_contains",
            "observation_contains",
            "observation_not_contains",
            "state_contains",
            "state_not_contains",
        }:
            if self.contains is None:
                raise ValueError(f"{self.kind} requires 'contains'")
            if self.field is None and not self.kind.startswith("observation_"):
                raise ValueError(f"{self.kind} requires 'field'")
        if self.kind in {"result_equals", "state_equals", "state_exists"}:
            if self.field is None:
                raise ValueError(f"{self.kind} requires 'field'")
        if self.kind == "pending_max" and self.max_value is None:
            raise ValueError("pending_max requires 'max_value'")
        if (
            self.kind in {"state_count_equals", "state_count_max"}
            and self.field is None
        ):
            raise ValueError(f"{self.kind} requires 'field'")
        if self.kind == "state_count_max" and self.max_value is None:
            raise ValueError("state_count_max requires 'max_value'")
        if self.kind == "time_max_ms" and self.max_value is None:
            raise ValueError("time_max_ms requires 'max_value'")
        return self


class WorkflowStepSpec(BaseModel):
    step_id: str
    description: str
    tool: Optional[str] = None
    graph_domain: Optional[
        Literal[
            "comm_graph",
            "doc_graph",
            "work_graph",
            "identity_graph",
            "revenue_graph",
            "obs_graph",
            "data_graph",
            "ops_graph",
            "property_graph",
            "campaign_graph",
            "inventory_graph",
        ]
    ] = None
    graph_action: Optional[str] = None
    args: Dict[str, Any] = Field(default_factory=dict)
    expect: List[AssertionSpec] = Field(default_factory=list)
    on_failure: str = "fail"

    @model_validator(mode="after")
    def _validate_execution_shape(self) -> "WorkflowStepSpec":
        has_tool = bool(self.tool)
        has_graph = self.graph_domain is not None or self.graph_action is not None
        if has_tool and has_graph:
            raise ValueError(
                "workflow step must declare either tool or graph_domain/graph_action, not both"
            )
        if not has_tool and not has_graph:
            raise ValueError(
                "workflow step must declare either tool or graph_domain/graph_action"
            )
        if has_graph and (self.graph_domain is None or self.graph_action is None):
            raise ValueError(
                "graph-native workflow step requires both graph_domain and graph_action"
            )
        if self.tool is not None and not self.tool.strip():
            raise ValueError("workflow step tool cannot be empty")
        if self.graph_action is not None and not self.graph_action.strip():
            raise ValueError("workflow step graph_action cannot be empty")
        return self


class FailurePathSpec(BaseModel):
    name: str
    trigger_step: str
    recovery_steps: List[str] = Field(default_factory=list)
    notes: Optional[str] = None


class ObjectiveSpec(BaseModel):
    statement: str
    success: List[str] = Field(default_factory=list)


class WorkflowScenarioSpec(BaseModel):
    name: str
    objective: ObjectiveSpec
    world: Dict[str, Any] = Field(default_factory=dict)
    actors: List[ActorSpec] = Field(default_factory=list)
    constraints: List[ConstraintSpec] = Field(default_factory=list)
    approvals: List[ApprovalSpec] = Field(default_factory=list)
    steps: List[WorkflowStepSpec] = Field(default_factory=list)
    success_assertions: List[AssertionSpec] = Field(default_factory=list)
    failure_paths: List[FailurePathSpec] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_unique_steps(self) -> "WorkflowScenarioSpec":
        seen: set[str] = set()
        for step in self.steps:
            if step.step_id in seen:
                raise ValueError(f"duplicate step_id: {step.step_id}")
            seen.add(step.step_id)
        return self
