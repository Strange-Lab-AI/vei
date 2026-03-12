from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from vei.contract.models import ContractEvaluationResult


class ValidationIssue(BaseModel):
    code: str
    message: str
    step_id: Optional[str] = None
    severity: str = "error"
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ValidationReport(BaseModel):
    ok: bool
    issues: List[ValidationIssue] = Field(default_factory=list)


class WorkflowOutcomeValidation(BaseModel):
    ok: bool
    workflow_name: str
    contract_name: Optional[str] = None
    static_validation: ValidationReport
    dynamic_validation: ValidationReport
    step_count: int = 0
    success_assertion_count: int = 0
    success_assertions_passed: int = 0
    success_assertions_failed: int = 0
    forbidden_predicate_count: int = 0
    forbidden_predicates_failed: int = 0
    policy_invariant_count: int = 0
    policy_invariants_failed: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class StepExecution(BaseModel):
    step_id: str
    tool: str
    resolved_tool: Optional[str] = None
    graph_action_ref: Optional[str] = None
    graph_domain: Optional[str] = None
    graph_action: Optional[str] = None
    graph_intent: Optional[str] = None
    ok: bool
    result: Any = None
    observation: Dict[str, Any] = Field(default_factory=dict)
    assertion_failures: List[str] = Field(default_factory=list)
    object_refs: List[str] = Field(default_factory=list)
    time_ms: int = 0


class ScenarioRunResult(BaseModel):
    ok: bool
    workflow_name: str
    static_validation: ValidationReport
    dynamic_validation: ValidationReport
    steps: List[StepExecution] = Field(default_factory=list)
    artifacts_dir: Optional[str] = None
    branch: str = "main"
    initial_snapshot_id: Optional[int] = None
    final_snapshot_id: Optional[int] = None
    initial_snapshot_label: Optional[str] = None
    final_snapshot_label: Optional[str] = None
    final_state: Dict[str, Any] = Field(default_factory=dict)
    contract_validation: Optional[ContractEvaluationResult] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
