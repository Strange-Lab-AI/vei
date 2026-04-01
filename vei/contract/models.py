from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from vei.scenario_engine.models import AssertionSpec

ContractSurface = Literal[
    "oracle_state",
    "visible_observation",
    "tool_result",
    "pending",
    "time",
]


class ContractPredicateSpec(BaseModel):
    name: str
    source: ContractSurface
    assertion: AssertionSpec
    description: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ObservationBoundarySpec(BaseModel):
    allowed_tools: List[str] = Field(default_factory=list)
    focus_hints: List[str] = Field(default_factory=list)
    hidden_state_fields: List[str] = Field(default_factory=list)
    description: Optional[str] = None


class PolicyInvariantSpec(BaseModel):
    name: str
    description: str
    required: bool = True
    evidence: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RewardTermSpec(BaseModel):
    name: str
    weight: float
    term_type: Literal["success", "penalty"] = "success"
    description: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class InterventionRuleSpec(BaseModel):
    name: str
    trigger: str
    action: str
    actor: Optional[str] = None
    required: bool = True
    evidence: Optional[str] = None


class ContractSpec(BaseModel):
    name: str
    workflow_name: str
    scenario_name: Optional[str] = None
    success_predicates: List[ContractPredicateSpec] = Field(default_factory=list)
    forbidden_predicates: List[ContractPredicateSpec] = Field(default_factory=list)
    observation_boundary: ObservationBoundarySpec = Field(
        default_factory=ObservationBoundarySpec
    )
    policy_invariants: List[PolicyInvariantSpec] = Field(default_factory=list)
    reward_terms: List[RewardTermSpec] = Field(default_factory=list)
    intervention_rules: List[InterventionRuleSpec] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ContractValidationIssue(BaseModel):
    code: str
    message: str
    severity: str = "error"
    predicate_name: Optional[str] = None
    source: Optional[ContractSurface] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ContractValidationReport(BaseModel):
    ok: bool
    issues: List[ContractValidationIssue] = Field(default_factory=list)


class ContractEvaluationResult(BaseModel):
    ok: bool
    contract_name: str
    workflow_name: str
    static_validation: ContractValidationReport
    dynamic_validation: ContractValidationReport
    success_predicate_count: int = 0
    success_predicates_passed: int = 0
    success_predicates_failed: int = 0
    forbidden_predicate_count: int = 0
    forbidden_predicates_failed: int = 0
    policy_invariant_count: int = 0
    policy_invariants_failed: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)
