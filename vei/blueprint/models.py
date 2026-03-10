from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


CapabilityDomain = Literal[
    "comm_graph",
    "doc_graph",
    "work_graph",
    "identity_graph",
    "revenue_graph",
    "obs_graph",
    "data_graph",
    "ops_graph",
]

FacadeSurface = Literal["mcp", "api", "ui", "chat", "email", "cli"]


class FacadeManifest(BaseModel):
    name: str
    title: str
    domain: CapabilityDomain
    router_module: str
    description: str
    surfaces: List[FacadeSurface] = Field(default_factory=list)
    primary_tools: List[str] = Field(default_factory=list)
    state_roots: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)


class BlueprintScenarioSummary(BaseModel):
    name: str
    difficulty: str = "standard"
    benchmark_family: Optional[str] = None
    tool_families: List[str] = Field(default_factory=list)
    expected_steps_min: Optional[int] = None
    expected_steps_max: Optional[int] = None
    tags: List[str] = Field(default_factory=list)


class BlueprintContractSummary(BaseModel):
    name: str
    workflow_name: str
    success_predicate_count: int = 0
    forbidden_predicate_count: int = 0
    policy_invariant_count: int = 0
    intervention_rule_count: int = 0
    observation_focus_hints: List[str] = Field(default_factory=list)
    hidden_state_fields: List[str] = Field(default_factory=list)


class BlueprintSpec(BaseModel):
    name: str
    title: str
    description: str
    family_name: Optional[str] = None
    workflow_name: Optional[str] = None
    workflow_variant: Optional[str] = None
    scenario: BlueprintScenarioSummary
    contract: Optional[BlueprintContractSummary] = None
    capability_domains: List[CapabilityDomain] = Field(default_factory=list)
    facades: List[FacadeManifest] = Field(default_factory=list)
    state_roots: List[str] = Field(default_factory=list)
    surfaces: List[FacadeSurface] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
