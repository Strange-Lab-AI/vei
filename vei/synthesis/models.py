from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

TrainingFormat = Literal["conversations", "trajectories", "demonstrations", "authoring"]


class RunbookStep(BaseModel):
    index: int
    domain: str = ""
    action: str = ""
    tool: str = ""
    args_template: Dict[str, Any] = Field(default_factory=dict)
    precondition: str = ""
    postcondition: str = ""
    contract_predicates: List[str] = Field(default_factory=list)
    decision_point: bool = False


class Runbook(BaseModel):
    title: str
    scenario_name: str = ""
    contract_name: str = ""
    steps: List[RunbookStep] = Field(default_factory=list)
    decision_points: int = 0
    total_steps: int = 0
    success_rate: Optional[float] = None


class TrainingExample(BaseModel):
    format: TrainingFormat
    run_id: str = ""
    sequence_index: int = 0
    data: Dict[str, Any] = Field(default_factory=dict)


class TrainingSet(BaseModel):
    format: TrainingFormat
    scenario_name: str = ""
    example_count: int = 0
    examples: List[TrainingExample] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AgentConfig(BaseModel):
    system_prompt: str = ""
    tools: List[Dict[str, Any]] = Field(default_factory=list)
    guardrails: List[str] = Field(default_factory=list)
    success_criteria: List[str] = Field(default_factory=list)
    context_summary: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)
