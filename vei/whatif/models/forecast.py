from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from ._base import WhatIfForecastBackend
from .world import WhatIfBusinessStateChange, WhatIfEventReference


class WhatIfHistoricalScore(BaseModel):
    backend: Literal[
        "historical",
        "heuristic",
        "e_jepa",
        "heuristic_baseline",
        "reference",
    ] = "historical"
    future_event_count: int = 0
    future_escalation_count: int = 0
    future_assignment_count: int = 0
    future_approval_count: int = 0
    future_external_event_count: int = 0
    risk_score: float = 0.0
    stock_return_5d: float | None = None
    credit_action_30d: float | None = None
    ferc_action_180d: float | None = None
    summary: str = ""


class WhatIfInterventionSpec(BaseModel):
    label: str
    prompt: str
    objective: str = ""
    scenario_id: str | None = None
    thread_id: str | None = None
    branch_event_id: str | None = None


class WhatIfLLMGeneratedMessage(BaseModel):
    actor_id: str
    surface: str = "mail"
    to: str
    subject: str
    body_text: str
    delay_ms: int
    conversation_anchor: str = ""
    rationale: str = ""


class WhatIfLLMUsage(BaseModel):
    provider: str
    model: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    estimated_cost_usd: float | None = None


class WhatIfLLMReplayResult(BaseModel):
    status: Literal["ok", "skipped", "error"] = "ok"
    provider: str
    model: str
    prompt: str
    summary: str = ""
    messages: list[WhatIfLLMGeneratedMessage] = Field(default_factory=list)
    usage: WhatIfLLMUsage | None = None
    scheduled_event_count: int = 0
    delivered_event_count: int = 0
    inbox_count: int = 0
    top_subjects: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    error: str | None = None


class WhatIfCounterfactualEstimateDelta(BaseModel):
    risk_score_delta: float = 0.0
    future_event_delta: int = 0
    escalation_delta: int = 0
    assignment_delta: int = 0
    approval_delta: int = 0
    external_event_delta: int = 0
    stock_return_5d_delta: float | None = None
    credit_action_30d_delta: float | None = None
    ferc_action_180d_delta: float | None = None


class WhatIfCounterfactualEstimateArtifacts(BaseModel):
    cache_root: Path | None = None
    dataset_root: Path | None = None
    checkpoint_path: Path | None = None
    decoder_path: Path | None = None


class WhatIfCounterfactualEstimateResult(BaseModel):
    status: Literal["ok", "skipped", "error"] = "ok"
    backend: WhatIfForecastBackend = "heuristic_baseline"
    prompt: str
    summary: str = ""
    baseline: WhatIfHistoricalScore = Field(default_factory=WhatIfHistoricalScore)
    predicted: WhatIfHistoricalScore = Field(default_factory=WhatIfHistoricalScore)
    delta: WhatIfCounterfactualEstimateDelta = Field(
        default_factory=WhatIfCounterfactualEstimateDelta
    )
    branch_event: WhatIfEventReference | None = None
    horizon_event_count: int = 0
    surprise_score: float | None = None
    current_state_summary: dict[str, float] = Field(default_factory=dict)
    predicted_state_summary: dict[str, float] = Field(default_factory=dict)
    actual_state_summary: dict[str, float] = Field(default_factory=dict)
    business_state_change: WhatIfBusinessStateChange | None = None
    artifacts: WhatIfCounterfactualEstimateArtifacts | None = None
    notes: list[str] = Field(default_factory=list)
    error: str | None = None


__all__ = [
    "WhatIfCounterfactualEstimateArtifacts",
    "WhatIfCounterfactualEstimateDelta",
    "WhatIfCounterfactualEstimateResult",
    "WhatIfForecastBackend",
    "WhatIfHistoricalScore",
    "WhatIfInterventionSpec",
    "WhatIfLLMGeneratedMessage",
    "WhatIfLLMReplayResult",
    "WhatIfLLMUsage",
]
