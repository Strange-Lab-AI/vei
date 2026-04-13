from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from ._base import WhatIfExperimentMode, WhatIfObjectivePackId
from .episode import WhatIfEpisodeMaterialization, WhatIfReplaySummary
from .forecast import (
    WhatIfCounterfactualEstimateResult,
    WhatIfForecastBackend,
    WhatIfInterventionSpec,
    WhatIfLLMReplayResult,
)
from .world import WhatIfBusinessStateChange, WhatIfResult


class WhatIfExperimentArtifacts(BaseModel):
    root: Path
    result_json_path: Path
    overview_markdown_path: Path
    llm_json_path: Path | None = None
    forecast_json_path: Path | None = None


class WhatIfExperimentResult(BaseModel):
    version: Literal["1", "2"] = "2"
    mode: WhatIfExperimentMode = "both"
    label: str
    intervention: WhatIfInterventionSpec
    selection: WhatIfResult
    materialization: WhatIfEpisodeMaterialization
    baseline: WhatIfReplaySummary
    llm_result: WhatIfLLMReplayResult | None = None
    forecast_result: WhatIfCounterfactualEstimateResult | None = None
    artifacts: WhatIfExperimentArtifacts


class WhatIfObjectivePack(BaseModel):
    pack_id: WhatIfObjectivePackId
    title: str
    summary: str
    weights: dict[str, float] = Field(default_factory=dict)
    evidence_labels: list[str] = Field(default_factory=list)


class WhatIfCandidateIntervention(BaseModel):
    label: str
    prompt: str


class WhatIfOutcomeSignals(BaseModel):
    exposure_risk: float = 0.0
    delay_risk: float = 0.0
    relationship_protection: float = 0.0
    message_count: int = 0
    outside_message_count: int = 0
    avg_delay_ms: int = 0
    internal_only: bool = False
    reassurance_count: int = 0
    hold_count: int = 0


class WhatIfOutcomeScore(BaseModel):
    objective_pack_id: WhatIfObjectivePackId
    overall_score: float = 0.0
    components: dict[str, float] = Field(default_factory=dict)
    evidence: list[str] = Field(default_factory=list)


class WhatIfRankedRolloutResult(BaseModel):
    rollout_index: int
    seed: int
    llm_result: WhatIfLLMReplayResult
    outcome_signals: WhatIfOutcomeSignals
    outcome_score: WhatIfOutcomeScore


class WhatIfShadowOutcomeScore(BaseModel):
    backend: WhatIfForecastBackend
    outcome_signals: WhatIfOutcomeSignals
    outcome_score: WhatIfOutcomeScore
    forecast_result: WhatIfCounterfactualEstimateResult


class WhatIfCandidateRanking(BaseModel):
    intervention: WhatIfCandidateIntervention
    rank: int = 0
    rollout_count: int = 0
    average_outcome_signals: WhatIfOutcomeSignals = Field(
        default_factory=WhatIfOutcomeSignals
    )
    outcome_score: WhatIfOutcomeScore
    reason: str = ""
    rollouts: list[WhatIfRankedRolloutResult] = Field(default_factory=list)
    shadow: WhatIfShadowOutcomeScore | None = None
    business_state_change: WhatIfBusinessStateChange | None = None


class WhatIfRankedExperimentArtifacts(BaseModel):
    root: Path
    result_json_path: Path
    overview_markdown_path: Path


class WhatIfRankedExperimentResult(BaseModel):
    version: Literal["1"] = "1"
    label: str
    objective_pack: WhatIfObjectivePack
    selection: WhatIfResult
    materialization: WhatIfEpisodeMaterialization
    baseline: WhatIfReplaySummary
    candidates: list[WhatIfCandidateRanking] = Field(default_factory=list)
    recommended_candidate_label: str = ""
    artifacts: WhatIfRankedExperimentArtifacts


__all__ = [
    "WhatIfCandidateIntervention",
    "WhatIfCandidateRanking",
    "WhatIfExperimentArtifacts",
    "WhatIfExperimentMode",
    "WhatIfExperimentResult",
    "WhatIfObjectivePack",
    "WhatIfObjectivePackId",
    "WhatIfOutcomeScore",
    "WhatIfOutcomeSignals",
    "WhatIfRankedExperimentArtifacts",
    "WhatIfRankedExperimentResult",
    "WhatIfRankedRolloutResult",
    "WhatIfShadowOutcomeScore",
]
