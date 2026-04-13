from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from ._base import (
    WhatIfBackendScoreStatus,
    WhatIfObjectivePackId,
    WhatIfOutcomeBackendId,
    WhatIfResearchHypothesisLabel,
)
from .episode import WhatIfEpisodeMaterialization, WhatIfReplaySummary
from .experiment import (
    WhatIfObjectivePack,
    WhatIfOutcomeScore,
    WhatIfOutcomeSignals,
    WhatIfRankedRolloutResult,
)
from .forecast import WhatIfHistoricalScore


class WhatIfResearchCandidate(BaseModel):
    candidate_id: str
    label: str
    prompt: str
    expected_hypotheses: dict[WhatIfObjectivePackId, WhatIfResearchHypothesisLabel] = (
        Field(default_factory=dict)
    )


class WhatIfResearchCase(BaseModel):
    case_id: str
    title: str
    event_id: str
    thread_id: str | None = None
    summary: str = ""
    candidates: list[WhatIfResearchCandidate] = Field(default_factory=list)


class WhatIfResearchPack(BaseModel):
    pack_id: str
    title: str
    summary: str
    objective_pack_ids: list[WhatIfObjectivePackId] = Field(default_factory=list)
    rollout_seeds: list[int] = Field(default_factory=list)
    cases: list[WhatIfResearchCase] = Field(default_factory=list)


class WhatIfBranchSummaryFeature(BaseModel):
    name: str
    value: float


class WhatIfSequenceStep(BaseModel):
    step_index: int
    phase: Literal["history", "branch", "generated", "historical_future"] = "history"
    event_type: str
    actor_id: str
    subject: str = ""
    delay_ms: int = 0
    recipient_scope: Literal["internal", "external", "mixed", "unknown"] = "unknown"
    external_recipient_count: int = 0
    cc_recipient_count: int = 0
    attachment_flag: bool = False
    escalation_flag: bool = False
    approval_flag: bool = False
    legal_flag: bool = False
    trading_flag: bool = False
    review_flag: bool = False
    urgency_flag: bool = False
    conflict_flag: bool = False


class WhatIfTreatmentTraceStep(BaseModel):
    step_index: int
    source: str
    tag: str
    value: float = 1.0


class WhatIfBackendBranchContract(BaseModel):
    case_id: str
    objective_pack_id: WhatIfObjectivePackId
    intervention_label: str
    summary_features: list[WhatIfBranchSummaryFeature] = Field(default_factory=list)
    sequence_steps: list[WhatIfSequenceStep] = Field(default_factory=list)
    treatment_trace: list[WhatIfTreatmentTraceStep] = Field(default_factory=list)
    average_rollout_signals: WhatIfOutcomeSignals = Field(
        default_factory=WhatIfOutcomeSignals
    )
    historical_outcome_signals: WhatIfOutcomeSignals = Field(
        default_factory=WhatIfOutcomeSignals
    )
    baseline_forecast: WhatIfHistoricalScore = Field(
        default_factory=WhatIfHistoricalScore
    )
    notes: list[str] = Field(default_factory=list)


class WhatIfResearchDatasetRow(BaseModel):
    row_id: str
    split: Literal["train", "validation", "test", "evaluation"] = "train"
    source_kind: Literal["historical", "counterfactual", "evaluation"] = "historical"
    thread_id: str
    branch_event_id: str
    contract: WhatIfBackendBranchContract
    outcome_signals: WhatIfOutcomeSignals = Field(default_factory=WhatIfOutcomeSignals)


class WhatIfResearchDatasetManifest(BaseModel):
    root: Path
    historical_row_count: int = 0
    counterfactual_row_count: int = 0
    evaluation_row_count: int = 0
    split_row_counts: dict[str, int] = Field(default_factory=dict)
    split_paths: dict[str, str] = Field(default_factory=dict)
    heldout_thread_ids: list[str] = Field(default_factory=list)


class WhatIfBackendScore(BaseModel):
    backend: WhatIfOutcomeBackendId
    status: WhatIfBackendScoreStatus = "ok"
    effective_backend: str | None = None
    outcome_signals: WhatIfOutcomeSignals = Field(default_factory=WhatIfOutcomeSignals)
    outcome_score: WhatIfOutcomeScore
    rank: int = 0
    confidence: float | None = None
    notes: list[str] = Field(default_factory=list)
    artifact_paths: dict[str, str] = Field(default_factory=dict)
    error: str | None = None


class WhatIfPackCandidateResult(BaseModel):
    candidate: WhatIfResearchCandidate
    expected_hypothesis: WhatIfResearchHypothesisLabel = "middle_expected"
    rank: int = 0
    rollout_seeds: list[int] = Field(default_factory=list)
    rollout_count: int = 0
    average_outcome_signals: WhatIfOutcomeSignals = Field(
        default_factory=WhatIfOutcomeSignals
    )
    outcome_score: WhatIfOutcomeScore
    rank_stability: float = 0.0
    reason: str = ""
    rollouts: list[WhatIfRankedRolloutResult] = Field(default_factory=list)
    backend_scores: list[WhatIfBackendScore] = Field(default_factory=list)
    contract_path: str | None = None


class WhatIfPackObjectiveResult(BaseModel):
    objective_pack: WhatIfObjectivePack
    recommended_candidate_label: str = ""
    candidates: list[WhatIfPackCandidateResult] = Field(default_factory=list)
    backend_recommendations: dict[str, str] = Field(default_factory=dict)
    expected_order_ok: bool = False


class WhatIfPackCaseResult(BaseModel):
    case: WhatIfResearchCase
    materialization: WhatIfEpisodeMaterialization
    baseline: WhatIfReplaySummary
    historical_outcome_signals: WhatIfOutcomeSignals = Field(
        default_factory=WhatIfOutcomeSignals
    )
    objectives: list[WhatIfPackObjectiveResult] = Field(default_factory=list)
    artifacts_root: Path | None = None


class WhatIfPackRunArtifacts(BaseModel):
    root: Path
    result_json_path: Path
    overview_markdown_path: Path
    dataset_root: Path
    pilot_markdown_path: Path


class WhatIfPackRunResult(BaseModel):
    version: Literal["1"] = "1"
    pack: WhatIfResearchPack
    integrated_backends: list[WhatIfOutcomeBackendId] = Field(default_factory=list)
    pilot_backends: list[WhatIfOutcomeBackendId] = Field(default_factory=list)
    dataset: WhatIfResearchDatasetManifest
    cases: list[WhatIfPackCaseResult] = Field(default_factory=list)
    hypothesis_pass_rate: float = 0.0
    hypothesis_pass_count: int = 0
    hypothesis_total_count: int = 0
    artifacts: WhatIfPackRunArtifacts


__all__ = [
    "WhatIfBackendBranchContract",
    "WhatIfBackendScore",
    "WhatIfBackendScoreStatus",
    "WhatIfBranchSummaryFeature",
    "WhatIfOutcomeBackendId",
    "WhatIfPackCandidateResult",
    "WhatIfPackCaseResult",
    "WhatIfPackObjectiveResult",
    "WhatIfPackRunArtifacts",
    "WhatIfPackRunResult",
    "WhatIfResearchCandidate",
    "WhatIfResearchCase",
    "WhatIfResearchDatasetManifest",
    "WhatIfResearchDatasetRow",
    "WhatIfResearchHypothesisLabel",
    "WhatIfResearchPack",
    "WhatIfSequenceStep",
    "WhatIfTreatmentTraceStep",
]
