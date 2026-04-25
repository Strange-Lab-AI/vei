from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from ._base import (
    WhatIfAttachmentPolicy,
    WhatIfBenchmarkModelId,
    WhatIfBenchmarkSplit,
    WhatIfBusinessObjectivePackId,
    WhatIfCoordinationBreadth,
    WhatIfDecisionPosture,
    WhatIfEscalationLevel,
    WhatIfObjectivePackId,
    WhatIfOutsideSharingPosture,
    WhatIfOwnerClarity,
    WhatIfReassuranceStyle,
    WhatIfResearchHypothesisLabel,
    WhatIfReviewPath,
)
from .experiment import WhatIfOutcomeSignals
from .research import WhatIfBranchSummaryFeature, WhatIfSequenceStep
from .world import WhatIfEventReference, WhatIfPublicContext


class WhatIfActionSchema(BaseModel):
    event_type: str = ""
    recipient_scope: Literal["internal", "external", "mixed", "unknown"] = "unknown"
    external_recipient_count: int = 0
    attachment_policy: WhatIfAttachmentPolicy = "none"
    hold_required: bool = False
    legal_review_required: bool = False
    trading_review_required: bool = False
    escalation_level: WhatIfEscalationLevel = "none"
    owner_clarity: WhatIfOwnerClarity = "unclear"
    reassurance_style: WhatIfReassuranceStyle = "low"
    review_path: WhatIfReviewPath = "none"
    coordination_breadth: WhatIfCoordinationBreadth = "narrow"
    outside_sharing_posture: WhatIfOutsideSharingPosture = "internal_only"
    decision_posture: WhatIfDecisionPosture = "review"
    action_tags: list[str] = Field(default_factory=list)


class WhatIfObservedOutcomeTargets(BaseModel):
    any_external_send: bool = False
    external_send_count: int = 0
    future_message_count: int = 0
    thread_end_duration_ms: int = 0
    first_follow_up_delay_ms: int = 0
    avg_follow_up_delay_ms: int = 0
    escalation_count: int = 0
    legal_involvement_count: int = 0
    attachment_recirculation_count: int = 0
    reassurance_count: int = 0


class WhatIfObservedEvidenceHeads(BaseModel):
    any_external_spread: bool = False
    outside_recipient_count: int = 0
    outside_forward_count: int = 0
    outside_attachment_spread_count: int = 0
    legal_follow_up_count: int = 0
    review_loop_count: int = 0
    markup_loop_count: int = 0
    executive_escalation_count: int = 0
    executive_mention_count: int = 0
    urgency_spike_count: int = 0
    participant_fanout: int = 0
    cc_expansion_count: int = 0
    cross_functional_loop_count: int = 0
    time_to_first_follow_up_ms: int = 0
    time_to_thread_end_ms: int = 0
    review_delay_burden_ms: int = 0
    reassurance_count: int = 0
    apology_repair_count: int = 0
    commitment_clarity_count: int = 0
    blame_pressure_count: int = 0
    internal_disagreement_count: int = 0
    attachment_recirculation_count: int = 0
    version_turn_count: int = 0


class WhatIfBusinessOutcomeHeads(BaseModel):
    enterprise_risk: float = 0.0
    commercial_position_proxy: float = 0.0
    org_strain_proxy: float = 0.0
    stakeholder_trust: float = 0.0
    execution_drag: float = 0.0


class WhatIfFutureStateHeads(BaseModel):
    regulatory_exposure: float = 0.0
    accounting_control_pressure: float = 0.0
    liquidity_stress: float = 0.0
    governance_response: float = 0.0
    evidence_control: float = 0.0
    external_confidence_pressure: float = 0.0


class WhatIfBusinessObjectivePack(BaseModel):
    pack_id: WhatIfBusinessObjectivePackId
    title: str
    summary: str
    weights: dict[str, float] = Field(default_factory=dict)
    evidence_labels: list[str] = Field(default_factory=list)


class WhatIfBusinessObjectiveScore(BaseModel):
    objective_pack_id: WhatIfBusinessObjectivePackId
    overall_score: float = 0.0
    components: dict[str, float] = Field(default_factory=dict)
    evidence: list[str] = Field(default_factory=list)


class WhatIfJudgeRubric(BaseModel):
    objective_pack_id: WhatIfBusinessObjectivePackId
    title: str
    question: str
    criteria: list[str] = Field(default_factory=list)
    decision_rule: str = ""


class WhatIfJudgedPairwiseComparison(BaseModel):
    left_candidate_id: str
    right_candidate_id: str
    preferred_candidate_id: str = ""
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    evidence_references: list[str] = Field(default_factory=list)
    rationale: str = ""


class WhatIfJudgedRanking(BaseModel):
    case_id: str
    objective_pack_id: WhatIfBusinessObjectivePackId
    judge_id: str = ""
    judge_model: str = ""
    ordered_candidate_ids: list[str] = Field(default_factory=list)
    pairwise_comparisons: list[WhatIfJudgedPairwiseComparison] = Field(
        default_factory=list
    )
    confidence: float | None = None
    uncertainty_flag: bool = False
    evidence_references: list[str] = Field(default_factory=list)
    notes: str = ""


class WhatIfAuditRecord(BaseModel):
    case_id: str
    objective_pack_id: WhatIfBusinessObjectivePackId
    submission_id: str = ""
    submitted_at: str = ""
    reviewer_id: str = ""
    ordered_candidate_ids: list[str] = Field(default_factory=list)
    pairwise_comparisons: list[WhatIfJudgedPairwiseComparison] = Field(
        default_factory=list
    )
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    status: Literal["pending", "completed"] = "pending"
    agreement_with_judge: bool | None = None
    notes: str = ""


class WhatIfPreBranchContract(BaseModel):
    case_id: str
    thread_id: str
    branch_event_id: str
    branch_event: WhatIfEventReference
    action_schema: WhatIfActionSchema = Field(default_factory=WhatIfActionSchema)
    summary_features: list[WhatIfBranchSummaryFeature] = Field(default_factory=list)
    sequence_steps: list[WhatIfSequenceStep] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class WhatIfBenchmarkDatasetRow(BaseModel):
    row_id: str
    split: WhatIfBenchmarkSplit = "train"
    thread_id: str
    branch_event_id: str
    contract: WhatIfPreBranchContract
    observed_evidence_heads: WhatIfObservedEvidenceHeads = Field(
        default_factory=WhatIfObservedEvidenceHeads
    )
    observed_business_outcomes: WhatIfBusinessOutcomeHeads = Field(
        default_factory=WhatIfBusinessOutcomeHeads
    )
    observed_future_state: WhatIfFutureStateHeads = Field(
        default_factory=WhatIfFutureStateHeads
    )
    observed_targets: WhatIfObservedOutcomeTargets = Field(
        default_factory=WhatIfObservedOutcomeTargets
    )
    observed_outcome_signals: WhatIfOutcomeSignals = Field(
        default_factory=WhatIfOutcomeSignals
    )


class WhatIfBenchmarkCandidate(BaseModel):
    candidate_id: str
    label: str
    prompt: str
    action_schema: WhatIfActionSchema = Field(default_factory=WhatIfActionSchema)
    expected_hypotheses: dict[
        WhatIfBusinessObjectivePackId, WhatIfResearchHypothesisLabel
    ] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class WhatIfBenchmarkCase(BaseModel):
    case_id: str
    title: str
    event_id: str
    thread_id: str
    summary: str = ""
    case_family: str = ""
    branch_event: WhatIfEventReference
    history_preview: list[WhatIfEventReference] = Field(default_factory=list)
    objective_dossier_paths: dict[str, str] = Field(default_factory=dict)
    candidates: list[WhatIfBenchmarkCandidate] = Field(default_factory=list)
    public_context: WhatIfPublicContext | None = None


class WhatIfPanelJudgment(BaseModel):
    case_id: str
    objective_pack_id: WhatIfObjectivePackId
    judge_id: str = ""
    ordered_candidate_ids: list[str] = Field(default_factory=list)
    confidence: float | None = None
    abstained: bool = False
    notes: str = ""


class WhatIfBenchmarkDatasetManifest(BaseModel):
    root: Path
    split_row_counts: dict[str, int] = Field(default_factory=dict)
    split_paths: dict[str, str] = Field(default_factory=dict)
    heldout_cases_path: str = ""
    judge_template_path: str = ""
    audit_template_path: str = ""
    dossier_root: str = ""
    heldout_thread_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class WhatIfBenchmarkBuildArtifacts(BaseModel):
    root: Path
    manifest_path: Path
    heldout_cases_path: Path
    judge_template_path: Path
    audit_template_path: Path
    dossier_root: Path


class WhatIfBenchmarkBuildResult(BaseModel):
    version: Literal["2"] = "2"
    label: str
    heldout_pack_id: str
    dataset: WhatIfBenchmarkDatasetManifest
    cases: list[WhatIfBenchmarkCase] = Field(default_factory=list)
    artifacts: WhatIfBenchmarkBuildArtifacts
    metadata: dict[str, Any] = Field(default_factory=dict)


class WhatIfObservedForecastMetrics(BaseModel):
    auroc_any_external_spread: float | None = None
    brier_any_external_spread: float = 0.0
    calibration_error_any_external_spread: float = 0.0
    evidence_head_mae: dict[str, float] = Field(default_factory=dict)
    business_head_mae: dict[str, float] = Field(default_factory=dict)
    objective_score_mae: dict[str, float] = Field(default_factory=dict)
    future_state_head_mae: dict[str, float] = Field(default_factory=dict)


class WhatIfBenchmarkTrainArtifacts(BaseModel):
    root: Path
    model_path: Path
    metadata_path: Path
    train_result_path: Path


class WhatIfBenchmarkTrainResult(BaseModel):
    version: Literal["1"] = "1"
    model_id: WhatIfBenchmarkModelId
    dataset_root: Path
    train_loss: float = 0.0
    validation_loss: float = 0.0
    epoch_count: int = 0
    train_row_count: int = 0
    validation_row_count: int = 0
    notes: list[str] = Field(default_factory=list)
    artifacts: WhatIfBenchmarkTrainArtifacts


class WhatIfCounterfactualCandidatePrediction(BaseModel):
    candidate: WhatIfBenchmarkCandidate
    expected_hypothesis: WhatIfResearchHypothesisLabel = "middle_expected"
    rank: int = 0
    predicted_evidence_heads: WhatIfObservedEvidenceHeads = Field(
        default_factory=WhatIfObservedEvidenceHeads
    )
    predicted_business_outcomes: WhatIfBusinessOutcomeHeads = Field(
        default_factory=WhatIfBusinessOutcomeHeads
    )
    predicted_objective_score: WhatIfBusinessObjectiveScore


class WhatIfCounterfactualObjectiveEvaluation(BaseModel):
    objective_pack: WhatIfBusinessObjectivePack
    recommended_candidate_label: str = ""
    candidates: list[WhatIfCounterfactualCandidatePrediction] = Field(
        default_factory=list
    )
    expected_order_ok: bool = False


class WhatIfBenchmarkCaseEvaluation(BaseModel):
    case: WhatIfBenchmarkCase
    objectives: list[WhatIfCounterfactualObjectiveEvaluation] = Field(
        default_factory=list
    )


class WhatIfDominanceSummary(BaseModel):
    total_checks: int = 0
    passed_checks: int = 0
    pass_rate: float = 0.0


class WhatIfBenchmarkMetricSummary(BaseModel):
    count: int = 0
    mean: float = 0.0
    std: float = 0.0
    min: float = 0.0
    max: float = 0.0


class WhatIfPanelSummary(BaseModel):
    available: bool = False
    judgment_count: int = 0
    top1_agreement: float | None = None
    pairwise_accuracy: float | None = None
    kendall_tau: float | None = None


class WhatIfJudgeSummary(BaseModel):
    available: bool = False
    judgment_count: int = 0
    top1_agreement: float | None = None
    pairwise_accuracy: float | None = None
    kendall_tau: float | None = None
    uncertainty_count: int = 0
    low_confidence_count: int = 0


class WhatIfAuditSummary(BaseModel):
    available: bool = False
    queue_count: int = 0
    completed_count: int = 0
    agreement_rate: float | None = None


class WhatIfRolloutStressSummary(BaseModel):
    available: bool = False
    compared_case_objectives: int = 0
    agreement_count: int = 0
    agreement_rate: float | None = None


class WhatIfBenchmarkJudgeArtifacts(BaseModel):
    root: Path
    result_path: Path
    audit_queue_path: Path


class WhatIfBenchmarkJudgeResult(BaseModel):
    version: Literal["1"] = "1"
    build_root: Path
    judge_model: str
    judgments: list[WhatIfJudgedRanking] = Field(default_factory=list)
    audit_queue: list[WhatIfAuditRecord] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    artifacts: WhatIfBenchmarkJudgeArtifacts


class WhatIfBenchmarkEvalArtifacts(BaseModel):
    root: Path
    eval_result_path: Path
    prediction_jsonl_path: Path


class WhatIfBenchmarkStudyRun(BaseModel):
    model_id: WhatIfBenchmarkModelId
    seed: int
    train_loss: float = 0.0
    validation_loss: float = 0.0
    observed_auroc_any_external_spread: float = 0.0
    dominance_passed_checks: int = 0
    dominance_total_checks: int = 0
    dominance_pass_rate: float = 0.0
    judge_top1_agreement: float | None = None
    judge_pairwise_accuracy: float | None = None
    business_head_mae: dict[str, float] = Field(default_factory=dict)
    objective_pass_rates: dict[WhatIfBusinessObjectivePackId, float] = Field(
        default_factory=dict
    )
    train_result_path: Path
    eval_result_path: Path


class WhatIfBenchmarkStudyModelSummary(BaseModel):
    model_id: WhatIfBenchmarkModelId
    run_count: int = 0
    seeds: list[int] = Field(default_factory=list)
    train_loss: WhatIfBenchmarkMetricSummary = Field(
        default_factory=WhatIfBenchmarkMetricSummary
    )
    validation_loss: WhatIfBenchmarkMetricSummary = Field(
        default_factory=WhatIfBenchmarkMetricSummary
    )
    observed_auroc_any_external_spread: WhatIfBenchmarkMetricSummary = Field(
        default_factory=WhatIfBenchmarkMetricSummary
    )
    dominance_passed_checks: WhatIfBenchmarkMetricSummary = Field(
        default_factory=WhatIfBenchmarkMetricSummary
    )
    dominance_pass_rate: WhatIfBenchmarkMetricSummary = Field(
        default_factory=WhatIfBenchmarkMetricSummary
    )
    judge_top1_agreement: WhatIfBenchmarkMetricSummary | None = None
    judge_pairwise_accuracy: WhatIfBenchmarkMetricSummary | None = None
    business_head_mae: dict[str, WhatIfBenchmarkMetricSummary] = Field(
        default_factory=dict
    )
    objective_pass_rates: dict[
        WhatIfBusinessObjectivePackId, WhatIfBenchmarkMetricSummary
    ] = Field(default_factory=dict)


class WhatIfBenchmarkStudyArtifacts(BaseModel):
    root: Path
    result_path: Path
    overview_path: Path


class WhatIfBenchmarkStudyResult(BaseModel):
    version: Literal["1"] = "1"
    label: str
    build_root: Path
    models: list[WhatIfBenchmarkModelId] = Field(default_factory=list)
    seeds: list[int] = Field(default_factory=list)
    runs: list[WhatIfBenchmarkStudyRun] = Field(default_factory=list)
    summaries: list[WhatIfBenchmarkStudyModelSummary] = Field(default_factory=list)
    ranked_model_ids: list[WhatIfBenchmarkModelId] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    artifacts: WhatIfBenchmarkStudyArtifacts


class WhatIfBenchmarkEvalResult(BaseModel):
    version: Literal["2"] = "2"
    model_id: WhatIfBenchmarkModelId
    dataset_root: Path
    observed_metrics: WhatIfObservedForecastMetrics
    cases: list[WhatIfBenchmarkCaseEvaluation] = Field(default_factory=list)
    dominance_summary: WhatIfDominanceSummary = Field(
        default_factory=WhatIfDominanceSummary
    )
    judge_summary: WhatIfJudgeSummary = Field(default_factory=WhatIfJudgeSummary)
    audit_summary: WhatIfAuditSummary = Field(default_factory=WhatIfAuditSummary)
    panel_summary: WhatIfPanelSummary = Field(default_factory=WhatIfPanelSummary)
    rollout_stress_summary: WhatIfRolloutStressSummary = Field(
        default_factory=WhatIfRolloutStressSummary
    )
    notes: list[str] = Field(default_factory=list)
    artifacts: WhatIfBenchmarkEvalArtifacts


__all__ = [
    "WhatIfActionSchema",
    "WhatIfAttachmentPolicy",
    "WhatIfAuditRecord",
    "WhatIfAuditSummary",
    "WhatIfBenchmarkBuildArtifacts",
    "WhatIfBenchmarkBuildResult",
    "WhatIfBenchmarkCandidate",
    "WhatIfBenchmarkCase",
    "WhatIfBenchmarkCaseEvaluation",
    "WhatIfBenchmarkDatasetManifest",
    "WhatIfBenchmarkDatasetRow",
    "WhatIfBenchmarkEvalArtifacts",
    "WhatIfBenchmarkEvalResult",
    "WhatIfBenchmarkJudgeArtifacts",
    "WhatIfBenchmarkJudgeResult",
    "WhatIfBenchmarkMetricSummary",
    "WhatIfBenchmarkModelId",
    "WhatIfBenchmarkSplit",
    "WhatIfBenchmarkStudyArtifacts",
    "WhatIfBenchmarkStudyModelSummary",
    "WhatIfBenchmarkStudyResult",
    "WhatIfBenchmarkStudyRun",
    "WhatIfBenchmarkTrainArtifacts",
    "WhatIfBenchmarkTrainResult",
    "WhatIfBusinessObjectivePack",
    "WhatIfBusinessObjectivePackId",
    "WhatIfBusinessObjectiveScore",
    "WhatIfBusinessOutcomeHeads",
    "WhatIfFutureStateHeads",
    "WhatIfCoordinationBreadth",
    "WhatIfCounterfactualCandidatePrediction",
    "WhatIfCounterfactualObjectiveEvaluation",
    "WhatIfDecisionPosture",
    "WhatIfDominanceSummary",
    "WhatIfEscalationLevel",
    "WhatIfJudgeRubric",
    "WhatIfJudgedPairwiseComparison",
    "WhatIfJudgedRanking",
    "WhatIfJudgeSummary",
    "WhatIfObjectivePackId",
    "WhatIfObservedEvidenceHeads",
    "WhatIfObservedForecastMetrics",
    "WhatIfObservedOutcomeTargets",
    "WhatIfOutsideSharingPosture",
    "WhatIfOwnerClarity",
    "WhatIfPanelJudgment",
    "WhatIfPanelSummary",
    "WhatIfPreBranchContract",
    "WhatIfReassuranceStyle",
    "WhatIfReviewPath",
    "WhatIfRolloutStressSummary",
]
