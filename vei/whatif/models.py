from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

WhatIfSourceName = Literal["enron", "mail_archive"]
WhatIfScenarioId = Literal[
    "compliance_gateway",
    "escalation_firewall",
    "external_dlp",
    "approval_chain_enforcement",
]
WhatIfRenderFormat = Literal["json", "markdown"]
WhatIfExperimentMode = Literal["llm", "e_jepa", "e_jepa_proxy", "both"]
WhatIfForecastBackend = Literal["e_jepa", "e_jepa_proxy"]
WhatIfOutcomeBackendId = Literal[
    "e_jepa",
    "e_jepa_proxy",
    "ft_transformer",
    "ts2vec",
    "g_transformer",
    "decision_transformer",
    "trajectory_transformer",
    "dreamer_v3",
]
WhatIfObjectivePackId = Literal[
    "contain_exposure",
    "reduce_delay",
    "protect_relationship",
]
WhatIfResearchHypothesisLabel = Literal[
    "best_expected",
    "middle_expected",
    "worst_expected",
]
WhatIfBackendScoreStatus = Literal["ok", "skipped", "error", "fallback"]
WhatIfBenchmarkModelId = Literal[
    "jepa_latent",
    "ft_transformer",
    "sequence_transformer",
    "treatment_transformer",
]
WhatIfBusinessObjectivePackId = Literal[
    "minimize_enterprise_risk",
    "protect_commercial_position",
    "reduce_org_strain",
    "preserve_stakeholder_trust",
    "maintain_execution_velocity",
]
WhatIfBenchmarkSplit = Literal["train", "validation", "test", "heldout"]
WhatIfAttachmentPolicy = Literal["none", "present", "sanitized"]
WhatIfEscalationLevel = Literal["none", "manager", "executive"]
WhatIfOwnerClarity = Literal["unclear", "single_owner", "multi_owner"]
WhatIfReassuranceStyle = Literal["low", "medium", "high"]
WhatIfReviewPath = Literal[
    "none",
    "internal_legal",
    "outside_counsel",
    "business_owner",
    "cross_functional",
    "hr",
    "executive",
]
WhatIfCoordinationBreadth = Literal["single_owner", "narrow", "targeted", "broad"]
WhatIfOutsideSharingPosture = Literal[
    "internal_only",
    "status_only",
    "limited_external",
    "broad_external",
]
WhatIfDecisionPosture = Literal["hold", "review", "resolve", "escalate"]


class WhatIfArtifactFlags(BaseModel):
    consult_legal_specialist: bool = False
    consult_trading_specialist: bool = False
    has_attachment_reference: bool = False
    is_escalation: bool = False
    is_forward: bool = False
    is_reply: bool = False
    cc_count: int = 0
    bcc_count: int = 0
    to_count: int = 0
    to_recipients: list[str] = Field(default_factory=list)
    cc_recipients: list[str] = Field(default_factory=list)
    subject: str = ""
    norm_subject: str = ""
    body_sha1: str = ""
    custodian_id: str = ""
    message_id: str = ""
    folder: str = ""
    source: str = ""


class WhatIfEvent(BaseModel):
    event_id: str
    timestamp: str
    timestamp_ms: int
    actor_id: str
    target_id: str = ""
    event_type: str
    thread_id: str
    subject: str = ""
    snippet: str = ""
    flags: WhatIfArtifactFlags = Field(default_factory=WhatIfArtifactFlags)


class WhatIfActorProfile(BaseModel):
    actor_id: str
    email: str
    display_name: str
    custodian_ids: list[str] = Field(default_factory=list)
    event_count: int = 0
    sent_count: int = 0
    received_count: int = 0
    flagged_event_count: int = 0


class WhatIfThreadSummary(BaseModel):
    thread_id: str
    subject: str
    event_count: int = 0
    actor_ids: list[str] = Field(default_factory=list)
    first_timestamp: str = ""
    last_timestamp: str = ""
    legal_event_count: int = 0
    trading_event_count: int = 0
    escalation_event_count: int = 0
    assignment_event_count: int = 0
    approval_event_count: int = 0
    forward_event_count: int = 0
    attachment_event_count: int = 0
    external_recipient_event_count: int = 0
    event_type_counts: dict[str, int] = Field(default_factory=dict)


class WhatIfScenario(BaseModel):
    scenario_id: WhatIfScenarioId
    title: str
    description: str
    decision_branches: list[str] = Field(default_factory=list)


class WhatIfWorldSummary(BaseModel):
    source: WhatIfSourceName = "enron"
    organization_name: str = ""
    organization_domain: str = ""
    event_count: int = 0
    thread_count: int = 0
    actor_count: int = 0
    custodian_count: int = 0
    first_timestamp: str = ""
    last_timestamp: str = ""
    event_type_counts: dict[str, int] = Field(default_factory=dict)
    key_actor_ids: list[str] = Field(default_factory=list)


class WhatIfWorld(BaseModel):
    source: WhatIfSourceName = "enron"
    source_dir: Path
    summary: WhatIfWorldSummary
    scenarios: list[WhatIfScenario] = Field(default_factory=list)
    actors: list[WhatIfActorProfile] = Field(default_factory=list)
    threads: list[WhatIfThreadSummary] = Field(default_factory=list)
    events: list[WhatIfEvent] = Field(default_factory=list)
    metadata: dict[str, str | int | float | bool] = Field(default_factory=dict)

    @property
    def rosetta_dir(self) -> Path:
        return self.source_dir


class WhatIfActorImpact(BaseModel):
    actor_id: str
    display_name: str
    affected_event_count: int = 0
    affected_thread_count: int = 0
    reasons: list[str] = Field(default_factory=list)


class WhatIfThreadImpact(BaseModel):
    thread_id: str
    subject: str
    affected_event_count: int = 0
    participant_count: int = 0
    reasons: list[str] = Field(default_factory=list)


class WhatIfConsequence(BaseModel):
    thread_id: str
    subject: str
    actor_id: str = ""
    detail: str
    severity: Literal["low", "medium", "high"] = "medium"


class WhatIfResult(BaseModel):
    scenario: WhatIfScenario
    prompt: str | None = None
    world_summary: WhatIfWorldSummary
    matched_event_count: int = 0
    affected_thread_count: int = 0
    affected_actor_count: int = 0
    blocked_forward_count: int = 0
    blocked_escalation_count: int = 0
    delayed_assignment_count: int = 0
    timeline_impact: str = ""
    top_actors: list[WhatIfActorImpact] = Field(default_factory=list)
    top_threads: list[WhatIfThreadImpact] = Field(default_factory=list)
    top_consequences: list[WhatIfConsequence] = Field(default_factory=list)
    decision_branches: list[str] = Field(default_factory=list)


class WhatIfForecast(BaseModel):
    backend: Literal["historical", "heuristic", "e_jepa", "e_jepa_proxy"] = "historical"
    future_event_count: int = 0
    future_escalation_count: int = 0
    future_assignment_count: int = 0
    future_approval_count: int = 0
    future_external_event_count: int = 0
    risk_score: float = 0.0
    summary: str = ""


class WhatIfEventReference(BaseModel):
    event_id: str
    timestamp: str
    actor_id: str
    target_id: str = ""
    event_type: str
    thread_id: str
    subject: str = ""
    snippet: str = ""
    to_recipients: list[str] = Field(default_factory=list)
    cc_recipients: list[str] = Field(default_factory=list)
    has_attachment_reference: bool = False
    is_forward: bool = False
    is_reply: bool = False
    is_escalation: bool = False


class WhatIfEventMatch(BaseModel):
    event: WhatIfEventReference
    match_reasons: list[str] = Field(default_factory=list)
    reason_labels: list[str] = Field(default_factory=list)
    thread_event_count: int = 0
    participant_count: int = 0


class WhatIfEventSearchResult(BaseModel):
    source: WhatIfSourceName = "enron"
    filters: dict[str, str | int | bool] = Field(default_factory=dict)
    match_count: int = 0
    truncated: bool = False
    matches: list[WhatIfEventMatch] = Field(default_factory=list)


class WhatIfEpisodeManifest(BaseModel):
    version: Literal["1", "2"] = "2"
    source: WhatIfSourceName = "enron"
    source_dir: Path
    workspace_root: Path
    organization_name: str
    organization_domain: str
    thread_id: str
    thread_subject: str
    branch_event_id: str
    branch_timestamp: str
    branch_event: WhatIfEventReference
    history_message_count: int = 0
    future_event_count: int = 0
    baseline_dataset_path: str
    content_notice: str
    actor_ids: list[str] = Field(default_factory=list)
    baseline_future_preview: list[WhatIfEventReference] = Field(default_factory=list)
    forecast: WhatIfForecast = Field(default_factory=WhatIfForecast)


class WhatIfEpisodeMaterialization(BaseModel):
    manifest_path: Path
    bundle_path: Path
    context_snapshot_path: Path
    baseline_dataset_path: Path
    workspace_root: Path
    organization_name: str
    organization_domain: str
    thread_id: str
    branch_event_id: str
    branch_event: WhatIfEventReference
    history_message_count: int = 0
    future_event_count: int = 0
    baseline_future_preview: list[WhatIfEventReference] = Field(default_factory=list)
    forecast: WhatIfForecast = Field(default_factory=WhatIfForecast)


class WhatIfReplaySummary(BaseModel):
    workspace_root: Path
    baseline_dataset_path: Path
    scheduled_event_count: int = 0
    delivered_event_count: int = 0
    current_time_ms: int = 0
    pending_events: dict[str, int] = Field(default_factory=dict)
    inbox_count: int = 0
    top_subjects: list[str] = Field(default_factory=list)
    baseline_future_preview: list[WhatIfEventReference] = Field(default_factory=list)
    forecast: WhatIfForecast = Field(default_factory=WhatIfForecast)


class WhatIfInterventionSpec(BaseModel):
    label: str
    prompt: str
    objective: str = ""
    scenario_id: str | None = None
    thread_id: str | None = None
    branch_event_id: str | None = None


class WhatIfLLMGeneratedMessage(BaseModel):
    actor_id: str
    to: str
    subject: str
    body_text: str
    delay_ms: int
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


class WhatIfForecastDelta(BaseModel):
    risk_score_delta: float = 0.0
    future_event_delta: int = 0
    escalation_delta: int = 0
    assignment_delta: int = 0
    approval_delta: int = 0
    external_event_delta: int = 0


class WhatIfForecastArtifacts(BaseModel):
    cache_root: Path | None = None
    dataset_root: Path | None = None
    checkpoint_path: Path | None = None
    decoder_path: Path | None = None


class WhatIfForecastResult(BaseModel):
    status: Literal["ok", "skipped", "error"] = "ok"
    backend: Literal["e_jepa", "e_jepa_proxy"] = "e_jepa_proxy"
    prompt: str
    summary: str = ""
    baseline: WhatIfForecast = Field(default_factory=WhatIfForecast)
    predicted: WhatIfForecast = Field(default_factory=WhatIfForecast)
    delta: WhatIfForecastDelta = Field(default_factory=WhatIfForecastDelta)
    branch_event: WhatIfEventReference | None = None
    horizon_event_count: int = 0
    surprise_score: float | None = None
    current_state_summary: dict[str, float] = Field(default_factory=dict)
    predicted_state_summary: dict[str, float] = Field(default_factory=dict)
    actual_state_summary: dict[str, float] = Field(default_factory=dict)
    artifacts: WhatIfForecastArtifacts | None = None
    notes: list[str] = Field(default_factory=list)
    error: str | None = None


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
    forecast_result: WhatIfForecastResult | None = None
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
    forecast_result: WhatIfForecastResult


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
    baseline_forecast: WhatIfForecast = Field(default_factory=WhatIfForecast)
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


class WhatIfObservedForecastMetrics(BaseModel):
    auroc_any_external_spread: float | None = None
    brier_any_external_spread: float = 0.0
    calibration_error_any_external_spread: float = 0.0
    evidence_head_mae: dict[str, float] = Field(default_factory=dict)
    business_head_mae: dict[str, float] = Field(default_factory=dict)
    objective_score_mae: dict[str, float] = Field(default_factory=dict)


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
