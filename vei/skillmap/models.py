"""Company skill-map Pydantic schema and literals."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

SkillStatus = Literal["draft", "active", "gap", "retired"]
SkillCandidateType = Literal[
    "flagship_skill",
    "support_skill",
    "workflow",
    "preprocessor",
    "gap",
]
SkillExecutionMode = Literal["read_only", "shadow", "approval_gated"]
SkillFreshnessStatus = Literal["fresh", "stale", "unknown"]
SkillIssueSeverity = Literal["error", "warning"]
SkillReviewStatus = Literal["unreviewed", "reviewed"]
SkillReplayBackend = Literal["historical_replay", "counterfactual_probe"]
SkillReplayOutcome = Literal["supported", "partial", "unsupported", "not_tested"]
SkillDeploymentReadiness = Literal[
    "not_tested",
    "needs_review",
    "shadow_ready",
    "activation_candidate",
]

CONTEXT_SNAPSHOT_FILE = "context_snapshot.json"
SKILLMAP_CLUSTER_TOOL = "skillmap.cluster"
SKILLMAP_LLM_TOOL = "skillmap.propose"

USEFULNESS_SCORE_KEYS = [
    "company_specificity",
    "repeat_frequency",
    "business_consequence",
    "actionability",
    "evidence_coverage",
    "risk_if_wrong",
    "replay_testability",
]


class SkillEvidenceRef(BaseModel):
    ref_type: Literal[
        "event",
        "evidence_digest",
        "knowledge_asset",
        "case",
        "graph_plan",
        "source",
        "structure_hypothesis",
    ]
    ref_id: str
    source: str = ""
    surface: str = ""
    title: str = ""
    timestamp: str = ""
    snippet: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class SkillTrigger(BaseModel):
    description: str
    signals: list[str] = Field(default_factory=list)


class SkillOutputArtifact(BaseModel):
    artifact_id: str
    title: str
    kind: str = ""
    schema_hint: str = ""
    required: bool = True


class SkillStep(BaseModel):
    step_id: str
    instruction: str
    tool: str = ""
    graph_domain: str = ""
    graph_action: str = ""
    args: dict[str, Any] = Field(default_factory=dict)
    read_only: bool = True
    requires_approval: bool = False


class SkillReplayResult(BaseModel):
    replay_id: str
    backend: SkillReplayBackend = "historical_replay"
    outcome: SkillReplayOutcome = "not_tested"
    case_id: str = ""
    thread_id: str = ""
    branch_event_id: str = ""
    branch_timestamp: str = ""
    historical_event_count: int = 0
    future_event_count: int = 0
    matched_event_count: int = 0
    alignment_score: float = 0.0
    counterfactual_prompt: str = ""
    observed_future_surfaces: list[str] = Field(default_factory=list)
    expected_actions: list[str] = Field(default_factory=list)
    evidence_refs: list[SkillEvidenceRef] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class CompanySkill(BaseModel):
    skill_id: str
    title: str
    summary: str
    status: SkillStatus = "draft"
    domain: str = ""
    candidate_type: SkillCandidateType = "support_skill"
    usefulness_score: float = 0.0
    usefulness_rationale: str = ""
    trigger: SkillTrigger
    negative_triggers: list[str] = Field(default_factory=list)
    goal: str
    prerequisites: list[str] = Field(default_factory=list)
    steps: list[SkillStep] = Field(default_factory=list)
    output_artifacts: list[SkillOutputArtifact] = Field(default_factory=list)
    evidence_refs: list[SkillEvidenceRef] = Field(default_factory=list)
    allowed_actions: list[str] = Field(default_factory=list)
    blocked_actions: list[str] = Field(default_factory=list)
    freshness_status: SkillFreshnessStatus = "unknown"
    replay_score: float = 0.0
    replay_results: list[SkillReplayResult] = Field(default_factory=list)
    replay_checks: list[str] = Field(default_factory=list)
    deployment_readiness: SkillDeploymentReadiness = "not_tested"
    confidence: float = 0.0
    owner: str = ""
    reviewer: str = ""
    review_status: SkillReviewStatus = "unreviewed"
    execution_mode: SkillExecutionMode = "shadow"
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SkillMapGap(BaseModel):
    gap_id: str
    title: str
    severity: SkillIssueSeverity = "warning"
    reason: str
    recommendation: str
    evidence_refs: list[SkillEvidenceRef] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SkillValidationIssue(BaseModel):
    severity: SkillIssueSeverity
    code: str
    message: str
    skill_id: str = ""
    gap_id: str = ""


class SkillMapValidation(BaseModel):
    ok: bool = True
    error_count: int = 0
    warning_count: int = 0
    active_skill_count: int = 0
    draft_skill_count: int = 0
    gap_count: int = 0
    issues: list[SkillValidationIssue] = Field(default_factory=list)


class CompanySkillMap(BaseModel):
    schema_version: Literal["company_skill_map_v1"] = "company_skill_map_v1"
    organization_name: str
    organization_domain: str = ""
    generated_at: str
    source_ref: str
    source_providers: list[str] = Field(default_factory=list)
    canonical_event_count: int = 0
    skill_count: int = 0
    skills: list[CompanySkill] = Field(default_factory=list)
    gaps: list[SkillMapGap] = Field(default_factory=list)
    validation: SkillMapValidation = Field(default_factory=SkillMapValidation)
    metadata: dict[str, Any] = Field(default_factory=dict)
