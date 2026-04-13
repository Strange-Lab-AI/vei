from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from ._base import (
    WhatIfBusinessConfidence,
    WhatIfBusinessImpactEffect,
    WhatIfBusinessImpactMagnitude,
    WhatIfBusinessStateLevel,
    WhatIfRenderFormat,
    WhatIfScenarioId,
    WhatIfSourceName,
)


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
    case_id: str = ""
    surface: str = ""
    conversation_anchor: str = ""
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
    case_id: str = ""
    surface: str = "mail"
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


class WhatIfPublicFinancialSnapshot(BaseModel):
    snapshot_id: str
    as_of: str
    kind: str
    label: str
    source_ids: list[str] = Field(default_factory=list)
    summary: str = ""
    metrics: dict[str, int | float | str] = Field(default_factory=dict)


class WhatIfPublicNewsEvent(BaseModel):
    event_id: str
    timestamp: str
    category: str
    headline: str
    summary: str = ""
    source_ids: list[str] = Field(default_factory=list)


class WhatIfPublicContext(BaseModel):
    version: str = "1"
    pack_name: str = ""
    organization_name: str = ""
    organization_domain: str = ""
    prepared_at: str = ""
    integration_hint: str = ""
    window_start: str = ""
    window_end: str = ""
    branch_timestamp: str = ""
    financial_snapshots: list[WhatIfPublicFinancialSnapshot] = Field(
        default_factory=list
    )
    public_news_events: list[WhatIfPublicNewsEvent] = Field(default_factory=list)


class WhatIfBusinessStateSnapshot(BaseModel):
    exposure: float = 0.0
    trust: float = 0.0
    coordination_load: float = 0.0
    execution_delay: float = 0.0
    deal_position: float = 0.0
    governance_pressure: float = 0.0


class WhatIfBusinessStateIndicator(BaseModel):
    state_id: str
    label: str
    value: float = 0.0
    level: WhatIfBusinessStateLevel = "medium"
    summary: str = ""


class WhatIfBusinessConsequenceEstimate(BaseModel):
    consequence_id: str
    label: str
    effect: WhatIfBusinessImpactEffect = "flat"
    magnitude: WhatIfBusinessImpactMagnitude = "flat"
    summary: str = ""


class WhatIfBusinessStateAssessment(BaseModel):
    method: str = "historical_v1"
    confidence: WhatIfBusinessConfidence = "medium"
    summary: str = ""
    snapshot: WhatIfBusinessStateSnapshot = Field(
        default_factory=WhatIfBusinessStateSnapshot
    )
    indicators: list[WhatIfBusinessStateIndicator] = Field(default_factory=list)
    implications: list[str] = Field(default_factory=list)


class WhatIfBusinessStateImpact(BaseModel):
    state_id: str
    label: str
    baseline_value: float = 0.0
    predicted_value: float = 0.0
    delta: float = 0.0
    effect: WhatIfBusinessImpactEffect = "flat"
    magnitude: WhatIfBusinessImpactMagnitude = "flat"
    summary: str = ""


class WhatIfBusinessStateChange(BaseModel):
    method: str = "forecast_v1"
    confidence: WhatIfBusinessConfidence = "medium"
    summary: str = ""
    baseline: WhatIfBusinessStateSnapshot = Field(
        default_factory=WhatIfBusinessStateSnapshot
    )
    predicted: WhatIfBusinessStateSnapshot = Field(
        default_factory=WhatIfBusinessStateSnapshot
    )
    impacts: list[WhatIfBusinessStateImpact] = Field(default_factory=list)
    consequence_estimates: list[WhatIfBusinessConsequenceEstimate] = Field(
        default_factory=list
    )
    tradeoffs: list[str] = Field(default_factory=list)
    net_effect_score: float = 0.0


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


class WhatIfEventReference(BaseModel):
    event_id: str
    timestamp: str
    actor_id: str
    target_id: str = ""
    event_type: str
    thread_id: str
    case_id: str = ""
    surface: str = ""
    conversation_anchor: str = ""
    subject: str = ""
    snippet: str = ""
    to_recipients: list[str] = Field(default_factory=list)
    cc_recipients: list[str] = Field(default_factory=list)
    has_attachment_reference: bool = False
    is_forward: bool = False
    is_reply: bool = False
    is_escalation: bool = False


class WhatIfSituationThread(BaseModel):
    thread_id: str
    subject: str
    surface: str = "mail"
    case_id: str = ""
    actor_ids: list[str] = Field(default_factory=list)
    first_timestamp: str = ""
    last_timestamp: str = ""


class WhatIfSituationLink(BaseModel):
    thread_id_a: str
    thread_id_b: str
    link_type: Literal["token", "actor_time", "actor_text", "text_time"] = "token"
    shared_actor_ids: list[str] = Field(default_factory=list)
    shared_terms: list[str] = Field(default_factory=list)
    time_gap_ms: int = 0
    weight: float = 0.0


class WhatIfSituationCluster(BaseModel):
    situation_id: str
    label: str
    thread_ids: list[str] = Field(default_factory=list)
    surfaces: list[str] = Field(default_factory=list)
    actor_ids: list[str] = Field(default_factory=list)
    case_ids: list[str] = Field(default_factory=list)
    first_timestamp: str = ""
    last_timestamp: str = ""
    link_count: int = 0
    anchor_terms: list[str] = Field(default_factory=list)


class WhatIfSituationGraph(BaseModel):
    links: list[WhatIfSituationLink] = Field(default_factory=list)
    clusters: list[WhatIfSituationCluster] = Field(default_factory=list)


class WhatIfSituationContext(BaseModel):
    situation_id: str
    label: str
    surfaces: list[str] = Field(default_factory=list)
    actor_ids: list[str] = Field(default_factory=list)
    anchor_terms: list[str] = Field(default_factory=list)
    related_threads: list[WhatIfSituationThread] = Field(default_factory=list)
    related_history: list[WhatIfEventReference] = Field(default_factory=list)


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


class WhatIfCaseSummary(BaseModel):
    case_id: str
    title: str = ""
    event_count: int = 0
    thread_count: int = 0
    surfaces: list[str] = Field(default_factory=list)
    thread_ids: list[str] = Field(default_factory=list)
    first_timestamp: str = ""
    last_timestamp: str = ""
    anchor_tokens: list[str] = Field(default_factory=list)


class WhatIfCaseRecord(BaseModel):
    record_id: str
    provider: str
    surface: str = ""
    label: str
    summary: str = ""
    related_ids: list[str] = Field(default_factory=list)


class WhatIfCaseContext(BaseModel):
    case_id: str
    title: str = ""
    related_history: list[WhatIfEventReference] = Field(default_factory=list)
    records: list[WhatIfCaseRecord] = Field(default_factory=list)


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


class WhatIfWorld(BaseModel):
    source: WhatIfSourceName = "enron"
    source_dir: Path
    summary: WhatIfWorldSummary
    scenarios: list[WhatIfScenario] = Field(default_factory=list)
    actors: list[WhatIfActorProfile] = Field(default_factory=list)
    threads: list[WhatIfThreadSummary] = Field(default_factory=list)
    cases: list[WhatIfCaseSummary] = Field(default_factory=list)
    events: list[WhatIfEvent] = Field(default_factory=list)
    situation_graph: WhatIfSituationGraph | None = None
    metadata: dict[str, str | int | float | bool] = Field(default_factory=dict)
    public_context: WhatIfPublicContext | None = None

    @property
    def rosetta_dir(self) -> Path:
        return self.source_dir


__all__ = [
    "WhatIfActorImpact",
    "WhatIfActorProfile",
    "WhatIfArtifactFlags",
    "WhatIfBusinessConfidence",
    "WhatIfBusinessConsequenceEstimate",
    "WhatIfBusinessImpactEffect",
    "WhatIfBusinessImpactMagnitude",
    "WhatIfBusinessStateAssessment",
    "WhatIfBusinessStateChange",
    "WhatIfBusinessStateImpact",
    "WhatIfBusinessStateIndicator",
    "WhatIfBusinessStateLevel",
    "WhatIfBusinessStateSnapshot",
    "WhatIfCaseContext",
    "WhatIfCaseRecord",
    "WhatIfCaseSummary",
    "WhatIfConsequence",
    "WhatIfEvent",
    "WhatIfEventMatch",
    "WhatIfEventReference",
    "WhatIfEventSearchResult",
    "WhatIfPublicContext",
    "WhatIfPublicFinancialSnapshot",
    "WhatIfPublicNewsEvent",
    "WhatIfRenderFormat",
    "WhatIfResult",
    "WhatIfScenario",
    "WhatIfScenarioId",
    "WhatIfSituationCluster",
    "WhatIfSituationContext",
    "WhatIfSituationGraph",
    "WhatIfSituationLink",
    "WhatIfSituationThread",
    "WhatIfSourceName",
    "WhatIfThreadImpact",
    "WhatIfThreadSummary",
    "WhatIfWorld",
    "WhatIfWorldSummary",
]
