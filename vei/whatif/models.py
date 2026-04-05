from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

WhatIfSourceName = Literal["enron"]
WhatIfScenarioId = Literal[
    "compliance_gateway",
    "escalation_firewall",
    "external_dlp",
    "approval_chain_enforcement",
]
WhatIfRenderFormat = Literal["json", "markdown"]
WhatIfExperimentMode = Literal["llm", "e_jepa", "e_jepa_proxy", "both"]
WhatIfForecastBackend = Literal["e_jepa", "e_jepa_proxy"]


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
    rosetta_dir: Path
    summary: WhatIfWorldSummary
    scenarios: list[WhatIfScenario] = Field(default_factory=list)
    actors: list[WhatIfActorProfile] = Field(default_factory=list)
    threads: list[WhatIfThreadSummary] = Field(default_factory=list)
    events: list[WhatIfEvent] = Field(default_factory=list)
    metadata: dict[str, str | int | float | bool] = Field(default_factory=dict)


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
