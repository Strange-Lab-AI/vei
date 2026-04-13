from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from ._base import WhatIfSourceName
from .forecast import WhatIfHistoricalScore
from .world import (
    WhatIfBusinessStateAssessment,
    WhatIfCaseContext,
    WhatIfEventReference,
    WhatIfPublicContext,
    WhatIfSituationContext,
)


class WhatIfEpisodeManifest(BaseModel):
    version: Literal["1", "2"] = "2"
    source: WhatIfSourceName = "enron"
    source_dir: Path
    workspace_root: Path
    organization_name: str
    organization_domain: str
    thread_id: str
    thread_subject: str
    case_id: str = ""
    surface: str = "mail"
    branch_event_id: str
    branch_timestamp: str
    branch_event: WhatIfEventReference
    history_message_count: int = 0
    future_event_count: int = 0
    baseline_dataset_path: str
    content_notice: str
    actor_ids: list[str] = Field(default_factory=list)
    history_preview: list[WhatIfEventReference] = Field(default_factory=list)
    baseline_future_preview: list[WhatIfEventReference] = Field(default_factory=list)
    forecast: WhatIfHistoricalScore = Field(default_factory=WhatIfHistoricalScore)
    public_context: WhatIfPublicContext | None = None
    case_context: WhatIfCaseContext | None = None
    situation_context: WhatIfSituationContext | None = None
    historical_business_state: WhatIfBusinessStateAssessment | None = None


class WhatIfEpisodeMaterialization(BaseModel):
    manifest_path: Path
    bundle_path: Path
    context_snapshot_path: Path
    baseline_dataset_path: Path
    workspace_root: Path
    organization_name: str
    organization_domain: str
    thread_id: str
    case_id: str = ""
    surface: str = "mail"
    branch_event_id: str
    branch_event: WhatIfEventReference
    history_message_count: int = 0
    future_event_count: int = 0
    history_preview: list[WhatIfEventReference] = Field(default_factory=list)
    baseline_future_preview: list[WhatIfEventReference] = Field(default_factory=list)
    forecast: WhatIfHistoricalScore = Field(default_factory=WhatIfHistoricalScore)
    public_context: WhatIfPublicContext | None = None
    case_context: WhatIfCaseContext | None = None
    situation_context: WhatIfSituationContext | None = None
    historical_business_state: WhatIfBusinessStateAssessment | None = None


class WhatIfReplaySummary(BaseModel):
    workspace_root: Path
    baseline_dataset_path: Path
    surface: str = "mail"
    scheduled_event_count: int = 0
    delivered_event_count: int = 0
    current_time_ms: int = 0
    pending_events: dict[str, int] = Field(default_factory=dict)
    inbox_count: int = 0
    top_subjects: list[str] = Field(default_factory=list)
    visible_item_count: int = 0
    top_items: list[str] = Field(default_factory=list)
    baseline_future_preview: list[WhatIfEventReference] = Field(default_factory=list)
    forecast: WhatIfHistoricalScore = Field(default_factory=WhatIfHistoricalScore)


class WhatIfDecisionOption(BaseModel):
    option_id: str
    label: str
    summary: str = ""
    prompt: str


class WhatIfDecisionScene(BaseModel):
    source: WhatIfSourceName = "enron"
    organization_name: str
    organization_domain: str
    thread_id: str
    thread_subject: str
    case_id: str = ""
    surface: str = "mail"
    branch_event_id: str
    branch_event: WhatIfEventReference
    history_message_count: int = 0
    future_event_count: int = 0
    content_notice: str = ""
    branch_summary: str = ""
    historical_action_summary: str = ""
    historical_outcome_summary: str = ""
    stakes_summary: str = ""
    decision_question: str = ""
    history_preview: list[WhatIfEventReference] = Field(default_factory=list)
    historical_future_preview: list[WhatIfEventReference] = Field(default_factory=list)
    candidate_options: list[WhatIfDecisionOption] = Field(default_factory=list)
    public_context: WhatIfPublicContext | None = None
    case_context: WhatIfCaseContext | None = None
    situation_context: WhatIfSituationContext | None = None
    historical_business_state: WhatIfBusinessStateAssessment | None = None


__all__ = [
    "WhatIfDecisionOption",
    "WhatIfDecisionScene",
    "WhatIfEpisodeManifest",
    "WhatIfEpisodeMaterialization",
    "WhatIfReplaySummary",
]
