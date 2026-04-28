from __future__ import annotations

from pydantic import BaseModel, Field

from vei.whatif.api import WhatIfBusinessOutcomeHeads, WhatIfFutureStateHeads


class PublicDemoCandidateInput(BaseModel):
    label: str
    action: str


class PublicDemoEvidenceEvent(BaseModel):
    event_id: str
    timestamp: str
    subject: str
    snippet: str
    actor_id: str = ""
    surface: str = ""


class PublicDemoTimelinePoint(BaseModel):
    event_id: str
    timestamp: str
    label: str
    summary: str
    visible_event_count: int = 0
    is_default: bool = False


class PublicDemoSourceSummary(BaseModel):
    source_id: str
    title: str
    summary: str
    source_dir: str | None = None
    default_topic: str = "all_public_record"
    default_as_of: str = "1837-09-06"
    first_timestamp: str = ""
    last_timestamp: str = ""
    event_count: int = 0


class PublicDemoStatusResponse(BaseModel):
    available: bool = False
    source: PublicDemoSourceSummary | None = None
    topic: str = "all_public_record"
    as_of: str = "1837-09-06"
    historical_cutoff: str = ""
    state_summary: str = ""
    timeline_points: list[PublicDemoTimelinePoint] = Field(default_factory=list)
    evidence_events: list[PublicDemoEvidenceEvent] = Field(default_factory=list)
    suggested_candidate_actions: list[PublicDemoCandidateInput] = Field(
        default_factory=list
    )
    scoring_available: bool = False
    scoring_source: str = "live_jepa"
    scoring_checkpoint_path: str = ""
    scoring_unavailable_reason: str = ""
    caveat: str = ""
    unavailable_reason: str = ""


class PublicDemoChatRequest(BaseModel):
    source_id: str = "news_americanstories_public_world"
    as_of: str = "1837-09-06"
    message: str
    selected_event_ids: list[str] = Field(default_factory=list)
    topic: str = "all_public_record"


class PublicDemoChatResponse(BaseModel):
    source_id: str
    topic: str
    as_of: str
    historical_cutoff: str
    assistant_text: str
    cited_event_ids: list[str] = Field(default_factory=list)
    cited_events: list[PublicDemoEvidenceEvent] = Field(default_factory=list)
    suggested_candidate_actions: list[PublicDemoCandidateInput] = Field(
        default_factory=list
    )
    caveat: str = ""


class PublicDemoScoreRequest(BaseModel):
    source_id: str = "news_americanstories_public_world"
    as_of: str = "1837-09-06"
    topic: str = "all_public_record"
    decision_title: str = ""
    candidates: list[PublicDemoCandidateInput] = Field(default_factory=list)


class PublicDemoScoredCandidate(BaseModel):
    candidate_id: str
    rank: int
    label: str
    action: str
    score: float = 0.0
    score_label: str = "strategic usefulness"
    predicted_business_heads: WhatIfBusinessOutcomeHeads = Field(
        default_factory=WhatIfBusinessOutcomeHeads
    )
    predicted_future_heads: WhatIfFutureStateHeads = Field(
        default_factory=WhatIfFutureStateHeads
    )
    reason: str = ""
    source: str = "live_jepa"


class PublicDemoScoreResponse(BaseModel):
    source_id: str
    topic: str
    as_of: str
    decision_title: str
    historical_cutoff: str
    scoring_source: str
    scoring_artifact_path: str = ""
    scoring_checkpoint_path: str = ""
    candidates: list[PublicDemoScoredCandidate] = Field(default_factory=list)
    evidence_events: list[PublicDemoEvidenceEvent] = Field(default_factory=list)
    caveat: str = ""


__all__ = [
    "PublicDemoCandidateInput",
    "PublicDemoChatRequest",
    "PublicDemoChatResponse",
    "PublicDemoEvidenceEvent",
    "PublicDemoScoredCandidate",
    "PublicDemoScoreRequest",
    "PublicDemoScoreResponse",
    "PublicDemoSourceSummary",
    "PublicDemoStatusResponse",
    "PublicDemoTimelinePoint",
]
