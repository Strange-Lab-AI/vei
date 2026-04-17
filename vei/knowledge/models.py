from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

KnowledgeAssetKind = Literal[
    "transcript",
    "deliverable",
    "proposal",
    "sop",
    "pricing",
    "note",
    "email_summary",
    "meeting_notes",
    "metric_snapshot",
    "brief",
    "weekly_review",
]
KnowledgeAssetStatus = Literal["active", "stale", "superseded", "expired"]
KnowledgeEdgeKind = Literal["cites", "supersedes", "derived_from", "applies_to"]
KnowledgeComposeMode = Literal["heuristic_baseline", "llm"]


class KnowledgeProvenance(BaseModel):
    source: str
    source_id: str = ""
    import_id: str = ""
    captured_at: str = ""
    shelf_life_ms: Optional[int] = None
    authority: float = 1.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class KnowledgeClaim(BaseModel):
    claim_id: str
    text: str
    citation_asset_ids: List[str] = Field(default_factory=list)
    section: Optional[str] = None
    metric_key: Optional[str] = None
    metric_value: Optional[float | int | str] = None


class KnowledgeCitationSpan(BaseModel):
    asset_id: str
    marker: str
    section: Optional[str] = None
    quote: str = ""


class KnowledgeMetricBinding(BaseModel):
    metric_key: str
    expected_value: float | int | str
    cited_asset_id: str
    source_field: str = ""


class KnowledgeCompositionValidation(BaseModel):
    citations_present: bool = True
    citations_resolve: bool = True
    sources_within_shelf_life: bool = True
    numbers_reconcile: bool = True
    format_matches_template: bool = True
    issues: List[str] = Field(default_factory=list)


class KnowledgeCompositionDetails(BaseModel):
    target: Literal["proposal", "brief", "weekly_review"] = "proposal"
    template_id: str = ""
    subject_object_ref: str = ""
    mode: KnowledgeComposeMode = "heuristic_baseline"
    provider: Optional[str] = None
    model: Optional[str] = None
    prompt: str = ""
    required_sections: List[str] = Field(default_factory=list)
    sections: List[str] = Field(default_factory=list)
    claims: List[KnowledgeClaim] = Field(default_factory=list)
    citation_spans: List[KnowledgeCitationSpan] = Field(default_factory=list)
    metric_bindings: List[KnowledgeMetricBinding] = Field(default_factory=list)
    validation: KnowledgeCompositionValidation = Field(
        default_factory=KnowledgeCompositionValidation
    )
    reviewer_feedback: List[str] = Field(default_factory=list)


class KnowledgeAsset(BaseModel):
    asset_id: str
    kind: KnowledgeAssetKind
    title: str
    body: str
    summary: str = ""
    tags: List[str] = Field(default_factory=list)
    provenance: KnowledgeProvenance
    linked_object_refs: List[str] = Field(default_factory=list)
    # Older asset ids that this asset replaces.
    supersedes: List[str] = Field(default_factory=list)
    derived_from: List[str] = Field(default_factory=list)
    status: KnowledgeAssetStatus = "active"
    metrics: Dict[str, float | int | str] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    composition: Optional[KnowledgeCompositionDetails] = None


class KnowledgeEdge(BaseModel):
    edge_id: str
    kind: KnowledgeEdgeKind
    from_asset_id: str
    to_ref: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class KnowledgeRetrieveRequest(BaseModel):
    query: str = ""
    scope_refs: List[str] = Field(default_factory=list)
    kinds: List[KnowledgeAssetKind] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    limit: int = 8
    now_ms: Optional[int] = None


class KnowledgeRetrieveHit(BaseModel):
    asset: KnowledgeAsset
    score: float
    reasons: List[str] = Field(default_factory=list)


class KnowledgeComposeRequest(BaseModel):
    target: Literal["proposal", "brief", "weekly_review"] = "proposal"
    template_id: str = ""
    subject_object_ref: str
    scope_refs: List[str] = Field(default_factory=list)
    kinds: List[KnowledgeAssetKind] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    seed_outline: List[str] = Field(default_factory=list)
    prompt: str = ""
    mode: KnowledgeComposeMode = "heuristic_baseline"
    provider: str = "openai"
    model: str = ""
    limit: int = 8


class KnowledgeComposeResult(BaseModel):
    artifact: KnowledgeAsset
    retrieved_assets: List[KnowledgeAsset] = Field(default_factory=list)
    validation: KnowledgeCompositionValidation = Field(
        default_factory=KnowledgeCompositionValidation
    )
    notes: List[str] = Field(default_factory=list)
    usage: Dict[str, Any] = Field(default_factory=dict)


class KnowledgeStoreSnapshot(BaseModel):
    assets: Dict[str, KnowledgeAsset] = Field(default_factory=dict)
    edges: List[KnowledgeEdge] = Field(default_factory=list)
    events: List[Dict[str, Any]] = Field(default_factory=list)
    asset_seq: int = 1
    edge_seq: int = 1
    metadata: Dict[str, Any] = Field(default_factory=dict)
