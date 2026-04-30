"""Typed report models for VEI Control provenance."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ActivityNode(BaseModel):
    id: str
    kind: str
    label: str = ""
    event_ids: list[str] = Field(default_factory=list)


class EvidenceQuality(BaseModel):
    source_granularity: str = ""
    source_integrity: str = "imported"
    time_confidence: str = "unknown"
    object_confidence: str = "absent"
    link_confidence: str = "unknown"
    identity_confidence: str = "absent"
    warnings: list[str] = Field(default_factory=list)


class ActivityEdge(BaseModel):
    source: str
    target: str
    kind: str
    event_ids: list[str] = Field(default_factory=list)
    link_kind: str = ""
    confidence: str = ""


class CompanyActivityGraph(BaseModel):
    schema_version: str = "company_activity_graph_v1"
    node_count: int = 0
    edge_count: int = 0
    nodes: list[ActivityNode] = Field(default_factory=list)
    edges: list[ActivityEdge] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class TimelineItem(BaseModel):
    event_id: str
    ts_ms: int = 0
    kind: str
    actor_id: str = ""
    object_ids: list[str] = Field(default_factory=list)
    source_id: str = ""
    source_granularity: str = ""
    summary: str = ""
    link_refs: list[str] = Field(default_factory=list)
    evidence_quality: EvidenceQuality | None = None


class ProvenanceTimeline(BaseModel):
    schema_version: str = "provenance_timeline_v1"
    event_count: int = 0
    items: list[TimelineItem] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class BlastRadiusReport(BaseModel):
    schema_version: str = "blast_radius_v1"
    anchor_event_id: str
    reached_nodes: list[ActivityNode] = Field(default_factory=list)
    reached_edges: list[ActivityEdge] = Field(default_factory=list)
    read_objects: list[str] = Field(default_factory=list)
    written_objects: list[str] = Field(default_factory=list)
    artifacts: list[str] = Field(default_factory=list)
    policies: list[str] = Field(default_factory=list)
    approvals: list[str] = Field(default_factory=list)
    observed: list[str] = Field(default_factory=list)
    inferred: list[str] = Field(default_factory=list)
    unknowns: list[str] = Field(default_factory=list)
    evidence_quality: list[EvidenceQuality] = Field(default_factory=list)


class AccessItem(BaseModel):
    kind: str
    id: str
    label: str = ""
    source: str = ""
    event_ids: list[str] = Field(default_factory=list)


class AccessReviewReport(BaseModel):
    schema_version: str = "access_review_v1"
    agent_id: str
    touched_objects: list[str] = Field(default_factory=list)
    tools_used: list[str] = Field(default_factory=list)
    policy_decisions: list[str] = Field(default_factory=list)
    source_granularities: list[str] = Field(default_factory=list)
    observed_access: list[AccessItem] = Field(default_factory=list)
    configured_access: list[AccessItem] = Field(default_factory=list)
    reachable_sensitive_assets: list[AccessItem] = Field(default_factory=list)
    unused_permissions: list[AccessItem] = Field(default_factory=list)
    new_access_since_last_review: list[AccessItem] = Field(default_factory=list)
    recommended_revocations: list[AccessItem] = Field(default_factory=list)
    evidence_quality: list[EvidenceQuality] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class AgentInventoryItem(BaseModel):
    agent_id: str
    display_name: str = ""
    event_count: int = 0
    tools_used: list[str] = Field(default_factory=list)
    touched_objects: list[str] = Field(default_factory=list)
    configured_access: list[AccessItem] = Field(default_factory=list)
    evidence_quality: list[EvidenceQuality] = Field(default_factory=list)


class PolicyReplayHit(BaseModel):
    event_id: str
    original_decision: str = ""
    replay_decision: str
    reason: str
    event_kind: str


class PolicyReplayReport(BaseModel):
    schema_version: str = "policy_replay_v1"
    policy_name: str = ""
    event_count: int = 0
    hit_count: int = 0
    hits: list[PolicyReplayHit] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class EvidencePack(BaseModel):
    schema_version: str = "evidence_pack_v1"
    timeline: ProvenanceTimeline
    agents: list[AgentInventoryItem] = Field(default_factory=list)
    access_reviews: list[AccessReviewReport] = Field(default_factory=list)
    blast_radius: BlastRadiusReport | None = None
    policy_replay: PolicyReplayReport | None = None
    warnings: list[str] = Field(default_factory=list)


class OTelExport(BaseModel):
    resource_spans: list[dict[str, Any]] = Field(default_factory=list)
