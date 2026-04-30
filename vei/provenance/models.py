"""Typed report models for VEI Control provenance."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ActivityNode(BaseModel):
    id: str
    kind: str
    label: str = ""
    event_ids: list[str] = Field(default_factory=list)


class ActivityEdge(BaseModel):
    source: str
    target: str
    kind: str
    event_ids: list[str] = Field(default_factory=list)


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
    unknowns: list[str] = Field(default_factory=list)


class AccessReviewReport(BaseModel):
    schema_version: str = "access_review_v1"
    agent_id: str
    touched_objects: list[str] = Field(default_factory=list)
    tools_used: list[str] = Field(default_factory=list)
    policy_decisions: list[str] = Field(default_factory=list)
    source_granularities: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


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


class OTelExport(BaseModel):
    resource_spans: list[dict[str, Any]] = Field(default_factory=list)
