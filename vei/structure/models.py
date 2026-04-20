from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

HypothesisStatus = Literal["open", "resolved"]
CaseSource = Literal["explicit", "inferred"]


class StructureEvidence(BaseModel):
    event_ids: List[str] = Field(default_factory=list)
    surfaces: List[str] = Field(default_factory=list)
    provenance_sources: List[str] = Field(default_factory=list)


class DerivedEntity(BaseModel):
    entity_id: str
    entity_type: str
    title: Optional[str] = None
    canonical_ref: Optional[str] = None
    aliases: List[str] = Field(default_factory=list)
    confidence: float = 0.0
    evidence: StructureEvidence = Field(default_factory=StructureEvidence)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DerivedRelation(BaseModel):
    relation_id: str
    relation_type: str
    source_entity_id: str
    target_entity_id: str
    confidence: float = 0.0
    evidence: StructureEvidence = Field(default_factory=StructureEvidence)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DerivedCase(BaseModel):
    case_id: str
    title: str
    case_source: CaseSource = "inferred"
    confidence: float = 0.0
    anchor_refs: List[str] = Field(default_factory=list)
    event_ids: List[str] = Field(default_factory=list)
    entity_ids: List[str] = Field(default_factory=list)
    surfaces: List[str] = Field(default_factory=list)
    evidence: StructureEvidence = Field(default_factory=StructureEvidence)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DerivedTimeline(BaseModel):
    timeline_id: str
    label: str
    case_id: Optional[str] = None
    event_ids: List[str] = Field(default_factory=list)
    entity_ids: List[str] = Field(default_factory=list)
    start_ts_ms: int = 0
    end_ts_ms: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DerivedHypothesis(BaseModel):
    hypothesis_id: str
    title: str
    summary: str
    status: HypothesisStatus = "open"
    confidence: float = 0.0
    candidate_entity_ids: List[str] = Field(default_factory=list)
    evidence: StructureEvidence = Field(default_factory=StructureEvidence)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class StructureView(BaseModel):
    source_mode: str
    total_event_count: int = 0
    entities: List[DerivedEntity] = Field(default_factory=list)
    cases: List[DerivedCase] = Field(default_factory=list)
    relations: List[DerivedRelation] = Field(default_factory=list)
    timelines: List[DerivedTimeline] = Field(default_factory=list)
    hypotheses: List[DerivedHypothesis] = Field(default_factory=list)
    suggested_investigations: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class StructureMetrics(BaseModel):
    entity_link_precision: float = 0.0
    entity_link_recall: float = 0.0
    entity_link_quality: float = 0.0
    hidden_case_discovery: float = 0.0
    relation_precision: float = 0.0
    relation_recall: float = 0.0
    relation_recovery: float = 0.0
    event_ordering: float = 0.0
    action_choice_under_uncertainty: float = 0.0


class StructureTruthComparison(BaseModel):
    source_mode: str
    metrics: StructureMetrics = Field(default_factory=StructureMetrics)
    truth_entity_refs: List[str] = Field(default_factory=list)
    derived_entity_refs: List[str] = Field(default_factory=list)
    missing_entity_refs: List[str] = Field(default_factory=list)
    extra_entity_refs: List[str] = Field(default_factory=list)
    truth_relation_refs: List[str] = Field(default_factory=list)
    derived_relation_refs: List[str] = Field(default_factory=list)
    missing_relation_refs: List[str] = Field(default_factory=list)
    extra_relation_refs: List[str] = Field(default_factory=list)
    truth_hidden_case_refs: List[str] = Field(default_factory=list)
    discovered_hidden_case_refs: List[str] = Field(default_factory=list)
    expected_ambiguity_refs: List[str] = Field(default_factory=list)
    satisfied_ambiguity_refs: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


__all__ = [
    "DerivedCase",
    "DerivedEntity",
    "DerivedHypothesis",
    "DerivedRelation",
    "DerivedTimeline",
    "StructureEvidence",
    "StructureMetrics",
    "StructureTruthComparison",
    "StructureView",
]
