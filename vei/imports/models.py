from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from vei.grounding.models import IdentityGovernanceBundle

ImportWedge = Literal["identity_access_governance"]
ImportFileType = Literal["csv", "json"]
ImportOrigin = Literal["imported", "derived", "simulated"]
IssueSeverity = Literal["info", "warning", "error"]
ImportSourceKind = Literal["file", "connector_snapshot"]
IdentityResolutionStatus = Literal["resolved", "ambiguous", "unmatched", "external"]


class ImportSourceManifest(BaseModel):
    source_id: str
    source_system: str
    source_kind: ImportSourceKind = "file"
    file_type: ImportFileType
    relative_path: str
    collected_at: str
    mapping_profile: str
    redaction_status: str = "none"
    description: Optional[str] = None
    provenance_prefix: Optional[str] = None
    connector_id: Optional[str] = None
    connector_metadata: Dict[str, Any] = Field(default_factory=dict)


class ImportPackage(BaseModel):
    version: Literal["1"] = "1"
    name: str
    title: str
    description: str
    wedge: ImportWedge = "identity_access_governance"
    organization_name: str
    organization_domain: str
    timezone: str = "UTC"
    sources: List[ImportSourceManifest] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class MappingProfileSpec(BaseModel):
    name: str
    source_system: str
    file_type: ImportFileType
    expected_fields: List[str] = Field(default_factory=list)
    required_fields: List[str] = Field(default_factory=list)
    field_aliases: Dict[str, str] = Field(default_factory=dict)
    list_fields: List[str] = Field(default_factory=list)
    bool_fields: List[str] = Field(default_factory=list)
    int_fields: List[str] = Field(default_factory=list)
    root_list_key: Optional[str] = None


class MappingOverrideSpec(BaseModel):
    source_id: str
    mapping_profile: str
    field_aliases: Dict[str, str] = Field(default_factory=dict)
    default_values: Dict[str, Any] = Field(default_factory=dict)
    ignored_fields: List[str] = Field(default_factory=list)
    value_aliases: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class MappingIssue(BaseModel):
    code: str
    message: str
    severity: IssueSeverity = "warning"
    source_id: Optional[str] = None
    record_key: Optional[str] = None
    row_number: Optional[int] = None
    field: Optional[str] = None
    raw_value: Any = None


class ImportSourceSummary(BaseModel):
    source_id: str
    source_system: str
    mapping_profile: str
    override_path: Optional[str] = None
    override_applied: bool = False
    loaded_record_count: int = 0
    normalized_record_count: int = 0
    dropped_record_count: int = 0
    issue_count: int = 0
    unknown_fields: List[str] = Field(default_factory=list)
    redaction_status: str = "none"


class RedactionReport(BaseModel):
    source_id: str
    status: str = "none"
    redacted_field_count: int = 0
    redacted_fields: List[str] = Field(default_factory=list)
    sample_preview: Dict[str, Any] = Field(default_factory=dict)


class ProvenanceRecord(BaseModel):
    object_ref: str
    label: str
    origin: ImportOrigin = "imported"
    source_id: Optional[str] = None
    source_system: Optional[str] = None
    raw_record_ref: Optional[str] = None
    mapping_profile: Optional[str] = None
    redaction_status: Optional[str] = None
    redacted_fields: List[str] = Field(default_factory=list)
    lineage: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class IdentityResolutionLink(BaseModel):
    principal_ref: str
    principal_label: str
    principal_type: str
    status: IdentityResolutionStatus
    matched_refs: List[str] = Field(default_factory=list)
    candidate_refs: List[str] = Field(default_factory=list)
    reason: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class IdentityReconciliationSummary(BaseModel):
    subject_count: int = 0
    resolved_count: int = 0
    ambiguous_count: int = 0
    unmatched_count: int = 0
    external_count: int = 0
    links: List[IdentityResolutionLink] = Field(default_factory=list)


class NormalizationReport(BaseModel):
    ok: bool
    package_name: str
    issue_count: int = 0
    warning_count: int = 0
    error_count: int = 0
    dropped_record_count: int = 0
    normalized_counts: Dict[str, int] = Field(default_factory=dict)
    source_summaries: List[ImportSourceSummary] = Field(default_factory=list)
    issues: List[MappingIssue] = Field(default_factory=list)
    identity_reconciliation: Optional[IdentityReconciliationSummary] = None


class GeneratedScenarioCandidate(BaseModel):
    name: str
    title: str
    description: str
    scenario_name: str
    workflow_name: str
    workflow_variant: Optional[str] = None
    workflow_parameters: Dict[str, Any] = Field(default_factory=dict)
    inspection_focus: str = "summary"
    tags: List[str] = Field(default_factory=list)
    hidden_faults: Dict[str, Any] = Field(default_factory=dict)
    actor_hints: List[str] = Field(default_factory=list)
    contract_overrides: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ImportPackageArtifacts(BaseModel):
    package: ImportPackage
    normalized_bundle: Optional[IdentityGovernanceBundle] = None
    normalization_report: NormalizationReport
    provenance: List[ProvenanceRecord] = Field(default_factory=list)
    redaction_reports: List[RedactionReport] = Field(default_factory=list)
    generated_scenarios: List[GeneratedScenarioCandidate] = Field(default_factory=list)


class ImportReview(BaseModel):
    package: ImportPackage
    normalization_report: NormalizationReport
    redaction_reports: List[RedactionReport] = Field(default_factory=list)
    generated_scenarios: List[GeneratedScenarioCandidate] = Field(default_factory=list)
    source_overrides: List[MappingOverrideSpec] = Field(default_factory=list)
    suggested_override_paths: Dict[str, str] = Field(default_factory=dict)


__all__ = [
    "GeneratedScenarioCandidate",
    "IdentityReconciliationSummary",
    "IdentityResolutionLink",
    "IdentityResolutionStatus",
    "ImportReview",
    "ImportFileType",
    "ImportOrigin",
    "ImportPackage",
    "ImportPackageArtifacts",
    "ImportSourceManifest",
    "ImportSourceSummary",
    "ImportWedge",
    "MappingIssue",
    "MappingOverrideSpec",
    "MappingProfileSpec",
    "NormalizationReport",
    "ProvenanceRecord",
    "RedactionReport",
]
