from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, TypeAlias, TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator

ContextLiveProviderName = Literal["slack", "jira", "google", "okta", "gmail", "teams"]
ContextProviderName = Literal[
    "slack",
    "jira",
    "google",
    "okta",
    "gmail",
    "teams",
    "crm",
    "salesforce",
    "mail_archive",
]
ContextSnapshotRole = Literal["company_history_bundle", "workspace_seed"]


# ---------------------------------------------------------------------------
# Per-provider typed data models
# ---------------------------------------------------------------------------


class ContextSourcePayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def items(self):
        return self.model_dump(mode="python").items()

    def keys(self):
        return self.model_dump(mode="python").keys()

    def values(self):
        return self.model_dump(mode="python").values()


class GenericSourceData(ContextSourcePayload):
    pass


class SlackSourceData(ContextSourcePayload):
    channels: List[Dict[str, Any]] = Field(default_factory=list)
    users: List[Dict[str, Any]] = Field(default_factory=list)


class GmailSourceData(ContextSourcePayload):
    threads: List[Dict[str, Any]] = Field(default_factory=list)
    profile: Dict[str, Any] = Field(default_factory=dict)


class JiraSourceData(ContextSourcePayload):
    issues: List[Dict[str, Any]] = Field(default_factory=list)
    projects: List[Dict[str, Any]] = Field(default_factory=list)
    parse_warnings: List[str] = Field(default_factory=list)


class GoogleSourceData(ContextSourcePayload):
    users: List[Dict[str, Any]] = Field(default_factory=list)
    documents: List[Dict[str, Any]] = Field(default_factory=list)
    drive_shares: List[Dict[str, Any]] = Field(default_factory=list)
    parse_warnings: List[str] = Field(default_factory=list)


class CrmSourceData(ContextSourcePayload):
    companies: List[Dict[str, Any]] = Field(default_factory=list)
    contacts: List[Dict[str, Any]] = Field(default_factory=list)
    deals: List[Dict[str, Any]] = Field(default_factory=list)
    parse_warnings: List[str] = Field(default_factory=list)


class TeamsSourceData(ContextSourcePayload):
    channels: List[Dict[str, Any]] = Field(default_factory=list)
    profile: Dict[str, Any] = Field(default_factory=dict)


class OktaSourceData(ContextSourcePayload):
    users: List[Dict[str, Any]] = Field(default_factory=list)
    groups: List[Dict[str, Any]] = Field(default_factory=list)
    applications: List[Dict[str, Any]] = Field(default_factory=list)


class MailArchiveSourceData(ContextSourcePayload):
    threads: List[Dict[str, Any]] = Field(default_factory=list)
    actors: List[Dict[str, Any]] = Field(default_factory=list)
    profile: Dict[str, Any] = Field(default_factory=dict)


ContextSourceData: TypeAlias = (
    SlackSourceData
    | GmailSourceData
    | JiraSourceData
    | GoogleSourceData
    | CrmSourceData
    | TeamsSourceData
    | OktaSourceData
    | MailArchiveSourceData
    | GenericSourceData
)

_SOURCE_DATA_MODEL_MAP: Dict[str, type[BaseModel]] = {
    "slack": SlackSourceData,
    "gmail": GmailSourceData,
    "jira": JiraSourceData,
    "google": GoogleSourceData,
    "crm": CrmSourceData,
    "salesforce": CrmSourceData,
    "teams": TeamsSourceData,
    "okta": OktaSourceData,
    "mail_archive": MailArchiveSourceData,
}

_PayloadT = TypeVar("_PayloadT", bound=ContextSourcePayload)


class ContextProviderConfig(BaseModel):
    provider: ContextLiveProviderName
    token_env: str = ""
    base_url: Optional[str] = None
    scopes: List[str] = Field(default_factory=list)
    filters: Dict[str, Any] = Field(default_factory=dict)
    timeout_s: int = 30
    limit: int = 200


class ContextSourceResult(BaseModel):
    provider: str
    captured_at: str
    status: Literal["ok", "partial", "error", "empty"] = "ok"
    record_counts: Dict[str, int] = Field(default_factory=dict)
    data: ContextSourceData = Field(default_factory=GenericSourceData)
    error: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _coerce_payload(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        provider = str(values.get("provider", ""))
        payload = values.get("data")
        model_cls = _SOURCE_DATA_MODEL_MAP.get(provider, GenericSourceData)
        if isinstance(payload, model_cls):
            return values
        if isinstance(payload, ContextSourcePayload):
            values["data"] = payload
            return values
        if isinstance(payload, BaseModel):
            values["data"] = model_cls.model_validate(payload.model_dump(mode="python"))
            return values
        if isinstance(payload, dict):
            values["data"] = model_cls.model_validate(payload)
            return values
        values["data"] = model_cls()
        return values

    def typed_data(self) -> ContextSourceData:
        return self.data


class ContextSnapshot(BaseModel):
    version: Literal["1"] = "1"
    organization_name: str
    organization_domain: str = ""
    captured_at: str = ""
    sources: List[ContextSourceResult] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def source_for(self, provider: str) -> Optional[ContextSourceResult]:
        for source in self.sources:
            if source.provider == provider:
                return source
        return None


def snapshot_role(snapshot: ContextSnapshot) -> ContextSnapshotRole:
    role = str(snapshot.metadata.get("snapshot_role", "") or "").strip().lower()
    if role in {"company_history_bundle", "workspace_seed"}:
        return role  # type: ignore[return-value]
    whatif = snapshot.metadata.get("whatif")
    if isinstance(whatif, dict) and str(whatif.get("branch_event_id") or "").strip():
        return "workspace_seed"
    return "company_history_bundle"


def with_snapshot_role(
    snapshot: ContextSnapshot,
    role: ContextSnapshotRole,
) -> ContextSnapshot:
    metadata = dict(snapshot.metadata)
    metadata["snapshot_role"] = role
    return snapshot.model_copy(update={"metadata": metadata})


def source_payload(
    source: ContextSourceResult | None,
    payload_type: type[_PayloadT],
) -> _PayloadT | None:
    if source is None or source.status == "error":
        return None
    if isinstance(source.data, payload_type):
        return source.data
    return None


class ContextDiffEntry(BaseModel):
    kind: Literal["added", "removed", "changed"]
    domain: str
    item_id: str
    detail: str = ""


class ContextDiff(BaseModel):
    before_captured_at: str = ""
    after_captured_at: str = ""
    entries: List[ContextDiffEntry] = Field(default_factory=list)
    summary: str = ""

    @property
    def added(self) -> List[ContextDiffEntry]:
        return [e for e in self.entries if e.kind == "added"]

    @property
    def removed(self) -> List[ContextDiffEntry]:
        return [e for e in self.entries if e.kind == "removed"]

    @property
    def changed(self) -> List[ContextDiffEntry]:
        return [e for e in self.entries if e.kind == "changed"]


class BundleVerificationCheck(BaseModel):
    code: str
    passed: bool
    severity: Literal["info", "warning", "error"] = "error"
    provider: Optional[str] = None
    detail: str = ""


class BundleVerificationResult(BaseModel):
    ok: bool
    snapshot_path: str = ""
    organization_name: str = ""
    organization_domain: str = ""
    source_status: Dict[str, str] = Field(default_factory=dict)
    checks: List[BundleVerificationCheck] = Field(default_factory=list)

    @property
    def error_count(self) -> int:
        return sum(
            1 for check in self.checks if not check.passed and check.severity == "error"
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1
            for check in self.checks
            if not check.passed and check.severity == "warning"
        )


class ContextStatusFinding(BaseModel):
    code: str
    severity: Literal["info", "warning", "error"] = "info"
    provider: Optional[str] = None
    detail: str = ""


class ContextProviderStatusSummary(BaseModel):
    provider: str
    status: Literal["ok", "partial", "error", "empty"] = "ok"
    record_counts: Dict[str, int] = Field(default_factory=dict)
    first_timestamp: str = ""
    last_timestamp: str = ""
    timestamp_quality: str = ""
    duplicate_id_findings: List[ContextStatusFinding] = Field(default_factory=list)
    identity_cleanup_findings: List[ContextStatusFinding] = Field(default_factory=list)


class ContextSnapshotStatusSummary(BaseModel):
    snapshot_role: ContextSnapshotRole = "company_history_bundle"
    organization_name: str = ""
    organization_domain: str = ""
    captured_at: str = ""
    first_timestamp: str = ""
    last_timestamp: str = ""
    providers: List[ContextProviderStatusSummary] = Field(default_factory=list)
    duplicate_id_findings: List[ContextStatusFinding] = Field(default_factory=list)
    identity_cleanup_findings: List[ContextStatusFinding] = Field(default_factory=list)
    timestamp_quality: List[ContextStatusFinding] = Field(default_factory=list)
