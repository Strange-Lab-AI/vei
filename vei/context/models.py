from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field

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


# ---------------------------------------------------------------------------
# Per-provider typed data models
#
# These document the expected shape of ``ContextSourceResult.data`` for each
# provider.  The actual ``data`` field remains ``Dict[str, Any]`` for backward
# compatibility; use ``ContextSourceResult.typed_data()`` to get a validated
# instance.
# ---------------------------------------------------------------------------


class SlackSourceData(BaseModel):
    """Shape of ContextSourceResult.data for the Slack provider."""

    channels: List[Dict[str, Any]] = Field(default_factory=list)
    users: List[Dict[str, Any]] = Field(default_factory=list)


class GmailSourceData(BaseModel):
    """Shape of ContextSourceResult.data for the Gmail provider."""

    threads: List[Dict[str, Any]] = Field(default_factory=list)
    profile: Dict[str, Any] = Field(default_factory=dict)


class JiraSourceData(BaseModel):
    """Shape of ContextSourceResult.data for the Jira provider."""

    issues: List[Dict[str, Any]] = Field(default_factory=list)
    projects: List[Dict[str, Any]] = Field(default_factory=list)
    parse_warnings: List[str] = Field(default_factory=list)


class GoogleSourceData(BaseModel):
    """Shape of ContextSourceResult.data for the Google Workspace provider."""

    users: List[Dict[str, Any]] = Field(default_factory=list)
    documents: List[Dict[str, Any]] = Field(default_factory=list)
    drive_shares: List[Dict[str, Any]] = Field(default_factory=list)
    parse_warnings: List[str] = Field(default_factory=list)


class CrmSourceData(BaseModel):
    """Shape of ContextSourceResult.data for CRM / Salesforce providers."""

    companies: List[Dict[str, Any]] = Field(default_factory=list)
    contacts: List[Dict[str, Any]] = Field(default_factory=list)
    deals: List[Dict[str, Any]] = Field(default_factory=list)
    parse_warnings: List[str] = Field(default_factory=list)


class TeamsSourceData(BaseModel):
    """Shape of ContextSourceResult.data for the Microsoft Teams provider."""

    channels: List[Dict[str, Any]] = Field(default_factory=list)
    profile: Dict[str, Any] = Field(default_factory=dict)


class OktaSourceData(BaseModel):
    """Shape of ContextSourceResult.data for the Okta provider."""

    users: List[Dict[str, Any]] = Field(default_factory=list)
    groups: List[Dict[str, Any]] = Field(default_factory=list)
    applications: List[Dict[str, Any]] = Field(default_factory=list)


class MailArchiveSourceData(BaseModel):
    """Shape of ContextSourceResult.data for the mail_archive provider."""

    threads: List[Dict[str, Any]] = Field(default_factory=list)
    actors: List[Dict[str, Any]] = Field(default_factory=list)


ContextSourceData = Union[
    SlackSourceData,
    GmailSourceData,
    JiraSourceData,
    GoogleSourceData,
    CrmSourceData,
    TeamsSourceData,
    OktaSourceData,
    MailArchiveSourceData,
    Dict[str, Any],
]

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
    data: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None

    def typed_data(self) -> ContextSourceData:
        """Return data validated against the provider-specific model, or as raw dict."""
        model_cls = _SOURCE_DATA_MODEL_MAP.get(self.provider)
        if model_cls is not None and isinstance(self.data, dict):
            return model_cls.model_validate(self.data)
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
