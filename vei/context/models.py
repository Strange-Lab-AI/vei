from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

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
