"""Per-tenant training data opt-in manifest.

Controls which data domains are available for cross-tenant training.
Default is structured-only (no text bodies).
"""

from __future__ import annotations

from typing import List

from pydantic import BaseModel, Field


class TenantTrainingManifest(BaseModel):
    """Per-tenant opt-in for training data sharing."""

    tenant_id: str
    allowed_domains: List[str] = Field(
        default_factory=lambda: ["structured"],
        description=(
            "Which domains may be shared. 'structured' means structured "
            "fields only. Add 'mail_body', 'chat_body', 'doc_body' to "
            "opt in to text content."
        ),
    )
    structured_only: bool = True
    notes: List[str] = Field(default_factory=list)


def default_manifest(tenant_id: str) -> TenantTrainingManifest:
    """Return a structured-only manifest (the safe default)."""
    return TenantTrainingManifest(tenant_id=tenant_id)
