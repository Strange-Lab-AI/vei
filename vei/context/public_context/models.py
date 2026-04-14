from __future__ import annotations

from pydantic import BaseModel, Field


class WhatIfPublicFinancialSnapshot(BaseModel):
    snapshot_id: str
    as_of: str
    kind: str
    label: str
    source_ids: list[str] = Field(default_factory=list)
    summary: str = ""
    metrics: dict[str, int | float | str] = Field(default_factory=dict)


class WhatIfPublicNewsEvent(BaseModel):
    event_id: str
    timestamp: str
    category: str
    headline: str
    summary: str = ""
    source_ids: list[str] = Field(default_factory=list)


class WhatIfPublicContext(BaseModel):
    version: str = "1"
    pack_name: str = ""
    organization_name: str = ""
    organization_domain: str = ""
    prepared_at: str = ""
    integration_hint: str = ""
    window_start: str = ""
    window_end: str = ""
    branch_timestamp: str = ""
    financial_snapshots: list[WhatIfPublicFinancialSnapshot] = Field(
        default_factory=list
    )
    public_news_events: list[WhatIfPublicNewsEvent] = Field(default_factory=list)


__all__ = [
    "WhatIfPublicContext",
    "WhatIfPublicFinancialSnapshot",
    "WhatIfPublicNewsEvent",
]
