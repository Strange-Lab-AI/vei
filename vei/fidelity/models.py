from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

FidelitySurface = Literal[
    "slack",
    "docs",
    "tickets",
    "identity",
    "property",
    "campaign",
    "inventory",
    "service_ops",
    "revenue_graph",
]
FidelityStatus = Literal["ok", "warning", "error"]


class TwinFidelityCheck(BaseModel):
    name: str
    status: FidelityStatus
    summary: str
    payload: dict[str, Any] = Field(default_factory=dict)


class TwinFidelityCase(BaseModel):
    surface: FidelitySurface
    title: str
    boundary_contract: str
    why_it_matters: str
    resolved_tool: str | None = None
    status: FidelityStatus = "ok"
    checks: list[TwinFidelityCheck] = Field(default_factory=list)


class TwinFidelityReport(BaseModel):
    version: Literal["1"] = "1"
    generated_at: str
    workspace_root: str
    company_name: str
    status: FidelityStatus = "ok"
    summary: str
    cases: list[TwinFidelityCase] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
