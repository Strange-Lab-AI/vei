from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from vei.pilot.models import PilotStatus

ExerciseRunner = Literal["workflow", "scripted", "external"]


class ExerciseCompatibilityEndpoint(BaseModel):
    method: Literal["GET", "POST"]
    path: str
    description: str


class ExerciseCompatibilitySurface(BaseModel):
    surface: str
    title: str
    base_path: str
    endpoints: list[ExerciseCompatibilityEndpoint] = Field(default_factory=list)


class ExerciseCatalogItem(BaseModel):
    scenario_variant: str
    crisis_name: str
    summary: str
    contract_variant: str
    objective_summary: str
    active: bool = False


class ExerciseComparisonRow(BaseModel):
    runner: ExerciseRunner
    label: str
    run_id: str | None = None
    status: str = "missing"
    success: bool | None = None
    contract_ok: bool | None = None
    issue_count: int = 0
    action_count: int = 0
    summary: str = ""


class ExerciseManifest(BaseModel):
    version: Literal["1"] = "1"
    workspace_root: Path
    workspace_name: str
    company_name: str
    archetype: str
    crisis_name: str
    scenario_variant: str
    contract_variant: str
    success_criteria: list[str] = Field(default_factory=list)
    supported_api_subset: list[ExerciseCompatibilitySurface] = Field(
        default_factory=list
    )
    catalog: list[ExerciseCatalogItem] = Field(default_factory=list)
    recommended_first_move: str = ""


class ExerciseStatus(BaseModel):
    manifest: ExerciseManifest
    pilot: PilotStatus
    comparison: list[ExerciseComparisonRow] = Field(default_factory=list)
