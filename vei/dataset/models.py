from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from vei.synthesis.models import TrainingFormat
from vei.twin.models import TwinArchetype, TwinCrisisLevel, TwinDensityLevel

DatasetSplitName = Literal["train", "validation", "test"]


class DatasetBuildSpec(BaseModel):
    output_root: Path
    workspace_roots: list[Path] = Field(default_factory=list)
    snapshot_path: str | None = None
    organization_name: str = ""
    organization_domain: str = ""
    archetypes: list[TwinArchetype] = Field(default_factory=list)
    density_levels: list[TwinDensityLevel] = Field(default_factory=list)
    crisis_levels: list[TwinCrisisLevel] = Field(default_factory=list)
    seeds: list[int] = Field(default_factory=list)
    include_external_sample: bool = True
    formats: list[TrainingFormat] = Field(default_factory=list)


class DatasetRunRecord(BaseModel):
    workspace_root: Path
    variant_id: str
    archetype: str
    scenario_variant: str | None = None
    contract_variant: str | None = None
    density_level: TwinDensityLevel = "medium"
    crisis_level: TwinCrisisLevel = "escalated"
    run_id: str
    runner: str
    split: DatasetSplitName
    status: str
    success: bool | None = None
    contract_ok: bool | None = None
    issue_count: int = 0
    action_count: int = 0


class DatasetExampleManifest(BaseModel):
    format: TrainingFormat
    split: DatasetSplitName
    example_count: int = 0
    path: str


class DatasetSplitManifest(BaseModel):
    split: DatasetSplitName
    run_count: int = 0
    example_count: int = 0
    run_ids: list[str] = Field(default_factory=list)


class DatasetBundle(BaseModel):
    version: Literal["1"] = "1"
    spec: DatasetBuildSpec
    environment_count: int = 0
    run_count: int = 0
    runs: list[DatasetRunRecord] = Field(default_factory=list)
    exports: list[DatasetExampleManifest] = Field(default_factory=list)
    splits: list[DatasetSplitManifest] = Field(default_factory=list)
    reward_summary: dict[str, float] = Field(default_factory=dict)
    matrix_path: str | None = None
    generated_at: str = ""
