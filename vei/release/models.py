from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

ReleaseKind = Literal["dataset", "benchmark", "nightly"]


class ReleaseArtifact(BaseModel):
    path: str
    sha256: str
    size_bytes: int
    kind: str


class ReleaseManifest(BaseModel):
    release_id: str
    version: str
    kind: ReleaseKind
    label: str
    created_at_utc: str
    root_dir: str
    source: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    artifacts: List[ReleaseArtifact] = Field(default_factory=list)


class DatasetReleaseResult(BaseModel):
    manifest: ReleaseManifest
    release_dir: Path
    manifest_path: Path


class BenchmarkReleaseResult(BaseModel):
    manifest: ReleaseManifest
    release_dir: Path
    manifest_path: Path


class NightlyReleaseResult(BaseModel):
    manifest: ReleaseManifest
    release_dir: Path
    manifest_path: Path
    corpus_release: DatasetReleaseResult
    rollout_release: DatasetReleaseResult
    benchmark_release: BenchmarkReleaseResult
    llm_benchmark_release: Optional[BenchmarkReleaseResult] = None
