from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from vei.benchmark.models import BenchmarkDiagnostics, BenchmarkMetrics, BenchmarkRunner


RunStatus = Literal["queued", "running", "ok", "error"]


class RunContractSummary(BaseModel):
    contract_name: Optional[str] = None
    ok: Optional[bool] = None
    success_assertion_count: int = 0
    success_assertions_passed: int = 0
    success_assertions_failed: int = 0
    issue_count: int = 0
    evaluation_path: Optional[str] = None


class RunArtifactIndex(BaseModel):
    run_dir: str
    artifacts_dir: str
    state_dir: str
    blueprint_asset_path: Optional[str] = None
    blueprint_path: Optional[str] = None
    contract_path: Optional[str] = None
    timeline_path: Optional[str] = None
    benchmark_result_path: Optional[str] = None
    score_path: Optional[str] = None
    workflow_result_path: Optional[str] = None
    transcript_path: Optional[str] = None
    trace_path: Optional[str] = None


class RunSnapshotRef(BaseModel):
    snapshot_id: int
    branch: str
    label: Optional[str] = None
    time_ms: int = 0
    path: str


class RunTimelineEvent(BaseModel):
    index: int
    kind: Literal[
        "trace_call",
        "trace_event",
        "workflow_step",
        "receipt",
        "snapshot",
        "contract",
    ]
    label: str
    channel: str = "World"
    time_ms: int = 0
    tool: Optional[str] = None
    resolved_tool: Optional[str] = None
    graph_action_ref: Optional[str] = None
    object_refs: List[str] = Field(default_factory=list)
    branch: Optional[str] = None
    snapshot_id: Optional[int] = None
    payload: Dict[str, Any] = Field(default_factory=dict)


class RunManifest(BaseModel):
    version: Literal["1"] = "1"
    run_id: str
    workspace_name: str
    scenario_name: str
    runner: BenchmarkRunner
    status: RunStatus
    started_at: str
    completed_at: Optional[str] = None
    seed: int = 42042
    branch: Optional[str] = None
    model: Optional[str] = None
    provider: Optional[str] = None
    workflow_name: Optional[str] = None
    workflow_variant: Optional[str] = None
    success: Optional[bool] = None
    metrics: BenchmarkMetrics = Field(default_factory=BenchmarkMetrics)
    diagnostics: BenchmarkDiagnostics = Field(default_factory=BenchmarkDiagnostics)
    contract: RunContractSummary = Field(default_factory=RunContractSummary)
    artifacts: RunArtifactIndex
    snapshots: List[RunSnapshotRef] = Field(default_factory=list)
    error: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
