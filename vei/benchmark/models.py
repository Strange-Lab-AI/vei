from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


BenchmarkRunner = Literal["scripted", "bc", "llm", "workflow"]
BenchmarkWorkflowValueType = Literal["str", "int", "float", "bool"]


class BenchmarkCaseSpec(BaseModel):
    runner: BenchmarkRunner
    scenario_name: str
    family_name: Optional[str] = None
    workflow_name: Optional[str] = None
    workflow_variant: Optional[str] = None
    blueprint_asset_path: Optional[Path] = None
    seed: int = 42042
    artifacts_dir: Path
    branch: Optional[str] = None
    dataset_path: Optional[Path] = None
    replay_mode: Optional[Literal["overlay", "strict"]] = None
    score_mode: Literal["email", "full"] = "full"
    frontier: bool = False
    model: Optional[str] = None
    provider: Optional[str] = None
    bc_model_path: Optional[Path] = None
    task: Optional[str] = None
    max_steps: int = 12
    tool_top_k: int = 0
    step_timeout_s: int = 180
    episode_timeout_s: int = 900
    use_llm_judge: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BenchmarkFamilyManifest(BaseModel):
    name: str
    title: str
    description: str
    workflow_name: Optional[str] = None
    primary_workflow_variant: Optional[str] = None
    workflow_variants: List[str] = Field(default_factory=list)
    scenario_names: List[str] = Field(default_factory=list)
    primary_dimensions: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)


class BenchmarkWorkflowParameter(BaseModel):
    name: str
    value: str | int | float | bool
    value_type: BenchmarkWorkflowValueType
    description: Optional[str] = None


class BenchmarkWorkflowVariantManifest(BaseModel):
    family_name: str
    workflow_name: str
    variant_name: str
    title: str
    description: str
    scenario_name: str
    parameters: List[BenchmarkWorkflowParameter] = Field(default_factory=list)


class BenchmarkMetrics(BaseModel):
    elapsed_ms: int = 0
    actions: int = 0
    time_ms: int = 0
    latency_p95_ms: int = 0
    llm_calls: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    estimated_cost_usd: Optional[float] = None


class BenchmarkDiagnostics(BaseModel):
    branch: str = "main"
    benchmark_family: Optional[str] = None
    workflow_name: Optional[str] = None
    workflow_variant: Optional[str] = None
    workflow_valid: Optional[bool] = None
    workflow_step_count: int = 0
    snapshot_count: int = 0
    initial_snapshot_id: Optional[int] = None
    final_snapshot_id: Optional[int] = None
    latest_snapshot_label: Optional[str] = None
    replay_mode: Optional[str] = None
    replay_scheduled: int = 0
    pending_events: int = 0
    actor_modes: Dict[str, str] = Field(default_factory=dict)
    actor_status: Dict[str, str] = Field(default_factory=dict)
    receipt_count: int = 0
    connector_receipt_count: int = 0
    state_head: Optional[int] = None
    policy_warning_count: int = 0
    policy_error_count: int = 0
    scenario_metadata: Dict[str, Any] = Field(default_factory=dict)


class BenchmarkCaseResult(BaseModel):
    spec: BenchmarkCaseSpec
    status: Literal["ok", "error"] = "ok"
    success: bool = False
    score: Dict[str, Any] = Field(default_factory=dict)
    raw_score: Dict[str, Any] = Field(default_factory=dict)
    metrics: BenchmarkMetrics = Field(default_factory=BenchmarkMetrics)
    diagnostics: BenchmarkDiagnostics = Field(default_factory=BenchmarkDiagnostics)
    error: Optional[str] = None


class BenchmarkBatchSummary(BaseModel):
    total_runs: int = 0
    success_count: int = 0
    success_rate: float = 0.0
    average_composite_score: float = 0.0
    total_actions: int = 0
    total_time_ms: int = 0
    p95_latency_ms: int = 0
    llm_calls: int = 0
    total_prompt_tokens: int = 0
    total_completion_tokens: int = 0
    total_tokens: int = 0
    estimated_cost_usd: Optional[float] = None


class BenchmarkBatchResult(BaseModel):
    run_id: str
    results: List[BenchmarkCaseResult] = Field(default_factory=list)
    summary: BenchmarkBatchSummary = Field(default_factory=BenchmarkBatchSummary)


class BenchmarkDemoSpec(BaseModel):
    family_name: str
    compare_runner: Literal["scripted", "bc", "llm"] = "scripted"
    workflow_variant: Optional[str] = None
    seed: int = 42042
    artifacts_root: Path
    run_id: str
    score_mode: Literal["email", "full"] = "full"
    max_steps: int = 40
    compare_model: Optional[str] = None
    compare_provider: Optional[str] = None
    compare_bc_model_path: Optional[Path] = None
    compare_task: Optional[str] = None


class BenchmarkDemoResult(BaseModel):
    run_id: str
    family_name: str
    scenario_name: str
    baseline_workflow_name: str
    baseline_workflow_variant: Optional[str] = None
    compare_runner: Literal["scripted", "bc", "llm"]
    compare_model: Optional[str] = None
    demo_dir: Path
    state_dir: Path
    aggregate_results_path: Path
    benchmark_summary_path: Path
    report_markdown_path: Path
    report_csv_path: Path
    report_json_path: Path
    baseline_artifacts_dir: Path
    comparison_artifacts_dir: Path
    baseline_blueprint_asset_path: Path
    comparison_blueprint_asset_path: Path
    baseline_blueprint_path: Path
    comparison_blueprint_path: Path
    baseline_contract_path: Path
    comparison_contract_path: Path
    baseline_branch: Optional[str] = None
    comparison_branch: Optional[str] = None
    baseline_success: bool = False
    comparison_success: bool = False
    baseline_score: float = 0.0
    comparison_score: float = 0.0
    baseline_assertions_passed: int = 0
    baseline_assertions_total: int = 0
    comparison_assertions_passed: int = 0
    comparison_assertions_total: int = 0
    summary: BenchmarkBatchSummary
    inspection_commands: List[str] = Field(default_factory=list)


class BenchmarkSuiteSpec(BaseModel):
    family_names: List[str] = Field(default_factory=list)
    seed: int = 42042
    artifacts_root: Path
    run_id: str
    score_mode: Literal["email", "full"] = "full"


class BenchmarkSuiteResult(BaseModel):
    run_id: str
    family_names: List[str] = Field(default_factory=list)
    scenario_names: Dict[str, str] = Field(default_factory=dict)
    workflow_variants: Dict[str, Optional[str]] = Field(default_factory=dict)
    suite_dir: Path
    aggregate_results_path: Path
    benchmark_summary_path: Path
    report_markdown_path: Path
    report_csv_path: Path
    report_json_path: Path
    case_artifacts_dirs: Dict[str, Path] = Field(default_factory=dict)
    blueprint_asset_paths: Dict[str, Path] = Field(default_factory=dict)
    blueprint_paths: Dict[str, Path] = Field(default_factory=dict)
    contract_paths: Dict[str, Path] = Field(default_factory=dict)
    summary: BenchmarkBatchSummary


class BenchmarkShowcaseExample(BaseModel):
    name: str
    title: str
    description: str
    family_name: str
    workflow_variant: Optional[str] = None
    compare_runner: Literal["scripted", "bc", "llm"] = "scripted"
    key_surfaces: List[str] = Field(default_factory=list)
    proves: List[str] = Field(default_factory=list)


class BenchmarkShowcaseSpec(BaseModel):
    example_names: List[str] = Field(default_factory=list)
    compare_runner: Literal["scripted", "bc", "llm"] = "scripted"
    seed: int = 42042
    artifacts_root: Path
    run_id: str
    score_mode: Literal["email", "full"] = "full"
    max_steps: int = 40
    compare_model: Optional[str] = None
    compare_provider: Optional[str] = None
    compare_bc_model_path: Optional[Path] = None
    compare_task: Optional[str] = None


class BenchmarkShowcaseExampleResult(BaseModel):
    example: BenchmarkShowcaseExample
    demo: BenchmarkDemoResult


class BenchmarkShowcaseResult(BaseModel):
    run_id: str
    showcase_dir: Path
    overview_markdown_path: Path
    overview_json_path: Path
    example_count: int = 0
    baseline_success_count: int = 0
    comparison_success_count: int = 0
    examples: List[BenchmarkShowcaseExampleResult] = Field(default_factory=list)
