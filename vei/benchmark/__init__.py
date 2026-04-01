from __future__ import annotations

from importlib import import_module
from typing import Any

from vei.benchmark.dimensions import score_enterprise_dimensions
from vei.benchmark.families import (
    BenchmarkFamilyManifest,
    get_benchmark_family_manifest,
    list_benchmark_family_manifest,
)
from vei.benchmark.models import (
    BenchmarkBatchResult,
    BenchmarkBatchSummary,
    BenchmarkCaseResult,
    BenchmarkCaseSpec,
    BenchmarkDiagnostics,
    BenchmarkMetrics,
    BenchmarkShowcaseExample,
    BenchmarkShowcaseExampleResult,
    BenchmarkShowcaseResult,
    BenchmarkWorkflowVariantManifest,
)
from vei.benchmark.showcase import (
    get_showcase_example,
    list_showcase_examples,
    render_showcase_overview,
    resolve_showcase_examples,
)
from vei.benchmark.workflows import (
    get_benchmark_family_workflow_spec,
    get_benchmark_family_workflow_variant,
    list_benchmark_family_workflow_specs,
    list_benchmark_family_workflow_variants,
    resolve_benchmark_workflow_name,
)


def __getattr__(name: str) -> Any:
    if name in {
        "FRONTIER_SCENARIO_SETS",
        "resolve_scenarios",
        "run_benchmark_batch",
        "run_benchmark_case",
    }:
        module = import_module("vei.benchmark.api")
        return getattr(module, name)
    raise AttributeError(name)


__all__ = [
    "FRONTIER_SCENARIO_SETS",
    "BenchmarkFamilyManifest",
    "BenchmarkBatchResult",
    "BenchmarkBatchSummary",
    "BenchmarkCaseResult",
    "BenchmarkCaseSpec",
    "BenchmarkDiagnostics",
    "BenchmarkMetrics",
    "BenchmarkShowcaseExample",
    "BenchmarkShowcaseExampleResult",
    "BenchmarkShowcaseResult",
    "BenchmarkWorkflowVariantManifest",
    "get_benchmark_family_manifest",
    "get_benchmark_family_workflow_spec",
    "get_benchmark_family_workflow_variant",
    "get_showcase_example",
    "list_benchmark_family_manifest",
    "list_benchmark_family_workflow_specs",
    "list_benchmark_family_workflow_variants",
    "list_showcase_examples",
    "render_showcase_overview",
    "resolve_benchmark_workflow_name",
    "resolve_scenarios",
    "resolve_showcase_examples",
    "run_benchmark_batch",
    "run_benchmark_case",
    "score_enterprise_dimensions",
]
