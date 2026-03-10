from __future__ import annotations

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
)


def __getattr__(name: str) -> Any:
    if name in {
        "FRONTIER_SCENARIO_SETS",
        "resolve_scenarios",
        "run_benchmark_batch",
        "run_benchmark_case",
    }:
        from vei.benchmark.api import (
            FRONTIER_SCENARIO_SETS,
            resolve_scenarios,
            run_benchmark_batch,
            run_benchmark_case,
        )

        return {
            "FRONTIER_SCENARIO_SETS": FRONTIER_SCENARIO_SETS,
            "resolve_scenarios": resolve_scenarios,
            "run_benchmark_batch": run_benchmark_batch,
            "run_benchmark_case": run_benchmark_case,
        }[name]
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
    "get_benchmark_family_manifest",
    "list_benchmark_family_manifest",
    "resolve_scenarios",
    "run_benchmark_batch",
    "run_benchmark_case",
    "score_enterprise_dimensions",
]
