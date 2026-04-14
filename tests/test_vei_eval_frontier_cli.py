from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from vei.benchmark.models import (
    BenchmarkBatchResult,
    BenchmarkBatchSummary,
    BenchmarkCaseResult,
    BenchmarkCaseSpec,
)
from vei.cli.vei_eval import app as eval_app

pytestmark = pytest.mark.integration


def _make_batch(specs: list[BenchmarkCaseSpec], run_id: str) -> BenchmarkBatchResult:
    results = [
        BenchmarkCaseResult(
            spec=spec,
            status="ok",
            success=True,
            score={"success": True, "composite_score": 1.0, "steps_taken": 0},
        )
        for spec in specs
    ]
    return BenchmarkBatchResult(
        run_id=run_id,
        results=results,
        summary=BenchmarkBatchSummary(
            total_runs=len(results),
            success_count=len(results),
            success_rate=1.0 if results else 0.0,
            average_composite_score=1.0 if results else 0.0,
        ),
    )


def test_frontier_benchmark_explicit_scenario_does_not_expand_to_all(
    tmp_path: Path, monkeypatch
) -> None:
    captured_specs: list[BenchmarkCaseSpec] = []

    def fake_run_benchmark_batch(
        specs: list[BenchmarkCaseSpec], *, run_id: str, output_dir: Path | None = None
    ) -> BenchmarkBatchResult:
        captured_specs.extend(specs)
        return _make_batch(specs, run_id)

    monkeypatch.setattr(
        "vei.cli.vei_eval.run_benchmark_batch", fake_run_benchmark_batch
    )

    runner = CliRunner()
    result = runner.invoke(
        eval_app,
        [
            "benchmark",
            "--runner",
            "scripted",
            "--frontier",
            "--scenario",
            "f1_budget_reconciliation",
            "--artifacts-root",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert [spec.scenario_name for spec in captured_specs] == [
        "f1_budget_reconciliation"
    ]
    assert all(spec.frontier for spec in captured_specs)


def test_frontier_list_is_available_on_unified_eval_cli() -> None:
    runner = CliRunner()
    result = runner.invoke(eval_app, ["frontier-list"])

    assert result.exit_code == 0, result.output
    assert "Frontier Evaluation Scenarios" in result.output
    assert "f1_budget_reconciliation" in result.output


def test_frontier_score_is_available_on_unified_eval_cli(
    tmp_path: Path, monkeypatch
) -> None:
    trace_path = tmp_path / "trace.jsonl"
    trace_path.write_text("{}\n", encoding="utf-8")
    output_path = tmp_path / "frontier_score.json"

    monkeypatch.setattr(
        "vei.score_frontier.compute_frontier_score",
        lambda artifacts_dir, use_llm_judge=False: {
            "success": True,
            "composite_score": 1.0,
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        eval_app,
        [
            "frontier-score",
            "--artifacts-dir",
            str(tmp_path),
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(output_path.read_text(encoding="utf-8"))["success"] is True
