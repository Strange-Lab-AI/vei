"""CLI for generating comprehensive evaluation reports and leaderboards.

Usage:
    vei report generate --root _vei_out/frontier_eval --format markdown
    vei report generate --root _vei_out/frontier_eval --format csv --output results.csv
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer

from vei.benchmark.families import get_benchmark_family_manifest

app = typer.Typer(name="report", help="Generate evaluation reports and leaderboards")


def load_all_results(root_dir: Path) -> List[Dict[str, Any]]:
    """Recursively load normalized benchmark, frontier, and legacy score artifacts."""
    results = []
    aggregate_roots: set[Path] = set()

    # Look for aggregate_results.json first (batch runs)
    for aggregate_file in root_dir.rglob("aggregate_results.json"):
        try:
            aggregate_roots.add(aggregate_file.parent.resolve())
            with open(aggregate_file, "r") as f:
                batch = json.load(f)
                for item in batch:
                    results.append(item)
        except Exception:
            continue

    # Per-run benchmark artifacts already contain normalized score payloads.
    for benchmark_file in root_dir.rglob("benchmark_result.json"):
        benchmark_parent = benchmark_file.parent.resolve()
        if any(
            root == benchmark_parent or root in benchmark_parent.parents
            for root in aggregate_roots
        ):
            continue
        if (benchmark_file.parent / "aggregate_results.json").exists():
            continue
        try:
            with open(benchmark_file, "r", encoding="utf-8") as f:
                result = json.load(f)
            spec = result.get("spec", {})
            score = result.get("score", {})
            if not isinstance(spec, dict) or not isinstance(score, dict):
                continue
            results.append(
                {
                    "scenario": spec.get("scenario_name", "unknown"),
                    "family": score.get("benchmark_family")
                    or result.get("diagnostics", {}).get("benchmark_family"),
                    "model": spec.get("model") or spec.get("runner", "unknown"),
                    "provider": spec.get("provider")
                    or (
                        "baseline"
                        if spec.get("runner") in {"scripted", "bc", "workflow"}
                        else "unknown"
                    ),
                    "runner": spec.get("runner", "unknown"),
                    "status": result.get("status", "unknown"),
                    "score": score,
                    "diagnostics": result.get("diagnostics", {}),
                    "metrics": result.get("metrics", {}),
                    "artifacts_dir": str(benchmark_file.parent),
                }
            )
        except Exception:
            continue

    # Also look for individual score files
    for score_file in root_dir.rglob("frontier_score.json"):
        score_parent = score_file.parent.resolve()
        if any(
            root == score_parent or root in score_parent.parents
            for root in aggregate_roots
        ):
            continue
        if (score_file.parent / "benchmark_result.json").exists():
            continue
        try:
            with open(score_file, "r") as f:
                score = json.load(f)

                # Infer metadata from path
                parts = score_file.parts
                scenario = "unknown"
                model = "unknown"

                # Try to extract from path structure
                for i, part in enumerate(parts):
                    if part.startswith("f") and "_" in part:
                        scenario = part
                    if i > 0 and "_" in parts[i - 1]:
                        model = parts[i - 1]

                results.append(
                    {
                        "scenario": scenario,
                        "model": model,
                        "provider": "unknown",
                        "score": score,
                    }
                )
        except Exception:
            continue

    # Also check for legacy score.json
    for score_file in root_dir.rglob("score.json"):
        score_parent = score_file.parent.resolve()
        if any(
            root == score_parent or root in score_parent.parents
            for root in aggregate_roots
        ):
            continue
        # Skip if we already have frontier_score.json in same dir
        if (score_file.parent / "frontier_score.json").exists():
            continue
        if (score_file.parent / "benchmark_result.json").exists():
            continue

        try:
            with open(score_file, "r") as f:
                score = json.load(f)

                # Convert legacy format to frontier format
                frontier_score = {
                    "success": score.get("success", False),
                    "composite_score": 1.0 if score.get("success") else 0.0,
                    "dimensions": {
                        "correctness": (
                            1.0
                            if score.get("subgoals", {}).get("email_parsed")
                            else 0.0
                        ),
                        "completeness": (
                            sum(score.get("subgoals", {}).values()) / 4.0
                            if score.get("subgoals")
                            else 0.0
                        ),
                        "efficiency": 1.0,
                        "communication_quality": 0.5,
                        "domain_knowledge": 0.5,
                        "safety_alignment": 1.0,
                    },
                    "steps_taken": score.get("costs", {}).get("actions", 0),
                    "time_elapsed_ms": score.get("costs", {}).get("time_ms", 0),
                    "legacy": True,
                }

                parts = score_file.parts
                scenario = "unknown"
                model = "unknown"

                for part in parts:
                    if "macrocompute" in part or part.startswith("p"):
                        scenario = part
                    if any(x in part for x in ["gpt", "claude", "gemini", "grok"]):
                        model = part

                results.append(
                    {
                        "scenario": scenario,
                        "model": model,
                        "provider": "unknown",
                        "score": frontier_score,
                    }
                )
        except Exception:
            continue

    return _attach_workflow_baseline_deltas(results)


def generate_csv_report(results: List[Dict], output_path: Path) -> None:
    """Generate CSV report with all results."""
    _attach_workflow_baseline_deltas(results)

    dimension_keys = _ordered_dimension_keys(results)
    rows = []
    for r in results:
        score = r["score"]
        dims = score.get("dimensions", {})
        baseline = r.get("baseline", {})
        baseline_delta = r.get("baseline_delta", {})
        delta_dims = baseline_delta.get("dimension_deltas", {})

        row = {
            "model": r["model"],
            "provider": r.get("provider", "unknown"),
            "runner": r.get("runner", "unknown"),
            "status": r.get("status", "unknown"),
            "scenario": r["scenario"],
            "family": r.get("family") or score.get("benchmark_family"),
            "workflow_name": r.get("diagnostics", {}).get("workflow_name"),
            "workflow_variant": r.get("diagnostics", {}).get("workflow_variant"),
            "workflow_valid": r.get("diagnostics", {}).get("workflow_valid"),
            "workflow_step_count": r.get("diagnostics", {}).get(
                "workflow_step_count", 0
            ),
            "success": score.get("success", False),
            "composite_score": score.get("composite_score", 0.0),
            "steps_taken": score.get("steps_taken", 0),
            "time_ms": score.get("time_elapsed_ms", 0),
            "difficulty": score.get("scenario_difficulty", "unknown"),
            "latency_p95_ms": r.get("metrics", {}).get("latency_p95_ms", 0),
            "llm_calls": r.get("metrics", {}).get("llm_calls", 0),
            "total_tokens": r.get("metrics", {}).get("total_tokens", 0),
            "estimated_cost_usd": r.get("metrics", {}).get("estimated_cost_usd"),
            "baseline_available": baseline.get("available", False),
            "baseline_workflow_name": baseline.get("workflow_name"),
            "baseline_workflow_variant": baseline.get("workflow_variant"),
            "baseline_workflow_valid": baseline.get("workflow_valid"),
            "baseline_workflow_issue_count": baseline.get("workflow_issue_count"),
            "baseline_workflow_success_assertion_count": baseline.get(
                "workflow_success_assertion_count"
            ),
            "baseline_workflow_success_assertions_passed": baseline.get(
                "workflow_success_assertions_passed"
            ),
            "baseline_workflow_success_assertions_failed": baseline.get(
                "workflow_success_assertions_failed"
            ),
            "baseline_success": baseline.get("success"),
            "baseline_composite_score": baseline.get("composite_score"),
            "baseline_steps_taken": baseline.get("steps_taken"),
            "baseline_time_ms": baseline.get("time_ms"),
            "workflow_valid_delta": baseline_delta.get("workflow_valid_delta"),
            "workflow_issue_count_delta": baseline_delta.get(
                "workflow_issue_count_delta"
            ),
            "workflow_success_assertion_count_delta": baseline_delta.get(
                "workflow_success_assertion_count_delta"
            ),
            "workflow_success_assertions_passed_delta": baseline_delta.get(
                "workflow_success_assertions_passed_delta"
            ),
            "workflow_success_assertions_failed_delta": baseline_delta.get(
                "workflow_success_assertions_failed_delta"
            ),
            "success_delta": baseline_delta.get("success_delta"),
            "composite_score_delta": baseline_delta.get("composite_score_delta"),
            "steps_delta": baseline_delta.get("steps_taken_delta"),
            "time_delta_ms": baseline_delta.get("time_ms_delta"),
            "initial_snapshot_id": r.get("diagnostics", {}).get("initial_snapshot_id"),
            "final_snapshot_id": r.get("diagnostics", {}).get("final_snapshot_id"),
            "artifacts_dir": r.get("artifacts_dir"),
        }
        for key in dimension_keys:
            row[key] = dims.get(key, 0.0)
            row[f"delta_{key}"] = delta_dims.get(key)
        rows.append(row)

    if not rows:
        return

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def generate_markdown_leaderboard(results: List[Dict]) -> str:
    """Generate markdown leaderboard."""
    _attach_workflow_baseline_deltas(results)

    if not results:
        return "No results to display."

    # Group by model
    by_model = defaultdict(list)
    for r in results:
        by_model[r["model"]].append(r)

    # Calculate aggregate stats per model
    model_stats = []
    for model, model_results in by_model.items():
        scores = [r["score"] for r in model_results]

        success_count = sum(1 for s in scores if s.get("success"))
        success_rate = success_count / len(scores) if scores else 0.0
        avg_composite = (
            sum(s.get("composite_score", 0.0) for s in scores) / len(scores)
            if scores
            else 0.0
        )
        avg_steps = (
            sum(s.get("steps_taken", 0) for s in scores) / len(scores)
            if scores
            else 0.0
        )

        # Aggregate dimension scores
        dims = defaultdict(list)
        for s in scores:
            for k, v in s.get("dimensions", {}).items():
                dims[k].append(v)
        avg_dims = {k: sum(v) / len(v) if v else 0.0 for k, v in dims.items()}

        model_stats.append(
            {
                "model": model,
                "provider": model_results[0].get("provider", "unknown"),
                "scenarios_run": len(scores),
                "success_count": success_count,
                "success_rate": success_rate,
                "avg_composite": avg_composite,
                "avg_steps": avg_steps,
                "avg_dims": avg_dims,
            }
        )

    # Sort by success rate, then composite score
    model_stats.sort(
        key=lambda x: (x["success_rate"], x["avg_composite"]), reverse=True
    )

    # Build markdown
    dimension_keys = _ordered_dimension_keys(results)
    lines = [
        "# 🏆 Frontier Model Evaluation Leaderboard",
        "",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"**Total Evaluations:** {len(results)}",
        f"**Models Tested:** {len(model_stats)}",
        "",
        "---",
        "",
        "## Overall Rankings",
        "",
        "| Rank | Model | Provider | Success Rate | Avg Score | Scenarios | Avg Steps |",
        "|------|-------|----------|--------------|-----------|-----------|-----------|",
    ]

    for idx, stat in enumerate(model_stats, 1):
        rank_emoji = {1: "🥇", 2: "🥈", 3: "🥉"}.get(idx, f"{idx}")
        success_pct = f"{stat['success_rate']*100:.1f}%"
        success_icon = (
            "✅"
            if stat["success_rate"] >= 0.7
            else "⚠️" if stat["success_rate"] >= 0.3 else "❌"
        )

        lines.append(
            f"| {rank_emoji} | **{stat['model']}** | {stat['provider']} | {success_icon} {success_pct} | "
            f"{stat['avg_composite']:.3f} | {stat['scenarios_run']} | {stat['avg_steps']:.1f} |"
        )

    lines.extend(["", "---", "", "## Dimension Breakdown", ""])

    dimension_header = " | ".join(_dimension_label(key) for key in dimension_keys)
    lines.append(f"| Model | {dimension_header} |")
    lines.append("|-------|" + "|".join("---" for _ in dimension_keys) + "|")

    for stat in model_stats:
        dims = stat["avg_dims"]
        values = " | ".join(f"{dims.get(key, 0):.2f}" for key in dimension_keys)
        lines.append(f"| {stat['model']} | {values} |")

    workflow_baselines = _select_workflow_baselines(results)
    if workflow_baselines:
        lines.extend(["", "---", "", "## Workflow Baselines", ""])
        lines.append(
            "| Scenario | Family | Baseline Workflow | Score | Steps | Time (ms) |"
        )
        lines.append(
            "|----------|--------|-------------------|-------|-------|-----------|"
        )
        for (family, scenario), baseline_result in sorted(
            workflow_baselines.items(), key=lambda item: (item[0][1], item[0][0])
        ):
            baseline = _build_baseline_summary(baseline_result)
            workflow_label = (
                f"{baseline.get('workflow_name')} ({baseline.get('workflow_variant')})"
                if baseline.get("workflow_variant")
                else str(baseline.get("workflow_name"))
            )
            lines.append(
                f"| {scenario} | {family} | {workflow_label} | "
                f"{baseline.get('composite_score', 0.0):.3f} | "
                f"{baseline.get('steps_taken', 0)} | {baseline.get('time_ms', 0)} |"
            )

    lines.extend(["", "---", "", "## Detailed Results by Scenario", ""])

    # Group by scenario
    by_scenario = defaultdict(list)
    for r in results:
        by_scenario[r["scenario"]].append(r)

    for scenario, scenario_results in sorted(by_scenario.items()):
        lines.append(f"### {scenario}")
        lines.append("")
        lines.append(
            "| Model | Success | Score | Δ Score | Steps | Δ Steps | Assertions | Δ Pass | Baseline | Dimensions |"
        )
        lines.append(
            "|-------|---------|-------|---------|-------|---------|------------|--------|----------|------------|"
        )

        for r in sorted(
            scenario_results,
            key=lambda x: x["score"].get("composite_score", 0),
            reverse=True,
        ):
            score = r["score"]
            success_icon = "✅" if score.get("success") else "❌"
            composite = score.get("composite_score", 0.0)
            steps = score.get("steps_taken", 0)
            baseline = r.get("baseline", {})
            baseline_delta = r.get("baseline_delta", {})
            workflow_validation = score.get("workflow_validation", {})
            baseline_label = (
                f"{baseline.get('workflow_name')}:{baseline.get('workflow_variant')}"
                if baseline.get("available") and baseline.get("workflow_variant")
                else (
                    str(baseline.get("workflow_name"))
                    if baseline.get("available")
                    else "n/a"
                )
            )

            # Top 3 dimensions
            dims = score.get("dimensions", {})
            top_dims = sorted(dims.items(), key=lambda x: x[1], reverse=True)[:3]
            dims_str = ", ".join([f"{k[:3]}:{v:.2f}" for k, v in top_dims])
            assertions_str = (
                f"{workflow_validation.get('success_assertions_passed', 0)}/"
                f"{workflow_validation.get('success_assertion_count', 0)}"
                if workflow_validation
                else "n/a"
            )

            lines.append(
                f"| {r['model']} | {success_icon} | {composite:.3f} | "
                f"{_format_signed_float(baseline_delta.get('composite_score_delta'))} | "
                f"{steps} | {_format_signed_int(baseline_delta.get('steps_taken_delta'))} | "
                f"{assertions_str} | "
                f"{_format_signed_int(baseline_delta.get('workflow_success_assertions_passed_delta'))} | "
                f"{baseline_label} | {dims_str} |"
            )

        lines.append("")

    lines.extend(
        [
            "---",
            "",
            "## Insights & Recommendations",
            "",
            f"### Best Overall: {model_stats[0]['model']}" if model_stats else "",
            (
                f"- Success rate: {model_stats[0]['success_rate']*100:.1f}%"
                if model_stats
                else ""
            ),
            (
                f"- Average composite score: {model_stats[0]['avg_composite']:.3f}"
                if model_stats
                else ""
            ),
            (
                f"- Average steps: {model_stats[0]['avg_steps']:.1f}"
                if model_stats
                else ""
            ),
            "",
            "### Performance Trends",
        ]
    )

    # Identify strengths/weaknesses
    if model_stats:
        best_model = model_stats[0]
        dims = best_model["avg_dims"]
        sorted_dims = sorted(dims.items(), key=lambda x: x[1], reverse=True)

        lines.append(f"**{best_model['model']} strengths:**")
        for dim, score in sorted_dims[:2]:
            lines.append(f"- {dim}: {score:.3f}")

        lines.append("")
        lines.append("**Areas for improvement:**")
        for dim, score in sorted_dims[-2:]:
            lines.append(f"- {dim}: {score:.3f}")

    return "\n".join(lines)


@app.command(name="generate")
def generate_report(
    root: Path = typer.Option(..., help="Root directory containing evaluation results"),
    format: str = typer.Option("markdown", help="Output format: markdown, csv, json"),
    output: Optional[Path] = typer.Option(
        None, help="Output file path (defaults to stdout for markdown)"
    ),
    include_legacy: bool = typer.Option(True, help="Include legacy score.json files"),
) -> None:
    """Generate comprehensive evaluation report from results directory."""

    if not root.exists():
        typer.echo(f"❌ Directory not found: {root}", err=True)
        raise typer.Exit(1)

    typer.echo(f"📊 Loading results from: {root}")

    results = load_all_results(root)

    if not results:
        typer.echo("⚠️  No results found", err=True)
        raise typer.Exit(1)

    typer.echo(f"✅ Loaded {len(results)} evaluation results")

    if format == "csv":
        output_path = output or (root / "leaderboard.csv")
        generate_csv_report(results, output_path)
        typer.echo(f"✅ CSV report saved to: {output_path}")

    elif format == "json":
        output_path = output or (root / "leaderboard.json")
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2)
        typer.echo(f"✅ JSON report saved to: {output_path}")

    elif format == "markdown":
        markdown = generate_markdown_leaderboard(results)

        if output:
            with open(output, "w") as f:
                f.write(markdown)
            typer.echo(f"✅ Markdown report saved to: {output}")
        else:
            typer.echo(markdown)

    else:
        typer.echo(f"❌ Unknown format: {format}", err=True)
        typer.echo("Supported formats: markdown, csv, json", err=True)
        raise typer.Exit(1)


@app.command(name="compare")
def compare_models(
    root: Path = typer.Option(..., help="Root directory containing evaluation results"),
    models: str = typer.Option(..., help="Comma-separated list of models to compare"),
    scenario: Optional[str] = typer.Option(None, help="Filter by specific scenario"),
) -> None:
    """Compare specific models head-to-head."""

    results = load_all_results(root)

    if not results:
        typer.echo("⚠️  No results found", err=True)
        raise typer.Exit(1)

    model_list = [m.strip() for m in models.split(",")]

    # Filter results
    filtered = [r for r in results if r["model"] in model_list]
    if scenario:
        filtered = [r for r in filtered if r["scenario"] == scenario]

    if not filtered:
        typer.echo(f"⚠️  No results found for models: {models}", err=True)
        raise typer.Exit(1)

    # Group by model
    by_model = defaultdict(list)
    for r in filtered:
        by_model[r["model"]].append(r)

    # Print comparison
    typer.echo("=" * 80)
    typer.echo(f"🔬 Model Comparison: {', '.join(model_list)}")
    if scenario:
        typer.echo(f"   Scenario: {scenario}")
    typer.echo("=" * 80)
    typer.echo("")

    for model in model_list:
        model_results = by_model.get(model, [])

        if not model_results:
            typer.echo(f"{model}: No results")
            continue

        scores = [r["score"] for r in model_results]
        success_rate = sum(1 for s in scores if s.get("success")) / len(scores)
        avg_composite = sum(s.get("composite_score", 0.0) for s in scores) / len(scores)
        avg_steps = sum(s.get("steps_taken", 0) for s in scores) / len(scores)

        typer.echo(f"{'='*80}")
        typer.echo(f"Model: {model}")
        typer.echo(f"  Scenarios: {len(scores)}")
        typer.echo(f"  Success Rate: {success_rate*100:.1f}%")
        typer.echo(f"  Avg Composite Score: {avg_composite:.3f}")
        typer.echo(f"  Avg Steps: {avg_steps:.1f}")

        # Dimension averages
        dims = defaultdict(list)
        for s in scores:
            for k, v in s.get("dimensions", {}).items():
                dims[k].append(v)

        typer.echo("  Dimensions:")
        for dim, values in sorted(dims.items()):
            avg = sum(values) / len(values) if values else 0.0
            typer.echo(f"    - {dim}: {avg:.3f}")

        typer.echo("")


@app.command(name="summary")
def quick_summary(
    root: Path = typer.Option(..., help="Root directory containing evaluation results"),
) -> None:
    """Print a quick summary of evaluation results."""

    results = load_all_results(root)

    if not results:
        typer.echo("⚠️  No results found", err=True)
        raise typer.Exit(1)

    # Overall stats
    success_count = sum(1 for r in results if r["score"].get("success"))
    success_rate = success_count / len(results)
    avg_composite = sum(r["score"].get("composite_score", 0.0) for r in results) / len(
        results
    )

    # By model
    by_model = defaultdict(list)
    for r in results:
        by_model[r["model"]].append(r)

    # By scenario
    by_scenario = defaultdict(list)
    for r in results:
        by_scenario[r["scenario"]].append(r)

    typer.echo("=" * 70)
    typer.echo("📊 Evaluation Summary")
    typer.echo("=" * 70)
    typer.echo(f"Total Evaluations: {len(results)}")
    typer.echo(
        f"Success Rate: {success_rate*100:.1f}% ({success_count}/{len(results)})"
    )
    typer.echo(f"Avg Composite Score: {avg_composite:.3f}")
    typer.echo(
        f"Workflow Baselines: {len(_select_workflow_baselines(results))} "
        f"({sum(1 for r in results if r.get('baseline', {}).get('available'))} runs with deltas)"
    )
    typer.echo("")
    typer.echo(f"Models Tested: {len(by_model)}")
    for model, model_results in sorted(
        by_model.items(), key=lambda x: len(x[1]), reverse=True
    ):
        model_success = sum(1 for r in model_results if r["score"].get("success"))
        typer.echo(
            f"  - {model}: {len(model_results)} scenarios ({model_success} successes)"
        )

    typer.echo("")
    typer.echo(f"Scenarios Tested: {len(by_scenario)}")
    for scenario, scenario_results in sorted(by_scenario.items()):
        scenario_success = sum(1 for r in scenario_results if r["score"].get("success"))
        typer.echo(
            f"  - {scenario}: {len(scenario_results)} runs ({scenario_success} successes)"
        )

    typer.echo("=" * 70)


def _ordered_dimension_keys(results: List[Dict[str, Any]]) -> List[str]:
    preferred = [
        "evidence_preservation",
        "blast_radius_minimization",
        "least_privilege",
        "oversharing_avoidance",
        "deadline_compliance",
        "comms_correctness",
        "safe_rollback",
        "correctness",
        "completeness",
        "efficiency",
        "communication_quality",
        "domain_knowledge",
        "safety_alignment",
    ]
    seen = {
        key
        for item in results
        for key in item.get("score", {}).get("dimensions", {}).keys()
    }
    ordered = [key for key in preferred if key in seen]
    ordered.extend(sorted(seen - set(ordered)))
    return ordered


def _dimension_label(name: str) -> str:
    return name.replace("_", " ").title()


def _attach_workflow_baseline_deltas(
    results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    baselines = _select_workflow_baselines(results)
    for result in results:
        baseline = baselines.get(_baseline_key(result))
        result["baseline"] = _build_baseline_summary(baseline)
        result["baseline_delta"] = _build_baseline_delta(result, baseline)
    return results


def _select_workflow_baselines(
    results: List[Dict[str, Any]],
) -> Dict[tuple[str, str], Dict[str, Any]]:
    grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for result in results:
        key = _baseline_key(result)
        if key is None or result.get("runner") != "workflow":
            continue
        grouped[key].append(result)

    baselines: Dict[tuple[str, str], Dict[str, Any]] = {}
    for key, candidates in grouped.items():
        primary_variant = _primary_variant_for_family(key[0])
        ranked = sorted(
            candidates,
            key=lambda item: _baseline_rank(item, primary_variant),
            reverse=True,
        )
        if ranked:
            baselines[key] = ranked[0]
    return baselines


def _baseline_key(result: Dict[str, Any]) -> tuple[str, str] | None:
    family = result.get("family") or result.get("score", {}).get("benchmark_family")
    scenario = result.get("scenario")
    if not family or not scenario:
        return None
    return (str(family), str(scenario))


def _primary_variant_for_family(family_name: str) -> str | None:
    try:
        return get_benchmark_family_manifest(family_name).primary_workflow_variant
    except KeyError:
        return None


def _baseline_rank(
    result: Dict[str, Any], primary_variant: str | None
) -> tuple[int, int, int, int]:
    diagnostics = result.get("diagnostics", {})
    variant = diagnostics.get("workflow_variant")
    return (
        1 if primary_variant and variant == primary_variant else 0,
        1 if diagnostics.get("workflow_name") else 0,
        1 if result.get("status") == "ok" else 0,
        1 if result.get("score", {}).get("success") else 0,
    )


def _build_baseline_summary(baseline: Dict[str, Any] | None) -> Dict[str, Any]:
    if baseline is None:
        return {"available": False}
    score = baseline.get("score", {})
    diagnostics = baseline.get("diagnostics", {})
    workflow_validation = baseline.get("score", {}).get("workflow_validation", {})
    return {
        "available": True,
        "scenario": baseline.get("scenario"),
        "family": baseline.get("family") or score.get("benchmark_family"),
        "model": baseline.get("model"),
        "provider": baseline.get("provider"),
        "runner": baseline.get("runner"),
        "status": baseline.get("status"),
        "success": bool(score.get("success", False)),
        "workflow_name": diagnostics.get("workflow_name"),
        "workflow_variant": diagnostics.get("workflow_variant"),
        "workflow_valid": workflow_validation.get(
            "ok", diagnostics.get("workflow_valid")
        ),
        "workflow_issue_count": int(workflow_validation.get("issue_count", 0)),
        "workflow_success_assertion_count": int(
            workflow_validation.get("success_assertion_count", 0)
        ),
        "workflow_success_assertions_passed": int(
            workflow_validation.get("success_assertions_passed", 0)
        ),
        "workflow_success_assertions_failed": int(
            workflow_validation.get("success_assertions_failed", 0)
        ),
        "composite_score": float(score.get("composite_score", 0.0)),
        "steps_taken": int(score.get("steps_taken", 0)),
        "time_ms": int(score.get("time_elapsed_ms", 0)),
        "artifacts_dir": baseline.get("artifacts_dir"),
    }


def _build_baseline_delta(
    result: Dict[str, Any], baseline: Dict[str, Any] | None
) -> Dict[str, Any]:
    if baseline is None:
        return {"available": False, "dimension_deltas": {}}
    result_score = result.get("score", {})
    baseline_score = baseline.get("score", {})
    result_dims = result_score.get("dimensions", {})
    baseline_dims = baseline_score.get("dimensions", {})
    dimension_keys = sorted(set(result_dims) | set(baseline_dims))
    result_workflow = result_score.get("workflow_validation", {})
    baseline_workflow = baseline_score.get("workflow_validation", {})
    workflow_valid_delta = None
    workflow_issue_count_delta = None
    workflow_success_assertion_count_delta = None
    workflow_success_assertions_passed_delta = None
    workflow_success_assertions_failed_delta = None
    if result_workflow:
        workflow_valid_delta = int(bool(result_workflow.get("ok", False))) - int(
            bool(baseline_workflow.get("ok", False))
        )
        workflow_issue_count_delta = int(result_workflow.get("issue_count", 0)) - int(
            baseline_workflow.get("issue_count", 0)
        )
        workflow_success_assertion_count_delta = int(
            result_workflow.get("success_assertion_count", 0)
        ) - int(baseline_workflow.get("success_assertion_count", 0))
        workflow_success_assertions_passed_delta = int(
            result_workflow.get("success_assertions_passed", 0)
        ) - int(baseline_workflow.get("success_assertions_passed", 0))
        workflow_success_assertions_failed_delta = int(
            result_workflow.get("success_assertions_failed", 0)
        ) - int(baseline_workflow.get("success_assertions_failed", 0))
    return {
        "available": True,
        "workflow_valid_delta": workflow_valid_delta,
        "workflow_issue_count_delta": workflow_issue_count_delta,
        "workflow_success_assertion_count_delta": workflow_success_assertion_count_delta,
        "workflow_success_assertions_passed_delta": workflow_success_assertions_passed_delta,
        "workflow_success_assertions_failed_delta": workflow_success_assertions_failed_delta,
        "success_delta": int(bool(result_score.get("success", False)))
        - int(bool(baseline_score.get("success", False))),
        "composite_score_delta": float(result_score.get("composite_score", 0.0))
        - float(baseline_score.get("composite_score", 0.0)),
        "steps_taken_delta": int(result_score.get("steps_taken", 0))
        - int(baseline_score.get("steps_taken", 0)),
        "time_ms_delta": int(result_score.get("time_elapsed_ms", 0))
        - int(baseline_score.get("time_elapsed_ms", 0)),
        "dimension_deltas": {
            key: float(result_dims.get(key, 0.0)) - float(baseline_dims.get(key, 0.0))
            for key in dimension_keys
        },
    }


def _format_signed_float(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):+0.3f}"


def _format_signed_int(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{int(value):+d}"


if __name__ == "__main__":
    app()
