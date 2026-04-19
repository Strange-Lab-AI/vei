"""vei bench — unified benchmark harness for agent evaluation.

Combines scenario listing, benchmark execution, and scorecard
rendering into a single top-level command.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Run agent benchmarks and produce scorecards.",
)

console = Console()


@app.command("list")
def list_benchmarks(
    kind: str = typer.Option(
        "all",
        help="What to list: scenarios | verticals | families | all",
    ),
) -> None:
    """List available scenarios, vertical packs, and benchmark families."""
    from vei.benchmark.families import list_benchmark_family_manifest
    from vei.verticals.packs import list_vertical_pack_manifests
    from vei.world.manifest import build_scenario_manifest
    from vei.world.scenarios import list_scenarios

    normalized = kind.strip().lower()
    show_all = normalized == "all"

    if show_all or normalized == "scenarios":
        catalog = list_scenarios()
        table = Table(title="Scenarios", show_lines=False)
        table.add_column("Name", style="cyan", min_width=30)
        table.add_column("Type", style="green")
        table.add_column("Difficulty", style="yellow")
        table.add_column("Family")
        table.add_column("Surfaces")
        for name, scenario in sorted(catalog.items()):
            manifest = build_scenario_manifest(name, scenario)
            table.add_row(
                name,
                manifest.scenario_type,
                manifest.difficulty,
                manifest.benchmark_family or "-",
                ", ".join(manifest.tool_families[:4]) or "-",
            )
        console.print(table)

    if show_all or normalized == "verticals":
        packs = list_vertical_pack_manifests()
        table = Table(title="Vertical Packs", show_lines=False)
        table.add_column("Name", style="cyan", min_width=25)
        table.add_column("Company", style="green")
        table.add_column("Surfaces")
        table.add_column("Proves")
        for pack in packs:
            table.add_row(
                pack.name,
                pack.company_name,
                ", ".join(pack.key_surfaces[:4]),
                ", ".join(pack.proves[:3]),
            )
        console.print(table)

    if show_all or normalized == "families":
        families = list_benchmark_family_manifest()
        table = Table(title="Benchmark Families", show_lines=False)
        table.add_column("Name", style="cyan", min_width=25)
        table.add_column("Title", style="green")
        table.add_column("Role")
        table.add_column("Workflow")
        table.add_column("Scenarios", justify="right")
        table.add_column("Tags")
        for family in families:
            table.add_row(
                family.name,
                family.title,
                family.benchmark_role,
                family.workflow_name or "-",
                str(len(family.scenario_names)),
                ", ".join(family.tags[:3]) or "-",
            )
        console.print(table)


@app.command()
def run(
    scenario: list[str] = typer.Option(
        [], "--scenario", "-s", help="Scenario(s) to benchmark"
    ),
    family: list[str] = typer.Option(
        [], "--family", "-f", help="Benchmark family/families"
    ),
    runner: str = typer.Option(
        "scripted", help="Runner: scripted | bc | llm | workflow"
    ),
    seed: int = typer.Option(42042, help="Seed for reproducibility"),
    max_steps: int = typer.Option(40, help="Maximum steps per scenario"),
    model: Optional[str] = typer.Option(None, help="Model name for llm runner"),
    provider: str = typer.Option("auto", help="LLM provider for llm runner"),
    output: Path = typer.Option(
        Path("_vei_out/bench"), help="Output directory for artifacts"
    ),
    run_id: Optional[str] = typer.Option(None, help="Custom run identifier"),
) -> None:
    """Run benchmarks against one or more scenarios and produce a scorecard."""
    from vei.benchmark.api import (
        resolve_scenarios,
        run_benchmark_batch,
    )
    from vei.benchmark.api import BenchmarkCaseSpec
    from vei.benchmark.workflows import resolve_benchmark_workflow_name

    normalized_runner = runner.strip().lower()
    if normalized_runner not in {"scripted", "bc", "llm", "workflow"}:
        raise typer.BadParameter("runner must be one of: scripted, bc, llm, workflow")
    if normalized_runner == "llm" and not model:
        raise typer.BadParameter("llm runner requires --model")

    if not scenario and not family:
        scenario = ["multi_channel"]

    scenario_names = resolve_scenarios(
        scenario_names=scenario,
        family_names=family,
    )

    batch_id = run_id or f"bench_{normalized_runner}_{int(time.time())}"
    run_dir = output / batch_id

    specs = [
        BenchmarkCaseSpec(
            runner=normalized_runner,  # type: ignore[arg-type]
            scenario_name=name,
            workflow_name=(
                resolve_benchmark_workflow_name(scenario_name=name)
                if normalized_runner == "workflow"
                else None
            ),
            seed=seed,
            artifacts_dir=run_dir / name,
            branch=name,
            score_mode="full",
            frontier=name.startswith("f"),
            model=model,
            provider=provider if normalized_runner == "llm" else None,
            max_steps=max_steps,
        )
        for name in scenario_names
    ]

    console.print(
        f"[bold]Running {len(specs)} benchmark(s) "
        f"with runner={normalized_runner}...[/bold]"
    )
    batch = run_benchmark_batch(specs, run_id=batch_id, output_dir=run_dir)

    _render_scorecard(batch, run_dir)


@app.command()
def scorecard(
    artifacts_dir: Path = typer.Argument(
        ..., help="Path to benchmark run directory with results"
    ),
) -> None:
    """Render a scorecard from existing benchmark results."""
    summary_path = artifacts_dir / "benchmark_summary.json"
    if not summary_path.exists():
        raise typer.BadParameter(f"No benchmark_summary.json found in {artifacts_dir}")

    from vei.benchmark.api import BenchmarkBatchResult

    data = json.loads(summary_path.read_text(encoding="utf-8"))
    batch = BenchmarkBatchResult.model_validate(data)
    _render_scorecard(batch, artifacts_dir)


def _render_scorecard(batch: object, run_dir: Path) -> None:
    table = Table(title="Scorecard", show_lines=True)
    table.add_column("Scenario", style="cyan", min_width=25)
    table.add_column("Status", justify="center")
    table.add_column("Score", justify="right")
    table.add_column("Actions", justify="right")
    table.add_column("Time (ms)", justify="right")

    for result in batch.results:
        status = "[green]PASS[/green]" if result.success else "[red]FAIL[/red]"
        composite = result.score.get("composite_score", 0.0)
        score_str = f"{float(composite):.2f}" if composite else "—"
        table.add_row(
            result.spec.scenario_name,
            status,
            score_str,
            str(result.metrics.actions),
            str(result.metrics.elapsed_ms),
        )

    console.print(table)

    summary = batch.summary
    console.print("\n[bold]Summary[/bold]")
    console.print(
        f"  Scenarios: {summary.total_runs}  |  "
        f"Passed: {summary.success_count}  |  "
        f"Rate: {summary.success_rate:.0%}  |  "
        f"Avg score: {summary.average_composite_score:.2f}"
    )
    if summary.estimated_cost_usd is not None:
        console.print(f"  Est. cost: ${summary.estimated_cost_usd:.4f}")
    console.print(f"  Artifacts: {run_dir}\n")

    scorecard_data = {
        "run_id": batch.run_id,
        "total": summary.total_runs,
        "passed": summary.success_count,
        "success_rate": summary.success_rate,
        "average_score": summary.average_composite_score,
        "total_actions": summary.total_actions,
        "total_time_ms": summary.total_time_ms,
        "estimated_cost_usd": summary.estimated_cost_usd,
        "cases": [
            {
                "scenario": r.spec.scenario_name,
                "runner": r.spec.runner,
                "success": r.success,
                "score": r.score,
                "actions": r.metrics.actions,
                "elapsed_ms": r.metrics.elapsed_ms,
            }
            for r in batch.results
        ],
    }
    scorecard_path = run_dir / "scorecard.json"
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(scorecard_data, indent=2), encoding="utf-8")
    console.print(f"  Scorecard written to {scorecard_path}")
