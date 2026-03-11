from __future__ import annotations

import os
import json
import shlex
import time
from contextlib import contextmanager
from pathlib import Path

import typer
from typer.models import OptionInfo

from vei.benchmark.api import (
    FRONTIER_SCENARIO_SETS,
    get_benchmark_family_manifest,
    list_benchmark_family_manifest,
    resolve_benchmark_workflow_name,
    resolve_scenarios,
    run_benchmark_batch,
    run_benchmark_case,
)
from vei.benchmark.models import (
    BenchmarkCaseSpec,
    BenchmarkDemoResult,
    BenchmarkDemoSpec,
    BenchmarkShowcaseExampleResult,
    BenchmarkShowcaseResult,
    BenchmarkShowcaseSpec,
    BenchmarkSuiteResult,
    BenchmarkSuiteSpec,
)
from vei.benchmark.showcase import (
    render_showcase_overview,
    resolve_showcase_examples,
)
from vei.benchmark.workflows import get_benchmark_family_workflow_spec
from vei.benchmark.workflows import get_benchmark_family_workflow_variant
from vei.cli.vei_report import (
    generate_csv_report,
    generate_markdown_leaderboard,
    load_all_results,
)


app = typer.Typer(add_completion=False)


def _coerce_option(value: object, default: str) -> str:
    return default if isinstance(value, OptionInfo) else str(value)


@contextmanager
def _temporary_env_var(name: str, value: str | None):
    previous = os.environ.get(name)
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


def _shell_quote(value: str | Path) -> str:
    return shlex.quote(str(value))


def _run_benchmark_demo(spec: BenchmarkDemoSpec) -> BenchmarkDemoResult:
    manifest = get_benchmark_family_manifest(spec.family_name)
    workflow_name = manifest.workflow_name
    if workflow_name is None:
        raise ValueError(
            f"benchmark family {spec.family_name} does not define a workflow baseline"
        )

    workflow_variant = spec.workflow_variant or manifest.primary_workflow_variant
    scenario_name = manifest.scenario_names[0]
    if workflow_variant is not None:
        scenario_name = get_benchmark_family_workflow_variant(
            workflow_name, workflow_variant
        ).scenario_name

    if spec.compare_runner == "llm" and not spec.compare_model:
        raise ValueError("llm demo requires compare_model")
    if spec.compare_runner == "bc" and spec.compare_bc_model_path is None:
        raise ValueError("bc demo requires compare_bc_model_path")

    demo_dir = spec.artifacts_root / spec.run_id
    demo_dir.mkdir(parents=True, exist_ok=True)
    state_dir = demo_dir / "state"
    baseline_artifacts_dir = demo_dir / "baseline" / scenario_name
    comparison_artifacts_dir = demo_dir / "comparison" / scenario_name
    aggregate_results_path = demo_dir / "aggregate_results.json"
    benchmark_summary_path = demo_dir / "benchmark_summary.json"
    report_markdown_path = demo_dir / "leaderboard.md"
    report_csv_path = demo_dir / "leaderboard.csv"
    report_json_path = demo_dir / "leaderboard.json"

    specs = [
        BenchmarkCaseSpec(
            runner="workflow",
            scenario_name=scenario_name,
            workflow_name=workflow_name,
            workflow_variant=workflow_variant,
            seed=spec.seed,
            artifacts_dir=baseline_artifacts_dir,
            branch=f"{spec.family_name}.baseline",
            score_mode=spec.score_mode,
        ),
        BenchmarkCaseSpec(
            runner=spec.compare_runner,
            scenario_name=scenario_name,
            seed=spec.seed,
            artifacts_dir=comparison_artifacts_dir,
            branch=f"{spec.family_name}.{spec.compare_runner}",
            score_mode=spec.score_mode,
            model=spec.compare_model,
            provider=(spec.compare_provider if spec.compare_runner == "llm" else None),
            bc_model_path=spec.compare_bc_model_path,
            task=spec.compare_task,
            max_steps=spec.max_steps,
        ),
    ]

    with _temporary_env_var("VEI_STATE_DIR", str(state_dir)):
        batch = run_benchmark_batch(specs, run_id=spec.run_id, output_dir=demo_dir)

    results = load_all_results(demo_dir)
    report_markdown_path.write_text(
        generate_markdown_leaderboard(results), encoding="utf-8"
    )
    generate_csv_report(results, report_csv_path)
    report_json_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    baseline_result = next(
        result for result in batch.results if result.spec.runner == "workflow"
    )
    comparison_result = next(
        result for result in batch.results if result.spec.runner == spec.compare_runner
    )
    baseline_validation = _workflow_validation_summary(baseline_result.score)
    comparison_validation = _workflow_validation_summary(comparison_result.score)
    baseline_branch = baseline_result.diagnostics.branch or baseline_result.spec.branch
    comparison_branch = (
        comparison_result.diagnostics.branch or comparison_result.spec.branch
    )
    inspection_commands: list[str] = []
    if baseline_branch:
        inspection_commands.extend(
            [
                f"vei-world list --state-dir {_shell_quote(state_dir)} --branch {_shell_quote(baseline_branch)}",
                f"vei-world show --state-dir {_shell_quote(state_dir)} --branch {_shell_quote(baseline_branch)} --receipts-tail 5",
            ]
        )
    if comparison_branch:
        inspection_commands.extend(
            [
                f"vei-world list --state-dir {_shell_quote(state_dir)} --branch {_shell_quote(comparison_branch)}",
                f"vei-world receipts --state-dir {_shell_quote(state_dir)} --branch {_shell_quote(comparison_branch)} --tail 10",
            ]
        )
        if (
            comparison_result.diagnostics.initial_snapshot_id is not None
            and comparison_result.diagnostics.final_snapshot_id is not None
        ):
            inspection_commands.append(
                "vei-world diff "
                f"--state-dir {_shell_quote(state_dir)} "
                f"--branch {_shell_quote(comparison_branch)} "
                f"--snapshot-from {comparison_result.diagnostics.initial_snapshot_id} "
                f"--snapshot-to {comparison_result.diagnostics.final_snapshot_id}"
            )

    result = BenchmarkDemoResult(
        run_id=spec.run_id,
        family_name=spec.family_name,
        scenario_name=scenario_name,
        baseline_workflow_name=workflow_name,
        baseline_workflow_variant=workflow_variant,
        compare_runner=spec.compare_runner,
        compare_model=spec.compare_model,
        demo_dir=demo_dir,
        state_dir=state_dir,
        aggregate_results_path=aggregate_results_path,
        benchmark_summary_path=benchmark_summary_path,
        report_markdown_path=report_markdown_path,
        report_csv_path=report_csv_path,
        report_json_path=report_json_path,
        baseline_artifacts_dir=baseline_artifacts_dir,
        comparison_artifacts_dir=comparison_artifacts_dir,
        baseline_blueprint_asset_path=baseline_artifacts_dir / "blueprint_asset.json",
        comparison_blueprint_asset_path=(
            comparison_artifacts_dir / "blueprint_asset.json"
        ),
        baseline_blueprint_path=baseline_artifacts_dir / "blueprint.json",
        comparison_blueprint_path=comparison_artifacts_dir / "blueprint.json",
        baseline_contract_path=baseline_artifacts_dir / "contract.json",
        comparison_contract_path=comparison_artifacts_dir / "contract.json",
        baseline_branch=baseline_branch,
        comparison_branch=comparison_branch,
        baseline_success=baseline_result.success,
        comparison_success=comparison_result.success,
        baseline_score=float(baseline_result.score.get("composite_score", 0.0)),
        comparison_score=float(comparison_result.score.get("composite_score", 0.0)),
        baseline_assertions_passed=baseline_validation["passed"],
        baseline_assertions_total=baseline_validation["total"],
        comparison_assertions_passed=comparison_validation["passed"],
        comparison_assertions_total=comparison_validation["total"],
        summary=batch.summary,
        inspection_commands=inspection_commands,
    )
    (demo_dir / "demo_result.json").write_text(
        json.dumps(result.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )
    return result


def _workflow_validation_summary(score: dict[str, object]) -> dict[str, int]:
    validation = score.get("workflow_validation")
    if not isinstance(validation, dict):
        return {"passed": 0, "total": 0}
    passed = int(validation.get("success_assertions_passed", 0) or 0)
    total = int(validation.get("success_assertion_count", 0) or 0)
    return {"passed": passed, "total": total}


def _run_benchmark_showcase(spec: BenchmarkShowcaseSpec) -> BenchmarkShowcaseResult:
    showcase_dir = spec.artifacts_root / spec.run_id
    showcase_dir.mkdir(parents=True, exist_ok=True)
    examples_dir = showcase_dir / "examples"
    examples = resolve_showcase_examples(spec.example_names)
    example_results: list[BenchmarkShowcaseExampleResult] = []

    for example in examples:
        demo = _run_benchmark_demo(
            BenchmarkDemoSpec(
                family_name=example.family_name,
                compare_runner=spec.compare_runner,
                workflow_variant=example.workflow_variant,
                seed=spec.seed,
                artifacts_root=examples_dir,
                run_id=example.name,
                score_mode=spec.score_mode,
                max_steps=spec.max_steps,
                compare_model=spec.compare_model,
                compare_provider=spec.compare_provider,
                compare_bc_model_path=spec.compare_bc_model_path,
                compare_task=spec.compare_task,
            )
        )
        example_results.append(
            BenchmarkShowcaseExampleResult(example=example, demo=demo)
        )

    result = BenchmarkShowcaseResult(
        run_id=spec.run_id,
        showcase_dir=showcase_dir,
        overview_markdown_path=showcase_dir / "showcase_overview.md",
        overview_json_path=showcase_dir / "showcase_result.json",
        example_count=len(example_results),
        baseline_success_count=sum(
            1 for item in example_results if item.demo.baseline_success
        ),
        comparison_success_count=sum(
            1 for item in example_results if item.demo.comparison_success
        ),
        examples=example_results,
    )
    result.overview_markdown_path.write_text(
        render_showcase_overview(result),
        encoding="utf-8",
    )
    result.overview_json_path.write_text(
        json.dumps(result.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )
    return result


def _run_benchmark_suite(spec: BenchmarkSuiteSpec) -> BenchmarkSuiteResult:
    selected_manifests = (
        [get_benchmark_family_manifest(name) for name in spec.family_names]
        if spec.family_names
        else list_benchmark_family_manifest()
    )
    if not selected_manifests:
        raise ValueError("benchmark suite requires at least one family")

    suite_dir = spec.artifacts_root / spec.run_id
    suite_dir.mkdir(parents=True, exist_ok=True)
    aggregate_results_path = suite_dir / "aggregate_results.json"
    benchmark_summary_path = suite_dir / "benchmark_summary.json"
    report_markdown_path = suite_dir / "leaderboard.md"
    report_csv_path = suite_dir / "leaderboard.csv"
    report_json_path = suite_dir / "leaderboard.json"

    scenario_names: dict[str, str] = {}
    workflow_variants: dict[str, str | None] = {}
    case_artifacts_dirs: dict[str, Path] = {}
    blueprint_paths: dict[str, Path] = {}
    blueprint_asset_paths: dict[str, Path] = {}
    contract_paths: dict[str, Path] = {}
    specs: list[BenchmarkCaseSpec] = []
    for manifest in selected_manifests:
        workflow_name = manifest.workflow_name
        if workflow_name is None:
            raise ValueError(
                f"benchmark family {manifest.name} does not define a workflow baseline"
            )
        workflow_variant = manifest.primary_workflow_variant
        scenario_name = manifest.scenario_names[0]
        if workflow_variant is not None:
            scenario_name = get_benchmark_family_workflow_variant(
                workflow_name, workflow_variant
            ).scenario_name
        artifacts_dir = suite_dir / manifest.name / scenario_name
        scenario_names[manifest.name] = scenario_name
        workflow_variants[manifest.name] = workflow_variant
        case_artifacts_dirs[manifest.name] = artifacts_dir
        blueprint_asset_paths[manifest.name] = artifacts_dir / "blueprint_asset.json"
        blueprint_paths[manifest.name] = artifacts_dir / "blueprint.json"
        contract_paths[manifest.name] = artifacts_dir / "contract.json"
        specs.append(
            BenchmarkCaseSpec(
                runner="workflow",
                scenario_name=scenario_name,
                workflow_name=workflow_name,
                workflow_variant=workflow_variant,
                seed=spec.seed,
                artifacts_dir=artifacts_dir,
                branch=f"{manifest.name}.baseline",
                score_mode=spec.score_mode,
            )
        )

    batch = run_benchmark_batch(specs, run_id=spec.run_id, output_dir=suite_dir)
    results = load_all_results(suite_dir)
    report_markdown_path.write_text(
        generate_markdown_leaderboard(results), encoding="utf-8"
    )
    generate_csv_report(results, report_csv_path)
    report_json_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    result = BenchmarkSuiteResult(
        run_id=spec.run_id,
        family_names=[manifest.name for manifest in selected_manifests],
        scenario_names=scenario_names,
        workflow_variants=workflow_variants,
        suite_dir=suite_dir,
        aggregate_results_path=aggregate_results_path,
        benchmark_summary_path=benchmark_summary_path,
        report_markdown_path=report_markdown_path,
        report_csv_path=report_csv_path,
        report_json_path=report_json_path,
        case_artifacts_dirs=case_artifacts_dirs,
        blueprint_asset_paths=blueprint_asset_paths,
        blueprint_paths=blueprint_paths,
        contract_paths=contract_paths,
        summary=batch.summary,
    )
    (suite_dir / "suite_result.json").write_text(
        json.dumps(result.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )
    return result


@app.command()
def scripted(
    seed: int = typer.Option(42042, help="Router seed"),
    dataset: Path = typer.Option(Path("-"), help="Optional dataset JSON for replay"),
    artifacts: Path = typer.Option(Path("_vei_out/eval"), help="Artifacts directory"),
    scenario: str = typer.Option("multi_channel", help="Scenario name"),
    score_success_mode: str = typer.Option(
        "email", help="Score success criteria: email|full."
    ),
) -> None:
    scenario = _coerce_option(scenario, "multi_channel")
    score_success_mode = _coerce_option(score_success_mode, "email")
    spec = BenchmarkCaseSpec(
        runner="scripted",
        scenario_name=scenario,
        seed=seed,
        artifacts_dir=artifacts,
        dataset_path=None if dataset == Path("-") else dataset,
        replay_mode="overlay" if dataset != Path("-") else None,
        score_mode=score_success_mode.lower().strip(),
    )
    result = run_benchmark_case(spec)
    typer.echo(json.dumps(result.raw_score, indent=2))


@app.command()
def bc(
    model: Path = typer.Option(
        ..., "--model", "-m", exists=True, readable=True, help="Trained BC policy"
    ),
    seed: int = typer.Option(42042, help="Router seed"),
    dataset: Path = typer.Option(Path("-"), help="Optional dataset JSON for replay"),
    artifacts: Path = typer.Option(
        Path("_vei_out/eval_bc"), help="Artifacts directory"
    ),
    max_steps: int = typer.Option(12, help="Max policy steps"),
    scenario: str = typer.Option("multi_channel", help="Scenario name"),
    score_success_mode: str = typer.Option(
        "email", help="Score success criteria: email|full."
    ),
) -> None:
    scenario = _coerce_option(scenario, "multi_channel")
    score_success_mode = _coerce_option(score_success_mode, "email")
    spec = BenchmarkCaseSpec(
        runner="bc",
        scenario_name=scenario,
        seed=seed,
        artifacts_dir=artifacts,
        dataset_path=None if dataset == Path("-") else dataset,
        replay_mode="overlay" if dataset != Path("-") else None,
        score_mode=score_success_mode.lower().strip(),
        bc_model_path=model,
        max_steps=max_steps,
    )
    result = run_benchmark_case(spec)
    typer.echo(json.dumps(result.raw_score, indent=2))


@app.command()
def benchmark(
    runner: str = typer.Option("llm", help="Runner: scripted|bc|llm|workflow"),
    scenario: list[str] = typer.Option(
        [], "--scenario", "-s", help="Scenario(s) to run"
    ),
    family: list[str] = typer.Option(
        [], "--family", "-f", help="Benchmark family/families to run"
    ),
    scenario_set: str | None = typer.Option(
        None,
        help=f"Scenario set to run ({', '.join(FRONTIER_SCENARIO_SETS.keys())})",
    ),
    workflow_name: str | None = typer.Option(
        None, help="Optional explicit workflow name when runner=workflow"
    ),
    workflow_variant: str | None = typer.Option(
        None, help="Optional workflow variant name when runner=workflow"
    ),
    model: str | None = typer.Option(None, help="Model name for llm runner"),
    provider: str = typer.Option(
        "auto", help="LLM provider: openai, anthropic, google, openrouter, auto"
    ),
    bc_model: Path | None = typer.Option(
        None, exists=True, readable=True, help="BC policy file when runner=bc"
    ),
    dataset: Path | None = typer.Option(None, help="Optional replay dataset"),
    artifacts_root: Path = typer.Option(
        Path("_vei_out/benchmark"), help="Root directory for benchmark artifacts"
    ),
    seed: int = typer.Option(42042, help="Seed for reproducibility"),
    max_steps: int = typer.Option(40, help="Maximum steps per scenario"),
    task: str | None = typer.Option(None, help="Task prompt for llm runner"),
    tool_top_k: int = typer.Option(0, help="Visible tool top-k for llm runner"),
    score_success_mode: str = typer.Option(
        "full", help="Score success criteria: email|full."
    ),
    frontier: bool = typer.Option(
        False, help="Use frontier scoring for every selected scenario"
    ),
    use_llm_judge: bool = typer.Option(
        False, help="Use LLM-as-judge during frontier scoring"
    ),
    run_id: str | None = typer.Option(None, help="Optional benchmark run id"),
) -> None:
    normalized_runner = runner.strip().lower()
    if normalized_runner not in {"scripted", "bc", "llm", "workflow"}:
        raise typer.BadParameter("runner must be one of scripted|bc|llm|workflow")
    if normalized_runner == "llm" and not model:
        raise typer.BadParameter("llm runner requires --model")
    if normalized_runner == "bc" and bc_model is None:
        raise typer.BadParameter("bc runner requires --bc-model")
    if workflow_name and normalized_runner != "workflow":
        raise typer.BadParameter("--workflow-name only applies when runner=workflow")
    if workflow_variant and normalized_runner != "workflow":
        raise typer.BadParameter("--workflow-variant only applies when runner=workflow")

    selected_families = family
    if scenario or family or scenario_set:
        selected = scenario
    elif normalized_runner == "workflow":
        selected = []
        selected_families = [item.name for item in list_benchmark_family_manifest()]
    else:
        selected = (
            FRONTIER_SCENARIO_SETS["all_frontier"] if frontier else ["multi_channel"]
        )
    scenario_names = resolve_scenarios(
        scenario_names=selected,
        scenario_set=scenario_set,
        family_names=selected_families,
    )
    if workflow_name:
        if len(scenario_names) != 1:
            raise typer.BadParameter(
                "--workflow-name requires exactly one selected scenario"
            )
        try:
            get_benchmark_family_workflow_spec(workflow_name)
        except KeyError as exc:
            raise typer.BadParameter(str(exc)) from exc
    if workflow_variant:
        if len(scenario_names) != 1:
            raise typer.BadParameter(
                "--workflow-variant requires exactly one selected scenario"
            )
        resolved_workflow_name = workflow_name or resolve_benchmark_workflow_name(
            scenario_name=scenario_names[0]
        )
        if resolved_workflow_name is None:
            raise typer.BadParameter(
                f"no benchmark workflow is registered for scenario {scenario_names[0]}"
            )
        try:
            get_benchmark_family_workflow_variant(
                resolved_workflow_name, workflow_variant
            )
        except KeyError as exc:
            raise typer.BadParameter(str(exc)) from exc

    batch_id = run_id or f"{normalized_runner}_{int(time.time())}"
    run_dir = artifacts_root / batch_id
    specs = [
        BenchmarkCaseSpec(
            runner=normalized_runner,  # type: ignore[arg-type]
            scenario_name=scenario_name,
            workflow_name=(
                workflow_name
                or resolve_benchmark_workflow_name(scenario_name=scenario_name)
                if normalized_runner == "workflow"
                else None
            ),
            workflow_variant=(
                workflow_variant if normalized_runner == "workflow" else None
            ),
            seed=seed,
            artifacts_dir=run_dir / scenario_name,
            branch=scenario_name,
            dataset_path=dataset,
            replay_mode="overlay" if dataset else None,
            score_mode=score_success_mode.lower().strip(),
            frontier=frontier or scenario_name.startswith("f"),
            model=model,
            provider=provider if normalized_runner == "llm" else None,
            bc_model_path=bc_model,
            task=task,
            max_steps=max_steps,
            tool_top_k=tool_top_k,
            use_llm_judge=use_llm_judge,
        )
        for scenario_name in scenario_names
    ]
    batch = run_benchmark_batch(specs, run_id=batch_id, output_dir=run_dir)
    typer.echo(json.dumps(batch.summary.model_dump(), indent=2))
    typer.echo(f"results saved to {run_dir}")


@app.command()
def demo(
    family: str = typer.Option("security_containment", help="Benchmark family to demo"),
    compare_runner: str = typer.Option(
        "scripted", help="Comparison runner: scripted|bc|llm"
    ),
    workflow_variant: str | None = typer.Option(
        None, help="Optional baseline workflow variant override"
    ),
    artifacts_root: Path = typer.Option(
        Path("_vei_out/demo"), help="Root directory for demo artifacts"
    ),
    run_id: str | None = typer.Option(None, help="Optional demo run id"),
    seed: int = typer.Option(42042, help="Seed for reproducibility"),
    score_success_mode: str = typer.Option(
        "full", help="Score success criteria: email|full."
    ),
    max_steps: int = typer.Option(40, help="Maximum steps for comparison runner"),
    compare_model: str | None = typer.Option(
        None, help="Model name when compare-runner=llm"
    ),
    compare_provider: str = typer.Option(
        "auto", help="LLM provider when compare-runner=llm"
    ),
    compare_bc_model: Path | None = typer.Option(
        None,
        exists=True,
        readable=True,
        help="BC policy file when compare-runner=bc",
    ),
    compare_task: str | None = typer.Option(
        None, help="Optional task prompt when compare-runner=llm"
    ),
) -> None:
    normalized_family = family.strip().lower()
    normalized_runner = compare_runner.strip().lower()
    if normalized_runner not in {"scripted", "bc", "llm"}:
        raise typer.BadParameter("compare-runner must be one of scripted|bc|llm")
    try:
        manifest = get_benchmark_family_manifest(normalized_family)
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if normalized_runner == "llm" and not compare_model:
        raise typer.BadParameter("llm demo requires --compare-model")
    if normalized_runner == "bc" and compare_bc_model is None:
        raise typer.BadParameter("bc demo requires --compare-bc-model")
    if workflow_variant:
        workflow_name = manifest.workflow_name
        if workflow_name is None:
            raise typer.BadParameter(
                f"benchmark family {normalized_family} has no workflow baseline"
            )
        try:
            get_benchmark_family_workflow_variant(workflow_name, workflow_variant)
        except KeyError as exc:
            raise typer.BadParameter(str(exc)) from exc

    demo_run_id = run_id or f"demo_{normalized_family}_{int(time.time())}"
    typer.echo(
        f"Starting benchmark demo for {normalized_family}: "
        f"workflow baseline vs {normalized_runner}"
    )
    result = _run_benchmark_demo(
        BenchmarkDemoSpec(
            family_name=normalized_family,
            compare_runner=normalized_runner,  # type: ignore[arg-type]
            workflow_variant=workflow_variant,
            seed=seed,
            artifacts_root=artifacts_root,
            run_id=demo_run_id,
            score_mode=score_success_mode.lower().strip(),
            max_steps=max_steps,
            compare_model=compare_model,
            compare_provider=compare_provider,
            compare_bc_model_path=compare_bc_model,
            compare_task=compare_task,
        )
    )
    typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))


@app.command()
def suite(
    family: list[str] = typer.Option(
        [], "--family", "-f", help="Optional family subset for the canonical suite"
    ),
    artifacts_root: Path = typer.Option(
        Path("_vei_out/suite"), help="Root directory for suite artifacts"
    ),
    run_id: str | None = typer.Option(None, help="Optional suite run id"),
    seed: int = typer.Option(42042, help="Seed for reproducibility"),
    score_success_mode: str = typer.Option(
        "full", help="Score success criteria: email|full."
    ),
) -> None:
    selected_families = [item.strip().lower() for item in family if item.strip()]
    for family_name in selected_families:
        try:
            get_benchmark_family_manifest(family_name)
        except KeyError as exc:
            raise typer.BadParameter(str(exc)) from exc

    suite_run_id = run_id or f"suite_{int(time.time())}"
    typer.echo(
        "Starting canonical benchmark-family suite for "
        + ", ".join(
            selected_families
            or [item.name for item in list_benchmark_family_manifest()]
        )
    )
    result = _run_benchmark_suite(
        BenchmarkSuiteSpec(
            family_names=selected_families,
            seed=seed,
            artifacts_root=artifacts_root,
            run_id=suite_run_id,
            score_mode=score_success_mode.lower().strip(),
        )
    )
    typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))


@app.command()
def showcase(
    example: list[str] = typer.Option(
        [],
        "--example",
        "-e",
        help="Optional subset of named complex examples to run",
    ),
    compare_runner: str = typer.Option(
        "scripted", help="Comparison runner for all showcase examples: scripted|bc|llm"
    ),
    artifacts_root: Path = typer.Option(
        Path("_vei_out/showcase"), help="Root directory for showcase artifacts"
    ),
    run_id: str | None = typer.Option(None, help="Optional showcase run id"),
    seed: int = typer.Option(42042, help="Seed for reproducibility"),
    score_success_mode: str = typer.Option(
        "full", help="Score success criteria: email|full."
    ),
    max_steps: int = typer.Option(40, help="Maximum steps for comparison runner"),
    compare_model: str | None = typer.Option(
        None, help="Model name when compare-runner=llm"
    ),
    compare_provider: str = typer.Option(
        "auto", help="LLM provider when compare-runner=llm"
    ),
    compare_bc_model: Path | None = typer.Option(
        None,
        exists=True,
        readable=True,
        help="BC policy file when compare-runner=bc",
    ),
    compare_task: str | None = typer.Option(
        None, help="Optional task prompt when compare-runner=llm"
    ),
) -> None:
    normalized_runner = compare_runner.strip().lower()
    if normalized_runner not in {"scripted", "bc", "llm"}:
        raise typer.BadParameter("compare-runner must be one of scripted|bc|llm")
    if normalized_runner == "llm" and not compare_model:
        raise typer.BadParameter("llm showcase requires --compare-model")
    if normalized_runner == "bc" and compare_bc_model is None:
        raise typer.BadParameter("bc showcase requires --compare-bc-model")
    for example_name in example:
        try:
            resolve_showcase_examples([example_name])
        except KeyError as exc:
            raise typer.BadParameter(str(exc)) from exc

    showcase_run_id = run_id or f"showcase_{int(time.time())}"
    typer.echo(
        "Starting complex-example showcase for "
        + ", ".join(example or [item.name for item in resolve_showcase_examples()])
    )
    result = _run_benchmark_showcase(
        BenchmarkShowcaseSpec(
            example_names=example,
            compare_runner=normalized_runner,  # type: ignore[arg-type]
            seed=seed,
            artifacts_root=artifacts_root,
            run_id=showcase_run_id,
            score_mode=score_success_mode.lower().strip(),
            max_steps=max_steps,
            compare_model=compare_model,
            compare_provider=compare_provider,
            compare_bc_model_path=compare_bc_model,
            compare_task=compare_task,
        )
    )
    typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))


if __name__ == "__main__":
    app()
