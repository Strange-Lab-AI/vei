from __future__ import annotations

import json
from pathlib import Path

import typer

from vei.project_settings import default_model_for_provider
from vei.whatif import (
    build_branch_point_benchmark,
    default_forecast_backend,
    evaluate_branch_point_benchmark_model,
    get_research_pack,
    judge_branch_point_benchmark,
    list_branch_point_benchmark_models,
    list_objective_packs,
    list_research_packs,
    list_supported_scenarios,
    load_branch_point_benchmark_build_result,
    load_branch_point_benchmark_eval_result,
    load_branch_point_benchmark_judge_result,
    load_branch_point_benchmark_study_result,
    load_branch_point_benchmark_train_result,
    load_experiment_result,
    load_research_pack_run_result,
    load_ranked_experiment_result,
    load_world,
    materialize_episode,
    replay_episode_baseline,
    run_research_pack,
    run_branch_point_benchmark_study,
    search_events,
    run_counterfactual_experiment,
    run_ranked_counterfactual_experiment,
    train_branch_point_benchmark_model,
    run_whatif,
)
from vei.whatif.render import (
    render_benchmark_build,
    render_benchmark_eval,
    render_benchmark_judge,
    render_benchmark_study,
    render_benchmark_train,
    render_episode,
    render_event_search,
    render_experiment,
    render_ranked_experiment,
    render_research_pack_run,
    render_replay,
    render_result,
    render_world_summary,
)

app = typer.Typer(
    add_completion=False,
    help="Explore counterfactuals and materialize replayable what-if episodes.",
)
pack_app = typer.Typer(
    add_completion=False,
    help="Run Enron research packs and compare multiple outcome backends.",
)
benchmark_app = typer.Typer(
    add_completion=False,
    help="Build, train, and evaluate pre-branch Enron benchmark models.",
)


def _emit(payload: object, *, format: str) -> None:
    if format == "markdown":
        typer.echo(str(payload))
        return
    typer.echo(json.dumps(payload, indent=2))


def _resolve_benchmark_model_id(model_id: str) -> str:
    normalized = model_id.strip()
    if normalized in list_branch_point_benchmark_models():
        return normalized
    choices = ", ".join(list_branch_point_benchmark_models())
    raise typer.BadParameter(
        f"Unknown benchmark model id: {model_id}. Choose one of: {choices}"
    )


def _resolve_benchmark_model_ids(model_ids: list[str] | None) -> list[str]:
    requested = model_ids or [
        "jepa_latent",
        "full_context_transformer",
        "treatment_transformer",
    ]
    return [_resolve_benchmark_model_id(model_id) for model_id in requested]


@app.command("scenarios")
def list_scenarios_command(
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """List the currently supported what-if scenarios."""

    scenarios = list_supported_scenarios()
    if format == "markdown":
        lines = ["# What-If Scenarios", ""]
        for scenario in scenarios:
            lines.append(f"- `{scenario.scenario_id}`: {scenario.description}")
        typer.echo("\n".join(lines))
        return
    _emit([scenario.model_dump(mode="json") for scenario in scenarios], format=format)


@app.command("objectives")
def list_objectives_command(
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """List ranked what-if objective packs."""

    packs = list_objective_packs()
    if format == "markdown":
        lines = ["# Ranked What-If Objectives", ""]
        for pack in packs:
            lines.append(f"- `{pack.pack_id}`: {pack.summary}")
        typer.echo("\n".join(lines))
        return
    _emit([pack.model_dump(mode="json") for pack in packs], format=format)


@pack_app.command("list")
def list_packs_command(
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """List the built-in research packs."""

    packs = list_research_packs()
    if format == "markdown":
        lines = ["# What-If Research Packs", ""]
        for pack in packs:
            lines.append(f"- `{pack.pack_id}`: {pack.summary}")
        typer.echo("\n".join(lines))
        return
    _emit([pack.model_dump(mode="json") for pack in packs], format=format)


@app.command("explore")
def explore_command(
    source: str = typer.Option(
        "auto", help="What-if source: auto | enron | mail_archive | company_history"
    ),
    source_dir: Path = typer.Option(
        ...,
        "--source-dir",
        "--rosetta-dir",
        help="Historical source directory or file",
    ),
    scenario: str | None = typer.Option(None, help="Supported scenario id"),
    prompt: str | None = typer.Option(None, help="Plain-English question"),
    date_from: str | None = typer.Option(None, help="Optional ISO start timestamp"),
    date_to: str | None = typer.Option(None, help="Optional ISO end timestamp"),
    custodian: list[str] | None = typer.Option(None, help="Optional custodian filters"),
    max_events: int | None = typer.Option(None, help="Optional event cap"),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Run deterministic what-if analysis over the source history."""

    if scenario is None and prompt is None:
        world = load_world(
            source=source,
            source_dir=source_dir,
            time_window=_time_window(date_from, date_to),
            custodian_filter=custodian or [],
            max_events=max_events,
        )
        payload = (
            render_world_summary(world)
            if format == "markdown"
            else world.summary.model_dump(mode="json")
        )
        _emit(payload, format=format)
        return

    world = load_world(
        source=source,
        source_dir=source_dir,
        time_window=_time_window(date_from, date_to),
        custodian_filter=custodian or [],
        max_events=max_events,
    )
    result = run_whatif(world, scenario=scenario, prompt=prompt)
    payload = (
        render_result(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@app.command("open-episode")
def open_episode_command(
    source: str = typer.Option(
        "auto", help="What-if source: auto | enron | mail_archive | company_history"
    ),
    source_dir: Path = typer.Option(
        ...,
        "--source-dir",
        "--rosetta-dir",
        help="Historical source directory or file",
    ),
    root: Path = typer.Option(..., help="Workspace root for the replayable episode"),
    thread_id: str | None = typer.Option(None, help="Thread to materialize"),
    event_id: str | None = typer.Option(None, help="Optional branch event override"),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Build a strict historical workspace from one event or thread."""

    world = load_world(source=source, source_dir=source_dir)
    materialization = materialize_episode(
        world,
        root=root,
        thread_id=thread_id,
        event_id=event_id,
    )
    payload = (
        render_episode(materialization)
        if format == "markdown"
        else materialization.model_dump(mode="json")
    )
    _emit(payload, format=format)


@app.command("events")
def events_command(
    source: str = typer.Option(
        "auto", help="What-if source: auto | enron | mail_archive | company_history"
    ),
    source_dir: Path = typer.Option(
        ...,
        "--source-dir",
        "--rosetta-dir",
        help="Historical source directory or file",
    ),
    actor: str | None = typer.Option(None, help="Filter by sender email fragment"),
    participant: str | None = typer.Option(
        None,
        help="Filter by any participant or recipient email fragment",
    ),
    thread_id: str | None = typer.Option(None, help="Filter by thread id"),
    event_type: str | None = typer.Option(None, help="Filter by event type"),
    query: str | None = typer.Option(
        None,
        help="Match against event id, subject, actors, and recipients",
    ),
    flagged_only: bool = typer.Option(
        False,
        help="Only return events with policy-relevant flags",
    ),
    max_events: int | None = typer.Option(None, help="Optional event cap"),
    limit: int = typer.Option(20, help="Maximum number of events to return"),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Search historical events so you can branch from an exact point in time."""

    world = load_world(
        source=source,
        source_dir=source_dir,
        max_events=max_events,
    )
    result = search_events(
        world,
        actor=actor,
        participant=participant,
        thread_id=thread_id,
        event_type=event_type,
        query=query,
        flagged_only=flagged_only,
        limit=limit,
    )
    payload = (
        render_event_search(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@app.command("replay")
def replay_command(
    root: Path = typer.Option(
        ..., help="Workspace root from `vei whatif open-episode`"
    ),
    tick_ms: int = typer.Option(
        0,
        help="Optional logical time to advance after scheduling the baseline future",
    ),
    seed: int = typer.Option(42042, help="Deterministic replay seed"),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Schedule the saved historical future into the world sim and optionally advance time."""

    summary = replay_episode_baseline(root, tick_ms=tick_ms, seed=seed)
    payload = (
        render_replay(summary)
        if format == "markdown"
        else summary.model_dump(mode="json")
    )
    _emit(payload, format=format)


@app.command("experiment")
def experiment_command(
    source: str = typer.Option(
        "auto", help="What-if source: auto | enron | mail_archive | company_history"
    ),
    source_dir: Path = typer.Option(
        ...,
        "--source-dir",
        "--rosetta-dir",
        help="Historical source directory or file",
    ),
    artifacts_root: Path = typer.Option(
        Path("_vei_out/whatif_experiments"),
        help="Directory where experiment artifacts are written",
    ),
    label: str = typer.Option(..., help="Human-friendly label for this experiment"),
    counterfactual_prompt: str = typer.Option(
        ..., help="Counterfactual intervention prompt"
    ),
    selection_scenario: str | None = typer.Option(
        None,
        help="Optional supported scenario used to pick the candidate thread",
    ),
    selection_prompt: str | None = typer.Option(
        None,
        help="Optional plain-English question used to pick the candidate thread",
    ),
    thread_id: str | None = typer.Option(
        None,
        help="Optional explicit thread override",
    ),
    event_id: str | None = typer.Option(
        None,
        help="Optional explicit branch event override",
    ),
    mode: str = typer.Option(
        "both",
        help="Experiment mode: llm | e_jepa | e_jepa_proxy | both",
    ),
    forecast_backend: str = typer.Option(
        "auto",
        help="Forecast backend: auto | e_jepa | e_jepa_proxy",
    ),
    provider: str = typer.Option("openai", help="LLM provider for the actor path"),
    model: str = typer.Option(
        default_model_for_provider("openai"),
        help="LLM model for the actor path",
    ),
    seed: int = typer.Option(42042, help="Deterministic seed"),
    ejepa_epochs: int = typer.Option(4, help="Training epochs for the JEPA backend"),
    ejepa_batch_size: int = typer.Option(
        64,
        help="Batch size for the JEPA backend",
    ),
    ejepa_force_retrain: bool = typer.Option(
        False,
        help="Retrain the JEPA cache instead of reusing an existing checkpoint",
    ),
    ejepa_device: str | None = typer.Option(
        None,
        help="Optional device override for the JEPA backend",
    ),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Run a full what-if experiment and write result artifacts."""

    normalized_mode = mode.strip().lower()
    if normalized_mode not in {"llm", "e_jepa", "e_jepa_proxy", "both"}:
        raise typer.BadParameter("mode must be one of: llm, e_jepa, e_jepa_proxy, both")
    normalized_forecast_backend = forecast_backend.strip().lower()
    if normalized_forecast_backend not in {"auto", "e_jepa", "e_jepa_proxy"}:
        raise typer.BadParameter(
            "forecast-backend must be one of: auto, e_jepa, e_jepa_proxy"
        )
    world = load_world(source=source, source_dir=source_dir)
    result = run_counterfactual_experiment(
        world,
        artifacts_root=artifacts_root,
        label=label,
        counterfactual_prompt=counterfactual_prompt,
        selection_scenario=selection_scenario,
        selection_prompt=selection_prompt,
        thread_id=thread_id,
        event_id=event_id,
        mode=normalized_mode,
        forecast_backend=(
            default_forecast_backend()
            if normalized_forecast_backend == "auto"
            else normalized_forecast_backend
        ),
        provider=provider,
        model=model,
        seed=seed,
        ejepa_epochs=ejepa_epochs,
        ejepa_batch_size=ejepa_batch_size,
        ejepa_force_retrain=ejepa_force_retrain,
        ejepa_device=ejepa_device,
    )
    payload = (
        render_experiment(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@app.command("rank")
def rank_command(
    source: str = typer.Option(
        "auto", help="What-if source: auto | enron | mail_archive | company_history"
    ),
    source_dir: Path = typer.Option(
        ...,
        "--source-dir",
        "--rosetta-dir",
        help="Historical source directory or file",
    ),
    artifacts_root: Path = typer.Option(
        Path("_vei_out/whatif_ranked"),
        help="Directory where ranked experiment artifacts are written",
    ),
    label: str = typer.Option(..., help="Human-friendly label for this ranked run"),
    objective_pack_id: str = typer.Option(
        "contain_exposure",
        help="Objective pack id",
    ),
    candidate: list[str] = typer.Option(
        [],
        "--candidate",
        help="Candidate intervention prompt. Repeat this flag for multiple options.",
    ),
    selection_scenario: str | None = typer.Option(
        None,
        help="Optional supported scenario used to pick the candidate thread",
    ),
    selection_prompt: str | None = typer.Option(
        None,
        help="Optional plain-English question used to pick the candidate thread",
    ),
    thread_id: str | None = typer.Option(
        None,
        help="Optional explicit thread override",
    ),
    event_id: str | None = typer.Option(
        None,
        help="Optional explicit branch event override",
    ),
    rollout_count: int = typer.Option(
        4,
        help="How many LLM continuations to run per candidate",
    ),
    provider: str = typer.Option("openai", help="LLM provider for the actor path"),
    model: str = typer.Option(
        default_model_for_provider("openai"),
        help="LLM model for the actor path",
    ),
    seed: int = typer.Option(42042, help="Deterministic seed"),
    shadow_forecast_backend: str = typer.Option(
        "auto",
        help="Shadow forecast backend: auto | e_jepa | e_jepa_proxy",
    ),
    ejepa_epochs: int = typer.Option(4, help="Training epochs for the JEPA backend"),
    ejepa_batch_size: int = typer.Option(
        64,
        help="Batch size for the JEPA backend",
    ),
    ejepa_force_retrain: bool = typer.Option(
        False,
        help="Retrain the JEPA cache instead of reusing an existing checkpoint",
    ),
    ejepa_device: str | None = typer.Option(
        None,
        help="Optional device override for the JEPA backend",
    ),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Rank multiple counterfactual options from one exact branch point."""

    if not candidate:
        raise typer.BadParameter("Provide at least one --candidate option")
    normalized_shadow_backend = shadow_forecast_backend.strip().lower()
    if normalized_shadow_backend not in {"auto", "e_jepa", "e_jepa_proxy"}:
        raise typer.BadParameter(
            "shadow-forecast-backend must be one of: auto, e_jepa, e_jepa_proxy"
        )
    world = load_world(source=source, source_dir=source_dir)
    result = run_ranked_counterfactual_experiment(
        world,
        artifacts_root=artifacts_root,
        label=label,
        objective_pack_id=objective_pack_id,
        candidate_interventions=candidate,
        selection_scenario=selection_scenario,
        selection_prompt=selection_prompt,
        thread_id=thread_id,
        event_id=event_id,
        rollout_count=rollout_count,
        provider=provider,
        model=model,
        seed=seed,
        shadow_forecast_backend=(
            default_forecast_backend()
            if normalized_shadow_backend == "auto"
            else normalized_shadow_backend
        ),
        ejepa_epochs=ejepa_epochs,
        ejepa_batch_size=ejepa_batch_size,
        ejepa_force_retrain=ejepa_force_retrain,
        ejepa_device=ejepa_device,
    )
    payload = (
        render_ranked_experiment(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@app.command("show-result")
def show_result_command(
    root: Path = typer.Option(..., help="Experiment artifact root"),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Load a saved what-if experiment result from disk."""

    result = load_experiment_result(root)
    payload = (
        render_experiment(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@app.command("show-ranked-result")
def show_ranked_result_command(
    root: Path = typer.Option(..., help="Ranked experiment artifact root"),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Load a saved ranked what-if result from disk."""

    result = load_ranked_experiment_result(root)
    payload = (
        render_ranked_experiment(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@pack_app.command("run")
def run_pack_command(
    source: str = typer.Option(
        "auto", help="What-if source: auto | enron | mail_archive | company_history"
    ),
    source_dir: Path = typer.Option(
        ...,
        "--source-dir",
        "--rosetta-dir",
        help="Historical source directory or file",
    ),
    artifacts_root: Path = typer.Option(
        Path("_vei_out/whatif_research_packs"),
        help="Directory where research pack artifacts are written",
    ),
    label: str = typer.Option(..., help="Human-friendly label for this pack run"),
    pack_id: str = typer.Option(
        "enron_research_v1",
        help="Research pack id or path to a research-pack JSON file",
    ),
    provider: str = typer.Option("openai", help="LLM provider for the actor path"),
    model: str = typer.Option(
        default_model_for_provider("openai"),
        help="LLM model for the actor path",
    ),
    ejepa_epochs: int = typer.Option(4, help="Training epochs for the JEPA backend"),
    ejepa_batch_size: int = typer.Option(
        64,
        help="Batch size for the JEPA backend",
    ),
    ejepa_force_retrain: bool = typer.Option(
        False,
        help="Retrain the JEPA cache instead of reusing an existing checkpoint",
    ),
    ejepa_device: str | None = typer.Option(
        None,
        help="Optional device override for the JEPA backend",
    ),
    rollout_workers: int = typer.Option(
        4,
        min=1,
        help="How many counterfactual rollouts to generate at once for each candidate",
    ),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Run a historical what-if research pack and compare backend scores."""

    try:
        research_pack = get_research_pack(pack_id)
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc
    world = load_world(source=source, source_dir=source_dir)
    try:
        result = run_research_pack(
            world,
            artifacts_root=artifacts_root,
            label=label,
            research_pack=research_pack,
            provider=provider,
            model=model,
            ejepa_epochs=ejepa_epochs,
            ejepa_batch_size=ejepa_batch_size,
            ejepa_force_retrain=ejepa_force_retrain,
            ejepa_device=ejepa_device,
            rollout_workers=rollout_workers,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    payload = (
        render_research_pack_run(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@pack_app.command("show")
def show_pack_command(
    root: Path = typer.Option(..., help="Research pack artifact root"),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Load a saved research pack run from disk."""

    result = load_research_pack_run_result(root)
    payload = (
        render_research_pack_run(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@benchmark_app.command("models")
def list_benchmark_models_command(
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """List the trained benchmark model families."""

    models = list_branch_point_benchmark_models()
    if format == "markdown":
        lines = ["# Branch-Point Benchmark Models", ""]
        for model_id in models:
            lines.append(f"- `{model_id}`")
        typer.echo("\n".join(lines))
        return
    _emit(models, format=format)


@benchmark_app.command("build")
def build_benchmark_command(
    source: str = typer.Option(
        "auto", help="What-if source: auto | enron | mail_archive | company_history"
    ),
    source_dir: Path = typer.Option(
        ...,
        "--source-dir",
        "--rosetta-dir",
        help="Historical source directory or file",
    ),
    artifacts_root: Path = typer.Option(
        Path("_vei_out/whatif_benchmarks/branch_point_ranking_v2"),
        help="Directory where benchmark artifacts are written",
    ),
    label: str = typer.Option(
        ..., help="Human-friendly label for this benchmark build"
    ),
    heldout_pack_id: str = typer.Option(
        "enron_business_outcome_v1",
        help="Held-out benchmark pack used for counterfactual evaluation",
    ),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Build the pre-branch Enron benchmark dataset and held-out case pack."""

    world = load_world(source=source, source_dir=source_dir)
    try:
        result = build_branch_point_benchmark(
            world,
            artifacts_root=artifacts_root,
            label=label,
            heldout_pack_id=heldout_pack_id,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    payload = (
        render_benchmark_build(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@benchmark_app.command("train")
def train_benchmark_command(
    root: Path = typer.Option(..., help="Benchmark build root"),
    model_id: str = typer.Option(..., help="Model family id"),
    epochs: int = typer.Option(12, help="Training epochs"),
    batch_size: int = typer.Option(64, help="Training batch size"),
    learning_rate: float = typer.Option(1e-3, help="Training learning rate"),
    seed: int = typer.Option(42042, help="Training seed"),
    device: str | None = typer.Option(None, help="Optional device override"),
    runtime_root: Path | None = typer.Option(
        None,
        help="Optional JEPA runtime root with torch installed",
    ),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Train one benchmark model family against observed Enron futures."""

    resolved_model_id = _resolve_benchmark_model_id(model_id)
    result = train_branch_point_benchmark_model(
        root,
        model_id=resolved_model_id,
        epochs=epochs,
        batch_size=batch_size,
        learning_rate=learning_rate,
        seed=seed,
        device=device,
        runtime_root=runtime_root,
    )
    payload = (
        render_benchmark_train(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@benchmark_app.command("judge")
def judge_benchmark_command(
    root: Path = typer.Option(..., help="Benchmark build root"),
    model: str = typer.Option("gpt-4.1-mini", help="Locked LLM judge model"),
    judge_id: str = typer.Option(
        "benchmark_llm_judge",
        help="Judge id written into the ranking artifacts",
    ),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Judge held-out Enron counterfactual cases with a locked LLM rubric."""

    result = judge_branch_point_benchmark(
        root,
        model=model,
        judge_id=judge_id,
    )
    payload = (
        render_benchmark_judge(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@benchmark_app.command("eval")
def eval_benchmark_command(
    root: Path = typer.Option(..., help="Benchmark build root"),
    model_id: str = typer.Option(..., help="Model family id"),
    judged_rankings_path: Path | None = typer.Option(
        None,
        help="Optional judged ranking JSON file from `benchmark judge`",
    ),
    audit_records_path: Path | None = typer.Option(
        None,
        help="Optional completed audit record JSON file",
    ),
    panel_judgments_path: Path | None = typer.Option(
        None,
        help="Optional legacy panel judgment JSON file",
    ),
    research_pack_root: Path | None = typer.Option(
        None,
        help="Optional completed research pack root for rollout stress comparison",
    ),
    device: str | None = typer.Option(None, help="Optional device override"),
    runtime_root: Path | None = typer.Option(
        None,
        help="Optional JEPA runtime root with torch installed",
    ),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Evaluate one trained benchmark model on factual and held-out Enron cases."""

    resolved_model_id = _resolve_benchmark_model_id(model_id)
    result = evaluate_branch_point_benchmark_model(
        root,
        model_id=resolved_model_id,
        judged_rankings_path=judged_rankings_path,
        audit_records_path=audit_records_path,
        panel_judgments_path=panel_judgments_path,
        research_pack_root=research_pack_root,
        device=device,
        runtime_root=runtime_root,
    )
    payload = (
        render_benchmark_eval(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@benchmark_app.command("study")
def study_benchmark_command(
    root: Path = typer.Option(..., help="Benchmark build root"),
    label: str = typer.Option(..., help="Study label written under studies/"),
    model_id: list[str] | None = typer.Option(
        None,
        "--model-id",
        help="Model family id. Repeat to compare more than one.",
    ),
    seed: list[int] | None = typer.Option(
        None,
        "--seed",
        help="Training seed. Repeat to run more than one seed.",
    ),
    epochs: int = typer.Option(12, help="Training epochs"),
    batch_size: int = typer.Option(64, help="Training batch size"),
    learning_rate: float = typer.Option(1e-3, help="Training learning rate"),
    judged_rankings_path: Path | None = typer.Option(
        None,
        help="Optional judged ranking JSON file from `benchmark judge`",
    ),
    audit_records_path: Path | None = typer.Option(
        None,
        help="Optional completed audit record JSON file",
    ),
    panel_judgments_path: Path | None = typer.Option(
        None,
        help="Optional legacy panel judgment JSON file",
    ),
    research_pack_root: Path | None = typer.Option(
        None,
        help="Optional completed research pack root for rollout stress comparison",
    ),
    device: str | None = typer.Option(None, help="Optional device override"),
    runtime_root: Path | None = typer.Option(
        None,
        help="Optional JEPA runtime root with torch installed",
    ),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Run the matched-input benchmark study across models and seeds."""

    resolved_model_ids = _resolve_benchmark_model_ids(model_id)
    resolved_seeds = seed or [42042, 42043, 42044, 42045, 42046]
    result = run_branch_point_benchmark_study(
        root,
        label=label,
        model_ids=resolved_model_ids,
        seeds=resolved_seeds,
        epochs=epochs,
        batch_size=batch_size,
        learning_rate=learning_rate,
        judged_rankings_path=judged_rankings_path,
        audit_records_path=audit_records_path,
        panel_judgments_path=panel_judgments_path,
        research_pack_root=research_pack_root,
        device=device,
        runtime_root=runtime_root,
    )
    payload = (
        render_benchmark_study(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@benchmark_app.command("show-judge")
def show_benchmark_judge_command(
    root: Path = typer.Option(..., help="Benchmark build root"),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Load a saved benchmark judge result from disk."""

    result = load_branch_point_benchmark_judge_result(root)
    payload = (
        render_benchmark_judge(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@benchmark_app.command("show-build")
def show_benchmark_build_command(
    root: Path = typer.Option(..., help="Benchmark build root"),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Load a saved benchmark build result from disk."""

    result = load_branch_point_benchmark_build_result(root)
    payload = (
        render_benchmark_build(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@benchmark_app.command("show-train")
def show_benchmark_train_command(
    root: Path = typer.Option(..., help="Benchmark build root"),
    model_id: str = typer.Option(..., help="Model family id"),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Load a saved benchmark training result from disk."""

    resolved_model_id = _resolve_benchmark_model_id(model_id)
    result = load_branch_point_benchmark_train_result(
        root,
        model_id=resolved_model_id,
    )
    payload = (
        render_benchmark_train(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@benchmark_app.command("show-eval")
def show_benchmark_eval_command(
    root: Path = typer.Option(..., help="Benchmark build root"),
    model_id: str = typer.Option(..., help="Model family id"),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Load a saved benchmark evaluation result from disk."""

    resolved_model_id = _resolve_benchmark_model_id(model_id)
    result = load_branch_point_benchmark_eval_result(
        root,
        model_id=resolved_model_id,
    )
    payload = (
        render_benchmark_eval(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


@benchmark_app.command("show-study")
def show_benchmark_study_command(
    root: Path = typer.Option(..., help="Benchmark study root"),
    format: str = typer.Option("json", help="Output format: json | markdown"),
) -> None:
    """Load a saved benchmark study result from disk."""

    result = load_branch_point_benchmark_study_result(root)
    payload = (
        render_benchmark_study(result)
        if format == "markdown"
        else result.model_dump(mode="json")
    )
    _emit(payload, format=format)


def _time_window(
    date_from: str | None,
    date_to: str | None,
) -> tuple[str, str] | None:
    if not date_from and not date_to:
        return None
    if not date_from or not date_to:
        raise typer.BadParameter("Provide both --date-from and --date-to")
    return (date_from, date_to)


app.add_typer(pack_app, name="pack")
app.add_typer(benchmark_app, name="benchmark")
