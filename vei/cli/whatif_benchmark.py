from __future__ import annotations

import os
from importlib import import_module
from pathlib import Path
from typing import cast

import typer

from vei.context.api import build_canonical_history_readiness
from vei.whatif.api import load_world
from vei.whatif.benchmark import (
    list_branch_point_benchmark_models,
)
from vei.whatif.critical_decision_benchmark import (
    CriticalCandidateGenerationMode,
    run_critical_decision_benchmark,
)
from vei.whatif.multitenant_benchmark import (
    CandidateGenerationMode,
    MultiTenantBenchmarkSource,
)
from vei.whatif.news_state_points import (
    NewsStatePointCandidateInput,
    run_news_state_point_counterfactual,
)
from vei.whatif.render import (
    render_benchmark_build,
    render_benchmark_eval,
    render_benchmark_judge,
    render_benchmark_study,
    render_benchmark_train,
)

from .whatif_shared import emit_payload


def _cli_module():
    return import_module("vei.cli.vei_whatif")


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


def _parse_multitenant_inputs(entries: list[str]) -> list[MultiTenantBenchmarkSource]:
    sources: list[MultiTenantBenchmarkSource] = []
    for entry in entries:
        tenant_id, sep, raw_path = entry.partition("=")
        if not tenant_id.strip() or sep != "=" or not raw_path.strip():
            raise typer.BadParameter(
                "multi-tenant inputs must use tenant_id=/path/to/context_snapshot.json"
            )
        path = Path(raw_path).expanduser().resolve()
        readiness = build_canonical_history_readiness(path)
        if not readiness.ready_for_world_modeling:
            label = readiness.readiness_label
            notes = "; ".join(readiness.notes)
            raise typer.BadParameter(
                f"tenant {tenant_id.strip()!r} is not ready for world-model training "
                f"(readiness_label={label}). {notes}".strip()
            )
        world = load_world(
            source="company_history",
            source_dir=path,
            include_situation_graph=False,
        )
        sources.append(
            MultiTenantBenchmarkSource(
                tenant_id=tenant_id.strip(),
                world=world,
                display_name=world.summary.organization_name or tenant_id.strip(),
                readiness_required=True,
                readiness=readiness.model_dump(mode="json"),
            )
        )
    return sources


def _resolve_candidate_generation_mode(mode: str) -> CandidateGenerationMode:
    normalized = mode.strip().lower()
    if normalized in {"llm", "template"}:
        return cast(CandidateGenerationMode, normalized)
    raise typer.BadParameter("candidate-mode must be one of: llm, template")


def _resolve_critical_candidate_generation_mode(
    mode: str,
) -> CriticalCandidateGenerationMode:
    normalized = mode.strip().lower()
    if normalized in {"llm", "template"}:
        return cast(CriticalCandidateGenerationMode, normalized)
    raise typer.BadParameter("candidate-mode must be one of: llm, template")


def _parse_news_state_point_candidates(
    entries: list[str],
) -> list[NewsStatePointCandidateInput]:
    candidates: list[NewsStatePointCandidateInput] = []
    for index, entry in enumerate(entries, start=1):
        parts = [part.strip() for part in entry.split("::")]
        if len(parts) == 3 and all(parts):
            candidate_type, label, action = parts
        elif len(parts) == 2 and all(parts):
            candidate_type = ""
            label, action = parts
        else:
            candidate_type = ""
            action = entry.strip()
            label = f"Candidate {index}"
        if not action:
            raise typer.BadParameter("--candidate entries must include action text")
        candidates.append(
            NewsStatePointCandidateInput(
                label=label,
                action=action,
                candidate_type=candidate_type,
            )
        )
    return candidates


def register_benchmark_commands(benchmark_app: typer.Typer) -> None:
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
        emit_payload(models, format=format)

    @benchmark_app.command("build")
    def build_benchmark_command(
        source: str = typer.Option(
            "auto",
            help="What-if source: auto | enron | mail_archive | company_history",
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
            ...,
            help="Human-friendly label for this benchmark build",
        ),
        heldout_pack_id: str = typer.Option(
            "enron_business_outcome_v1",
            help="Held-out benchmark pack used for counterfactual evaluation",
        ),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Build the pre-branch Enron benchmark dataset and held-out case pack."""

        world = load_world(source=source, source_dir=source_dir)
        cli_module = _cli_module()
        try:
            result = cli_module.build_branch_point_benchmark(
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
        emit_payload(payload, format=format)

    @benchmark_app.command("build-multitenant")
    def build_multitenant_benchmark_command(
        source_input: list[str] = typer.Option(
            ...,
            "--input",
            help="Tenant context snapshot in tenant_id=/path/to/context_snapshot.json form. Repeat for each tenant.",
        ),
        artifacts_root: Path = typer.Option(
            Path("_vei_out/world_model_multitenant_jepa"),
            help="Directory where multi-tenant benchmark artifacts are written",
        ),
        label: str = typer.Option(
            "pooled_world_model",
            help="Human-friendly label for this benchmark build",
        ),
        heldout_cases_per_tenant: int = typer.Option(
            4,
            help="Final-tail decision cases to keep for each tenant",
        ),
        future_horizon_events: int = typer.Option(
            6,
            help="Post-branch events used as the finite factual forecast horizon",
        ),
        max_branch_rows_per_thread: int = typer.Option(
            24,
            min=1,
            help=(
                "Maximum branch rows sampled from one long thread. Keeps large "
                "mailbox threads from dominating the benchmark."
            ),
        ),
        candidate_mode: str = typer.Option(
            "template",
            help=(
                "Candidate generation mode: template | llm. API LLM mode is "
                "explicit opt-in; Codex-session models must be tested through Codex."
            ),
        ),
        candidate_model: str = typer.Option(
            "gpt-5-mini",
            help=(
                "Locked API model used to generate broad candidate actions when "
                "candidate-mode=llm"
            ),
        ),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Build a pooled Enron/Dispatch-style world-model benchmark."""

        sources = _parse_multitenant_inputs(source_input)
        result = _cli_module().build_multitenant_world_model_benchmark(
            sources,
            artifacts_root=artifacts_root,
            label=label,
            heldout_cases_per_tenant=heldout_cases_per_tenant,
            candidate_generation_mode=_resolve_candidate_generation_mode(
                candidate_mode
            ),
            candidate_model=candidate_model,
            future_horizon_events=future_horizon_events,
            max_branch_rows_per_thread=max_branch_rows_per_thread,
        )
        payload = (
            render_benchmark_build(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

    @benchmark_app.command("critical-decisions")
    def critical_decisions_command(
        source_input: list[str] = typer.Option(
            ...,
            "--input",
            help="Tenant context snapshot in tenant_id=/path/to/context_snapshot.json form. Repeat for each tenant.",
        ),
        checkpoint: Path | None = typer.Option(
            None,
            "--checkpoint",
            help=(
                "Learned JEPA checkpoint used to score candidates. Defaults to "
                "VEI_REFERENCE_BACKEND_CHECKPOINT when set."
            ),
        ),
        source_build_root: Path | None = typer.Option(
            None,
            "--source-build-root",
            help=(
                "Optional pooled benchmark root. When set, candidate decisions "
                "are selected only from that build's test/heldout branch ids."
            ),
        ),
        artifacts_root: Path = typer.Option(
            Path("_vei_out/world_model_critical_decisions"),
            help="Directory where critical-decision artifacts are written",
        ),
        label: str = typer.Option(
            "critical_decision_counterfactuals",
            help="Human-friendly label for this run",
        ),
        cases_per_tenant: int = typer.Option(
            4,
            min=1,
            help="Critical decision points selected for each tenant",
        ),
        candidates_per_decision: int = typer.Option(
            10,
            min=8,
            max=12,
            help="Concrete counterfactual actions generated per decision",
        ),
        candidate_mode: str = typer.Option(
            "template",
            help=(
                "Candidate generation mode: template | llm. API LLM mode is "
                "explicit opt-in; Codex-session models must be tested through Codex."
            ),
        ),
        candidate_model: str = typer.Option(
            "gpt-5-mini",
            help=(
                "Locked API model used to generate concrete candidate actions when "
                "candidate-mode=llm"
            ),
        ),
        model_id: str | None = typer.Option(
            None,
            help="Model id for the checkpoint. Defaults to the checkpoint parent directory name.",
        ),
        future_horizon_events: int = typer.Option(
            8,
            help="Post-branch events used for the factual context row attached to each decision",
        ),
        max_branch_rows_per_thread: int = typer.Option(
            32,
            min=1,
            help="Maximum branch rows sampled from one long thread before criticality selection",
        ),
        device: str | None = typer.Option(None, help="Optional device override"),
        runtime_root: Path | None = typer.Option(
            None,
            help="Optional JEPA runtime root with torch installed",
        ),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Select key decision points, generate concrete counterfactuals, and score them with JEPA."""

        checkpoint_path = checkpoint
        if checkpoint_path is None:
            raw_checkpoint = os.environ.get("VEI_REFERENCE_BACKEND_CHECKPOINT", "")
            if raw_checkpoint:
                checkpoint_path = Path(raw_checkpoint)
        if checkpoint_path is None:
            raise typer.BadParameter(
                "--checkpoint is required unless VEI_REFERENCE_BACKEND_CHECKPOINT is set"
            )
        sources = _parse_multitenant_inputs(source_input)
        resolved_model_id = (
            _resolve_benchmark_model_id(model_id) if model_id is not None else None
        )
        result = run_critical_decision_benchmark(
            sources,
            checkpoint_path=checkpoint_path,
            artifacts_root=artifacts_root,
            label=label,
            source_build_root=source_build_root,
            cases_per_tenant=cases_per_tenant,
            candidates_per_decision=candidates_per_decision,
            candidate_generation_mode=_resolve_critical_candidate_generation_mode(
                candidate_mode
            ),
            candidate_model=candidate_model,
            model_id=resolved_model_id,
            future_horizon_events=future_horizon_events,
            max_branch_rows_per_thread=max_branch_rows_per_thread,
            device=device,
            runtime_root=runtime_root,
        )
        if format == "markdown":
            lines = [
                "# Critical Decision Counterfactual Run",
                "",
                f"- Selected decisions: `{result.selected_decision_count}`",
                f"- Candidates: `{result.candidate_count}`",
                f"- CSV: `{result.artifacts.csv_path}`",
                f"- Markdown: `{result.artifacts.markdown_path}`",
                f"- Leakage report: `{result.artifacts.leakage_report_path}`",
            ]
            emit_payload("\n".join(lines), format=format)
            return
        emit_payload(result.model_dump(mode="json"), format=format)

    @benchmark_app.command("news-state-point")
    def news_state_point_command(
        source_input: list[str] = typer.Option(
            ...,
            "--input",
            help="One news context snapshot in tenant_id=/path/to/context_snapshot.json form.",
        ),
        checkpoint: Path | None = typer.Option(
            None,
            "--checkpoint",
            help=(
                "Learned JEPA checkpoint used to score candidates. Defaults to "
                "VEI_REFERENCE_BACKEND_CHECKPOINT when set."
            ),
        ),
        artifacts_root: Path = typer.Option(
            Path("_vei_out/news_world_model/state_points"),
            help="Directory where news state-point artifacts are written",
        ),
        label: str = typer.Option(
            "news_state_point",
            help="Human-friendly label for this run",
        ),
        topic: str = typer.Option(
            ...,
            help="Topic/state slice to build, e.g. banking_markets",
        ),
        as_of: str = typer.Option(
            ...,
            "--as-of",
            help="As-of date or timestamp used to build the pre-branch dossier",
        ),
        candidate: list[str] = typer.Option(
            ...,
            "--candidate",
            help=(
                "Human action to score. Use 'label::action' or "
                "'candidate_type::label::action'. Repeat for multiple actions."
            ),
        ),
        future_horizon_days: int = typer.Option(
            90,
            min=1,
            help="Recorded future horizon used only for observed target context",
        ),
        max_history_events: int = typer.Option(
            240,
            min=1,
            help="Maximum topic-matched pre-as-of events included in the state row",
        ),
        max_evidence_events: int = typer.Option(
            12,
            min=1,
            help="Maximum evidence events shown in the state dossier",
        ),
        device: str | None = typer.Option(None, help="Optional device override"),
        runtime_root: Path | None = typer.Option(
            None,
            help="Optional JEPA runtime root with torch installed",
        ),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Score human-supplied actions from an as-of news/history state point."""

        checkpoint_path = checkpoint
        if checkpoint_path is None:
            raw_checkpoint = os.environ.get("VEI_REFERENCE_BACKEND_CHECKPOINT", "")
            if raw_checkpoint:
                checkpoint_path = Path(raw_checkpoint)
        if checkpoint_path is None:
            raise typer.BadParameter(
                "--checkpoint is required unless VEI_REFERENCE_BACKEND_CHECKPOINT is set"
            )
        sources = _parse_multitenant_inputs(source_input)
        if len(sources) != 1:
            raise typer.BadParameter("news-state-point expects exactly one --input")
        result = run_news_state_point_counterfactual(
            sources[0].world,
            checkpoint_path=checkpoint_path,
            artifacts_root=artifacts_root,
            label=label,
            topic=topic,
            as_of=as_of,
            candidates=_parse_news_state_point_candidates(candidate),
            future_horizon_days=future_horizon_days,
            max_history_events=max_history_events,
            max_evidence_events=max_evidence_events,
            device=device,
            runtime_root=runtime_root,
        )
        if format == "markdown":
            lines = [
                "# News State-Point Counterfactual Run",
                "",
                f"- Topic: `{result.topic}`",
                f"- As of: `{result.as_of}`",
                f"- Synthetic state point: `{result.state_event_id}`",
                f"- History events: `{result.history_event_count}`",
                f"- Future horizon events: `{result.future_event_count}`",
                f"- Candidates: `{result.candidate_count}`",
                f"- CSV: `{result.artifacts.result_csv_path}`",
                f"- Markdown: `{result.artifacts.result_markdown_path}`",
            ]
            emit_payload("\n".join(lines), format=format)
            return
        emit_payload(result.model_dump(mode="json"), format=format)

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
        train_split: list[str] | None = typer.Option(
            None,
            "--train-split",
            help=(
                "Dataset split to use for fitting. Repeat to final-fit on more "
                "history, e.g. --train-split train --train-split validation."
            ),
        ),
        validation_split: list[str] | None = typer.Option(
            None,
            "--validation-split",
            help=(
                "Dataset split used for validation loss. Repeat if needed. "
                "Defaults to validation."
            ),
        ),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Train one benchmark model family against observed Enron futures."""

        resolved_model_id = _resolve_benchmark_model_id(model_id)
        result = _cli_module().train_branch_point_benchmark_model(
            root,
            model_id=resolved_model_id,
            epochs=epochs,
            batch_size=batch_size,
            learning_rate=learning_rate,
            seed=seed,
            device=device,
            runtime_root=runtime_root,
            train_splits=train_split,
            validation_splits=validation_split,
        )
        payload = (
            render_benchmark_train(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

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

        result = _cli_module().judge_branch_point_benchmark(
            root,
            model=model,
            judge_id=judge_id,
        )
        payload = (
            render_benchmark_judge(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

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
        result = _cli_module().evaluate_branch_point_benchmark_model(
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
        emit_payload(payload, format=format)

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
        result = _cli_module().run_branch_point_benchmark_study(
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
        emit_payload(payload, format=format)

    @benchmark_app.command("show-judge")
    def show_benchmark_judge_command(
        root: Path = typer.Option(..., help="Benchmark build root"),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Load a saved benchmark judge result from disk."""

        result = _cli_module().load_branch_point_benchmark_judge_result(root)
        payload = (
            render_benchmark_judge(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

    @benchmark_app.command("show-build")
    def show_benchmark_build_command(
        root: Path = typer.Option(..., help="Benchmark build root"),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Load a saved benchmark build result from disk."""

        result = _cli_module().load_branch_point_benchmark_build_result(root)
        payload = (
            render_benchmark_build(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

    @benchmark_app.command("show-train")
    def show_benchmark_train_command(
        root: Path = typer.Option(..., help="Benchmark build root"),
        model_id: str = typer.Option(..., help="Model family id"),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Load a saved benchmark training result from disk."""

        resolved_model_id = _resolve_benchmark_model_id(model_id)
        result = _cli_module().load_branch_point_benchmark_train_result(
            root,
            model_id=resolved_model_id,
        )
        payload = (
            render_benchmark_train(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

    @benchmark_app.command("show-eval")
    def show_benchmark_eval_command(
        root: Path = typer.Option(..., help="Benchmark build root"),
        model_id: str = typer.Option(..., help="Model family id"),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Load a saved benchmark evaluation result from disk."""

        resolved_model_id = _resolve_benchmark_model_id(model_id)
        result = _cli_module().load_branch_point_benchmark_eval_result(
            root,
            model_id=resolved_model_id,
        )
        payload = (
            render_benchmark_eval(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

    @benchmark_app.command("show-study")
    def show_benchmark_study_command(
        root: Path = typer.Option(..., help="Benchmark study root"),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Load a saved benchmark study result from disk."""

        result = _cli_module().load_branch_point_benchmark_study_result(root)
        payload = (
            render_benchmark_study(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)
