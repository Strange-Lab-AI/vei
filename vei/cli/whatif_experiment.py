from __future__ import annotations

from importlib import import_module
from pathlib import Path

import typer

from vei.project_settings import default_model_for_provider

from .whatif_shared import emit_payload, fail_if_artifact_validation_failed


def _whatif_api():
    return import_module("vei.whatif.api")


def _whatif_ejepa():
    return import_module("vei.whatif.ejepa")


def _whatif_render():
    return import_module("vei.whatif.render")


def _whatif_validation():
    return import_module("vei.whatif.artifact_validation")


def register_experiment_commands(app: typer.Typer) -> None:
    @app.command("experiment")
    def experiment_command(
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
            Path("_vei_out/whatif_experiments"),
            help="Directory where experiment artifacts are written",
        ),
        label: str = typer.Option(..., help="Human-friendly label for this experiment"),
        counterfactual_prompt: str = typer.Option(
            ...,
            help="Counterfactual intervention prompt",
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
        provider: str = typer.Option(
            "openai",
            help="LLM provider for the actor path",
        ),
        model: str = typer.Option(
            default_model_for_provider("openai"),
            help="LLM model for the actor path",
        ),
        seed: int = typer.Option(42042, help="Deterministic seed"),
        ejepa_epochs: int = typer.Option(
            4, help="Training epochs for the JEPA backend"
        ),
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

        api = _whatif_api()
        ejepa = _whatif_ejepa()
        render = _whatif_render()
        validation = _whatif_validation()
        normalized_mode = mode.strip().lower()
        if normalized_mode not in {"llm", "e_jepa", "e_jepa_proxy", "both"}:
            raise typer.BadParameter(
                "mode must be one of: llm, e_jepa, e_jepa_proxy, both"
            )
        normalized_forecast_backend = forecast_backend.strip().lower()
        if normalized_forecast_backend not in {"auto", "e_jepa", "e_jepa_proxy"}:
            raise typer.BadParameter(
                "forecast-backend must be one of: auto, e_jepa, e_jepa_proxy"
            )
        world = api.load_world(source=source, source_dir=source_dir)
        result = api.run_counterfactual_experiment(
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
                ejepa.default_forecast_backend()
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
        fail_if_artifact_validation_failed(
            issues=validation.validate_artifact_tree(result.artifacts.root),
            label="experiment artifacts",
        )
        payload = (
            render.render_experiment(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

    @app.command("rank")
    def rank_command(
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
        provider: str = typer.Option(
            "openai",
            help="LLM provider for the actor path",
        ),
        model: str = typer.Option(
            default_model_for_provider("openai"),
            help="LLM model for the actor path",
        ),
        seed: int = typer.Option(42042, help="Deterministic seed"),
        shadow_forecast_backend: str = typer.Option(
            "auto",
            help="Shadow forecast backend: auto | e_jepa | e_jepa_proxy",
        ),
        ejepa_epochs: int = typer.Option(
            4, help="Training epochs for the JEPA backend"
        ),
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

        api = _whatif_api()
        ejepa = _whatif_ejepa()
        render = _whatif_render()
        if not candidate:
            raise typer.BadParameter("Provide at least one --candidate option")
        normalized_shadow_backend = shadow_forecast_backend.strip().lower()
        if normalized_shadow_backend not in {"auto", "e_jepa", "e_jepa_proxy"}:
            raise typer.BadParameter(
                "shadow-forecast-backend must be one of: auto, e_jepa, e_jepa_proxy"
            )
        world = api.load_world(source=source, source_dir=source_dir)
        result = api.run_ranked_counterfactual_experiment(
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
                ejepa.default_forecast_backend()
                if normalized_shadow_backend == "auto"
                else normalized_shadow_backend
            ),
            ejepa_epochs=ejepa_epochs,
            ejepa_batch_size=ejepa_batch_size,
            ejepa_force_retrain=ejepa_force_retrain,
            ejepa_device=ejepa_device,
        )
        payload = (
            render.render_ranked_experiment(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

    @app.command("show-result")
    def show_result_command(
        root: Path = typer.Option(..., help="Experiment artifact root"),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Load a saved what-if experiment result from disk."""

        api = _whatif_api()
        render = _whatif_render()
        result = api.load_experiment_result(root)
        payload = (
            render.render_experiment(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

    @app.command("show-ranked-result")
    def show_ranked_result_command(
        root: Path = typer.Option(..., help="Ranked experiment artifact root"),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Load a saved ranked what-if result from disk."""

        api = _whatif_api()
        render = _whatif_render()
        result = api.load_ranked_experiment_result(root)
        payload = (
            render.render_ranked_experiment(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)
