from __future__ import annotations

from pathlib import Path

import typer
from importlib import import_module

from vei.project_settings import default_model_for_provider
from vei.whatif.api import load_world
from vei.whatif.research import (
    list_research_packs,
    load_research_pack_run_result,
)
from vei.whatif.render import render_research_pack_run

from .whatif_shared import emit_payload


def _cli_module():
    return import_module("vei.cli.vei_whatif")


def register_pack_commands(pack_app: typer.Typer) -> None:
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
        emit_payload([pack.model_dump(mode="json") for pack in packs], format=format)

    @pack_app.command("run")
    def run_pack_command(
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
            Path("_vei_out/whatif_research_packs"),
            help="Directory where research pack artifacts are written",
        ),
        label: str = typer.Option(..., help="Human-friendly label for this pack run"),
        pack_id: str = typer.Option(
            "enron_research_v1",
            help="Research pack id or path to a research-pack JSON file",
        ),
        provider: str = typer.Option(
            "openai",
            help="LLM provider for the actor path",
        ),
        model: str = typer.Option(
            default_model_for_provider("openai"),
            help="LLM model for the actor path",
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
        rollout_workers: int = typer.Option(
            4,
            min=1,
            help="How many counterfactual rollouts to generate at once for each candidate",
        ),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Run a historical what-if research pack and compare backend scores."""

        cli_module = _cli_module()
        try:
            research_pack = cli_module.get_research_pack(pack_id)
        except KeyError as exc:
            raise typer.BadParameter(str(exc)) from exc
        world = load_world(source=source, source_dir=source_dir)
        try:
            result = cli_module.run_research_pack(
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
        emit_payload(payload, format=format)

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
        emit_payload(payload, format=format)
