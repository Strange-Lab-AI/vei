from __future__ import annotations

import json
from pathlib import Path

import typer
from pydantic import ValidationError

from vei.dataset import build_dataset_bundle, load_dataset_bundle
from vei.dataset.models import DatasetBuildSpec

app = typer.Typer(
    add_completion=False,
    help="Build and inspect VEI dataset bundles from twin environments.",
)


def _emit(payload: object, indent: int) -> None:
    typer.echo(json.dumps(payload, indent=indent))


@app.command("build")
def build_command(
    output_root: Path = typer.Option(
        Path("./_vei_out/datasets/latest"),
        help="Output root for the dataset bundle",
    ),
    workspace_root: list[Path] = typer.Option(
        [],
        help="Existing workspace root(s) to include directly",
    ),
    snapshot_path: str | None = typer.Option(
        None,
        help="Optional context snapshot JSON for matrix generation",
    ),
    organization_name: str = typer.Option(
        "",
        help="Optional company name when generating a fresh matrix",
    ),
    organization_domain: str = typer.Option(
        "",
        help="Optional company domain when generating a fresh matrix",
    ),
    archetype: list[str] = typer.Option(
        [],
        help="Twin archetype(s) to generate when building a fresh matrix",
    ),
    density: list[str] = typer.Option(
        [],
        help="Density levels to generate",
    ),
    crisis: list[str] = typer.Option(
        [],
        help="Crisis levels to generate",
    ),
    seed: list[int] = typer.Option(
        [],
        help="Random seed(s) to use for matrix generation",
    ),
    include_external_sample: bool = typer.Option(
        True,
        help="Include one external-agent sample run when the workspace exposes a twin gateway",
    ),
    format: list[str] = typer.Option(
        [],
        help="Training formats to export",
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Build a dataset bundle from a matrix of environments or existing workspace roots."""

    spec = DatasetBuildSpec(
        output_root=output_root,
        workspace_roots=workspace_root,
        snapshot_path=snapshot_path,
        organization_name=organization_name,
        organization_domain=organization_domain,
        archetypes=archetype,
        density_levels=density,
        crisis_levels=crisis,
        seeds=seed,
        include_external_sample=include_external_sample,
        formats=format,
    )
    try:
        payload = build_dataset_bundle(spec)
    except (ValidationError, ValueError, FileNotFoundError, RuntimeError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(payload.model_dump(mode="json"), indent)


@app.command("status")
def status_command(
    root: Path = typer.Option(
        Path("./_vei_out/datasets/latest"),
        help="Dataset output root or workspace root with a dataset pointer",
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Show the latest dataset bundle for an output root or workspace."""

    try:
        payload = load_dataset_bundle(root)
    except (ValidationError, ValueError, FileNotFoundError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(payload.model_dump(mode="json"), indent)
