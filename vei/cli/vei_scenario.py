from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from vei.workspace.api import (
    create_workspace_scenario,
    generate_workspace_scenarios_from_import,
    list_workspace_scenarios,
    preview_workspace_scenario,
)


app = typer.Typer(
    add_completion=False,
    help="Manage workspace scenarios derived from one environment.",
)


def _emit(payload: object, indent: int) -> None:
    typer.echo(json.dumps(payload, indent=indent))


@app.command("list")
def list_scenarios(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """List workspace scenarios."""

    payload = [item.model_dump(mode="json") for item in list_workspace_scenarios(root)]
    _emit(payload, indent)


@app.command("create")
def create_scenario(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    name: str = typer.Option(..., help="New workspace scenario slug"),
    title: Optional[str] = typer.Option(None, help="Scenario title"),
    description: Optional[str] = typer.Option(None, help="Scenario description"),
    scenario_name: Optional[str] = typer.Option(
        None, help="Catalog scenario name override"
    ),
    workflow_name: Optional[str] = typer.Option(None, help="Workflow override"),
    workflow_variant: Optional[str] = typer.Option(
        None, help="Workflow variant override"
    ),
    inspection_focus: Optional[str] = typer.Option(
        None, help="Initial inspection focus hint"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Create a new workspace scenario entry."""

    payload = create_workspace_scenario(
        root,
        name=name,
        title=title,
        description=description,
        scenario_name=scenario_name,
        workflow_name=workflow_name,
        workflow_variant=workflow_variant,
        inspection_focus=inspection_focus,
    )
    _emit(payload.model_dump(mode="json"), indent)


@app.command("preview")
def preview_scenario(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    scenario_name: Optional[str] = typer.Option(None, help="Workspace scenario name"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Preview compiled blueprint, contract, and seeded state for a workspace scenario."""

    _emit(preview_workspace_scenario(root, scenario_name), indent)


@app.command("generate")
def generate_scenarios(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    replace_generated: bool = typer.Option(
        False, help="Replace previously generated import scenarios"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Materialize generated scenario candidates from imported workspace artifacts."""

    payload = generate_workspace_scenarios_from_import(
        root, replace_generated=replace_generated
    )
    _emit([item.model_dump(mode="json") for item in payload], indent)
