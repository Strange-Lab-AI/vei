from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from vei.workspace.api import (
    activate_workspace_scenario,
    activate_workspace_scenario_variant,
    create_workspace_scenario,
    generate_workspace_scenarios_from_import,
    list_workspace_scenario_variants,
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


@app.command("variants")
def list_scenario_variants(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """List available scenario variants for a vertical workspace."""

    try:
        payload = list_workspace_scenario_variants(root)
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
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


@app.command("activate")
def activate_scenario(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    scenario_name: Optional[str] = typer.Option(None, help="Workspace scenario name"),
    variant: Optional[str] = typer.Option(None, help="Scenario variant name"),
    bootstrap_contract: bool = typer.Option(
        False, help="Re-bootstrap the scenario contract after activation"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Make one workspace scenario active for subsequent runs and previews."""

    if bool(scenario_name) == bool(variant):
        raise typer.BadParameter("Provide exactly one of --scenario-name or --variant")
    try:
        if variant:
            payload = activate_workspace_scenario_variant(
                root, variant, bootstrap_contract=bootstrap_contract
            )
        else:
            payload = activate_workspace_scenario(
                root,
                scenario_name or "",
                bootstrap_contract=bootstrap_contract,
            )
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(payload.model_dump(mode="json"), indent)
