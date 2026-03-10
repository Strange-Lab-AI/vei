from __future__ import annotations

import json
from typing import Optional

import typer

from vei.blueprint.api import (
    build_blueprint_for_family,
    build_blueprint_for_scenario,
    list_blueprint_specs,
    list_facade_manifest,
)


app = typer.Typer(add_completion=False, help="Inspect VEI blueprints and facades.")


@app.command("list")
def list_blueprints() -> None:
    """List built-in benchmark-family blueprints."""

    for blueprint in list_blueprint_specs():
        typer.echo(blueprint.name)


@app.command("show")
def show_blueprint(
    family: Optional[str] = typer.Option(
        None, help="Benchmark family name to render as a blueprint"
    ),
    scenario: Optional[str] = typer.Option(
        None, help="Scenario name to render as a blueprint"
    ),
    workflow_name: Optional[str] = typer.Option(
        None, help="Optional workflow override when showing a scenario blueprint"
    ),
    workflow_variant: Optional[str] = typer.Option(
        None, help="Optional workflow variant override"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Render one blueprint as JSON."""

    if bool(family) == bool(scenario):
        raise typer.BadParameter("Provide exactly one of --family or --scenario")
    if family:
        blueprint = build_blueprint_for_family(family, variant_name=workflow_variant)
    else:
        blueprint = build_blueprint_for_scenario(
            scenario or "",
            workflow_name=workflow_name,
            workflow_variant=workflow_variant,
        )
    typer.echo(json.dumps(blueprint.model_dump(mode="json"), indent=indent))


@app.command("facades")
def facades(
    domain: Optional[str] = typer.Option(
        None, help="Optional capability domain filter"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Render the typed facade catalog."""

    entries = list_facade_manifest()
    if domain:
        entries = [item for item in entries if item.domain == domain]
    typer.echo(
        json.dumps([entry.model_dump(mode="json") for entry in entries], indent=indent)
    )


if __name__ == "__main__":
    app()
