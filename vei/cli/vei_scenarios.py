from __future__ import annotations

import json
from dataclasses import asdict
from typing import Optional

import typer

from vei.world.api import (
    get_catalog_scenario_manifest,
    list_catalog_scenario_manifest,
)
from vei.world.compiler import compile_scene, load_scene_spec
from vei.world.scenarios import get_scenario, list_scenarios

app = typer.Typer(add_completion=False)


@app.command()
def list() -> None:  # noqa: A003 - CLI name
    cats = list_scenarios()
    for name in cats.keys():
        typer.echo(name)


@app.command()
def dump(name: str, indent: int = typer.Option(2, help="Pretty indent")) -> None:
    scen = get_scenario(name)
    typer.echo(json.dumps(asdict(scen), indent=indent))


@app.command()
def manifest(
    name: Optional[str] = typer.Option(
        None, help="Scenario name. Omit to list manifest entries for all scenarios."
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    if name:
        entry = get_catalog_scenario_manifest(name)
        typer.echo(json.dumps(entry.model_dump(), indent=indent))
        return

    entries = [item.model_dump() for item in list_catalog_scenario_manifest()]
    typer.echo(json.dumps(entries, indent=indent))


@app.command()
def compile(
    path: str,
    indent: int = typer.Option(2, help="Pretty indent"),
    seed: int = typer.Option(42042, help="Seed for deterministic sampling"),
) -> None:
    spec = load_scene_spec(path)
    scen = compile_scene(spec, seed=seed)
    typer.echo(json.dumps(asdict(scen), indent=indent))


if __name__ == "__main__":
    app()
