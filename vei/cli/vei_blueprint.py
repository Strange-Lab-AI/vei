from __future__ import annotations

import json
from typing import Optional

import typer

from vei.blueprint.api import (
    build_blueprint_asset_for_example,
    build_blueprint_asset_for_family,
    build_blueprint_asset_for_scenario,
    build_blueprint_for_family,
    build_blueprint_for_scenario,
    compile_blueprint,
    create_world_session_from_blueprint,
    list_blueprint_builder_examples,
    list_blueprint_specs,
    list_facade_manifest,
)
from vei.grounding.api import (
    build_grounding_bundle_example,
    list_grounding_bundle_examples,
)


app = typer.Typer(add_completion=False, help="Inspect VEI blueprints and facades.")


@app.command("list")
def list_blueprints() -> None:
    """List built-in benchmark-family blueprints."""

    for blueprint in list_blueprint_specs():
        typer.echo(blueprint.name)


@app.command("examples")
def list_examples() -> None:
    """List built-in blueprint builder examples."""

    for name in list_blueprint_builder_examples():
        typer.echo(name)


@app.command("bundles")
def list_bundles(indent: int = typer.Option(2, help="Pretty indent")) -> None:
    """Render built-in grounding bundle manifests as JSON."""

    payload = [
        item.model_dump(mode="json") for item in list_grounding_bundle_examples()
    ]
    typer.echo(json.dumps(payload, indent=indent))


@app.command("bundle")
def show_bundle(
    example: str = typer.Option(..., help="Grounding bundle example name"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Render one grounding bundle authoring input as JSON."""

    bundle = build_grounding_bundle_example(example)
    typer.echo(json.dumps(bundle.model_dump(mode="json"), indent=indent))


@app.command("show")
def show_blueprint(
    family: Optional[str] = typer.Option(
        None, help="Benchmark family name to render as a blueprint"
    ),
    scenario: Optional[str] = typer.Option(
        None, help="Scenario name to render as a blueprint"
    ),
    example: Optional[str] = typer.Option(
        None, help="Builder example name to render as a blueprint"
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

    selected = sum(bool(value) for value in (family, scenario, example))
    if selected != 1:
        raise typer.BadParameter(
            "Provide exactly one of --family, --scenario, or --example"
        )
    if family:
        blueprint = build_blueprint_for_family(family, variant_name=workflow_variant)
    elif example:
        blueprint = compile_blueprint(build_blueprint_asset_for_example(example))
    else:
        blueprint = build_blueprint_for_scenario(
            scenario or "",
            workflow_name=workflow_name,
            workflow_variant=workflow_variant,
        )
    typer.echo(json.dumps(blueprint.model_dump(mode="json"), indent=indent))


@app.command("asset")
def show_blueprint_asset(
    family: Optional[str] = typer.Option(
        None, help="Benchmark family name to render as a blueprint asset"
    ),
    scenario: Optional[str] = typer.Option(
        None, help="Scenario name to render as a blueprint asset"
    ),
    example: Optional[str] = typer.Option(
        None, help="Builder example name to render as a blueprint asset"
    ),
    workflow_name: Optional[str] = typer.Option(
        None, help="Optional workflow override when showing a scenario blueprint asset"
    ),
    workflow_variant: Optional[str] = typer.Option(
        None, help="Optional workflow variant override"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Render a blueprint authoring asset as JSON."""

    selected = sum(bool(value) for value in (family, scenario, example))
    if selected != 1:
        raise typer.BadParameter(
            "Provide exactly one of --family, --scenario, or --example"
        )
    if family:
        asset = build_blueprint_asset_for_family(family, variant_name=workflow_variant)
    elif example:
        asset = build_blueprint_asset_for_example(example)
    else:
        asset = build_blueprint_asset_for_scenario(
            scenario or "",
            workflow_name=workflow_name,
            workflow_variant=workflow_variant,
        )
    typer.echo(json.dumps(asset.model_dump(mode="json"), indent=indent))


@app.command("compile")
def compile_blueprint_command(
    family: Optional[str] = typer.Option(
        None, help="Benchmark family name to compile as a blueprint"
    ),
    scenario: Optional[str] = typer.Option(
        None, help="Scenario name to compile as a blueprint"
    ),
    example: Optional[str] = typer.Option(
        None, help="Builder example name to compile as a blueprint"
    ),
    workflow_name: Optional[str] = typer.Option(
        None, help="Optional workflow override for scenario assets"
    ),
    workflow_variant: Optional[str] = typer.Option(
        None, help="Optional workflow variant override"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Compile a blueprint asset into a runnable compiled blueprint."""

    selected = sum(bool(value) for value in (family, scenario, example))
    if selected != 1:
        raise typer.BadParameter(
            "Provide exactly one of --family, --scenario, or --example"
        )
    if family:
        asset = build_blueprint_asset_for_family(family, variant_name=workflow_variant)
    elif example:
        asset = build_blueprint_asset_for_example(example)
    else:
        asset = build_blueprint_asset_for_scenario(
            scenario or "",
            workflow_name=workflow_name,
            workflow_variant=workflow_variant,
        )
    compiled = compile_blueprint(asset)
    typer.echo(json.dumps(compiled.model_dump(mode="json"), indent=indent))


@app.command("observe")
def observe_blueprint(
    family: Optional[str] = typer.Option(
        None, help="Benchmark family name to observe as a live blueprint-backed world"
    ),
    scenario: Optional[str] = typer.Option(
        None, help="Scenario name to observe as a live blueprint-backed world"
    ),
    example: Optional[str] = typer.Option(
        None, help="Builder example name to observe as a live blueprint-backed world"
    ),
    workflow_name: Optional[str] = typer.Option(
        None, help="Optional workflow override for scenario assets"
    ),
    workflow_variant: Optional[str] = typer.Option(
        None, help="Optional workflow variant override"
    ),
    focus: Optional[str] = typer.Option(
        None, help="Optional observation focus hint such as slack, docs, or summary"
    ),
    seed: int = typer.Option(42042, help="Deterministic seed"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Compile a blueprint and open a live world observation from it."""

    selected = sum(bool(value) for value in (family, scenario, example))
    if selected != 1:
        raise typer.BadParameter(
            "Provide exactly one of --family, --scenario, or --example"
        )
    if family:
        asset = build_blueprint_asset_for_family(family, variant_name=workflow_variant)
    elif example:
        asset = build_blueprint_asset_for_example(example)
    else:
        asset = build_blueprint_asset_for_scenario(
            scenario or "",
            workflow_name=workflow_name,
            workflow_variant=workflow_variant,
        )

    compiled = compile_blueprint(asset)
    session = create_world_session_from_blueprint(asset, seed=seed)
    payload = {
        "blueprint": compiled.model_dump(mode="json"),
        "observation": session.observe(focus_hint=focus),
        "pending": session.pending(),
    }
    typer.echo(json.dumps(payload, indent=indent))


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
