from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from vei.workspace.api import (
    compile_workspace,
    create_workspace_from_template,
    import_workspace,
    show_workspace,
)


app = typer.Typer(add_completion=False, help="Create and compile VEI workspaces.")


def _emit(payload: object, indent: int) -> None:
    typer.echo(json.dumps(payload, indent=indent))


@app.command("init")
def init_workspace(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    example: Optional[str] = typer.Option(
        None, help="Builder example name to initialize from"
    ),
    family: Optional[str] = typer.Option(
        None, help="Benchmark family name to initialize from"
    ),
    scenario: Optional[str] = typer.Option(
        None, help="Scenario name to initialize from"
    ),
    name: Optional[str] = typer.Option(None, help="Workspace slug override"),
    title: Optional[str] = typer.Option(None, help="Workspace title override"),
    description: Optional[str] = typer.Option(
        None, help="Workspace description override"
    ),
    workflow_name: Optional[str] = typer.Option(
        None, help="Workflow override when initializing from a scenario"
    ),
    workflow_variant: Optional[str] = typer.Option(
        None, help="Workflow variant override"
    ),
    overwrite: bool = typer.Option(False, help="Overwrite a non-empty workspace root"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Create a workspace from a built-in template source."""

    selected = sum(bool(value) for value in (example, family, scenario))
    if selected != 1:
        raise typer.BadParameter(
            "Provide exactly one of --example, --family, or --scenario"
        )

    if example:
        source_kind = "example"
        source_ref = example
    elif family:
        source_kind = "family"
        source_ref = family
    else:
        source_kind = "scenario"
        source_ref = scenario or ""

    create_workspace_from_template(
        root=root,
        source_kind=source_kind,
        source_ref=source_ref,
        name=name,
        title=title,
        description=description,
        workflow_name=workflow_name,
        workflow_variant=workflow_variant,
        overwrite=overwrite,
    )
    _emit(show_workspace(root).model_dump(mode="json"), indent)


@app.command("import")
def import_workspace_command(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    bundle: Optional[Path] = typer.Option(None, help="Grounding bundle JSON to import"),
    blueprint_asset: Optional[Path] = typer.Option(
        None, help="Blueprint asset JSON to import"
    ),
    compiled_blueprint: Optional[Path] = typer.Option(
        None, help="Compiled blueprint JSON to import"
    ),
    name: Optional[str] = typer.Option(None, help="Workspace slug override"),
    title: Optional[str] = typer.Option(None, help="Workspace title override"),
    description: Optional[str] = typer.Option(
        None, help="Workspace description override"
    ),
    overwrite: bool = typer.Option(False, help="Overwrite a non-empty workspace root"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Import a workspace from a bundle or blueprint file."""

    import_workspace(
        root=root,
        bundle_path=bundle,
        blueprint_asset_path=blueprint_asset,
        compiled_blueprint_path=compiled_blueprint,
        name=name,
        title=title,
        description=description,
        overwrite=overwrite,
    )
    _emit(show_workspace(root).model_dump(mode="json"), indent)


@app.command("show")
def show_workspace_command(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Show workspace summary and compiled/run status."""

    _emit(show_workspace(root).model_dump(mode="json"), indent)


@app.command("compile")
def compile_workspace_command(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Compile blueprint, contract, and scenario artifacts for a workspace."""

    _emit(compile_workspace(root).model_dump(mode="json"), indent)
