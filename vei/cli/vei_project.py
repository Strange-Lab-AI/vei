from __future__ import annotations

import json
from pathlib import Path
from typing import Literal, Optional

import typer

from vei.workspace.api import (
    compile_workspace,
    create_workspace_from_template,
    import_workspace,
    list_workspace_source_syncs,
    list_workspace_sources,
    load_workspace_import_review,
    show_workspace,
    sync_workspace_source,
)
from vei.imports.api import (
    normalize_identity_import_package,
    review_import_package,
    scaffold_mapping_override,
    validate_import_package,
)


app = typer.Typer(
    add_completion=False,
    help="Create, import, review, and compile VEI workspaces.",
)


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
        source_kind: Literal["example", "family", "scenario"] = "example"
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
    package: Optional[Path] = typer.Option(
        None, help="Import package directory or package.json manifest"
    ),
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

    try:
        import_workspace(
            root=root,
            package_path=package,
            bundle_path=bundle,
            blueprint_asset_path=blueprint_asset,
            compiled_blueprint_path=compiled_blueprint,
            name=name,
            title=title,
            description=description,
            overwrite=overwrite,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(show_workspace(root).model_dump(mode="json"), indent)


@app.command("validate-import")
def validate_import_command(
    package: Path = typer.Option(..., help="Import package directory or manifest"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Validate an import package before creating a workspace."""

    _emit(validate_import_package(package).model_dump(mode="json"), indent)


@app.command("normalize")
def normalize_import_command(
    package: Path = typer.Option(..., help="Import package directory or manifest"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Normalize an import package into a VEI grounding bundle preview."""

    artifacts = normalize_identity_import_package(package)
    payload = {
        "package": artifacts.package.model_dump(mode="json"),
        "normalization_report": artifacts.normalization_report.model_dump(mode="json"),
        "normalized_bundle": (
            artifacts.normalized_bundle.model_dump(mode="json")
            if artifacts.normalized_bundle is not None
            else None
        ),
        "generated_scenarios": [
            item.model_dump(mode="json") for item in artifacts.generated_scenarios
        ],
        "provenance_count": len(artifacts.provenance),
    }
    _emit(payload, indent)


@app.command("review-import")
def review_import_command(
    root: Optional[Path] = typer.Option(
        None, help="Workspace root directory for imported workspaces"
    ),
    package: Optional[Path] = typer.Option(
        None, help="Import package directory or manifest"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Review import diagnostics, overrides, and generated scenarios."""

    if bool(root) == bool(package):
        raise typer.BadParameter("Provide exactly one of --root or --package")
    payload = (
        load_workspace_import_review(root)
        if root is not None
        else review_import_package(package)
    )
    if payload is None:
        _emit({}, indent)
        return
    _emit(payload.model_dump(mode="json"), indent)


@app.command("scaffold-overrides")
def scaffold_override_command(
    package: Path = typer.Option(..., help="Import package directory or manifest"),
    source_id: str = typer.Option(..., help="Import source id to scaffold"),
    output: Optional[Path] = typer.Option(
        None, help="Optional override file output path"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Write a starter mapping-override file for one import source."""

    destination, payload = scaffold_mapping_override(
        package,
        source_id=source_id,
        output_path=output,
    )
    _emit(
        {
            "path": str(destination),
            "override": payload.model_dump(mode="json"),
        },
        indent,
    )


@app.command("sync-source")
def sync_source_command(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    connector: str = typer.Option(..., help="Source connector name such as okta"),
    config: Path = typer.Option(..., help="Connector config JSON path"),
    source_id: Optional[str] = typer.Option(
        None, help="Optional stable source id override"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Sync a live source snapshot into the workspace import pipeline."""

    try:
        record = sync_workspace_source(
            root,
            connector=connector,
            config_path=config,
            source_id=source_id,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(
        {
            "sync": record.model_dump(mode="json"),
            "sources": [
                item.model_dump(mode="json") for item in list_workspace_sources(root)
            ],
            "history": [
                item.model_dump(mode="json")
                for item in list_workspace_source_syncs(root)
            ],
        },
        indent,
    )


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
