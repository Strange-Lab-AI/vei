from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from vei.run.api import evaluate_run_workspace_contract, load_run_manifest
from vei.workspace.api import (
    bootstrap_workspace_contract,
    diff_workspace_contract,
    list_workspace_runs,
    load_workspace_contract,
    validate_workspace_contract,
)


app = typer.Typer(
    add_completion=False, help="Validate and inspect workspace contracts."
)


def _emit(payload: object, indent: int) -> None:
    typer.echo(json.dumps(payload, indent=indent))


def _resolve_run_id(root: Path, run_id: Optional[str]) -> str:
    if run_id:
        return run_id
    runs = list_workspace_runs(root)
    if not runs:
        raise typer.BadParameter("No runs found; provide --run-id")
    return runs[0].run_id


@app.command("show")
def show_contract(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    scenario_name: Optional[str] = typer.Option(None, help="Workspace scenario name"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Render the effective authorable contract for a workspace scenario."""

    _emit(load_workspace_contract(root, scenario_name).model_dump(mode="json"), indent)


@app.command("validate")
def validate_contract(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    scenario_name: Optional[str] = typer.Option(None, help="Workspace scenario name"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Validate a contract against the compiled workspace scenario."""

    _emit(validate_workspace_contract(root, scenario_name), indent)


@app.command("diff")
def diff_contract(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    scenario_name: Optional[str] = typer.Option(None, help="Workspace scenario name"),
    other_path: Optional[Path] = typer.Option(
        None, help="Optional other contract JSON to diff against"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Diff the workspace contract against another contract or the compiled effective one."""

    _emit(
        diff_workspace_contract(
            root, scenario_name=scenario_name, other_path=other_path
        ),
        indent,
    )


@app.command("eval")
def evaluate_contract(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    run_id: Optional[str] = typer.Option(None, help="Run id to evaluate"),
    scenario_name: Optional[str] = typer.Option(None, help="Workspace scenario name"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Evaluate the workspace contract against the latest state of a run."""

    resolved_root = root.expanduser().resolve()
    resolved_run_id = _resolve_run_id(resolved_root, run_id)
    result = evaluate_run_workspace_contract(
        resolved_root,
        run_id=resolved_run_id,
        scenario_name=scenario_name,
    )
    if result is None:
        raise typer.BadParameter(
            f"Could not evaluate contract for run: {resolved_run_id}"
        )
    payload = {
        "run_id": resolved_run_id,
        "manifest": load_run_manifest(
            resolved_root / "runs" / resolved_run_id / "run_manifest.json"
        ).model_dump(mode="json"),
        "contract_evaluation": result.model_dump(mode="json"),
    }
    _emit(payload, indent)


@app.command("bootstrap")
def bootstrap_contract(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    scenario_name: Optional[str] = typer.Option(None, help="Workspace scenario name"),
    overwrite: bool = typer.Option(
        False, help="Overwrite an existing authored contract for the scenario"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Bootstrap or refresh a workspace contract from imported policy and ACL state."""

    payload = bootstrap_workspace_contract(
        root, scenario_name=scenario_name, overwrite=overwrite
    )
    _emit(payload.model_dump(mode="json"), indent)
