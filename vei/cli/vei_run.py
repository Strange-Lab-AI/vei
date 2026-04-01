from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from vei.run.api import (
    launch_workspace_run,
    list_run_manifests,
    load_run_manifest,
    normalize_runner,
)
from vei.workspace.api import list_workspace_runs

app = typer.Typer(add_completion=False, help="Launch and inspect workspace runs.")


def _emit(payload: object, indent: int) -> None:
    typer.echo(json.dumps(payload, indent=indent))


def _resolve_run_id(root: Path, run_id: Optional[str]) -> str:
    if run_id:
        return run_id
    runs = list_workspace_runs(root)
    if not runs:
        raise typer.BadParameter("No runs found; provide --run-id")
    return runs[0].run_id


@app.command("start")
def start_run(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    runner: str = typer.Option(
        "workflow",
        help="workflow, scripted, bc, or llm runner (bc requires --bc-model)",
    ),
    scenario_name: Optional[str] = typer.Option(None, help="Workspace scenario name"),
    run_id: Optional[str] = typer.Option(None, help="Optional run id override"),
    seed: int = typer.Option(42042, help="Deterministic seed"),
    branch: Optional[str] = typer.Option(None, help="Optional branch name override"),
    model: Optional[str] = typer.Option(None, help="Model name for llm runs"),
    provider: Optional[str] = typer.Option(None, help="LLM provider override"),
    bc_model: Optional[Path] = typer.Option(
        None, help="Behavior-cloning checkpoint path for bc runs"
    ),
    task: Optional[str] = typer.Option(None, help="Task prompt for llm runs"),
    max_steps: int = typer.Option(12, help="Maximum agent steps"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Launch one run through the unified workspace/run lifecycle."""

    try:
        resolved_runner = normalize_runner(runner)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    try:
        manifest = launch_workspace_run(
            root,
            runner=resolved_runner,
            scenario_name=scenario_name,
            run_id=run_id,
            seed=seed,
            branch=branch,
            model=model,
            provider=provider,
            bc_model_path=bc_model,
            task=task,
            max_steps=max_steps,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(manifest.model_dump(mode="json"), indent)


@app.command("list")
def list_runs(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """List run manifests for a workspace."""

    payload = [item.model_dump(mode="json") for item in list_run_manifests(root)]
    _emit(payload, indent)


@app.command("show")
def show_run(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    run_id: Optional[str] = typer.Option(None, help="Run id"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Show one run manifest, defaulting to the latest run."""

    resolved_root = root.expanduser().resolve()
    resolved_run_id = _resolve_run_id(resolved_root, run_id)
    manifest = load_run_manifest(
        resolved_root / "runs" / resolved_run_id / "run_manifest.json"
    )
    _emit(manifest.model_dump(mode="json"), indent)
