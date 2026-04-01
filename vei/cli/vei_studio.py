from __future__ import annotations

import json
from pathlib import Path

import typer

from vei.fidelity import get_or_build_workspace_fidelity_report
from vei.playable import prepare_playable_workspace

app = typer.Typer(
    add_completion=False,
    help="Prepare and launch the playable VEI Studio mission experience.",
)


@app.command("play")
def play_command(
    root: Path = typer.Option(
        Path("_vei_out/playable_studio"),
        help="Workspace root for the playable mission world",
    ),
    world: str = typer.Option(
        "real_estate_management",
        help="World to load: real_estate_management, digital_marketing_agency, or storage_solutions",
    ),
    mission: str | None = typer.Option(
        None,
        help="Mission slug inside the selected world",
    ),
    objective: str | None = typer.Option(
        None,
        help="Objective variant override",
    ),
    compare_runner: str = typer.Option(
        "scripted",
        help="Comparison runner for the playable bundle: scripted|bc|llm",
    ),
    overwrite: bool = typer.Option(
        True,
        help="Recreate the workspace before preparing the playable mission",
    ),
    seed: int = typer.Option(42042, help="Seed for reproducibility"),
    max_steps: int = typer.Option(18, help="Max steps for comparison runs"),
    host: str = typer.Option("127.0.0.1", help="Bind host when serving UI"),
    port: int = typer.Option(3011, help="Bind port when serving UI"),
    serve: bool = typer.Option(
        True,
        "--serve/--no-serve",
        help="Serve the UI after preparing the playable workspace",
    ),
) -> None:
    normalized_runner = compare_runner.strip().lower()
    if normalized_runner not in {"scripted", "bc", "llm"}:
        raise typer.BadParameter("compare-runner must be one of scripted|bc|llm")
    try:
        state = prepare_playable_workspace(
            root,
            world=world,
            mission=mission,
            objective=objective,
            compare_runner=normalized_runner,
            overwrite=overwrite,
            seed=seed,
            max_steps=max_steps,
        )
    except (ValueError, KeyError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    fidelity = get_or_build_workspace_fidelity_report(root)
    payload = {
        "workspace_root": str(Path(root).expanduser().resolve()),
        "world": world,
        "mission": state.mission.mission_name,
        "objective": state.objective_variant,
        "run_id": state.run_id,
        "baseline_run_id": state.baseline_run_id,
        "comparison_run_id": state.comparison_run_id,
        "fidelity_status": fidelity.status,
        "ui_command": (
            "python -m vei.cli.vei ui serve "
            f"--root {Path(root).expanduser().resolve()} --host {host} --port {port}"
        ),
    }
    typer.echo(json.dumps(payload, indent=2))
    if not serve:
        return
    from vei.ui.app import serve_ui

    serve_ui(root, host=host, port=port)
