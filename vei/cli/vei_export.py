from __future__ import annotations

import json
from pathlib import Path

import typer

from vei.playable import export_mission_run

app = typer.Typer(
    add_completion=False,
    help="Export playable mission runs into downstream-ready preview bundles.",
)


@app.command("mission-run")
def export_mission_run_command(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    run_id: str = typer.Option(..., help="Mission run id"),
    export_format: str = typer.Option(
        ...,
        "--format",
        help="Export preview to render: rl, eval, or agent-ops",
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    try:
        payload = export_mission_run(root, run_id=run_id, export_format=export_format)
    except (ValueError, KeyError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(payload, indent=indent))
