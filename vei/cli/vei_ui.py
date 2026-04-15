from __future__ import annotations

from pathlib import Path

import typer

app = typer.Typer(add_completion=False, help="Serve the local VEI playback UI.")


@app.command("serve")
def serve(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    host: str = typer.Option("127.0.0.1", help="Bind host"),
    port: int = typer.Option(3010, help="Bind port"),
) -> None:
    """Serve the local VEI playback UI for one workspace."""

    from vei.ui.app import serve_ui

    serve_ui(root, host=host, port=port)


@app.command("play")
def play(
    root: Path = typer.Option(
        Path("_vei_out/playable_studio"),
        help="Workspace root for the playable mission world",
    ),
    world: str = typer.Option(
        "real_estate_management",
        help="World to load",
    ),
    mission: str | None = typer.Option(None, help="Mission slug"),
    objective: str | None = typer.Option(None, help="Objective variant override"),
    compare_runner: str = typer.Option(
        "scripted", help="Comparison runner: scripted|bc|llm"
    ),
    overwrite: bool = typer.Option(True, help="Recreate workspace before preparing"),
    seed: int = typer.Option(42042, help="Seed for reproducibility"),
    max_steps: int = typer.Option(18, help="Max steps for comparison runs"),
    host: str = typer.Option("127.0.0.1", help="Bind host"),
    port: int = typer.Option(3011, help="Bind port"),
    serve_ui_flag: bool = typer.Option(
        True, "--serve/--no-serve", help="Serve UI after preparing"
    ),
) -> None:
    """Prepare a playable mission world and optionally serve the UI."""
    import json

    from vei.fidelity import get_or_build_workspace_fidelity_report
    from vei.playable import prepare_playable_workspace

    normalized = compare_runner.strip().lower()
    if normalized not in {"scripted", "bc", "llm"}:
        raise typer.BadParameter("compare-runner must be one of scripted|bc|llm")
    try:
        state = prepare_playable_workspace(
            root,
            world=world,
            mission=mission,
            objective=objective,
            compare_runner=normalized,
            overwrite=overwrite,
            seed=seed,
            max_steps=max_steps,
        )
    except (ValueError, KeyError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    fidelity = get_or_build_workspace_fidelity_report(root)
    typer.echo(
        json.dumps(
            {
                "workspace_root": str(Path(root).expanduser().resolve()),
                "world": world,
                "mission": state.mission.mission_name,
                "objective": state.objective_variant,
                "run_id": state.run_id,
                "fidelity_status": fidelity.status,
            },
            indent=2,
        )
    )
    if not serve_ui_flag:
        return
    from vei.ui.app import serve_ui as _serve

    _serve(root, host=host, port=port)


if __name__ == "__main__":
    app()
