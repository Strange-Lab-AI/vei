from __future__ import annotations

import signal
import threading
import time
from pathlib import Path

import typer

from vei.twin.api import CustomerTwinBundle

app = typer.Typer(
    add_completion=False,
    help="One-command demo: spin up a living simulated enterprise with Studio + Twin Gateway.",
)


@app.command("run")
def quickstart_command(
    world: str = typer.Option(
        "real_estate_management",
        help="Built-in vertical: real_estate_management | digital_marketing_agency | storage_solutions",
    ),
    mission: str | None = typer.Option(
        None, help="Mission slug (first available if omitted)"
    ),
    root: Path = typer.Option(
        Path("_vei_out/quickstart"),
        help="Workspace root (created fresh each run)",
    ),
    studio_port: int = typer.Option(3011, help="Studio UI port"),
    gateway_port: int = typer.Option(3012, help="Twin Gateway port"),
    seed: int = typer.Option(42042, help="Seed for reproducibility"),
    no_baseline: bool = typer.Option(
        False, "--no-baseline", help="Skip the scripted baseline run"
    ),
) -> None:
    """Spin up a full VEI demo in one command.

    Creates a workspace, launches Studio UI and Twin Gateway, optionally runs
    a scripted baseline so you can immediately see events flowing, then waits
    for Ctrl-C.
    """
    from rich.console import Console
    from rich.panel import Panel

    console = Console()

    console.print("\n[bold]VEI Quickstart[/bold]", style="cyan")
    console.print(f"  World:   {world}")
    console.print(f"  Seed:    {seed}")
    console.print(f"  Root:    {root.resolve()}\n")

    # --- 1. Prepare workspace ---------------------------------------------------
    console.print("[dim]Preparing workspace...[/dim]")
    from vei.playable import prepare_playable_workspace

    try:
        state = prepare_playable_workspace(
            root,
            world=world,
            mission=mission,
            objective=None,
            compare_runner="scripted",
            overwrite=True,
            seed=seed,
            max_steps=18,
        )
    except (ValueError, KeyError) as exc:
        console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(1) from exc

    console.print(
        f"  [green]Workspace ready[/green] — mission: {state.mission.mission_name}, "
        f"run: {state.run_id}"
    )

    # --- 2. Build twin bundle for gateway ----------------------------------------
    console.print("[dim]Building twin gateway bundle...[/dim]")
    _ensure_twin_bundle(root, world, gateway_port)
    console.print("  [green]Twin gateway bundle ready[/green]")

    # --- 3. Launch servers -------------------------------------------------------
    shutdown = threading.Event()

    studio_thread = threading.Thread(
        target=_serve_studio,
        args=(root, studio_port, shutdown),
        daemon=True,
    )
    gateway_thread = threading.Thread(
        target=_serve_gateway,
        args=(root, gateway_port, shutdown),
        daemon=True,
    )
    studio_thread.start()
    gateway_thread.start()
    time.sleep(1.5)

    # --- 4. Run scripted baseline (optional) -------------------------------------
    if not no_baseline:
        console.print("[dim]Running scripted baseline to populate timeline...[/dim]")
        _run_scripted_baseline(root, state)
        console.print("  [green]Baseline complete — events are flowing[/green]")

    # --- 5. Print connection panel -----------------------------------------------
    twin_bundle = _load_twin_bundle(root)
    token = twin_bundle.gateway.auth_token if twin_bundle else "N/A"

    info_lines = [
        f"[bold]Studio UI[/bold]        http://127.0.0.1:{studio_port}",
        f"[bold]Twin Gateway[/bold]     http://127.0.0.1:{gateway_port}",
        "",
        f"[bold]Auth token[/bold]       {token}",
        "",
        "[bold]Mock API endpoints:[/bold]",
        f"  Slack    http://127.0.0.1:{gateway_port}/slack/api/",
        f"  Jira     http://127.0.0.1:{gateway_port}/jira/rest/api/3/",
        f"  Graph    http://127.0.0.1:{gateway_port}/graph/v1.0/",
        f"  SFDC     http://127.0.0.1:{gateway_port}/salesforce/services/data/v60.0/",
        "",
        "[bold]MCP endpoint[/bold]     python -m vei.router",
        "",
        "[dim]Press Ctrl-C to stop all servers.[/dim]",
    ]
    console.print(
        Panel("\n".join(info_lines), title="VEI is running", border_style="green")
    )

    # --- 6. Wait for Ctrl-C ------------------------------------------------------
    def _handle_signal(_sig, _frame):
        console.print("\n[yellow]Shutting down...[/yellow]")
        shutdown.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        while not shutdown.is_set():
            shutdown.wait(timeout=1.0)
    except KeyboardInterrupt:
        pass
    finally:
        shutdown.set()
        console.print("[green]VEI stopped.[/green]")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ensure_twin_bundle(root: Path, world: str, gateway_port: int) -> None:
    """Create a minimal twin bundle so the gateway can start."""
    import json
    import secrets

    from vei.workspace.api import load_workspace

    workspace_root = root.expanduser().resolve()
    manifest_path = workspace_root / "twin_manifest.json"
    if manifest_path.exists():
        return

    ws = load_workspace(workspace_root)
    bundle = {
        "workspace_root": str(workspace_root),
        "workspace_name": ws.name,
        "organization_name": ws.title or world.replace("_", " ").title(),
        "organization_domain": "example.com",
        "mold": {
            "archetype": world,
            "density_level": "medium",
            "named_team_expansion": "standard",
            "crisis_family": "operational",
            "redaction_mode": "none",
            "synthetic_expansion_strength": "light",
            "included_surfaces": [],
        },
        "context_snapshot_path": "",
        "blueprint_asset_path": str(ws.blueprint_asset_path),
        "gateway": {
            "auth_token": secrets.token_urlsafe(18),
            "surfaces": [
                {"name": "slack", "title": "Slack", "base_path": "/slack/api"},
                {"name": "jira", "title": "Jira", "base_path": "/jira/rest/api/3"},
                {
                    "name": "graph",
                    "title": "Microsoft Graph",
                    "base_path": "/graph/v1.0",
                },
                {
                    "name": "salesforce",
                    "title": "Salesforce",
                    "base_path": "/salesforce/services/data/v60.0",
                },
            ],
            "ui_command": f"python -m vei.cli.vei ui serve --root {workspace_root} --port 3011",
        },
        "summary": f"Quickstart twin for {world}",
        "metadata": {},
    }
    manifest_path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")


def _load_twin_bundle(root: Path) -> CustomerTwinBundle | None:
    try:
        from vei.twin import load_customer_twin

        return load_customer_twin(root)
    except Exception:  # noqa: BLE001
        return None


def _serve_studio(root: Path, port: int, shutdown: threading.Event) -> None:
    import uvicorn

    from vei.ui.api import create_ui_app

    app = create_ui_app(root)
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    shutdown.wait()
    server.should_exit = True


def _serve_gateway(root: Path, port: int, shutdown: threading.Event) -> None:
    import uvicorn

    from vei.twin.gateway import create_twin_gateway_app

    try:
        app = create_twin_gateway_app(root)
    except Exception:  # noqa: BLE001
        return

    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    shutdown.wait()
    server.should_exit = True


def _run_scripted_baseline(root: Path, state) -> None:
    """Execute 3-4 scripted moves so the timeline has content on first load."""
    from vei.playable import (
        apply_workspace_mission_move,
        load_workspace_mission_state,
    )

    workspace_root = root.expanduser().resolve()
    mission_state = load_workspace_mission_state(workspace_root)
    if not mission_state:
        return

    available = mission_state.available_moves or []
    for move in available[:3]:
        try:
            apply_workspace_mission_move(
                workspace_root,
                move_id=move.move_id,
                run_id=state.run_id,
            )
        except Exception:  # noqa: BLE001
            break
