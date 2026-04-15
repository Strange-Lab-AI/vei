from __future__ import annotations

import json
import signal
from datetime import UTC, datetime
from pathlib import Path

import typer

from vei.twin.api import build_twin_status, start_twin, stop_twin

app = typer.Typer(
    add_completion=False,
    help="One-command demo: spin up a living simulated enterprise with Studio + Twin Gateway.",
)


@app.command("run")
def quickstart_command(
    world: str = typer.Option(
        "real_estate_management",
        help="Built-in vertical: real_estate_management | digital_marketing_agency | storage_solutions | b2b_saas | service_ops",
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
    connector_mode: str = typer.Option(
        "sim",
        help="Governor connector mode for the gateway: sim | live",
    ),
    governor_demo: bool = typer.Option(
        False,
        help="Enable governor demo mode with staged agent activity.",
    ),
    governor_demo_interval_ms: int = typer.Option(
        1500,
        help="Autoplay interval for governor demo steps in milliseconds.",
    ),
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

    # --- 2. Launch shared twin runtime -------------------------------------------
    console.print("[dim]Launching Studio + twin gateway...[/dim]")
    try:
        start_twin(
            root,
            organization_name=getattr(state, "world_name", None),
            archetype=world,  # type: ignore[arg-type]
            gateway_port=gateway_port,
            studio_port=studio_port,
            connector_mode=connector_mode,
            governor_demo=governor_demo,
            governor_demo_interval_ms=governor_demo_interval_ms,
        )
    except (ValueError, RuntimeError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print("  [green]Twin runtime is live[/green]")

    # --- 4. Run scripted baseline (optional) -------------------------------------
    if not no_baseline:
        console.print("[dim]Running scripted baseline to populate timeline...[/dim]")
        _run_scripted_baseline(root, state)
        console.print("  [green]Baseline complete — events are flowing[/green]")

    # --- 5. Print connection panel -----------------------------------------------
    twin_status = build_twin_status(root)
    token = twin_status.manifest.bearer_token
    quickstart_info_path = _write_quickstart_info(root, twin_status)

    info_lines = [
        f"[bold]Studio UI[/bold]        {twin_status.manifest.studio_url}",
        f"[bold]Twin Gateway[/bold]     {twin_status.manifest.gateway_url}",
        f"[bold]Saved info[/bold]       {quickstart_info_path}",
        "",
        f"[bold]Auth token[/bold]       {token}",
        "",
        "[bold]Mock API endpoints:[/bold]",
        *_surface_endpoint_lines(twin_status),
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
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        signal.pause()
    except KeyboardInterrupt:
        pass
    finally:
        stop_twin(root)
        console.print("[green]VEI stopped.[/green]")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _write_quickstart_info(root: Path, twin_status) -> Path:
    workspace_root = root.expanduser().resolve()
    info_dir = workspace_root / ".vei"
    info_dir.mkdir(parents=True, exist_ok=True)
    info_path = info_dir / "quickstart.json"
    payload = {
        "written_at": datetime.now(UTC).isoformat(),
        "workspace_root": str(workspace_root),
        "studio_url": twin_status.manifest.studio_url,
        "gateway_url": twin_status.manifest.gateway_url,
        "gateway_status_url": twin_status.manifest.gateway_status_url,
        "bearer_token": twin_status.manifest.bearer_token,
        "supported_surfaces": [
            item.model_dump(mode="json")
            for item in twin_status.manifest.supported_surfaces
        ],
        "sample_curls": _sample_curls(twin_status),
    }
    info_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return info_path


def _sample_curls(twin_status) -> list[str]:
    gateway_url = twin_status.manifest.gateway_url.rstrip("/")
    token = twin_status.manifest.bearer_token
    curls = [
        f"curl -H 'Authorization: Bearer {token}' {gateway_url}/api/twin",
    ]
    for surface in twin_status.manifest.supported_surfaces:
        base_path = surface.base_path.rstrip("/")
        if surface.name == "slack":
            curls.append(
                f"curl -H 'Authorization: Bearer {token}' {gateway_url}{base_path}/conversations.list"
            )
        elif surface.name == "jira":
            curls.append(
                f"curl -H 'Authorization: Bearer {token}' {gateway_url}{base_path}/search"
            )
        elif surface.name == "graph":
            curls.append(
                f"curl -H 'Authorization: Bearer {token}' {gateway_url}{base_path}/me/messages"
            )
        elif surface.name == "salesforce":
            curls.append(
                f"curl -H 'Authorization: Bearer {token}' '{gateway_url}{base_path}/query?q=SELECT+Id+FROM+Opportunity+LIMIT+1'"
            )
        elif surface.name == "notes":
            curls.append(
                f"curl -H 'Authorization: Bearer {token}' {gateway_url}{base_path}/entries"
            )
    return curls


def _surface_endpoint_lines(twin_status) -> list[str]:
    gateway_url = twin_status.manifest.gateway_url.rstrip("/")
    surfaces = list(twin_status.manifest.supported_surfaces or [])
    if not surfaces:
        return ["  none"]

    return [
        f"  {surface.title:<8} {gateway_url}{surface.base_path.rstrip('/')}/"
        for surface in surfaces
    ]
