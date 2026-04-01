from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from vei.fidelity import get_or_build_workspace_fidelity_report
from vei.run.api import (
    build_run_timeline,
    diff_run_snapshots,
    get_run_capability_graphs,
    get_run_orientation,
    list_run_snapshots,
)
from vei.workspace.api import list_workspace_runs
from vei.workspace.api import load_workspace_provenance

app = typer.Typer(
    add_completion=False, help="Inspect run timelines, graphs, and snapshots."
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


def _load_or_build_events(root: Path, run_id: str):
    return build_run_timeline(root, run_id)


@app.command("orient")
def inspect_orientation(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    run_id: Optional[str] = typer.Option(None, help="Run id"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Show the agent-facing orientation for a run."""

    resolved_root = root.expanduser().resolve()
    resolved_run_id = _resolve_run_id(resolved_root, run_id)
    _emit(get_run_orientation(resolved_root, resolved_run_id), indent)


@app.command("graphs")
def inspect_graphs(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    run_id: Optional[str] = typer.Option(None, help="Run id"),
    domain: Optional[str] = typer.Option(
        None,
        help="Optional domain such as identity_graph, doc_graph, work_graph, revenue_graph, ops_graph, obs_graph, or data_graph",
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Show runtime capability graphs for a run."""

    resolved_root = root.expanduser().resolve()
    resolved_run_id = _resolve_run_id(resolved_root, run_id)
    payload = get_run_capability_graphs(resolved_root, resolved_run_id)
    if domain:
        normalized = domain.strip().lower()
        payload = {
            "run_id": resolved_run_id,
            "domain": normalized,
            "graph": payload.get(normalized),
        }
    _emit(payload, indent)


@app.command("events")
def inspect_events(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    run_id: Optional[str] = typer.Option(None, help="Run id"),
    kind: Optional[str] = typer.Option(
        None, help="Optional event kind filter such as workflow_step or receipt"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Show the normalized event timeline for a run."""

    resolved_root = root.expanduser().resolve()
    resolved_run_id = _resolve_run_id(resolved_root, run_id)
    events = [
        item.model_dump(mode="json")
        for item in _load_or_build_events(resolved_root, resolved_run_id)
    ]
    if kind:
        events = [item for item in events if item.get("kind") == kind]
    _emit({"run_id": resolved_run_id, "events": events}, indent)


@app.command("receipts")
def inspect_receipts(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    run_id: Optional[str] = typer.Option(None, help="Run id"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Show connector receipt events for a run."""

    resolved_root = root.expanduser().resolve()
    resolved_run_id = _resolve_run_id(resolved_root, run_id)
    events = [
        item.model_dump(mode="json")
        for item in _load_or_build_events(resolved_root, resolved_run_id)
        if item.kind == "receipt"
    ]
    _emit({"run_id": resolved_run_id, "receipts": events}, indent)


@app.command("snapshots")
def inspect_snapshots(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    run_id: Optional[str] = typer.Option(None, help="Run id"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """List snapshots captured for a run."""

    resolved_root = root.expanduser().resolve()
    resolved_run_id = _resolve_run_id(resolved_root, run_id)
    payload = [
        item.model_dump(mode="json")
        for item in list_run_snapshots(resolved_root, resolved_run_id)
    ]
    _emit({"run_id": resolved_run_id, "snapshots": payload}, indent)


@app.command("diff")
def inspect_snapshot_diff(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    run_id: Optional[str] = typer.Option(None, help="Run id"),
    snapshot_from: int = typer.Option(..., help="Snapshot id to diff from"),
    snapshot_to: int = typer.Option(..., help="Snapshot id to diff to"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Diff two snapshots from the same run."""

    resolved_root = root.expanduser().resolve()
    resolved_run_id = _resolve_run_id(resolved_root, run_id)
    _emit(
        diff_run_snapshots(
            resolved_root,
            resolved_run_id,
            snapshot_from=snapshot_from,
            snapshot_to=snapshot_to,
        ),
        indent,
    )


@app.command("provenance")
def inspect_provenance(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    object_ref: Optional[str] = typer.Option(
        None, help="Optional object reference such as identity_user:USR-ACQ-1"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Show imported, derived, and simulated provenance for workspace objects."""

    resolved_root = root.expanduser().resolve()
    records = [
        item.model_dump(mode="json")
        for item in load_workspace_provenance(resolved_root, object_ref)
    ]
    _emit({"object_ref": object_ref, "provenance": records}, indent)


@app.command("fidelity")
def inspect_fidelity(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    surface: Optional[str] = typer.Option(
        None,
        help="Optional surface filter such as slack, docs, tickets, identity, or property",
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Show boundary-fidelity validation for a playable workspace."""

    resolved_root = root.expanduser().resolve()
    report = get_or_build_workspace_fidelity_report(resolved_root).model_dump(
        mode="json"
    )
    if surface:
        normalized = surface.strip().lower()
        report["cases"] = [
            item for item in report["cases"] if item.get("surface") == normalized
        ]
    _emit(report, indent)
