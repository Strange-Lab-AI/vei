from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path

import typer

from vei.context.api import snapshot_role


def emit_payload(payload: object, *, format: str) -> None:
    if format == "markdown":
        typer.echo(str(payload))
        return
    typer.echo(json.dumps(payload, indent=2))


def reject_workspace_seed_snapshot(source_dir: Path) -> None:
    try:
        snapshot = import_module("vei.whatif.corpus").load_history_snapshot(source_dir)
    except Exception:  # noqa: BLE001
        return
    if snapshot_role(snapshot) != "workspace_seed":
        return
    raise typer.BadParameter(
        "This snapshot is a saved what-if workspace seed. Use the original "
        "normalized bundle or a workspace command such as "
        "`vei whatif scene --workspace-root ...`."
    )


def fail_if_artifact_validation_failed(
    *,
    issues: list[str],
    label: str,
) -> None:
    if not issues:
        return
    typer.echo(f"{label} validation failed:", err=True)
    for issue in issues:
        typer.echo(f"- {issue}", err=True)
    raise typer.Exit(code=1)


def time_window(
    date_from: str | None,
    date_to: str | None,
) -> tuple[str, str] | None:
    if not date_from and not date_to:
        return None
    if not date_from or not date_to:
        raise typer.BadParameter("Provide both --date-from and --date-to")
    return (date_from, date_to)
