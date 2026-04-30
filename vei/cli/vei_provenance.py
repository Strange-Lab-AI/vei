from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from vei.provenance.api import (
    access_review,
    blast_radius,
    build_evidence_pack,
    build_activity_graph,
    export_otel,
    inspect_timeline,
    load_policy_file,
    load_workspace_events,
    provenance_actor_id,
    replay_policy,
)

app = typer.Typer(no_args_is_help=True, help="Inspect VEI Control provenance.")


def _events(workspace: Path) -> list:
    return load_workspace_events(workspace)


def _validate_json_format(format: str) -> None:
    if format != "json":
        raise typer.BadParameter("only --format json is implemented for this command")


def _echo(payload: object) -> None:
    if hasattr(payload, "model_dump"):
        typer.echo(
            json.dumps(payload.model_dump(mode="json"), indent=2, sort_keys=True)
        )
    else:
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))


@app.command("inspect")
def inspect(
    workspace: Path = typer.Option(..., help="VEI workspace root"),
    format: str = typer.Option("json", help="json"),
) -> None:
    _validate_json_format(format)
    _echo(inspect_timeline(_events(workspace)))


@app.command("graph")
def graph(
    workspace: Path = typer.Option(..., help="VEI workspace root"),
    agent_id: Optional[str] = typer.Option(None, help="Optional agent filter"),
    format: str = typer.Option("json", help="json"),
) -> None:
    _validate_json_format(format)
    events = _events(workspace)
    if agent_id:
        events = [event for event in events if provenance_actor_id(event) == agent_id]
    _echo(build_activity_graph(events))


@app.command("blast-radius")
def blast_radius_cmd(
    event_id: str = typer.Option(..., help="Anchor canonical event id"),
    workspace: Path = typer.Option(..., help="VEI workspace root"),
    format: str = typer.Option("json", help="json"),
) -> None:
    _validate_json_format(format)
    _echo(blast_radius(_events(workspace), anchor_event_id=event_id))


@app.command("access-review")
def access_review_cmd(
    agent_id: str = typer.Option(..., help="Agent or actor id"),
    workspace: Path = typer.Option(..., help="VEI workspace root"),
    format: str = typer.Option("json", help="json"),
) -> None:
    _validate_json_format(format)
    _echo(access_review(_events(workspace), agent_id=agent_id))


@app.command("replay-policy")
def replay_policy_cmd(
    policy: Path = typer.Option(..., help="Policy JSON path"),
    workspace: Path = typer.Option(..., help="VEI workspace root"),
    format: str = typer.Option("json", help="json"),
) -> None:
    _validate_json_format(format)
    _echo(replay_policy(_events(workspace), policy=load_policy_file(policy)))


@app.command("evidence-pack")
def evidence_pack_cmd(
    workspace: Path = typer.Option(..., help="VEI workspace root"),
    agent_id: Optional[str] = typer.Option(None, help="Optional agent id"),
    event_id: Optional[str] = typer.Option(None, help="Optional anchor event id"),
    policy: Optional[Path] = typer.Option(None, help="Optional policy JSON path"),
    format: str = typer.Option("json", help="json"),
) -> None:
    _validate_json_format(format)
    _echo(
        build_evidence_pack(
            _events(workspace),
            agent_id=agent_id,
            anchor_event_id=event_id,
            policy=load_policy_file(policy) if policy else None,
        )
    )


@app.command("export")
def export(
    format: str = typer.Option(..., help="otel or evidence-pack"),
    workspace: Path = typer.Option(..., help="VEI workspace root"),
    output: Path = typer.Option(..., help="Output JSON path"),
) -> None:
    if format == "otel":
        payload = export_otel(_events(workspace))
    elif format == "evidence-pack":
        payload = build_evidence_pack(_events(workspace)).model_dump(mode="json")
    else:
        raise typer.BadParameter("only --format otel or evidence-pack is implemented")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    typer.echo(str(output))
