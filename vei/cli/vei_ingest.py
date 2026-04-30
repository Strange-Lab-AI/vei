from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from vei.ingest.api import agent_activity_ingest_status, ingest_agent_activity_source

app = typer.Typer(no_args_is_help=True, help="Ingest external evidence into VEI.")


def _validate_json_format(format: str) -> None:
    if format != "json":
        raise typer.BadParameter("only --format json is implemented for this command")


@app.command("agent-activity")
def agent_activity(
    source: str = typer.Option(
        ..., help="agent_activity_jsonl | mcp_transcript | openai_org"
    ),
    workspace: Path = typer.Option(..., help="VEI workspace root"),
    path: Optional[Path] = typer.Option(
        None, help="Input file or directory for file-backed sources"
    ),
    token_env: str = typer.Option(
        "OPENAI_ADMIN_KEY", help="Env var for OpenAI Admin API token"
    ),
    since: str = typer.Option("", help="Batch window/cursor hint, for example 7d"),
    tenant_id: str = typer.Option("", help="Optional tenant id"),
    format: str = typer.Option("json", help="json"),
) -> None:
    _validate_json_format(format)
    try:
        result = ingest_agent_activity_source(
            source=source,
            workspace=str(workspace),
            path=str(path) if path is not None else None,
            token_env=token_env,
            window=since,
            tenant_id=tenant_id,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2, sort_keys=True))


@app.command("status")
def status(
    workspace: Path = typer.Option(..., help="VEI workspace root"),
    format: str = typer.Option("json", help="json"),
) -> None:
    _validate_json_format(format)
    typer.echo(
        json.dumps(
            agent_activity_ingest_status(str(workspace)), indent=2, sort_keys=True
        )
    )
