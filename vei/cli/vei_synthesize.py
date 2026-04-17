from __future__ import annotations

from pathlib import Path
from typing import List

import typer

app = typer.Typer(add_completion=False)


@app.command()
def runbook(
    root: str = typer.Option(..., "--root", "-r", help="Workspace root directory"),
    run_id: str = typer.Option(..., "--run-id", help="Run ID to synthesize from"),
    output: str = typer.Option(
        "-", "--output", "-o", help="Output path or '-' for stdout"
    ),
) -> None:
    """Generate a structured runbook from a completed run."""
    from vei.synthesis.api import synthesize_runbook

    result = synthesize_runbook(root, run_id)
    text = result.model_dump_json(indent=2)
    if output != "-":
        Path(output).write_text(text, encoding="utf-8")
        typer.echo(
            f"Runbook: {result.total_steps} steps, "
            f"{result.decision_points} decision points -> {output}"
        )
    else:
        typer.echo(text)


@app.command("training-data")
def training_data(
    root: str = typer.Option(..., "--root", "-r", help="Workspace root directory"),
    run_id: List[str] = typer.Option(..., "--run-id", help="Run ID(s) to include"),
    format: str = typer.Option(
        "conversations",
        "--format",
        "-f",
        help="Output format: conversations, trajectories, demonstrations, authoring",
    ),
    output: str = typer.Option(
        "-", "--output", "-o", help="Output path or '-' for stdout"
    ),
) -> None:
    """Generate training data from completed runs."""
    from vei.synthesis.api import synthesize_training_set

    valid_formats = {"conversations", "trajectories", "demonstrations", "authoring"}
    fmt = format.strip().lower()
    if fmt not in valid_formats:
        raise typer.BadParameter(
            f"format must be one of: {', '.join(sorted(valid_formats))}"
        )
    result = synthesize_training_set(root, list(run_id), format=fmt)  # type: ignore[arg-type]
    text = result.model_dump_json(indent=2)
    if output != "-":
        Path(output).write_text(text, encoding="utf-8")
        typer.echo(f"Training set ({fmt}): {result.example_count} examples -> {output}")
    else:
        typer.echo(text)


@app.command("agent-config")
def agent_config(
    root: str = typer.Option(..., "--root", "-r", help="Workspace root directory"),
    run_id: str = typer.Option(..., "--run-id", help="Run ID to generate config from"),
    output: str = typer.Option(
        "-", "--output", "-o", help="Output path or '-' for stdout"
    ),
) -> None:
    """Generate an agent deployment configuration from a run."""
    from vei.synthesis.api import synthesize_agent_config

    result = synthesize_agent_config(root, run_id)
    text = result.model_dump_json(indent=2)
    if output != "-":
        Path(output).write_text(text, encoding="utf-8")
        typer.echo(
            f"Agent config: {len(result.tools)} tools, "
            f"{len(result.guardrails)} guardrails, "
            f"{len(result.success_criteria)} criteria -> {output}"
        )
    else:
        typer.echo(text)
