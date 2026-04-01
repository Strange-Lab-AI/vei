from __future__ import annotations

import json
from pathlib import Path

import typer

from vei.data.models import VEIDataset
from vei.data.ingest.slack_export import load_slack_export
from vei.data.ingest.mail_dir import load_mail_dir
from vei.data.ingest.tickets_dir import load_tickets
from vei.data.ingest.docs_dir import load_docs

app = typer.Typer(add_completion=False)


@app.command()
def slack(
    export_path: str = typer.Option(
        ..., "--export-path", "-e", help="Slack export directory"
    ),
    channel: str = typer.Option("#general", help="Channel name to annotate"),
    actor: str = typer.Option("user", help="Nominal actor"),
    output: str = typer.Option("-", help="Destination dataset path or '-' for stdout"),
) -> None:
    path = Path(export_path)
    if not path.exists():
        raise typer.BadParameter(f"export path does not exist: {export_path}")
    events = load_slack_export(path, channel=channel, actor=actor)
    dataset = VEIDataset(events=events)
    text = json.dumps(dataset.model_dump(), indent=2)
    if output != "-":
        Path(output).write_text(text, encoding="utf-8")
    else:
        typer.echo(text)


@app.command()
def mail(
    mail_dir: str = typer.Option(
        ..., "--mail-dir", help="Directory with JSON mail messages"
    ),
    folder: str = typer.Option("INBOX", help="Mail folder name"),
    output: str = typer.Option("-", help="Destination dataset path or '-' for stdout"),
) -> None:
    path = Path(mail_dir)
    if not path.exists():
        raise typer.BadParameter(f"mail directory missing: {mail_dir}")
    events = load_mail_dir(path, folder=folder)
    dataset = VEIDataset(events=events)
    text = json.dumps(dataset.model_dump(), indent=2)
    if output != "-":
        Path(output).write_text(text, encoding="utf-8")
    else:
        typer.echo(text)


@app.command()
def tickets(
    tickets_dir: str = typer.Option(
        ..., "--tickets-dir", help="Directory with ticket JSON updates"
    ),
    output: str = typer.Option("-", help="Destination dataset path or '-' for stdout"),
) -> None:
    path = Path(tickets_dir)
    if not path.exists():
        raise typer.BadParameter(f"tickets directory missing: {tickets_dir}")
    events = load_tickets(path)
    dataset = VEIDataset(events=events)
    text = json.dumps(dataset.model_dump(), indent=2)
    if output != "-":
        Path(output).write_text(text, encoding="utf-8")
    else:
        typer.echo(text)


@app.command()
def docs(
    docs_dir: str = typer.Option(
        ..., "--docs-dir", help="Directory with document JSON entries"
    ),
    output: str = typer.Option("-", help="Destination dataset path or '-' for stdout"),
) -> None:
    path = Path(docs_dir)
    if not path.exists():
        raise typer.BadParameter(f"docs directory missing: {docs_dir}")
    events = load_docs(path)
    dataset = VEIDataset(events=events)
    text = json.dumps(dataset.model_dump(), indent=2)
    if output != "-":
        Path(output).write_text(text, encoding="utf-8")
    else:
        typer.echo(text)


if __name__ == "__main__":
    app()
