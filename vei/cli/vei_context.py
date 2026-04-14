from __future__ import annotations

from pathlib import Path
from typing import List

import typer

app = typer.Typer(add_completion=False)


@app.command()
def normalize(
    source_dir: str = typer.Option(
        ..., "--source-dir", help="Path to a mixed export directory or snapshot"
    ),
    org: str = typer.Option("", "--org", help="Organization name"),
    domain: str = typer.Option("", "--domain", help="Organization domain"),
    output: str = typer.Option(
        "context_snapshot.json", "--output", "-o", help="Output snapshot path"
    ),
) -> None:
    """Normalize mixed raw exports into one context snapshot."""
    from vei.context.normalize import normalize_raw_exports

    snapshot = normalize_raw_exports(
        source_dir,
        organization_name=org,
        organization_domain=domain,
    )
    Path(output).write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")

    ok_count = sum(1 for source in snapshot.sources if source.status == "ok")
    partial_count = sum(1 for source in snapshot.sources if source.status == "partial")
    empty_count = sum(1 for source in snapshot.sources if source.status == "empty")
    error_count = sum(1 for source in snapshot.sources if source.status == "error")
    typer.echo(
        "Normalized "
        f"{len(snapshot.sources)} sources "
        f"(ok={ok_count}, partial={partial_count}, empty={empty_count}, error={error_count}) "
        f"-> {output}"
    )


@app.command()
def verify(
    snapshot: str = typer.Option(
        ..., "--snapshot", "-s", help="Path to context snapshot JSON"
    ),
    output: str = typer.Option(
        "-", "--output", "-o", help="Output verification JSON path or stdout"
    ),
) -> None:
    """Run structural checks against a context snapshot."""
    from vei.context.models import ContextSnapshot
    from vei.context.normalize import verify_context_snapshot

    path = Path(snapshot)
    if not path.exists():
        raise typer.BadParameter(f"snapshot file not found: {snapshot}")

    snap = ContextSnapshot.model_validate_json(path.read_text(encoding="utf-8"))
    result = verify_context_snapshot(snap, snapshot_path=path)
    text = result.model_dump_json(indent=2)
    if output != "-":
        Path(output).write_text(text, encoding="utf-8")
        typer.echo(
            f"Verified snapshot ({result.error_count} errors, {result.warning_count} warnings) -> {output}"
        )
        return
    typer.echo(text)


@app.command()
def public(
    company: str = typer.Option(..., "--company", help="Organization name"),
    domain: str = typer.Option(..., "--domain", help="Organization domain"),
    template_only: bool = typer.Option(
        False,
        "--template-only",
        help="Write a template without fetching live public data",
    ),
    output: str = typer.Option(
        "whatif_public_context.json",
        "--output",
        "-o",
        help="Output public context path",
    ),
) -> None:
    """Create a public-context sidecar for what-if company history."""
    from vei.context.normalize import build_public_context_sidecar

    context = build_public_context_sidecar(
        organization_name=company,
        organization_domain=domain,
        live=not template_only,
    )
    Path(output).write_text(context.model_dump_json(indent=2), encoding="utf-8")
    typer.echo(
        "Public context written "
        f"(financial={len(context.financial_snapshots)}, "
        f"events={len(context.public_news_events)}) -> {output}"
    )


@app.command()
def capture(
    provider: List[str] = typer.Option(
        ...,
        "--provider",
        "-p",
        help="Provider name (slack, jira, google, okta, gmail, teams)",
    ),
    org: str = typer.Option(..., "--org", help="Organization name"),
    domain: str = typer.Option("", "--domain", help="Organization domain"),
    output: str = typer.Option(
        "context_snapshot.json", "--output", "-o", help="Output snapshot path"
    ),
    base_url: str = typer.Option("", "--base-url", help="Base URL (for jira/okta)"),
    anonymize: bool = typer.Option(
        False, "--anonymize", help="Apply PII anonymization to captured data"
    ),
) -> None:
    """Capture live context from enterprise systems."""
    from vei.context.api import capture_context
    from vei.context.models import ContextProviderConfig

    env_map = {
        "slack": "VEI_SLACK_TOKEN",
        "jira": "VEI_JIRA_TOKEN",
        "google": "VEI_GOOGLE_TOKEN",
        "okta": "VEI_OKTA_TOKEN",
        "gmail": "VEI_GMAIL_TOKEN",
        "teams": "VEI_TEAMS_TOKEN",
    }
    url_map = {
        "jira": "VEI_JIRA_URL",
        "okta": "VEI_OKTA_ORG_URL",
    }

    configs = []
    for name in provider:
        name = name.strip().lower()
        resolved_url = base_url
        if not resolved_url and name in url_map:
            import os

            resolved_url = os.environ.get(url_map[name], "")
        configs.append(
            ContextProviderConfig(
                provider=name,  # type: ignore[arg-type]
                token_env=env_map.get(name, f"VEI_{name.upper()}_TOKEN"),
                base_url=resolved_url or None,
            )
        )

    snapshot = capture_context(
        configs, organization_name=org, organization_domain=domain
    )

    if anonymize:
        from vei.anonymize import anonymize_snapshot as do_anonymize

        snapshot = do_anonymize(snapshot)
        typer.echo("Anonymization applied.")

    text = snapshot.model_dump_json(indent=2)
    Path(output).write_text(text, encoding="utf-8")

    ok_count = sum(1 for s in snapshot.sources if s.status == "ok")
    err_count = sum(1 for s in snapshot.sources if s.status == "error")
    typer.echo(
        f"Captured {ok_count} providers"
        + (f" ({err_count} errors)" if err_count else "")
        + f" -> {output}"
    )


@app.command("ingest-slack")
def ingest_slack(
    export_dir: str = typer.Option(
        ..., "--export", "-e", help="Path to Slack workspace export directory"
    ),
    org: str = typer.Option(..., "--org", help="Organization name"),
    domain: str = typer.Option("", "--domain", help="Organization domain"),
    output: str = typer.Option(
        "context_snapshot.json", "--output", "-o", help="Output snapshot path"
    ),
    message_limit: int = typer.Option(200, "--limit", help="Max messages per channel"),
) -> None:
    """Ingest a Slack workspace export directory (offline, no API key needed)."""
    from vei.context.api import ingest_slack_export

    path = Path(export_dir)
    if not path.is_dir():
        raise typer.BadParameter(f"not a directory: {export_dir}")

    snapshot = ingest_slack_export(
        path,
        organization_name=org,
        organization_domain=domain,
        message_limit=message_limit,
    )
    text = snapshot.model_dump_json(indent=2)
    Path(output).write_text(text, encoding="utf-8")

    source = snapshot.source_for("slack")
    counts = source.record_counts if source else {}
    typer.echo(
        f"Ingested {counts.get('channels', 0)} channels, "
        f"{counts.get('messages', 0)} messages, "
        f"{counts.get('users', 0)} users -> {output}"
    )


@app.command("ingest-gmail")
def ingest_gmail(
    mbox_file: str = typer.Option(
        ..., "--mbox", "-m", help="Path to Gmail Takeout MBOX file"
    ),
    org: str = typer.Option(..., "--org", help="Organization name"),
    domain: str = typer.Option("", "--domain", help="Organization domain"),
    output: str = typer.Option(
        "context_snapshot.json", "--output", "-o", help="Output snapshot path"
    ),
    message_limit: int = typer.Option(200, "--limit", help="Max messages to parse"),
) -> None:
    """Ingest a Gmail Takeout MBOX file (offline, no API key needed)."""
    from vei.context.api import ingest_gmail_export

    path = Path(mbox_file)
    if not path.exists():
        raise typer.BadParameter(f"file not found: {mbox_file}")

    snapshot = ingest_gmail_export(
        path,
        organization_name=org,
        organization_domain=domain,
        message_limit=message_limit,
    )
    text = snapshot.model_dump_json(indent=2)
    Path(output).write_text(text, encoding="utf-8")

    source = snapshot.source_for("gmail")
    counts = source.record_counts if source else {}
    typer.echo(
        f"Ingested {counts.get('threads', 0)} threads, "
        f"{counts.get('messages', 0)} messages -> {output}"
    )


@app.command()
def hydrate(
    snapshot: str = typer.Option(
        ..., "--snapshot", "-s", help="Path to context snapshot JSON"
    ),
    output: str = typer.Option(
        "blueprint.json", "--output", "-o", help="Output blueprint path"
    ),
    scenario_name: str = typer.Option(
        "captured_context", "--scenario", help="Scenario name for the blueprint"
    ),
) -> None:
    """Hydrate a context snapshot into a VEI blueprint."""
    from vei.context.api import hydrate_blueprint
    from vei.context.models import ContextSnapshot

    path = Path(snapshot)
    if not path.exists():
        raise typer.BadParameter(f"snapshot file not found: {snapshot}")

    snap = ContextSnapshot.model_validate_json(path.read_text(encoding="utf-8"))
    asset = hydrate_blueprint(snap, scenario_name=scenario_name)
    text = asset.model_dump_json(indent=2)
    Path(output).write_text(text, encoding="utf-8")
    typer.echo(f"Blueprint written -> {output}")


@app.command()
def diff(
    before: str = typer.Option(..., "--before", help="Path to earlier snapshot"),
    after: str = typer.Option(..., "--after", help="Path to later snapshot"),
    output: str = typer.Option(
        "-", "--output", "-o", help="Output diff path or stdout"
    ),
) -> None:
    """Compare two context snapshots."""
    from vei.context.api import diff_snapshots
    from vei.context.models import ContextSnapshot

    before_snap = ContextSnapshot.model_validate_json(
        Path(before).read_text(encoding="utf-8")
    )
    after_snap = ContextSnapshot.model_validate_json(
        Path(after).read_text(encoding="utf-8")
    )
    result = diff_snapshots(before_snap, after_snap)
    text = result.model_dump_json(indent=2)
    if output != "-":
        Path(output).write_text(text, encoding="utf-8")
        typer.echo(f"Diff: {result.summary} -> {output}")
    else:
        typer.echo(text)


@app.command()
def status(
    snapshot: str = typer.Option(
        ..., "--snapshot", "-s", help="Path to context snapshot JSON"
    ),
    format: str = typer.Option("plain", help="Output format: plain | json | markdown"),
) -> None:
    """Show summary of a context snapshot."""
    from vei.context.models import ContextSnapshot
    from vei.context.normalize import summarize_context_snapshot

    path = Path(snapshot)
    if not path.exists():
        raise typer.BadParameter(f"snapshot file not found: {snapshot}")

    snap = ContextSnapshot.model_validate_json(path.read_text(encoding="utf-8"))
    summary = summarize_context_snapshot(snap)
    if format == "json":
        typer.echo(summary.model_dump_json(indent=2))
        return
    if format == "markdown":
        lines = [
            "# Context Status",
            "",
            f"- Snapshot role: {summary.snapshot_role}",
            f"- Organization: {summary.organization_name}",
            f"- Domain: {summary.organization_domain or '(missing)'}",
            f"- Captured at: {summary.captured_at or '(missing)'}",
            f"- Time range: {summary.first_timestamp or '(missing)'} to {summary.last_timestamp or '(missing)'}",
            "",
            "## Providers",
        ]
        for provider in summary.providers:
            counts = ", ".join(
                f"{key}={value}" for key, value in provider.record_counts.items()
            )
            lines.append(
                f"- `{provider.provider}` {provider.status} | {counts or 'no counts'} | "
                f"timestamps={provider.timestamp_quality or 'missing'}"
            )
        if summary.duplicate_id_findings:
            lines.extend(["", "## Duplicate IDs"])
            for finding in summary.duplicate_id_findings:
                lines.append(f"- {finding.provider or 'bundle'}: {finding.detail}")
        if summary.identity_cleanup_findings:
            lines.extend(["", "## Identity Cleanup"])
            for finding in summary.identity_cleanup_findings:
                lines.append(f"- {finding.provider or 'bundle'}: {finding.detail}")
        if summary.timestamp_quality:
            lines.extend(["", "## Timestamp Quality"])
            for finding in summary.timestamp_quality:
                lines.append(f"- {finding.provider or 'bundle'}: {finding.detail}")
        typer.echo("\n".join(lines))
        return

    typer.echo(f"Snapshot role: {summary.snapshot_role}")
    typer.echo(f"Organization:  {summary.organization_name}")
    typer.echo(f"Domain:        {summary.organization_domain or '(missing)'}")
    typer.echo(f"Captured at:   {summary.captured_at or '(missing)'}")
    typer.echo(
        f"Time range:    {summary.first_timestamp or '(missing)'} -> "
        f"{summary.last_timestamp or '(missing)'}"
    )
    typer.echo(f"Providers:     {len(summary.providers)}")
    for provider in summary.providers:
        counts = ", ".join(
            f"{key}={value}" for key, value in provider.record_counts.items()
        )
        typer.echo(
            f"  {provider.provider:10s} {provider.status:7s} "
            f"{counts or 'no counts'} | timestamps={provider.timestamp_quality or 'missing'}"
        )
    if summary.duplicate_id_findings:
        typer.echo("Duplicate IDs:")
        for finding in summary.duplicate_id_findings:
            typer.echo(f"  {finding.provider or 'bundle'}: {finding.detail}")
    if summary.identity_cleanup_findings:
        typer.echo("Identity cleanup:")
        for finding in summary.identity_cleanup_findings:
            typer.echo(f"  {finding.provider or 'bundle'}: {finding.detail}")
    if summary.timestamp_quality:
        typer.echo("Timestamp quality:")
        for finding in summary.timestamp_quality:
            typer.echo(f"  {finding.provider or 'bundle'}: {finding.detail}")
