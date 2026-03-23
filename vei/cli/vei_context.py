from __future__ import annotations

from pathlib import Path
from typing import List

import typer

app = typer.Typer(add_completion=False)


@app.command()
def capture(
    provider: List[str] = typer.Option(
        ..., "--provider", "-p", help="Provider name (slack, jira, google, okta)"
    ),
    org: str = typer.Option(..., "--org", help="Organization name"),
    domain: str = typer.Option("", "--domain", help="Organization domain"),
    output: str = typer.Option(
        "context_snapshot.json", "--output", "-o", help="Output snapshot path"
    ),
    base_url: str = typer.Option("", "--base-url", help="Base URL (for jira/okta)"),
) -> None:
    """Capture live context from enterprise systems."""
    from vei.context.api import capture_context
    from vei.context.models import ContextProviderConfig

    env_map = {
        "slack": "VEI_SLACK_TOKEN",
        "jira": "VEI_JIRA_TOKEN",
        "google": "VEI_GOOGLE_TOKEN",
        "okta": "VEI_OKTA_TOKEN",
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
    text = snapshot.model_dump_json(indent=2)
    Path(output).write_text(text, encoding="utf-8")

    ok_count = sum(1 for s in snapshot.sources if s.status == "ok")
    err_count = sum(1 for s in snapshot.sources if s.status == "error")
    typer.echo(
        f"Captured {ok_count} providers"
        + (f" ({err_count} errors)" if err_count else "")
        + f" -> {output}"
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
) -> None:
    """Show summary of a context snapshot."""
    from vei.context.models import ContextSnapshot

    path = Path(snapshot)
    if not path.exists():
        raise typer.BadParameter(f"snapshot file not found: {snapshot}")

    snap = ContextSnapshot.model_validate_json(path.read_text(encoding="utf-8"))
    typer.echo(f"Organization: {snap.organization_name}")
    typer.echo(f"Captured at:  {snap.captured_at}")
    typer.echo(f"Providers:    {len(snap.sources)}")
    for source in snap.sources:
        counts = ", ".join(f"{k}={v}" for k, v in source.record_counts.items())
        typer.echo(f"  {source.provider:8s}  {source.status:7s}  {counts}")
