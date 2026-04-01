from __future__ import annotations

import json
from pathlib import Path

import typer
from pydantic import ValidationError

from vei.context.models import ContextProviderConfig, ContextSnapshot
from vei.twin import (
    ContextMoldConfig,
    build_customer_twin,
    load_customer_twin,
    serve_customer_twin,
)

app = typer.Typer(
    add_completion=False,
    help="Build and serve customer-shaped agent twin environments.",
)


def _emit(payload: object, indent: int) -> None:
    typer.echo(json.dumps(payload, indent=indent))


@app.command("build")
def build_command(
    root: Path = typer.Option(Path("."), help="Workspace root for the twin"),
    snapshot: Path | None = typer.Option(
        None,
        help="Context snapshot JSON built with `vei context ...`",
    ),
    provider_configs: Path | None = typer.Option(
        None,
        help="JSON file containing a list of ContextProviderConfig objects",
    ),
    organization_name: str | None = typer.Option(
        None,
        help="Organization name override or required name for live capture",
    ),
    organization_domain: str = typer.Option(
        "",
        help="Organization domain override",
    ),
    archetype: str = typer.Option(
        "b2b_saas",
        help="Base world archetype to mold the twin against",
    ),
    scenario_variant: str | None = typer.Option(
        None,
        help="Optional vertical scenario variant to activate after build",
    ),
    contract_variant: str | None = typer.Option(
        None,
        help="Optional contract variant to activate after build",
    ),
    gateway_token: str | None = typer.Option(
        None,
        help="Optional bearer token override for the compatibility gateway",
    ),
    overwrite: bool = typer.Option(
        True,
        help="Overwrite an existing twin workspace root",
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Build a customer-shaped twin from a context snapshot or live provider configs."""

    snapshot_payload = (
        ContextSnapshot.model_validate_json(snapshot.read_text(encoding="utf-8"))
        if snapshot is not None
        else None
    )
    provider_payload = None
    if provider_configs is not None:
        raw = json.loads(provider_configs.read_text(encoding="utf-8"))
        provider_payload = [ContextProviderConfig.model_validate(item) for item in raw]

    try:
        bundle = build_customer_twin(
            root,
            snapshot=snapshot_payload,
            provider_configs=provider_payload,
            organization_name=organization_name,
            organization_domain=organization_domain,
            mold=ContextMoldConfig(
                archetype=archetype,  # type: ignore[arg-type]
                scenario_variant=scenario_variant,
                contract_variant=contract_variant,
            ),
            gateway_token=gateway_token,
            overwrite=overwrite,
        )
    except (ValidationError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(bundle.model_dump(mode="json"), indent)


@app.command("status")
def status_command(
    root: Path = typer.Option(Path("."), help="Twin workspace root"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Show the saved twin bundle and basic workspace status."""

    bundle = load_customer_twin(root)
    _emit(bundle.model_dump(mode="json"), indent)


@app.command("serve")
def serve_command(
    root: Path = typer.Option(Path("."), help="Twin workspace root"),
    host: str = typer.Option("127.0.0.1", help="Bind host"),
    port: int = typer.Option(3020, help="Bind port"),
) -> None:
    """Serve the compatibility gateway for a built twin."""

    serve_customer_twin(root, host=host, port=port)
