from __future__ import annotations

import json
from pathlib import Path

import typer
from pydantic import ValidationError

from vei.context.models import ContextProviderConfig, ContextSnapshot
from vei.exercise import (
    activate_exercise,
    build_exercise_status,
    start_exercise,
    stop_exercise,
)
from vei.twin.models import TwinArchetype

app = typer.Typer(
    add_completion=False,
    help="Launch and operate VEI exercise mode for outside-agent testing.",
)


def _emit(payload: object, indent: int) -> None:
    typer.echo(json.dumps(payload, indent=indent))


def _load_snapshot(path: Path | None) -> ContextSnapshot | None:
    if path is None:
        return None
    return ContextSnapshot.model_validate_json(path.read_text(encoding="utf-8"))


def _load_provider_configs(path: Path | None) -> list[ContextProviderConfig] | None:
    if path is None:
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    return [ContextProviderConfig.model_validate(item) for item in raw]


@app.command("up")
def up_command(
    root: Path = typer.Option(Path("."), help="Workspace root for the exercise"),
    snapshot: Path | None = typer.Option(
        None,
        help="Optional context snapshot JSON built with `vei context ...`",
    ),
    provider_configs: Path | None = typer.Option(
        None,
        help="Optional JSON file containing ContextProviderConfig objects",
    ),
    organization_name: str | None = typer.Option(
        None,
        help="Organization name override or starter company name",
    ),
    organization_domain: str = typer.Option(
        "",
        help="Organization domain override",
    ),
    archetype: TwinArchetype = typer.Option(
        "b2b_saas",
        help="Default customer twin archetype",
    ),
    scenario_variant: str | None = typer.Option(
        None,
        help="Optional crisis variant to activate after build",
    ),
    contract_variant: str | None = typer.Option(
        None,
        help="Optional success-criteria variant to activate after build",
    ),
    gateway_token: str | None = typer.Option(
        None,
        help="Optional bearer token override for the twin gateway",
    ),
    host: str = typer.Option("127.0.0.1", help="Bind host for both services"),
    gateway_port: int = typer.Option(3020, help="Twin gateway port"),
    studio_port: int = typer.Option(3011, help="Studio and Operator Console port"),
    rebuild: bool = typer.Option(
        False,
        help="Rebuild the twin workspace before launching",
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Build or load a twin and launch VEI exercise mode."""

    try:
        payload = start_exercise(
            root,
            snapshot=_load_snapshot(snapshot),
            provider_configs=_load_provider_configs(provider_configs),
            organization_name=organization_name,
            organization_domain=organization_domain,
            archetype=archetype,
            scenario_variant=scenario_variant,
            contract_variant=contract_variant,
            gateway_token=gateway_token,
            host=host,
            gateway_port=gateway_port,
            studio_port=studio_port,
            rebuild=rebuild,
        )
    except (ValidationError, ValueError, RuntimeError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(payload.model_dump(mode="json"), indent)


@app.command("status")
def status_command(
    root: Path = typer.Option(Path("."), help="Workspace root for the exercise"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Show the current exercise status."""

    try:
        payload = build_exercise_status(root)
    except (ValidationError, ValueError, FileNotFoundError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(payload.model_dump(mode="json"), indent)


@app.command("down")
def down_command(
    root: Path = typer.Option(Path("."), help="Workspace root for the exercise"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Stop the local exercise stack."""

    try:
        payload = stop_exercise(root)
    except (ValidationError, ValueError, FileNotFoundError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(payload.model_dump(mode="json"), indent)


@app.command("activate")
def activate_command(
    root: Path = typer.Option(Path("."), help="Workspace root for the exercise"),
    scenario_variant: str = typer.Option(..., help="Scenario variant to activate"),
    contract_variant: str | None = typer.Option(
        None,
        help="Optional contract variant to activate alongside the scenario",
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Switch the active exercise to another crisis."""

    try:
        payload = activate_exercise(
            root,
            scenario_variant=scenario_variant,
            contract_variant=contract_variant,
        )
    except (ValidationError, ValueError, FileNotFoundError, KeyError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(payload.model_dump(mode="json"), indent)
