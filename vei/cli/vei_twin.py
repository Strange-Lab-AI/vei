from __future__ import annotations

import http.client
import json
from pathlib import Path
from typing import Any

import typer
from pydantic import ValidationError

from vei.context.api import (
    ContextProviderConfig,
    ContextSnapshot,
    build_canonical_history_readiness,
)
from vei.governor import default_governor_workspace_config
from vei.twin import serve_customer_twin
from vei.twin.api import (
    build_customer_twin,
    build_twin_status,
    build_workspace_governor_status,
    finalize_twin,
    load_customer_twin,
    start_twin,
    stop_twin,
    reset_twin,
    sync_twin,
)
from vei.twin.api import ContextMoldConfig
from vei.twin.models import CustomerTwinBundle

app = typer.Typer(
    add_completion=False,
    help="Build, launch, and manage customer-shaped twin environments.",
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


def _default_token_env(provider: str) -> str:
    env_map = {
        "slack": "VEI_SLACK_TOKEN",
        "jira": "VEI_JIRA_TOKEN",
        "google": "VEI_GOOGLE_TOKEN",
        "okta": "VEI_OKTA_TOKEN",
        "gmail": "VEI_GMAIL_TOKEN",
        "teams": "VEI_TEAMS_TOKEN",
        "github": "VEI_GITHUB_TOKEN",
        "gitlab": "VEI_GITLAB_TOKEN",
        "clickup": "VEI_CLICKUP_TOKEN",
        "notion": "VEI_NOTION_TOKEN",
        "linear": "VEI_LINEAR_TOKEN",
        "granola": "VEI_GRANOLA_TOKEN",
    }
    return env_map.get(provider, f"VEI_{provider.upper()}_TOKEN")


def _default_base_url_env(provider: str) -> str | None:
    env_map = {
        "jira": "VEI_JIRA_URL",
        "okta": "VEI_OKTA_ORG_URL",
        "github": "VEI_GITHUB_URL",
        "gitlab": "VEI_GITLAB_URL",
        "clickup": "VEI_CLICKUP_URL",
    }
    return env_map.get(provider)


def _parse_provider_filters(entries: list[str]) -> dict[str, dict[str, Any]]:
    parsed: dict[str, dict[str, Any]] = {}
    for entry in entries:
        provider_name, separator, remainder = entry.partition(":")
        key, equals, raw_value = remainder.partition("=")
        provider = provider_name.strip().lower()
        filter_key = key.strip()
        value = raw_value.strip()
        if not provider or separator != ":" or equals != "=" or not filter_key:
            raise typer.BadParameter(
                "filters must use provider:key=value, for example github:repo=org/repo"
            )
        parsed.setdefault(provider, {})[filter_key] = value
    return parsed


def _parse_provider_base_urls(entries: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for entry in entries:
        provider_name, equals, raw_value = entry.partition("=")
        provider = provider_name.strip().lower()
        value = raw_value.strip()
        if not provider or equals != "=" or not value:
            raise typer.BadParameter("base URLs must use provider=https://host/path")
        parsed[provider] = value
    return parsed


def _build_provider_configs_from_flags(
    *,
    providers: list[str],
    filter_entries: list[str],
    base_url_entries: list[str],
) -> list[ContextProviderConfig]:
    import os

    parsed_filters = _parse_provider_filters(filter_entries)
    parsed_base_urls = _parse_provider_base_urls(base_url_entries)

    configs: list[ContextProviderConfig] = []
    for provider_name in providers:
        provider = provider_name.strip().lower()
        if not provider:
            continue
        base_url = parsed_base_urls.get(provider, "")
        env_name = _default_base_url_env(provider)
        if not base_url and env_name:
            base_url = os.environ.get(env_name, "")
        configs.append(
            ContextProviderConfig(
                provider=provider,  # type: ignore[arg-type]
                token_env=_default_token_env(provider),
                base_url=base_url or None,
                filters=parsed_filters.get(provider, {}),
            )
        )
    return configs


def _timeline_payload(root: Path, bundle: CustomerTwinBundle) -> dict[str, Any]:
    snapshot_path = root / bundle.context_snapshot_path
    readiness = build_canonical_history_readiness(snapshot_path)
    return {
        "snapshot_path": str(snapshot_path),
        "timeline_files": {
            "events": str(snapshot_path.parent / "canonical_events.jsonl"),
            "index": str(snapshot_path.parent / "canonical_event_index.json"),
        },
        "timeline_api_path": "/api/workspace/whatif/timeline?source=company_history",
        "readiness": readiness.model_dump(mode="json"),
    }


def _twin_status_payload(root: Path) -> dict[str, Any]:
    workspace_root = root.expanduser().resolve()
    bundle = load_customer_twin(workspace_root)
    live_governor = _gateway_json(bundle, "/api/governor")
    live_workforce = _gateway_json(bundle, "/api/workforce")
    workspace_status = build_workspace_governor_status(
        workspace_root,
        governor_payload=live_governor,
        workforce_payload=live_workforce,
    )

    service_status: dict[str, Any] = {}
    try:
        twin_status = build_twin_status(workspace_root)
    except FileNotFoundError:
        twin_status = None

    if twin_status is not None:
        service_status = {
            "studio_url": twin_status.manifest.studio_url,
            "gateway_url": twin_status.manifest.gateway_url,
            "gateway_status_url": twin_status.manifest.gateway_status_url,
            "bearer_token": twin_status.manifest.bearer_token,
            "services": twin_status.runtime.model_dump(mode="json").get("services", []),
        }

    return {
        "bundle": bundle.model_dump(mode="json"),
        "status": {
            **service_status,
            "active_run": workspace_status.active_run,
            "twin_status": workspace_status.twin_status,
            "request_count": workspace_status.request_count,
            "services_ready": workspace_status.services_ready,
            "active_agents": workspace_status.active_agents,
            "activity": workspace_status.activity,
            "outcome": workspace_status.outcome,
            "orchestrator": workspace_status.orchestrator,
            "orchestrator_sync": workspace_status.orchestrator_sync,
            "governor": workspace_status.governor,
            "workforce": workspace_status.workforce,
            "exercise": workspace_status.exercise,
        },
    }


def _gateway_json(bundle, path: str) -> dict[str, Any] | None:
    connection = http.client.HTTPConnection(
        bundle.gateway.host,
        bundle.gateway.port,
        timeout=2,
    )
    try:
        connection.request(
            "GET",
            path,
            headers={"Authorization": f"Bearer {bundle.gateway.auth_token}"},
        )
        response = connection.getresponse()
        raw = response.read().decode("utf-8")
        if not (200 <= response.status < 300) or not raw:
            return None
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None
    except (OSError, json.JSONDecodeError):
        return None
    finally:
        connection.close()


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
    connector_mode: str = typer.Option(
        "sim",
        help="Governor connector mode: sim | live",
    ),
    governor_demo: bool = typer.Option(
        False,
        help="Enable governor demo mode with staged agent activity.",
    ),
    governor_demo_interval_ms: int = typer.Option(
        1500,
        help="Autoplay interval for governor demo steps in milliseconds.",
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

    try:
        bundle = build_customer_twin(
            root,
            snapshot=_load_snapshot(snapshot),
            provider_configs=_load_provider_configs(provider_configs),
            organization_name=organization_name,
            organization_domain=organization_domain,
            mold=ContextMoldConfig(
                archetype=archetype,  # type: ignore[arg-type]
                scenario_variant=scenario_variant,
                contract_variant=contract_variant,
            ),
            mirror_config=default_governor_workspace_config(
                connector_mode=connector_mode,
                demo_mode=governor_demo,
                autoplay=governor_demo,
                demo_interval_ms=governor_demo_interval_ms,
                hero_world=archetype,
            ),
            gateway_token=gateway_token,
            overwrite=overwrite,
        )
    except (ValidationError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(bundle.model_dump(mode="json"), indent)


@app.command("onboard")
def onboard_command(
    root: Path = typer.Option(Path("."), help="Workspace root for the twin"),
    organization_name: str = typer.Option(..., "--org", help="Organization name"),
    organization_domain: str = typer.Option("", "--domain", help="Organization domain"),
    provider: list[str] = typer.Option(
        ...,
        "--provider",
        "-p",
        help="Context provider name to capture",
    ),
    provider_filter: list[str] = typer.Option(
        [],
        "--filter",
        help="Provider filter in provider:key=value form",
    ),
    provider_base_url: list[str] = typer.Option(
        [],
        "--base-url",
        help="Provider base URL in provider=https://host/path form",
    ),
    archetype: str = typer.Option("b2b_saas", help="World archetype to mold"),
    connector_mode: str = typer.Option("sim", help="Governor connector mode"),
    launch: bool = typer.Option(
        False,
        "--launch/--build-only",
        help="Start Studio and the governed twin after capture",
    ),
    host: str = typer.Option("127.0.0.1", help="Bind host when launching"),
    gateway_port: int = typer.Option(3020, help="Twin gateway port"),
    studio_port: int = typer.Option(3011, help="Studio UI port"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Capture live provider data, build a twin, and report timeline readiness."""

    configs = _build_provider_configs_from_flags(
        providers=provider,
        filter_entries=provider_filter,
        base_url_entries=provider_base_url,
    )
    if not configs:
        raise typer.BadParameter("at least one provider is required")

    workspace_root = root.expanduser().resolve()
    try:
        if launch:
            start_twin(
                workspace_root,
                provider_configs=configs,
                organization_name=organization_name,
                organization_domain=organization_domain,
                archetype=archetype,  # type: ignore[arg-type]
                connector_mode=connector_mode,
                host=host,
                gateway_port=gateway_port,
                studio_port=studio_port,
                rebuild=True,
            )
            bundle = load_customer_twin(workspace_root)
            payload = _twin_status_payload(workspace_root)
        else:
            bundle = build_customer_twin(
                workspace_root,
                provider_configs=configs,
                organization_name=organization_name,
                organization_domain=organization_domain,
                mold=ContextMoldConfig(archetype=archetype),  # type: ignore[arg-type]
            )
            payload = {"bundle": bundle.model_dump(mode="json")}
    except (ValidationError, ValueError, RuntimeError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    payload["timeline"] = _timeline_payload(workspace_root, bundle)
    payload["capture"] = {
        "providers": [
            {
                "provider": config.provider,
                "filters": dict(config.filters),
                "token_env": config.token_env,
                "base_url": config.base_url,
            }
            for config in configs
        ]
    }
    _emit(payload, indent)


@app.command("up")
def up_command(
    root: Path = typer.Option(Path("."), help="Twin workspace root"),
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
    organization_domain: str = typer.Option("", help="Organization domain override"),
    archetype: str = typer.Option("b2b_saas", help="World archetype to launch"),
    scenario_variant: str | None = typer.Option(
        None,
        help="Optional scenario variant to activate before launch",
    ),
    contract_variant: str | None = typer.Option(
        None,
        help="Optional contract variant to activate before launch",
    ),
    connector_mode: str = typer.Option(
        "sim",
        help="Governor connector mode: sim | live",
    ),
    governor_demo: bool = typer.Option(
        False,
        help="Enable governor demo mode with staged agent activity.",
    ),
    governor_demo_interval_ms: int = typer.Option(
        1500,
        help="Autoplay interval for governor demo steps in milliseconds.",
    ),
    gateway_token: str | None = typer.Option(
        None,
        help="Optional bearer token override for the compatibility gateway",
    ),
    host: str = typer.Option("127.0.0.1", help="Bind host"),
    gateway_port: int = typer.Option(3020, help="Twin gateway port"),
    studio_port: int = typer.Option(3011, help="Studio UI port"),
    rebuild: bool = typer.Option(False, help="Rebuild and restart the twin stack"),
    orchestrator: str | None = typer.Option(
        None,
        help="Optional workforce provider: paperclip",
    ),
    orchestrator_url: str | None = typer.Option(
        None,
        help="Optional workforce API base URL",
    ),
    orchestrator_company_id: str | None = typer.Option(
        None,
        help="Optional workforce company identifier",
    ),
    orchestrator_api_key_env: str | None = typer.Option(
        None,
        help="Environment variable holding the workforce API key",
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Launch Studio plus the governed twin runtime."""

    try:
        start_twin(
            root,
            snapshot=_load_snapshot(snapshot),
            provider_configs=_load_provider_configs(provider_configs),
            organization_name=organization_name,
            organization_domain=organization_domain,
            archetype=archetype,  # type: ignore[arg-type]
            scenario_variant=scenario_variant,
            contract_variant=contract_variant,
            connector_mode=connector_mode,
            governor_demo=governor_demo,
            governor_demo_interval_ms=governor_demo_interval_ms,
            gateway_token=gateway_token,
            host=host,
            gateway_port=gateway_port,
            studio_port=studio_port,
            rebuild=rebuild,
            orchestrator=orchestrator,
            orchestrator_url=orchestrator_url,
            orchestrator_company_id=orchestrator_company_id,
            orchestrator_api_key_env=orchestrator_api_key_env,
        )
    except (ValidationError, ValueError, RuntimeError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(_twin_status_payload(root), indent)


@app.command("status")
def status_command(
    root: Path = typer.Option(Path("."), help="Twin workspace root"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Show the saved twin bundle and current runtime status."""

    _emit(_twin_status_payload(root), indent)


@app.command("down")
def down_command(
    root: Path = typer.Option(Path("."), help="Twin workspace root"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Stop the governed twin runtime."""

    try:
        stop_twin(root)
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(_twin_status_payload(root), indent)


@app.command("reset")
def reset_command(
    root: Path = typer.Option(Path("."), help="Twin workspace root"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Reset the live gateway state without rebuilding the workspace."""

    try:
        reset_twin(root)
    except (FileNotFoundError, RuntimeError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(_twin_status_payload(root), indent)


@app.command("finalize")
def finalize_command(
    root: Path = typer.Option(Path("."), help="Twin workspace root"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Finalize the active governed run."""

    try:
        finalize_twin(root)
    except (FileNotFoundError, RuntimeError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(_twin_status_payload(root), indent)


@app.command("sync")
def sync_command(
    root: Path = typer.Option(Path("."), help="Twin workspace root"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Refresh workforce state from the configured orchestrator."""

    try:
        sync_twin(root)
    except (FileNotFoundError, RuntimeError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(_twin_status_payload(root), indent)


@app.command("serve")
def serve_command(
    root: Path = typer.Option(Path("."), help="Twin workspace root"),
    host: str = typer.Option("127.0.0.1", help="Bind host"),
    port: int = typer.Option(3020, help="Bind port"),
) -> None:
    """Serve the compatibility gateway for a built twin."""

    serve_customer_twin(root, host=host, port=port)
