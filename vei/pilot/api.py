from __future__ import annotations

import json
import os
import secrets
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from vei.context.api import (
    ContextProviderConfig,
    ContextSnapshot,
    ContextSourceResult,
)
from vei.whatif_filenames import CONTEXT_SNAPSHOT_FILE
from vei.governor import default_governor_workspace_config, governor_metadata_payload
from vei.orchestrators.api import (
    OrchestratorAgent,
    OrchestratorCommandResult,
    OrchestratorConfig,
    OrchestratorSnapshot,
    OrchestratorSyncHealth,
    build_orchestrator_client,
    external_approval_id_for,
    external_agent_id_for,
    external_task_id_for,
)
from vei.twin.api import build_customer_twin, load_customer_twin
from vei.twin.models import (
    ContextMoldConfig,
    CustomerTwinBundle,
    ExternalAgentIdentity,
    TwinActivityItem,
    TwinArchetype,
    TwinLaunchManifest,
    TwinLaunchRuntime,
    TwinLaunchSnippet,
    TwinLaunchStatus,
    TwinOutcomeSummary,
    TwinGatewayConfig,
    TwinServiceRecord,
)
from vei.workforce.api import build_workforce_state, workforce_command_from_result
from vei.workforce.models import WorkforceCommandRecord
from vei.workspace.api import (
    load_workspace,
    preview_workspace_scenario,
    write_workspace,
)

TWIN_LAUNCH_MANIFEST_FILE = "twin_launch_manifest.json"
TWIN_LAUNCH_GUIDE_FILE = "twin_launch_guide.md"
TWIN_LAUNCH_RUNTIME_FILE = "twin_launch_runtime.json"
TWIN_ORCHESTRATOR_CACHE_FILE = "twin_orchestrator_snapshot.json"
TWIN_ORCHESTRATOR_SYNC_FILE = "twin_orchestrator_sync.json"

# Legacy on-disk filenames for migration cleanup. These are only used by
# _remove_legacy_artifact to delete old-named files during twin start/sync.
# Remove after 2026-09-01 along with all _remove_legacy_artifact calls.
_LEGACY_PILOT_MANIFEST_FILE = "pilot_manifest.json"
_LEGACY_PILOT_GUIDE_FILE = "pilot_guide.md"
_LEGACY_PILOT_RUNTIME_FILE = "pilot_runtime.json"
_LEGACY_PILOT_ORCHESTRATOR_CACHE_FILE = "pilot_orchestrator_snapshot.json"
_LEGACY_PILOT_ORCHESTRATOR_SYNC_FILE = "pilot_orchestrator_sync.json"


def start_pilot(
    root: str | Path,
    *,
    snapshot: ContextSnapshot | None = None,
    provider_configs: list[ContextProviderConfig] | None = None,
    organization_name: str | None = None,
    organization_domain: str = "",
    archetype: TwinArchetype = "b2b_saas",
    scenario_variant: str | None = None,
    contract_variant: str | None = None,
    connector_mode: str = "sim",
    governor_demo: bool = False,
    governor_demo_interval_ms: int = 1500,
    gateway_token: str | None = None,
    host: str = "127.0.0.1",
    gateway_port: int = 3020,
    studio_port: int = 3011,
    rebuild: bool = False,
    orchestrator: str | None = None,
    orchestrator_url: str | None = None,
    orchestrator_company_id: str | None = None,
    orchestrator_api_key_env: str | None = None,
) -> TwinLaunchStatus:
    workspace_root = Path(root).expanduser().resolve()
    existing_manifest = _safe_load_manifest(workspace_root)
    resolved_orchestrator_config = _resolve_orchestrator_config(
        existing_config=(
            None if existing_manifest is None else existing_manifest.orchestrator
        ),
        provider=orchestrator,
        base_url=orchestrator_url,
        company_id=orchestrator_company_id,
        api_key_env=orchestrator_api_key_env,
    )
    if rebuild:
        _clear_stale_twin_listener(
            workspace_root,
            port=gateway_port,
            command_fragment=" twin serve ",
        )
        _clear_stale_twin_listener(
            workspace_root,
            port=studio_port,
            command_fragment=" ui serve ",
        )
    existing_runtime = _safe_load_runtime(workspace_root)
    if rebuild and existing_runtime is not None:
        stop_pilot(workspace_root)
        existing_runtime = _safe_load_runtime(workspace_root)

    bundle = _ensure_twin_bundle(
        workspace_root,
        snapshot=snapshot,
        provider_configs=provider_configs,
        organization_name=organization_name,
        organization_domain=organization_domain,
        archetype=archetype,
        scenario_variant=scenario_variant,
        contract_variant=contract_variant,
        connector_mode=connector_mode,
        governor_demo=governor_demo,
        governor_demo_interval_ms=governor_demo_interval_ms,
        gateway_token=gateway_token,
        rebuild=rebuild,
    )

    if existing_runtime is not None and _services_ready(existing_runtime):
        _persist_twin_launch_manifest(
            bundle=bundle,
            runtime=existing_runtime,
            orchestrator_config=resolved_orchestrator_config,
        )
        return build_pilot_status(
            workspace_root,
            force_orchestrator_sync=resolved_orchestrator_config is not None,
        )

    if existing_runtime is not None and any(
        service.pid is not None for service in existing_runtime.services
    ):
        stop_pilot(workspace_root)

    twin_dir = _twin_dir(workspace_root)
    twin_dir.mkdir(parents=True, exist_ok=True)

    gateway_url = f"http://{host}:{gateway_port}"
    studio_url = f"http://{host}:{studio_port}"

    gateway_log = twin_dir / "gateway.log"
    studio_log = twin_dir / "studio.log"

    gateway_pid = _spawn_service(
        [
            sys.executable,
            "-m",
            "vei.cli.vei",
            "twin",
            "serve",
            "--root",
            str(workspace_root),
            "--host",
            host,
            "--port",
            str(gateway_port),
        ],
        log_path=gateway_log,
    )
    try:
        _wait_for_ready(f"{gateway_url}/healthz")
        studio_pid = _spawn_service(
            [
                sys.executable,
                "-m",
                "vei.cli.vei",
                "ui",
                "serve",
                "--root",
                str(workspace_root),
                "--host",
                host,
                "--port",
                str(studio_port),
            ],
            log_path=studio_log,
        )
        try:
            _wait_for_ready(f"{studio_url}/api/workspace")
        except Exception:
            _stop_pid(studio_pid)
            raise
    except Exception:
        _stop_pid(gateway_pid)
        raise

    runtime = TwinLaunchRuntime(
        workspace_root=workspace_root,
        started_at=_iso_now(),
        updated_at=_iso_now(),
        services=[
            TwinServiceRecord(
                name="gateway",
                host=host,
                port=gateway_port,
                url=gateway_url,
                pid=gateway_pid,
                state="running",
                log_path=str(gateway_log),
            ),
            TwinServiceRecord(
                name="studio",
                host=host,
                port=studio_port,
                url=studio_url,
                pid=studio_pid,
                state="running",
                log_path=str(studio_log),
            ),
        ],
    )
    _write_json(
        workspace_root / TWIN_LAUNCH_RUNTIME_FILE,
        runtime.model_dump(mode="json"),
    )
    _remove_legacy_artifact(workspace_root, _LEGACY_PILOT_RUNTIME_FILE)

    manifest = _persist_twin_launch_manifest(
        bundle,
        runtime=runtime,
        orchestrator_config=resolved_orchestrator_config,
    )
    return build_pilot_status(
        workspace_root,
        force_orchestrator_sync=manifest.orchestrator is not None,
    )


def stop_pilot(root: str | Path) -> TwinLaunchStatus:
    workspace_root = Path(root).expanduser().resolve()
    runtime = _safe_load_runtime(workspace_root)
    manifest = load_pilot_manifest(workspace_root)
    if runtime is None:
        runtime = TwinLaunchRuntime(
            workspace_root=workspace_root,
            started_at="",
            updated_at=_iso_now(),
            services=[
                TwinServiceRecord(
                    name="gateway",
                    host=_host_from_url(manifest.gateway_url),
                    port=_port_from_url(manifest.gateway_url),
                    url=manifest.gateway_url,
                    state="stopped",
                ),
                TwinServiceRecord(
                    name="studio",
                    host=_host_from_url(manifest.studio_url),
                    port=_port_from_url(manifest.studio_url),
                    url=manifest.studio_url,
                    state="stopped",
                ),
            ],
        )
    for service in runtime.services:
        if service.pid is not None:
            _stop_pid(service.pid)
        service.pid = None
        service.state = "stopped"
    runtime.updated_at = _iso_now()
    _write_json(
        workspace_root / TWIN_LAUNCH_RUNTIME_FILE,
        runtime.model_dump(mode="json"),
    )
    _remove_legacy_artifact(workspace_root, _LEGACY_PILOT_RUNTIME_FILE)
    return build_pilot_status(workspace_root)


def load_pilot_manifest(root: str | Path) -> TwinLaunchManifest:
    workspace_root = Path(root).expanduser().resolve()
    return TwinLaunchManifest.model_validate_json(
        _artifact_path(
            workspace_root,
            TWIN_LAUNCH_MANIFEST_FILE,
            _LEGACY_PILOT_MANIFEST_FILE,
        ).read_text(encoding="utf-8")
    )


def load_pilot_runtime(root: str | Path) -> TwinLaunchRuntime:
    workspace_root = Path(root).expanduser().resolve()
    return TwinLaunchRuntime.model_validate_json(
        _artifact_path(
            workspace_root,
            TWIN_LAUNCH_RUNTIME_FILE,
            _LEGACY_PILOT_RUNTIME_FILE,
        ).read_text(encoding="utf-8")
    )


def build_pilot_status(
    root: str | Path,
    *,
    force_orchestrator_sync: bool = False,
) -> TwinLaunchStatus:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_pilot_manifest(workspace_root)
    runtime = _safe_load_runtime(workspace_root) or TwinLaunchRuntime(
        workspace_root=workspace_root,
        started_at="",
        updated_at="",
        services=[
            TwinServiceRecord(
                name="gateway",
                host=_host_from_url(manifest.gateway_url),
                port=_port_from_url(manifest.gateway_url),
                url=manifest.gateway_url,
                state="stopped",
            ),
            TwinServiceRecord(
                name="studio",
                host=_host_from_url(manifest.studio_url),
                port=_port_from_url(manifest.studio_url),
                url=manifest.studio_url,
                state="stopped",
            ),
        ],
    )
    for service in runtime.services:
        service.state = "running" if _service_alive(service) else "stopped"

    gateway_raw = _fetch_json(f"{manifest.gateway_url}/api/twin")
    history_raw = _fetch_json(f"{manifest.gateway_url}/api/twin/history")
    surfaces_raw = _fetch_json(f"{manifest.gateway_url}/api/twin/surfaces")

    gateway_payload = gateway_raw if isinstance(gateway_raw, dict) else None
    history_payload = history_raw if isinstance(history_raw, list) else []
    surfaces_payload = surfaces_raw if isinstance(surfaces_raw, dict) else {}

    twin_runtime = gateway_payload.get("runtime", {}) if gateway_payload else {}
    services_ready = _services_ready(runtime)
    orchestrator_snapshot, orchestrator_sync = _build_orchestrator_status(
        workspace_root,
        manifest=manifest,
        services_ready=services_ready,
        force_sync=force_orchestrator_sync,
    )
    _sync_workforce_gateway(
        manifest,
        orchestrator_snapshot=orchestrator_snapshot,
        orchestrator_sync=orchestrator_sync,
        enabled=services_ready,
    )
    workforce_commands = _fetch_workforce_commands(manifest)
    twin_activity = _build_activity(history_payload)
    if orchestrator_snapshot is not None:
        twin_activity = _attach_task_refs_to_activity(
            twin_activity,
            orchestrator_snapshot=orchestrator_snapshot,
        )
    activity = _merge_activity(
        twin_activity,
        _build_orchestrator_activity(orchestrator_snapshot),
        _build_workforce_command_activity(workforce_commands),
    )
    outcome = _build_outcome(
        gateway_payload,
        surfaces_payload,
        activity,
        orchestrator_snapshot=orchestrator_snapshot,
        workforce_commands=workforce_commands,
    )
    active_agents = _parse_active_agents(gateway_payload)
    return TwinLaunchStatus(
        manifest=manifest,
        runtime=runtime,
        active_run=twin_runtime.get("run_id"),
        twin_status=twin_runtime.get("status", "stopped"),
        request_count=int(twin_runtime.get("request_count", 0) or 0),
        services_ready=services_ready,
        active_agents=active_agents,
        activity=activity,
        outcome=outcome,
        orchestrator=orchestrator_snapshot,
        orchestrator_sync=orchestrator_sync,
    )


def reset_pilot_gateway(root: str | Path) -> TwinLaunchStatus:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_pilot_manifest(workspace_root)
    runtime = load_pilot_runtime(workspace_root)
    gateway = _service_by_name(runtime, "gateway")
    if gateway.pid is not None:
        _stop_pid(gateway.pid)
    log_path = (
        Path(gateway.log_path)
        if gateway.log_path
        else _twin_dir(workspace_root) / "gateway.log"
    )
    gateway.pid = _spawn_service(
        [
            sys.executable,
            "-m",
            "vei.cli.vei",
            "twin",
            "serve",
            "--root",
            str(workspace_root),
            "--host",
            gateway.host,
            "--port",
            str(gateway.port),
        ],
        log_path=log_path,
    )
    gateway.state = "running"
    runtime.updated_at = _iso_now()
    _write_json(
        workspace_root / TWIN_LAUNCH_RUNTIME_FILE,
        runtime.model_dump(mode="json"),
    )
    _remove_legacy_artifact(workspace_root, _LEGACY_PILOT_RUNTIME_FILE)
    _wait_for_ready(f"{manifest.gateway_url}/healthz")
    return build_pilot_status(
        workspace_root,
        force_orchestrator_sync=manifest.orchestrator is not None,
    )


def finalize_pilot_run(root: str | Path) -> TwinLaunchStatus:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_pilot_manifest(workspace_root)
    payload = _post_json(f"{manifest.gateway_url}/api/twin/finalize", payload={})
    if payload is None:
        raise RuntimeError("twin gateway is not reachable right now")
    return build_pilot_status(workspace_root)


def sync_pilot_orchestrator(root: str | Path) -> TwinLaunchStatus:
    workspace_root = Path(root).expanduser().resolve()
    return build_pilot_status(workspace_root, force_orchestrator_sync=True)


def pause_pilot_orchestrator_agent(root: str | Path, agent_id: str) -> TwinLaunchStatus:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_pilot_manifest(workspace_root)
    config = manifest.orchestrator
    if config is None:
        raise RuntimeError("twin orchestrator is not configured")
    client = build_orchestrator_client(config)
    result = client.pause_agent(
        _resolve_orchestrator_external_agent_id(workspace_root, agent_id)
    )
    _record_workforce_command(manifest, result=result)
    return build_pilot_status(workspace_root, force_orchestrator_sync=True)


def resume_pilot_orchestrator_agent(
    root: str | Path, agent_id: str
) -> TwinLaunchStatus:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_pilot_manifest(workspace_root)
    config = manifest.orchestrator
    if config is None:
        raise RuntimeError("twin orchestrator is not configured")
    client = build_orchestrator_client(config)
    result = client.resume_agent(
        _resolve_orchestrator_external_agent_id(workspace_root, agent_id)
    )
    _record_workforce_command(manifest, result=result)
    return build_pilot_status(workspace_root, force_orchestrator_sync=True)


def comment_on_pilot_orchestrator_task(
    root: str | Path,
    task_id: str,
    *,
    body: str,
) -> TwinLaunchStatus:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_pilot_manifest(workspace_root)
    config = manifest.orchestrator
    if config is None:
        raise RuntimeError("twin orchestrator is not configured")
    comment_body = body.strip()
    if not comment_body:
        raise RuntimeError("task guidance cannot be empty")
    client = build_orchestrator_client(config)
    result = client.comment_on_task(
        _resolve_orchestrator_external_task_id(workspace_root, task_id),
        comment_body,
    )
    _record_workforce_command(
        manifest,
        result=result,
        decision_note=comment_body,
    )
    return build_pilot_status(workspace_root, force_orchestrator_sync=True)


def approve_pilot_orchestrator_approval(
    root: str | Path,
    approval_id: str,
    *,
    decision_note: str | None = None,
) -> TwinLaunchStatus:
    workspace_root = Path(root).expanduser().resolve()
    return _act_on_pilot_orchestrator_approval(
        workspace_root,
        approval_id,
        action="approve",
        decision_note=decision_note,
    )


def reject_pilot_orchestrator_approval(
    root: str | Path,
    approval_id: str,
    *,
    decision_note: str | None = None,
) -> TwinLaunchStatus:
    workspace_root = Path(root).expanduser().resolve()
    return _act_on_pilot_orchestrator_approval(
        workspace_root,
        approval_id,
        action="reject",
        decision_note=decision_note,
    )


def request_revision_pilot_orchestrator_approval(
    root: str | Path,
    approval_id: str,
    *,
    decision_note: str | None = None,
) -> TwinLaunchStatus:
    workspace_root = Path(root).expanduser().resolve()
    return _act_on_pilot_orchestrator_approval(
        workspace_root,
        approval_id,
        action="request_revision",
        decision_note=decision_note,
    )


def activate_exercise(
    root: str | Path,
    *,
    scenario_variant: str,
    contract_variant: str | None = None,
):
    from .exercise import activate_exercise as _activate_exercise

    return _activate_exercise(
        root,
        scenario_variant=scenario_variant,
        contract_variant=contract_variant,
    )


def build_exercise_status(root: str | Path):
    from .exercise import build_exercise_status as _build_exercise_status

    return _build_exercise_status(root)


def _ensure_twin_bundle(
    workspace_root: Path,
    *,
    snapshot: ContextSnapshot | None,
    provider_configs: list[ContextProviderConfig] | None,
    organization_name: str | None,
    organization_domain: str,
    archetype: TwinArchetype,
    scenario_variant: str | None,
    contract_variant: str | None,
    connector_mode: str,
    governor_demo: bool,
    governor_demo_interval_ms: int,
    gateway_token: str | None,
    rebuild: bool,
) -> CustomerTwinBundle:
    manifest_path = workspace_root / "twin_manifest.json"
    if (
        manifest_path.exists()
        and not rebuild
        and snapshot is None
        and provider_configs is None
    ):
        return load_customer_twin(workspace_root)

    if snapshot is not None:
        resolved_name = organization_name or snapshot.organization_name
        resolved_domain = organization_domain or snapshot.organization_domain
    else:
        resolved_name = organization_name or "Pinnacle Analytics"
        resolved_domain = organization_domain
    resolved_snapshot = snapshot
    if resolved_snapshot is None and provider_configs is None:
        existing_workspace = _safe_load_workspace(workspace_root)
        if existing_workspace is not None:
            return _build_existing_workspace_twin_bundle(
                workspace_root,
                workspace=existing_workspace,
                archetype=archetype,
                scenario_variant=scenario_variant,
                contract_variant=contract_variant,
                connector_mode=connector_mode,
                governor_demo=governor_demo,
                governor_demo_interval_ms=governor_demo_interval_ms,
                gateway_token=gateway_token,
            )
        resolved_domain = resolved_domain or _default_domain(resolved_name)
        resolved_snapshot = _default_twin_snapshot(
            organization_name=resolved_name,
            organization_domain=resolved_domain,
        )

    mold = ContextMoldConfig(
        archetype=archetype,
        scenario_variant=scenario_variant,
        contract_variant=contract_variant,
    )
    return build_customer_twin(
        workspace_root,
        snapshot=resolved_snapshot,
        provider_configs=provider_configs,
        organization_name=resolved_name,
        organization_domain=resolved_domain,
        mold=mold,
        mirror_config=default_governor_workspace_config(
            connector_mode=connector_mode,
            demo_mode=governor_demo,
            autoplay=governor_demo,
            demo_interval_ms=governor_demo_interval_ms,
            hero_world=archetype,
        ),
        gateway_token=gateway_token,
        overwrite=True,
    )


def _build_existing_workspace_twin_bundle(
    workspace_root: Path,
    *,
    workspace=None,
    archetype: TwinArchetype,
    scenario_variant: str | None,
    contract_variant: str | None,
    connector_mode: str,
    governor_demo: bool,
    governor_demo_interval_ms: int,
    gateway_token: str | None,
) -> CustomerTwinBundle:
    workspace = workspace or load_workspace(workspace_root)
    preview = preview_workspace_scenario(workspace_root)
    resolved_name, resolved_domain = _resolve_workspace_identity(
        workspace_root,
        workspace=workspace,
        preview=preview,
    )
    resolved_archetype = str(workspace.source_ref or "").strip() or str(archetype)
    governor_config = default_governor_workspace_config(
        connector_mode=connector_mode,
        demo_mode=governor_demo,
        autoplay=governor_demo,
        demo_interval_ms=governor_demo_interval_ms,
        hero_world=resolved_archetype,
    )
    workspace.metadata = {
        **dict(workspace.metadata),
        "governor": governor_metadata_payload(governor_config),
    }
    write_workspace(workspace_root, workspace)

    context_snapshot_path = workspace_root / CONTEXT_SNAPSHOT_FILE
    bundle = CustomerTwinBundle(
        workspace_root=workspace_root,
        workspace_name=workspace.name,
        organization_name=resolved_name,
        organization_domain=resolved_domain,
        mold=ContextMoldConfig(
            archetype=resolved_archetype,  # type: ignore[arg-type]
            scenario_variant=scenario_variant,
            contract_variant=contract_variant,
        ),
        context_snapshot_path=(
            str(context_snapshot_path.relative_to(workspace_root))
            if context_snapshot_path.exists()
            else ""
        ),
        blueprint_asset_path=workspace.blueprint_asset_path,
        gateway=TwinGatewayConfig(
            auth_token=gateway_token or secrets.token_urlsafe(18),
            surfaces=[
                {"name": "slack", "title": "Slack", "base_path": "/slack/api"},
                {"name": "jira", "title": "Jira", "base_path": "/jira/rest/api/3"},
                {
                    "name": "graph",
                    "title": "Microsoft Graph",
                    "base_path": "/graph/v1.0",
                },
                {
                    "name": "salesforce",
                    "title": "Salesforce",
                    "base_path": "/salesforce/services/data/v60.0",
                },
            ],
            ui_command=(
                "python -m vei.cli.vei ui serve "
                f"--root {workspace_root} --host 127.0.0.1 --port 3011"
            ),
        ),
        summary=(
            f"{resolved_name} is ready as a governed twin "
            "workspace with compatibility routes for enterprise connectors."
        ),
        metadata={
            "preview": preview,
            "governor": governor_metadata_payload(governor_config),
        },
    )
    _write_json(workspace_root / "twin_manifest.json", bundle.model_dump(mode="json"))
    return bundle


def _safe_load_workspace(workspace_root: Path):
    try:
        return load_workspace(workspace_root)
    except FileNotFoundError:
        return None


def _resolve_workspace_identity(
    workspace_root: Path,
    *,
    workspace,
    preview: dict[str, Any],
) -> tuple[str, str]:
    workspace_name = str(workspace.title or workspace.name or "").strip()
    compiled = (
        preview.get("compiled_blueprint", {})
        if isinstance(preview.get("compiled_blueprint"), dict)
        else {}
    )
    compiled_name = str(compiled.get("title") or "").strip()
    if compiled_name and compiled_name != workspace_name:
        raise ValueError(
            "workspace identity mismatch: workspace title does not match compiled blueprint title"
        )

    customer_metadata = (
        workspace.metadata.get("customer_twin", {})
        if isinstance(workspace.metadata, dict)
        else {}
    )
    metadata_name = str(customer_metadata.get("organization_name") or "").strip()
    metadata_domain = str(customer_metadata.get("organization_domain") or "").strip()
    if metadata_name and metadata_name != workspace_name:
        raise ValueError(
            "workspace identity mismatch: workspace title does not match saved twin metadata"
        )

    resolved_name = metadata_name or workspace_name
    resolved_domain = metadata_domain

    snapshot_path = workspace_root / CONTEXT_SNAPSHOT_FILE
    if snapshot_path.exists():
        snapshot = ContextSnapshot.model_validate_json(
            snapshot_path.read_text(encoding="utf-8")
        )
        snapshot_name = snapshot.organization_name.strip()
        snapshot_domain = snapshot.organization_domain.strip()
        if snapshot_name and snapshot_name != resolved_name:
            raise ValueError(
                "workspace identity mismatch: workspace title does not match context snapshot organization"
            )
        if snapshot_domain:
            if resolved_domain and snapshot_domain != resolved_domain:
                raise ValueError(
                    "workspace identity mismatch: saved twin metadata does not match context snapshot domain"
                )
            resolved_domain = snapshot_domain

    if not resolved_name:
        raise ValueError("workspace identity is missing a company name")
    if not resolved_domain:
        resolved_domain = _default_domain(resolved_name)
    return resolved_name, resolved_domain


def _build_manifest(
    bundle: CustomerTwinBundle,
    *,
    studio_url: str,
    control_room_url: str,
    gateway_url: str,
    orchestrator_config: OrchestratorConfig | None,
) -> TwinLaunchManifest:
    preview = (
        bundle.metadata.get("preview", {}) if isinstance(bundle.metadata, dict) else {}
    )
    crisis_name = _resolve_crisis_name(preview)
    sample_client_path = str(
        (_repo_root() / "examples" / "governor_client.py").resolve()
    )
    manifest = TwinLaunchManifest(
        workspace_root=bundle.workspace_root,
        workspace_name=bundle.workspace_name,
        organization_name=bundle.organization_name,
        organization_domain=bundle.organization_domain,
        archetype=bundle.mold.archetype,
        crisis_name=str(crisis_name),
        studio_url=studio_url,
        control_room_url=control_room_url,
        gateway_url=gateway_url,
        gateway_status_url=f"{gateway_url}/api/twin",
        bearer_token=bundle.gateway.auth_token,
        supported_surfaces=bundle.gateway.surfaces,
        recommended_first_move=(
            "Connect a lightweight external agent, read Slack + Jira first, then "
            "inspect mail and CRM before taking one customer-safe action."
        ),
        sample_client_path=sample_client_path,
        orchestrator=orchestrator_config,
    )
    manifest.snippets = _build_snippets(manifest)
    return manifest


def _persist_twin_launch_manifest(
    bundle: CustomerTwinBundle,
    *,
    runtime: TwinLaunchRuntime,
    orchestrator_config: OrchestratorConfig | None,
) -> TwinLaunchManifest:
    gateway_url = _service_by_name(runtime, "gateway").url
    studio_url = _service_by_name(runtime, "studio").url
    manifest = _build_manifest(
        bundle,
        studio_url=studio_url,
        control_room_url=studio_url,
        gateway_url=gateway_url,
        orchestrator_config=orchestrator_config,
    )
    _write_json(
        bundle.workspace_root / TWIN_LAUNCH_MANIFEST_FILE,
        manifest.model_dump(mode="json"),
    )
    _remove_legacy_artifact(bundle.workspace_root, _LEGACY_PILOT_MANIFEST_FILE)
    (bundle.workspace_root / TWIN_LAUNCH_GUIDE_FILE).write_text(
        _render_twin_launch_guide(manifest),
        encoding="utf-8",
    )
    _remove_legacy_artifact(bundle.workspace_root, _LEGACY_PILOT_GUIDE_FILE)
    return manifest


def _build_snippets(manifest: TwinLaunchManifest) -> list[TwinLaunchSnippet]:
    env_block = (
        f'export VEI_TWIN_BASE_URL="{manifest.gateway_url}"\n'
        f'export VEI_TWIN_TOKEN="{manifest.bearer_token}"\n'
        'export VEI_AGENT_ID="starter-agent"\n'
        'export VEI_AGENT_NAME="starter-agent"\n'
        'export VEI_AGENT_ROLE="external-agent"'
    )
    python_snippet = (
        "import json\n"
        "from urllib.request import Request, urlopen\n\n"
        f'BASE_URL = "{manifest.gateway_url}"\n'
        f'TOKEN = "{manifest.bearer_token}"\n\n'
        "register = Request(\n"
        '    f"{BASE_URL}/api/governor/agents",\n'
        "    data=json.dumps({\n"
        '        "agent_id": "starter-agent",\n'
        '        "name": "starter-agent",\n'
        '        "mode": "proxy",\n'
        "    }).encode(),\n"
        "    headers={\n"
        '        "Authorization": f"Bearer {TOKEN}",\n'
        '        "Content-Type": "application/json",\n'
        "    },\n"
        '    method="POST",\n'
        ")\n"
        "urlopen(register)\n\n"
        "req = Request(\n"
        '    f"{BASE_URL}/slack/api/conversations.list",\n'
        "    headers={\n"
        '        "Authorization": f"Bearer {TOKEN}",\n'
        '        "X-VEI-Agent-Id": "starter-agent",\n'
        '        "X-VEI-Agent-Name": "starter-agent",\n'
        '        "X-VEI-Agent-Role": "external-agent",\n'
        "    },\n"
        ")\n"
        "print(json.loads(urlopen(req).read()))\n"
    )
    register_curl = (
        f"curl -X POST -H 'Authorization: Bearer {manifest.bearer_token}' "
        "-H 'Content-Type: application/json' "
        f"'{manifest.gateway_url}/api/governor/agents' "
        '-d \'{"agent_id":"starter-agent","name":"starter-agent","mode":"proxy"}\''
    )
    slack_curl = (
        f"curl -H 'Authorization: Bearer {manifest.bearer_token}' "
        "-H 'X-VEI-Agent-Id: starter-agent' "
        "-H 'X-VEI-Agent-Name: starter-agent' "
        "-H 'X-VEI-Agent-Role: external-agent' "
        f"'{manifest.gateway_url}/slack/api/conversations.list'"
    )
    jira_curl = (
        f"curl -H 'Authorization: Bearer {manifest.bearer_token}' "
        "-H 'X-VEI-Agent-Id: starter-agent' "
        "-H 'X-VEI-Agent-Name: starter-agent' "
        "-H 'X-VEI-Agent-Role: external-agent' "
        f"'{manifest.gateway_url}/jira/rest/api/3/search'"
    )
    graph_curl = (
        f"curl -H 'Authorization: Bearer {manifest.bearer_token}' "
        "-H 'X-VEI-Agent-Id: starter-agent' "
        "-H 'X-VEI-Agent-Name: starter-agent' "
        "-H 'X-VEI-Agent-Role: external-agent' "
        f"'{manifest.gateway_url}/graph/v1.0/me/messages'"
    )
    salesforce_curl = (
        f"curl -H 'Authorization: Bearer {manifest.bearer_token}' "
        "-H 'X-VEI-Agent-Id: starter-agent' "
        "-H 'X-VEI-Agent-Name: starter-agent' "
        "-H 'X-VEI-Agent-Role: external-agent' "
        f"'{manifest.gateway_url}/salesforce/services/data/v60.0/query?q=SELECT+Name+FROM+Opportunity'"
    )
    return [
        TwinLaunchSnippet(
            name="env",
            title="Launch env",
            language="bash",
            content=env_block,
        ),
        TwinLaunchSnippet(
            name="python",
            title="Python base URL usage",
            language="python",
            content=python_snippet,
        ),
        TwinLaunchSnippet(
            name="register",
            title="Register proxy agent",
            language="bash",
            content=register_curl,
        ),
        TwinLaunchSnippet(
            name="slack",
            title="Slack-style request",
            language="bash",
            content=slack_curl,
        ),
        TwinLaunchSnippet(
            name="jira",
            title="Jira-style request",
            language="bash",
            content=jira_curl,
        ),
        TwinLaunchSnippet(
            name="graph",
            title="Graph-style request",
            language="bash",
            content=graph_curl,
        ),
        TwinLaunchSnippet(
            name="salesforce",
            title="Salesforce-style request",
            language="bash",
            content=salesforce_curl,
        ),
    ]


def _resolve_crisis_name(preview: Any) -> str:
    if not isinstance(preview, dict):
        return "Customer twin run"
    active_variant = preview.get("active_scenario_variant")
    variants = preview.get("available_scenario_variants")
    if isinstance(active_variant, str) and isinstance(variants, list):
        for variant in variants:
            if not isinstance(variant, dict):
                continue
            if variant.get("name") != active_variant:
                continue
            title = variant.get("title")
            if isinstance(title, str) and title.strip():
                return title
    for key in ("scenario_variant_title", "scenario_name", "scenario_variant", "title"):
        value = preview.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return "Customer twin run"


def _render_twin_launch_guide(manifest: TwinLaunchManifest) -> str:
    snippets = "\n\n".join(
        f"### {snippet.title}\n\n```{snippet.language}\n{snippet.content}\n```"
        for snippet in manifest.snippets
    )
    surface_lines = "\n".join(
        f"- `{item.title}` at `{item.base_path}`"
        for item in manifest.supported_surfaces
    )
    orchestrator_lines = ""
    if manifest.orchestrator is not None:
        orchestrator_lines = (
            "## Orchestrator bridge\n\n"
            f"- Provider: **{manifest.orchestrator.provider}**\n"
            f"- Base URL: `{manifest.orchestrator.base_url}`\n"
            f"- Company ID: `{manifest.orchestrator.company_id}`\n"
            f"- API key env: `{manifest.orchestrator.api_key_env}`\n\n"
        )
    return (
        f"# Twin Launch Guide — {manifest.organization_name}\n\n"
        f"## What this is\n\n"
        f"- Company: **{manifest.organization_name}**\n"
        f"- Archetype: **{manifest.archetype.replace('_', ' ')}**\n"
        f"- Current crisis: **{manifest.crisis_name}**\n"
        f"- Studio: `{manifest.studio_url}`\n"
        f"- Control room: `{manifest.control_room_url}`\n"
        f"- Gateway: `{manifest.gateway_url}`\n\n"
        f"## Supported surfaces\n\n"
        f"{surface_lines}\n\n"
        f"## Recommended first move\n\n"
        f"{manifest.recommended_first_move}\n\n"
        f"{orchestrator_lines}"
        f"## Sample client\n\n"
        f"`python {manifest.sample_client_path} --base-url {manifest.gateway_url} --token {manifest.bearer_token}`\n\n"
        f"## Connection snippets\n\n"
        f"{snippets}\n\n"
        "## Reset or finalize\n\n"
        "- Reset the twin to baseline: use `vei twin reset`, or use the reset control in Studio.\n"
        "- Finalize the current run: use `vei twin finalize`, or use the finalize control in Studio.\n"
    )


def _build_activity(history_payload: Any) -> list[TwinActivityItem]:
    if not isinstance(history_payload, list):
        return []
    items: list[TwinActivityItem] = []
    for raw in history_payload:
        if not isinstance(raw, dict):
            continue
        if raw.get("kind") != "workflow_step":
            continue
        payload = raw.get("payload", {}) if isinstance(raw.get("payload"), dict) else {}
        agent = payload.get("agent", {}) if isinstance(payload, dict) else {}
        items.append(
            TwinActivityItem(
                label=str(raw.get("label", "")),
                channel=str(raw.get("channel", "World")),
                tool=raw.get("resolved_tool") or raw.get("tool"),
                status=raw.get("status"),
                timestamp=(
                    str(raw.get("created_at"))
                    if raw.get("created_at") is not None
                    else None
                ),
                detail=(
                    str(payload.get("summary"))
                    if isinstance(payload, dict) and payload.get("summary")
                    else None
                ),
                source_label="VEI",
                agent_id=(
                    str(agent.get("agent_id"))
                    if isinstance(agent, dict) and agent.get("agent_id")
                    else None
                ),
                object_refs=[str(item) for item in raw.get("object_refs", [])],
                agent_name=(
                    str(agent.get("name"))
                    if isinstance(agent, dict) and agent.get("name")
                    else None
                ),
                agent_role=(
                    str(agent.get("role"))
                    if isinstance(agent, dict) and agent.get("role")
                    else None
                ),
                agent_team=(
                    str(agent.get("team"))
                    if isinstance(agent, dict) and agent.get("team")
                    else None
                ),
                agent_source=(
                    str(agent.get("source"))
                    if isinstance(agent, dict) and agent.get("source")
                    else None
                ),
            )
        )
    return items[-6:][::-1]


def _build_orchestrator_activity(
    orchestrator_snapshot: OrchestratorSnapshot | None,
) -> list[TwinActivityItem]:
    if orchestrator_snapshot is None:
        return []
    return [
        TwinActivityItem(
            label=item.label,
            channel="Orchestrator",
            tool=item.action,
            status=item.status,
            timestamp=item.created_at,
            detail=item.detail,
            source_label=item.provider,
            agent_id=item.agent_id,
            object_refs=list(item.object_refs),
            agent_name=item.agent_name,
            agent_source=item.provider,
        )
        for item in orchestrator_snapshot.recent_activity[:8]
    ]


def _build_workforce_command_activity(
    workforce_commands: list[WorkforceCommandRecord],
) -> list[TwinActivityItem]:
    if not workforce_commands:
        return []
    items: list[TwinActivityItem] = []
    for command in workforce_commands[-8:]:
        object_refs = _workforce_command_refs(command)
        items.append(
            TwinActivityItem(
                label=_workforce_command_label(command.action),
                channel="VEI",
                tool=_workforce_command_tool(command),
                status="issued",
                timestamp=command.created_at or None,
                detail=_workforce_command_detail(command),
                source_label="VEI",
                agent_id=command.agent_id,
                object_refs=object_refs,
                agent_name="VEI operator",
                agent_source="VEI",
            )
        )
    return list(reversed(items))


def _attach_task_refs_to_activity(
    activity: list[TwinActivityItem],
    *,
    orchestrator_snapshot: OrchestratorSnapshot,
) -> list[TwinActivityItem]:
    task_refs_by_agent: dict[str, list[str]] = {}
    for task in orchestrator_snapshot.tasks:
        if not task.assignee_agent_id:
            continue
        refs = task_refs_by_agent.setdefault(task.assignee_agent_id, [])
        refs.append(task.task_id)
        if task.project_name:
            refs.append(f"project:{task.project_name}")
        if task.goal_name:
            refs.append(f"goal:{task.goal_name}")
    enriched: list[TwinActivityItem] = []
    for item in activity:
        refs = list(item.object_refs)
        for ref in task_refs_by_agent.get(item.agent_id or "", []):
            if ref not in refs:
                refs.append(ref)
        enriched.append(item.model_copy(update={"object_refs": refs}, deep=True))
    return enriched


def _merge_activity(
    twin_activity: list[TwinActivityItem],
    orchestrator_activity: list[TwinActivityItem],
    workforce_command_activity: list[TwinActivityItem],
) -> list[TwinActivityItem]:
    combined = [
        *workforce_command_activity,
        *orchestrator_activity,
        *twin_activity,
    ]
    combined.sort(
        key=lambda item: item.timestamp or "",
        reverse=True,
    )
    return combined[:12]


def _workforce_command_label(action: str) -> str:
    labels = {
        "sync": "Synced workforce",
        "pause": "Paused agent",
        "resume": "Resumed agent",
        "comment_task": "Guided task",
        "approve": "Approved request",
        "reject": "Rejected request",
        "request_revision": "Requested changes",
    }
    return labels.get(action, action.replace("_", " ").title())


def _workforce_command_tool(command: WorkforceCommandRecord) -> str | None:
    action = command.action
    if action == "comment_task":
        return f"{command.provider}.comment_on_task"
    if action in {"approve", "reject", "request_revision"}:
        return f"{command.provider}.manage_approval"
    if action in {"pause", "resume"}:
        return f"{command.provider}.{action}_agent"
    if action == "sync":
        return f"{command.provider}.sync"
    return command.provider


def _workforce_command_detail(command: WorkforceCommandRecord) -> str | None:
    detail_parts = [
        str(item).strip()
        for item in (command.message, command.decision_note)
        if str(item or "").strip()
    ]
    if not detail_parts:
        return None
    if len(detail_parts) == 1:
        return detail_parts[0]
    return " ".join(detail_parts)


def _workforce_command_refs(command: WorkforceCommandRecord) -> list[str]:
    refs: list[str] = []
    for value in (
        command.task_id,
        command.external_task_id,
        command.approval_id,
        command.external_approval_id,
        command.agent_id,
        command.external_agent_id,
        command.comment_id,
    ):
        ref = str(value or "").strip()
        if not ref or ref in refs:
            continue
        refs.append(ref)
    return refs


def _parse_active_agents(
    gateway_payload: dict[str, Any] | None,
) -> list[ExternalAgentIdentity]:
    if not isinstance(gateway_payload, dict):
        return []
    runtime = gateway_payload.get("runtime", {})
    if not isinstance(runtime, dict):
        return []
    metadata = runtime.get("metadata", {})
    if not isinstance(metadata, dict):
        return []
    raw_agents = metadata.get("agents", [])
    if not isinstance(raw_agents, list):
        return []
    agents: list[ExternalAgentIdentity] = []
    for item in raw_agents:
        if not isinstance(item, dict):
            continue
        agents.append(ExternalAgentIdentity.model_validate(item))
    return agents


def _build_outcome(
    gateway_payload: dict[str, Any] | None,
    surfaces_payload: dict[str, Any] | None,
    activity: list[TwinActivityItem],
    *,
    orchestrator_snapshot: OrchestratorSnapshot | None = None,
    workforce_commands: list[Any] | None = None,
) -> TwinOutcomeSummary:
    if gateway_payload is None:
        return TwinOutcomeSummary(
            status="stopped",
            summary="Twin services are not fully up yet.",
        )
    runtime = (
        gateway_payload.get("runtime", {}) if isinstance(gateway_payload, dict) else {}
    )
    manifest = (
        gateway_payload.get("manifest", {}) if isinstance(gateway_payload, dict) else {}
    )
    contract = manifest.get("contract", {}) if isinstance(manifest, dict) else {}
    contract_ok = contract.get("ok")
    issue_count = int(contract.get("issue_count", 0) or 0)
    current_tension = ""
    affected_surfaces: list[str] = []
    if isinstance(surfaces_payload, dict):
        current_tension = str(surfaces_payload.get("current_tension", ""))
        for panel in surfaces_payload.get("panels", []) or []:
            if isinstance(panel, dict) and panel.get("status") not in {None, "ok"}:
                affected_surfaces.append(
                    str(panel.get("title") or panel.get("surface"))
                )
    latest_tool = activity[0].tool if activity else None

    steering = {
        "comment_task",
        "approve",
        "reject",
        "request_revision",
        "pause",
        "resume",
    }
    cmds = workforce_commands or []
    vei_action_count = sum(1 for c in cmds if getattr(c, "action", None) in steering)
    downstream_response_count = 0
    if orchestrator_snapshot and vei_action_count:
        vei_times = sorted(
            c.created_at
            for c in cmds
            if getattr(c, "action", None) in steering and c.created_at
        )
        if vei_times:
            downstream_response_count = sum(
                1
                for a in (orchestrator_snapshot.recent_activity or [])
                if a.created_at and a.created_at > vei_times[0]
            )
    governance_active = vei_action_count > 0

    direction: Literal["improving", "stable", "declining", "unknown"]
    if runtime.get("status") == "completed" and contract_ok:
        direction = "improving"
        summary = "The current path is holding together and the company is in a healthier state."
    elif runtime.get("status") == "completed":
        direction = "declining"
        summary = "The run is complete, but the company still has unresolved risk."
    elif contract_ok and governance_active and downstream_response_count > 0:
        direction = "improving"
        summary = f"VEI steered {vei_action_count} action{'s' if vei_action_count != 1 else ''} and the outside team responded {downstream_response_count} time{'s' if downstream_response_count != 1 else ''}. The path is currently healthy."
    elif contract_ok:
        direction = "stable"
        summary = "The live path is currently healthy, but it should still be reviewed."
    elif issue_count and governance_active:
        direction = "declining"
        summary = f"VEI sent {vei_action_count} steering action{'s' if vei_action_count != 1 else ''} but {issue_count} issue{'s' if issue_count != 1 else ''} remain open."
    elif issue_count:
        direction = "declining"
        summary = "The live path is still under pressure and needs another action."
    else:
        direction = "unknown"
        summary = "The pilot is running and waiting for the next outside-agent action."
    return TwinOutcomeSummary(
        status=str(runtime.get("status", "stopped")),
        contract_ok=contract_ok if isinstance(contract_ok, bool) else None,
        issue_count=issue_count,
        summary=summary,
        latest_tool=latest_tool,
        current_tension=current_tension,
        affected_surfaces=affected_surfaces[:4],
        vei_action_count=vei_action_count,
        downstream_response_count=downstream_response_count,
        governance_active=governance_active,
        direction=direction,
    )


def _build_orchestrator_status(
    workspace_root: Path,
    *,
    manifest: TwinLaunchManifest,
    services_ready: bool,
    force_sync: bool,
) -> tuple[OrchestratorSnapshot | None, OrchestratorSyncHealth | None]:
    config = manifest.orchestrator
    if config is None:
        return None, OrchestratorSyncHealth(status="disabled")

    cached_snapshot = _load_orchestrator_snapshot_cache(workspace_root)
    cached_health = _load_orchestrator_sync_cache(workspace_root)
    if (
        cached_snapshot is not None
        and _orchestrator_snapshot_matches_config(cached_snapshot, config)
        and not force_sync
    ):
        return cached_snapshot, _cached_orchestrator_health(
            config=config,
            cached_snapshot=cached_snapshot,
            cached_health=cached_health,
        )

    now = _iso_now()
    health = (
        cached_health.model_copy(deep=True)
        if cached_health is not None
        else OrchestratorSyncHealth(provider=config.provider)
    )
    health.provider = config.provider
    health.last_attempt_at = now

    client = build_orchestrator_client(config)
    try:
        snapshot = client.fetch_snapshot()
        synced_agent_count = 0
        message = "Orchestrator snapshot refreshed."
        if services_ready or force_sync:
            synced_agent_count = _sync_orchestrator_agents_to_mirror(
                manifest=manifest,
                snapshot=snapshot,
                previous_snapshot=cached_snapshot,
            )
            message = f"Orchestrator snapshot refreshed and synced {synced_agent_count} routeable agents."
        else:
            message = "Orchestrator snapshot refreshed. Governor registration will resume once the twin services are live."
        health = OrchestratorSyncHealth(
            provider=config.provider,
            status="healthy",
            last_attempt_at=now,
            last_success_at=now,
            cache_used=False,
            synced_agent_count=synced_agent_count,
            message=message,
        )
        _write_json(
            workspace_root / TWIN_ORCHESTRATOR_CACHE_FILE,
            snapshot.model_dump(mode="json"),
        )
        _remove_legacy_artifact(workspace_root, _LEGACY_PILOT_ORCHESTRATOR_CACHE_FILE)
        _write_json(
            workspace_root / TWIN_ORCHESTRATOR_SYNC_FILE,
            health.model_dump(mode="json"),
        )
        _remove_legacy_artifact(workspace_root, _LEGACY_PILOT_ORCHESTRATOR_SYNC_FILE)
        return snapshot, health
    except Exception as exc:  # noqa: BLE001
        if cached_snapshot is not None:
            health.status = "stale"
            health.cache_used = True
            health.last_error = str(exc)
            if not health.message:
                health.message = "Using the last cached orchestrator snapshot."
            _write_json(
                workspace_root / TWIN_ORCHESTRATOR_SYNC_FILE,
                health.model_dump(mode="json"),
            )
            _remove_legacy_artifact(
                workspace_root,
                _LEGACY_PILOT_ORCHESTRATOR_SYNC_FILE,
            )
            return cached_snapshot, health
        health.status = "error"
        health.cache_used = False
        health.last_error = str(exc)
        health.message = "The orchestrator could not be reached."
        _write_json(
            workspace_root / TWIN_ORCHESTRATOR_SYNC_FILE,
            health.model_dump(mode="json"),
        )
        _remove_legacy_artifact(workspace_root, _LEGACY_PILOT_ORCHESTRATOR_SYNC_FILE)
        return None, health


def _sync_orchestrator_agents_to_mirror(
    *,
    manifest: TwinLaunchManifest,
    snapshot: OrchestratorSnapshot,
    previous_snapshot: OrchestratorSnapshot | None = None,
) -> int:
    supported_surfaces = [item.name for item in manifest.supported_surfaces]
    auth_headers = {"Authorization": f"Bearer {manifest.bearer_token}"}
    desired_agent_payloads: dict[str, dict[str, Any]] = {}
    for agent in snapshot.agents:
        if agent.integration_mode == "observe":
            continue
        allowed_surfaces = [
            surface
            for surface in (agent.allowed_surfaces or supported_surfaces)
            if surface in supported_surfaces
        ]
        desired_agent_payloads[agent.agent_id] = {
            "agent_id": agent.agent_id,
            "name": agent.name,
            "mode": agent.integration_mode,
            "role": agent.title or agent.role,
            "team": agent.team,
            "allowed_surfaces": allowed_surfaces,
            "policy_profile_id": agent.policy_profile_id
            or _default_policy_profile_for_orchestrator_agent(agent),
            "status": _mirror_status_for_orchestrator_agent(agent.status),
            "source": agent.provider,
            "metadata": {
                "provider": agent.provider,
                "external_agent_id": agent.external_agent_id,
                "integration_mode": agent.integration_mode,
                "managed_by": "pilot_orchestrator_bridge",
                "task_ids": list(agent.task_ids),
            },
        }

    current_mirror_agents = _load_mirror_agents(
        manifest,
        headers=auth_headers,
    )
    current_mirror_agent_ids = {
        str(item.get("agent_id") or "").strip()
        for item in current_mirror_agents
        if str(item.get("agent_id") or "").strip()
    }
    synced_agent_count = 0
    for agent_id, payload in desired_agent_payloads.items():
        response = _request_json(
            f"{manifest.gateway_url}/api/governor/agents",
            method="POST",
            payload=payload,
            headers=auth_headers,
            timeout_s=4.0,
        )
        if response is None:
            raise RuntimeError(
                f"failed to sync orchestrator agent into governor: {agent_id}"
            )
        synced_agent_count += 1

    stale_agent_ids = sorted(
        (
            _managed_orchestrator_mirror_agent_ids(
                snapshot=snapshot,
                previous_snapshot=previous_snapshot,
                current_mirror_agents=current_mirror_agents,
            )
            & current_mirror_agent_ids
        )
        - set(desired_agent_payloads)
    )
    for agent_id in stale_agent_ids:
        response = _request_json(
            f"{manifest.gateway_url}/api/governor/agents/{quote(agent_id, safe='')}",
            method="DELETE",
            headers=auth_headers,
            timeout_s=4.0,
        )
        if response is None:
            raise RuntimeError(
                f"failed to remove stale orchestrator agent from governor: {agent_id}"
            )
    return synced_agent_count


def _resolve_orchestrator_config(
    *,
    existing_config: OrchestratorConfig | None = None,
    provider: str | None,
    base_url: str | None,
    company_id: str | None,
    api_key_env: str | None,
) -> OrchestratorConfig | None:
    has_override = any(
        str(value or "").strip()
        for value in (provider, base_url, company_id, api_key_env)
    )
    if not has_override:
        return (
            None if existing_config is None else existing_config.model_copy(deep=True)
        )

    provider_value = (
        str(provider or "").strip().lower()
        or str(existing_config.provider if existing_config is not None else "")
        .strip()
        .lower()
    )
    if not provider_value:
        return None
    if provider_value != "paperclip":
        raise ValueError(f"unsupported orchestrator provider: {provider}")
    resolved_base_url = str(
        base_url or (existing_config.base_url if existing_config is not None else "")
    ).strip()
    if not resolved_base_url:
        raise ValueError("orchestrator_url is required when orchestrator is set")
    resolved_company_id = str(
        company_id
        or (existing_config.company_id if existing_config is not None else "")
    ).strip()
    if not resolved_company_id:
        raise ValueError("orchestrator_company_id is required when orchestrator is set")
    return OrchestratorConfig(
        provider="paperclip",
        base_url=resolved_base_url,
        company_id=resolved_company_id,
        api_key_env=str(
            api_key_env
            or (
                existing_config.api_key_env
                if existing_config is not None
                else "PAPERCLIP_API_KEY"
            )
        ).strip()
        or "PAPERCLIP_API_KEY",
    )


def _resolve_orchestrator_external_agent_id(
    workspace_root: Path,
    agent_id: str,
) -> str:
    snapshot = _load_orchestrator_snapshot_cache(workspace_root)
    if snapshot is not None:
        for item in snapshot.agents:
            if item.agent_id == agent_id or item.external_agent_id == agent_id:
                return item.external_agent_id
    return external_agent_id_for(agent_id)


def _resolve_orchestrator_external_task_id(
    workspace_root: Path,
    task_id: str,
) -> str:
    snapshot = _load_orchestrator_snapshot_cache(workspace_root)
    if snapshot is not None:
        for item in snapshot.tasks:
            if item.task_id == task_id or item.external_task_id == task_id:
                return item.external_task_id
    return external_task_id_for(task_id)


def _resolve_orchestrator_external_approval_id(
    workspace_root: Path,
    approval_id: str,
) -> str:
    snapshot = _load_orchestrator_snapshot_cache(workspace_root)
    if snapshot is not None:
        for item in snapshot.approvals:
            if (
                item.approval_id == approval_id
                or item.external_approval_id == approval_id
            ):
                return item.external_approval_id
    return external_approval_id_for(approval_id)


def _act_on_pilot_orchestrator_approval(
    workspace_root: Path,
    approval_id: str,
    *,
    action: str,
    decision_note: str | None,
) -> TwinLaunchStatus:
    manifest = load_pilot_manifest(workspace_root)
    config = manifest.orchestrator
    if config is None:
        raise RuntimeError("twin orchestrator is not configured")
    client = build_orchestrator_client(config)
    external_approval_id = _resolve_orchestrator_external_approval_id(
        workspace_root,
        approval_id,
    )
    if action == "approve":
        result = client.approve_approval(
            external_approval_id,
            decision_note=decision_note,
        )
    elif action == "reject":
        result = client.reject_approval(
            external_approval_id,
            decision_note=decision_note,
        )
    elif action == "request_revision":
        result = client.request_approval_revision(
            external_approval_id,
            decision_note=decision_note,
        )
    else:
        raise RuntimeError(f"unsupported approval action: {action}")
    _record_workforce_command(
        manifest,
        result=result,
        decision_note=decision_note,
    )
    return build_pilot_status(workspace_root, force_orchestrator_sync=True)


def _sync_workforce_gateway(
    manifest: TwinLaunchManifest,
    *,
    orchestrator_snapshot: OrchestratorSnapshot | None,
    orchestrator_sync: OrchestratorSyncHealth | None,
    enabled: bool,
) -> None:
    if not enabled:
        return
    if orchestrator_snapshot is None and orchestrator_sync is None:
        return
    payload = build_workforce_state(
        snapshot=orchestrator_snapshot,
        sync=orchestrator_sync,
    ).model_dump(mode="json")
    _post_json(
        f"{manifest.gateway_url}/api/workforce/sync",
        payload=payload,
        headers={"Authorization": f"Bearer {manifest.bearer_token}"},
    )


def _fetch_workforce_commands(
    manifest: TwinLaunchManifest,
) -> list[WorkforceCommandRecord]:
    raw = _request_json(
        f"{manifest.gateway_url}/api/workforce",
        headers={"Authorization": f"Bearer {manifest.bearer_token}"},
        timeout_s=2.0,
    )
    if raw is None:
        raw = _fetch_json(f"{manifest.gateway_url}/api/workforce")
    if not isinstance(raw, dict):
        return []
    items = raw.get("commands") or []
    if not isinstance(items, list):
        return []
    result_list: list[WorkforceCommandRecord] = []
    for entry in items:
        if isinstance(entry, dict):
            try:
                result_list.append(WorkforceCommandRecord.model_validate(entry))
            except Exception:
                continue
    return result_list


def _record_workforce_command(
    manifest: TwinLaunchManifest,
    *,
    result: OrchestratorCommandResult,
    decision_note: str | None = None,
) -> None:
    command = workforce_command_from_result(
        result,
        decision_note=decision_note,
    )
    response = _post_json(
        f"{manifest.gateway_url}/api/workforce/commands",
        payload=command.model_dump(mode="json"),
        headers={"Authorization": f"Bearer {manifest.bearer_token}"},
    )
    if response is None:
        raise RuntimeError(
            "The orchestrator action succeeded, but VEI could not record it in the control room."
        )


def _default_policy_profile_for_orchestrator_agent(agent: OrchestratorAgent) -> str:
    text = " ".join(
        part for part in (agent.role, agent.title, agent.name) if part
    ).lower()
    if any(
        token in text
        for token in ("approver", "manager", "lead", "director", "chief", "head")
    ):
        return "approver"
    if text:
        return "operator"
    return "observer"


def _mirror_status_for_orchestrator_agent(status: str | None) -> str:
    normalized = str(status or "").strip().lower()
    if normalized in {"running", "active"}:
        return "active"
    if normalized in {"paused", "idle", "waiting"}:
        return "idle"
    if normalized in {"error", "failed"}:
        return "error"
    return "registered"


def _load_orchestrator_snapshot_cache(
    workspace_root: Path,
) -> OrchestratorSnapshot | None:
    path = _artifact_path(
        workspace_root,
        TWIN_ORCHESTRATOR_CACHE_FILE,
        _LEGACY_PILOT_ORCHESTRATOR_CACHE_FILE,
    )
    if not path.exists():
        return None
    return OrchestratorSnapshot.model_validate_json(path.read_text(encoding="utf-8"))


def _load_orchestrator_sync_cache(
    workspace_root: Path,
) -> OrchestratorSyncHealth | None:
    path = _artifact_path(
        workspace_root,
        TWIN_ORCHESTRATOR_SYNC_FILE,
        _LEGACY_PILOT_ORCHESTRATOR_SYNC_FILE,
    )
    if not path.exists():
        return None
    return OrchestratorSyncHealth.model_validate_json(path.read_text(encoding="utf-8"))


def _orchestrator_snapshot_matches_config(
    snapshot: OrchestratorSnapshot,
    config: OrchestratorConfig,
) -> bool:
    return (
        snapshot.provider == config.provider
        and snapshot.company_id == config.company_id
    )


def _cached_orchestrator_health(
    *,
    config: OrchestratorConfig,
    cached_snapshot: OrchestratorSnapshot,
    cached_health: OrchestratorSyncHealth | None,
) -> OrchestratorSyncHealth:
    if cached_health is not None:
        health = cached_health.model_copy(deep=True)
        health.provider = config.provider
        if not health.last_success_at:
            health.last_success_at = cached_snapshot.fetched_at
        return health
    return OrchestratorSyncHealth(
        provider=config.provider,
        status="healthy",
        last_success_at=cached_snapshot.fetched_at,
        message="Using the most recent orchestrator snapshot.",
    )


def _load_mirror_agents(
    manifest: TwinLaunchManifest,
    *,
    headers: dict[str, str],
) -> list[dict[str, Any]]:
    payload = _request_json(
        f"{manifest.gateway_url}/api/governor/agents",
        headers=headers,
        timeout_s=4.0,
    )
    if not isinstance(payload, dict):
        raise RuntimeError("failed to load current governor agents")
    agents = payload.get("agents") or []
    if not isinstance(agents, list):
        return []
    return [item for item in agents if isinstance(item, dict)]


def _managed_orchestrator_mirror_agent_ids(
    *,
    snapshot: OrchestratorSnapshot,
    previous_snapshot: OrchestratorSnapshot | None,
    current_mirror_agents: list[dict[str, Any]],
) -> set[str]:
    managed_agent_ids: set[str] = set()
    for source_snapshot in (previous_snapshot, snapshot):
        if source_snapshot is None:
            continue
        managed_agent_ids.update(
            agent.agent_id
            for agent in source_snapshot.agents
            if agent.integration_mode != "observe"
        )
    for payload in current_mirror_agents:
        metadata = payload.get("metadata")
        agent_id = str(payload.get("agent_id") or "").strip()
        if not agent_id or not isinstance(metadata, dict):
            continue
        if metadata.get("managed_by") == "pilot_orchestrator_bridge":
            managed_agent_ids.add(agent_id)
    return managed_agent_ids


def _default_twin_snapshot(
    *,
    organization_name: str,
    organization_domain: str,
) -> ContextSnapshot:
    fixture_path = Path(__file__).parent / "fixtures" / "default_snapshot.json"
    raw = fixture_path.read_text(encoding="utf-8").replace(
        "{{DOMAIN}}", organization_domain
    )
    payload = json.loads(raw)
    now = _iso_now()
    sources = [
        ContextSourceResult(
            provider=src["provider"],
            captured_at=now,
            status=src.get("status", "ok"),
            record_counts=src.get("record_counts", {}),
            data=src.get("data", {}),
        )
        for src in payload["sources"]
    ]
    return ContextSnapshot(
        organization_name=organization_name,
        organization_domain=organization_domain,
        captured_at=now,
        sources=sources,
    )


def _spawn_service(command: list[str], *, log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handle = log_path.open("a", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=_repo_root(),
        stdout=handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        close_fds=True,
    )
    handle.close()
    return process.pid


def _wait_for_ready(url: str, *, timeout_s: float = 20.0) -> None:
    deadline = time.time() + timeout_s
    last_error: str | None = None
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=2.0) as response:  # nosec B310
                if 200 <= response.status < 500:
                    return
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            last_error = str(exc)
            time.sleep(0.25)
    raise RuntimeError(
        f"service did not become ready: {url} ({last_error or 'unknown'})"
    )


def _fetch_json(url: str) -> dict[str, Any] | list[Any] | None:
    return _request_json(url, timeout_s=2.0)


def _request_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout_s: float = 4.0,
) -> dict[str, Any] | list[Any] | None:
    body = None
    merged_headers = dict(headers or {})
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        merged_headers["Content-Type"] = "application/json"
    request = Request(url, data=body, headers=merged_headers, method=method)
    try:
        with urlopen(request, timeout=timeout_s) as response:  # nosec B310
            raw = response.read().decode("utf-8")
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError):
        return None
    if not raw.strip():
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _post_json(
    url: str,
    *,
    payload: dict[str, Any],
    headers: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    result = _request_json(
        url,
        method="POST",
        payload=payload,
        headers=headers,
        timeout_s=4.0,
    )
    if isinstance(result, list):
        return None
    return result


def _service_alive(service: TwinServiceRecord) -> bool:
    if service.pid is None:
        return False
    try:
        os.kill(service.pid, 0)
    except OSError:
        return False
    return True


def _services_ready(runtime: TwinLaunchRuntime) -> bool:
    services = {service.name: service for service in runtime.services}
    gateway = services.get("gateway")
    studio = services.get("studio")
    if gateway is None or studio is None:
        return False
    if not _service_alive(gateway) or not _service_alive(studio):
        return False
    return (
        _fetch_json(f"{gateway.url}/healthz") is not None
        and _fetch_json(f"{studio.url}/api/workspace") is not None
    )


def _service_by_name(runtime: TwinLaunchRuntime, name: str) -> TwinServiceRecord:
    for service in runtime.services:
        if service.name == name:
            return service
    raise KeyError(f"unknown twin service: {name}")


def _stop_pid(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGTERM)
    except OSError:
        return
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except OSError:
            return
        time.sleep(0.1)
    try:
        os.killpg(pid, signal.SIGKILL)
    except OSError:
        return


def _clear_stale_twin_listener(
    workspace_root: Path,
    *,
    port: int,
    command_fragment: str,
) -> None:
    listener_pids = _listening_pids(port)
    if not listener_pids:
        return

    root_marker = f"--root {workspace_root}"
    for pid in listener_pids:
        command = _command_for_pid(pid)
        if (
            "vei.cli.vei" in command
            and command_fragment in command
            and root_marker in command
        ):
            _stop_pid(pid)
            continue
        raise RuntimeError(
            f"port {port} is already in use by another process: {command or pid}"
        )

    remaining = _listening_pids(port)
    if remaining:
        raise RuntimeError(
            f"port {port} is still busy after stopping stale twin services"
        )


def _listening_pids(port: int) -> list[int]:
    try:
        result = subprocess.run(
            ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return []

    if result.returncode not in {0, 1}:
        return []

    pids: list[int] = []
    for line in result.stdout.splitlines():
        value = line.strip()
        if not value:
            continue
        try:
            pids.append(int(value))
        except ValueError:
            continue
    return pids


def _command_for_pid(pid: int) -> str:
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _safe_load_runtime(workspace_root: Path) -> TwinLaunchRuntime | None:
    path = _artifact_path(
        workspace_root,
        TWIN_LAUNCH_RUNTIME_FILE,
        _LEGACY_PILOT_RUNTIME_FILE,
    )
    if not path.exists():
        return None
    return TwinLaunchRuntime.model_validate_json(path.read_text(encoding="utf-8"))


def _safe_load_manifest(workspace_root: Path) -> TwinLaunchManifest | None:
    path = _artifact_path(
        workspace_root,
        TWIN_LAUNCH_MANIFEST_FILE,
        _LEGACY_PILOT_MANIFEST_FILE,
    )
    if not path.exists():
        return None
    return TwinLaunchManifest.model_validate_json(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _artifact_path(workspace_root: Path, preferred: str, legacy: str) -> Path:
    preferred_path = workspace_root / preferred
    if preferred_path.exists():
        return preferred_path
    legacy_path = workspace_root / legacy
    if legacy_path.exists():
        return legacy_path
    return preferred_path


def _remove_legacy_artifact(workspace_root: Path, legacy_name: str) -> None:
    legacy_path = workspace_root / legacy_name
    if legacy_path.exists():
        legacy_path.unlink()


def _twin_dir(workspace_root: Path) -> Path:
    return workspace_root / ".twin"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _default_domain(organization_name: str) -> str:
    slug = "".join(
        ch.lower() if ch.isalnum() else "-" for ch in organization_name.strip()
    ).strip("-")
    slug = "-".join(part for part in slug.split("-") if part)
    return f"{slug or 'company'}.example.com"


def _host_from_url(url: str) -> str:
    return url.split("://", 1)[-1].split(":", 1)[0]


def _port_from_url(url: str) -> int:
    return int(url.rsplit(":", 1)[-1])
