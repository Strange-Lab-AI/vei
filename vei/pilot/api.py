from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from vei.context.models import (
    ContextProviderConfig,
    ContextSnapshot,
    ContextSourceResult,
)
from vei.mirror import default_mirror_workspace_config
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
    TwinArchetype,
)
from vei.workforce.api import build_workforce_state, workforce_command_from_result
from vei.workforce.models import WorkforceCommandRecord

from .models import (
    PilotActivityItem,
    PilotManifest,
    PilotOutcomeSummary,
    PilotRuntime,
    PilotServiceRecord,
    PilotSnippet,
    PilotStatus,
)

PILOT_MANIFEST_FILE = "pilot_manifest.json"
PILOT_GUIDE_FILE = "pilot_guide.md"
PILOT_RUNTIME_FILE = "pilot_runtime.json"
PILOT_ORCHESTRATOR_CACHE_FILE = "pilot_orchestrator_snapshot.json"
PILOT_ORCHESTRATOR_SYNC_FILE = "pilot_orchestrator_sync.json"


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
    mirror_demo: bool = False,
    mirror_demo_interval_ms: int = 1500,
    gateway_token: str | None = None,
    host: str = "127.0.0.1",
    gateway_port: int = 3020,
    studio_port: int = 3011,
    rebuild: bool = False,
    orchestrator: str | None = None,
    orchestrator_url: str | None = None,
    orchestrator_company_id: str | None = None,
    orchestrator_api_key_env: str | None = None,
) -> PilotStatus:
    workspace_root = Path(root).expanduser().resolve()
    resolved_orchestrator_config = _resolve_orchestrator_config(
        provider=orchestrator,
        base_url=orchestrator_url,
        company_id=orchestrator_company_id,
        api_key_env=orchestrator_api_key_env,
    )
    if rebuild:
        _clear_stale_pilot_listener(
            workspace_root,
            port=gateway_port,
            command_fragment=" twin serve ",
        )
        _clear_stale_pilot_listener(
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
        mirror_demo=mirror_demo,
        mirror_demo_interval_ms=mirror_demo_interval_ms,
        gateway_token=gateway_token,
        rebuild=rebuild,
    )

    if existing_runtime is not None and _services_ready(existing_runtime):
        return build_pilot_status(workspace_root)

    if existing_runtime is not None and any(
        service.pid is not None for service in existing_runtime.services
    ):
        stop_pilot(workspace_root)

    pilot_dir = _pilot_dir(workspace_root)
    pilot_dir.mkdir(parents=True, exist_ok=True)

    gateway_url = f"http://{host}:{gateway_port}"
    studio_url = f"http://{host}:{studio_port}"
    pilot_console_url = f"{studio_url}/pilot"

    gateway_log = pilot_dir / "gateway.log"
    studio_log = pilot_dir / "studio.log"

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

    runtime = PilotRuntime(
        workspace_root=workspace_root,
        started_at=_iso_now(),
        updated_at=_iso_now(),
        services=[
            PilotServiceRecord(
                name="gateway",
                host=host,
                port=gateway_port,
                url=gateway_url,
                pid=gateway_pid,
                state="running",
                log_path=str(gateway_log),
            ),
            PilotServiceRecord(
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
    _write_json(workspace_root / PILOT_RUNTIME_FILE, runtime.model_dump(mode="json"))

    manifest = _build_manifest(
        bundle,
        studio_url=studio_url,
        pilot_console_url=pilot_console_url,
        gateway_url=gateway_url,
        orchestrator_config=resolved_orchestrator_config,
    )
    _write_json(workspace_root / PILOT_MANIFEST_FILE, manifest.model_dump(mode="json"))
    (workspace_root / PILOT_GUIDE_FILE).write_text(
        _render_pilot_guide(manifest),
        encoding="utf-8",
    )
    return build_pilot_status(workspace_root)


def stop_pilot(root: str | Path) -> PilotStatus:
    workspace_root = Path(root).expanduser().resolve()
    runtime = _safe_load_runtime(workspace_root)
    manifest = load_pilot_manifest(workspace_root)
    if runtime is None:
        runtime = PilotRuntime(
            workspace_root=workspace_root,
            started_at="",
            updated_at=_iso_now(),
            services=[
                PilotServiceRecord(
                    name="gateway",
                    host=_host_from_url(manifest.gateway_url),
                    port=_port_from_url(manifest.gateway_url),
                    url=manifest.gateway_url,
                    state="stopped",
                ),
                PilotServiceRecord(
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
    _write_json(workspace_root / PILOT_RUNTIME_FILE, runtime.model_dump(mode="json"))
    return build_pilot_status(workspace_root)


def load_pilot_manifest(root: str | Path) -> PilotManifest:
    workspace_root = Path(root).expanduser().resolve()
    return PilotManifest.model_validate_json(
        (workspace_root / PILOT_MANIFEST_FILE).read_text(encoding="utf-8")
    )


def load_pilot_runtime(root: str | Path) -> PilotRuntime:
    workspace_root = Path(root).expanduser().resolve()
    return PilotRuntime.model_validate_json(
        (workspace_root / PILOT_RUNTIME_FILE).read_text(encoding="utf-8")
    )


def build_pilot_status(
    root: str | Path,
    *,
    force_orchestrator_sync: bool = False,
) -> PilotStatus:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_pilot_manifest(workspace_root)
    runtime = _safe_load_runtime(workspace_root) or PilotRuntime(
        workspace_root=workspace_root,
        started_at="",
        updated_at="",
        services=[
            PilotServiceRecord(
                name="gateway",
                host=_host_from_url(manifest.gateway_url),
                port=_port_from_url(manifest.gateway_url),
                url=manifest.gateway_url,
                state="stopped",
            ),
            PilotServiceRecord(
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
    )
    outcome = _build_outcome(
        gateway_payload,
        surfaces_payload,
        activity,
        orchestrator_snapshot=orchestrator_snapshot,
        workforce_commands=workforce_commands,
    )
    active_agents = _parse_active_agents(gateway_payload)
    return PilotStatus(
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


def reset_pilot_gateway(root: str | Path) -> PilotStatus:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_pilot_manifest(workspace_root)
    runtime = load_pilot_runtime(workspace_root)
    gateway = _service_by_name(runtime, "gateway")
    if gateway.pid is not None:
        _stop_pid(gateway.pid)
    log_path = (
        Path(gateway.log_path)
        if gateway.log_path
        else _pilot_dir(workspace_root) / "gateway.log"
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
    _write_json(workspace_root / PILOT_RUNTIME_FILE, runtime.model_dump(mode="json"))
    _wait_for_ready(f"{manifest.gateway_url}/healthz")
    return build_pilot_status(workspace_root)


def finalize_pilot_run(root: str | Path) -> PilotStatus:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_pilot_manifest(workspace_root)
    payload = _post_json(f"{manifest.gateway_url}/api/twin/finalize", payload={})
    if payload is None:
        raise RuntimeError("pilot gateway is not reachable right now")
    return build_pilot_status(workspace_root)


def sync_pilot_orchestrator(root: str | Path) -> PilotStatus:
    workspace_root = Path(root).expanduser().resolve()
    return build_pilot_status(workspace_root, force_orchestrator_sync=True)


def pause_pilot_orchestrator_agent(root: str | Path, agent_id: str) -> PilotStatus:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_pilot_manifest(workspace_root)
    config = manifest.orchestrator
    if config is None:
        raise RuntimeError("pilot orchestrator is not configured")
    client = build_orchestrator_client(config)
    result = client.pause_agent(
        _resolve_orchestrator_external_agent_id(workspace_root, agent_id)
    )
    _record_workforce_command(manifest, result=result)
    return build_pilot_status(workspace_root, force_orchestrator_sync=True)


def resume_pilot_orchestrator_agent(root: str | Path, agent_id: str) -> PilotStatus:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_pilot_manifest(workspace_root)
    config = manifest.orchestrator
    if config is None:
        raise RuntimeError("pilot orchestrator is not configured")
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
) -> PilotStatus:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_pilot_manifest(workspace_root)
    config = manifest.orchestrator
    if config is None:
        raise RuntimeError("pilot orchestrator is not configured")
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
) -> PilotStatus:
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
) -> PilotStatus:
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
) -> PilotStatus:
    workspace_root = Path(root).expanduser().resolve()
    return _act_on_pilot_orchestrator_approval(
        workspace_root,
        approval_id,
        action="request_revision",
        decision_note=decision_note,
    )


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
    mirror_demo: bool,
    mirror_demo_interval_ms: int,
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
        resolved_domain = resolved_domain or _default_domain(resolved_name)
        resolved_snapshot = _default_pilot_snapshot(
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
        mirror_config=default_mirror_workspace_config(
            connector_mode=connector_mode,
            demo_mode=mirror_demo,
            autoplay=mirror_demo,
            demo_interval_ms=mirror_demo_interval_ms,
            hero_world=archetype,
        ),
        gateway_token=gateway_token,
        overwrite=True,
    )


def _build_manifest(
    bundle: CustomerTwinBundle,
    *,
    studio_url: str,
    pilot_console_url: str,
    gateway_url: str,
    orchestrator_config: OrchestratorConfig | None,
) -> PilotManifest:
    preview = (
        bundle.metadata.get("preview", {}) if isinstance(bundle.metadata, dict) else {}
    )
    crisis_name = _resolve_crisis_name(preview)
    sample_client_path = str((_repo_root() / "examples" / "pilot_client.py").resolve())
    manifest = PilotManifest(
        workspace_root=bundle.workspace_root,
        workspace_name=bundle.workspace_name,
        organization_name=bundle.organization_name,
        organization_domain=bundle.organization_domain,
        archetype=bundle.mold.archetype,
        crisis_name=str(crisis_name),
        studio_url=studio_url,
        pilot_console_url=pilot_console_url,
        gateway_url=gateway_url,
        gateway_status_url=f"{gateway_url}/api/twin",
        bearer_token=bundle.gateway.auth_token,
        supported_surfaces=bundle.gateway.surfaces,
        recommended_first_exercise=(
            "Connect a lightweight external agent, read Slack + Jira first, then "
            "inspect mail and CRM before taking one customer-safe action."
        ),
        sample_client_path=sample_client_path,
        orchestrator=orchestrator_config,
    )
    manifest.snippets = _build_snippets(manifest)
    return manifest


def _build_snippets(manifest: PilotManifest) -> list[PilotSnippet]:
    env_block = (
        f'export VEI_PILOT_BASE_URL="{manifest.gateway_url}"\n'
        f'export VEI_PILOT_TOKEN="{manifest.bearer_token}"\n'
        'export VEI_AGENT_ID="starter-agent"\n'
        'export VEI_AGENT_NAME="starter-agent"\n'
        'export VEI_AGENT_ROLE="exercise-runner"'
    )
    python_snippet = (
        "import json\n"
        "from urllib.request import Request, urlopen\n\n"
        f'BASE_URL = "{manifest.gateway_url}"\n'
        f'TOKEN = "{manifest.bearer_token}"\n\n'
        "register = Request(\n"
        '    f"{BASE_URL}/api/mirror/agents",\n'
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
        '        "X-VEI-Agent-Role": "exercise-runner",\n'
        "    },\n"
        ")\n"
        "print(json.loads(urlopen(req).read()))\n"
    )
    register_curl = (
        f"curl -X POST -H 'Authorization: Bearer {manifest.bearer_token}' "
        "-H 'Content-Type: application/json' "
        f"'{manifest.gateway_url}/api/mirror/agents' "
        '-d \'{"agent_id":"starter-agent","name":"starter-agent","mode":"proxy"}\''
    )
    slack_curl = (
        f"curl -H 'Authorization: Bearer {manifest.bearer_token}' "
        "-H 'X-VEI-Agent-Id: starter-agent' "
        "-H 'X-VEI-Agent-Name: starter-agent' "
        "-H 'X-VEI-Agent-Role: exercise-runner' "
        f"'{manifest.gateway_url}/slack/api/conversations.list'"
    )
    jira_curl = (
        f"curl -H 'Authorization: Bearer {manifest.bearer_token}' "
        "-H 'X-VEI-Agent-Id: starter-agent' "
        "-H 'X-VEI-Agent-Name: starter-agent' "
        "-H 'X-VEI-Agent-Role: exercise-runner' "
        f"'{manifest.gateway_url}/jira/rest/api/3/search'"
    )
    graph_curl = (
        f"curl -H 'Authorization: Bearer {manifest.bearer_token}' "
        "-H 'X-VEI-Agent-Id: starter-agent' "
        "-H 'X-VEI-Agent-Name: starter-agent' "
        "-H 'X-VEI-Agent-Role: exercise-runner' "
        f"'{manifest.gateway_url}/graph/v1.0/me/messages'"
    )
    salesforce_curl = (
        f"curl -H 'Authorization: Bearer {manifest.bearer_token}' "
        "-H 'X-VEI-Agent-Id: starter-agent' "
        "-H 'X-VEI-Agent-Name: starter-agent' "
        "-H 'X-VEI-Agent-Role: exercise-runner' "
        f"'{manifest.gateway_url}/salesforce/services/data/v60.0/query?q=SELECT+Name+FROM+Opportunity'"
    )
    return [
        PilotSnippet(
            name="env",
            title="Launch env",
            language="bash",
            content=env_block,
        ),
        PilotSnippet(
            name="python",
            title="Python base URL usage",
            language="python",
            content=python_snippet,
        ),
        PilotSnippet(
            name="register",
            title="Register proxy agent",
            language="bash",
            content=register_curl,
        ),
        PilotSnippet(
            name="slack",
            title="Slack-style request",
            language="bash",
            content=slack_curl,
        ),
        PilotSnippet(
            name="jira",
            title="Jira-style request",
            language="bash",
            content=jira_curl,
        ),
        PilotSnippet(
            name="graph",
            title="Graph-style request",
            language="bash",
            content=graph_curl,
        ),
        PilotSnippet(
            name="salesforce",
            title="Salesforce-style request",
            language="bash",
            content=salesforce_curl,
        ),
    ]


def _resolve_crisis_name(preview: Any) -> str:
    if not isinstance(preview, dict):
        return "Customer pilot exercise"
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
    return "Customer pilot exercise"


def _render_pilot_guide(manifest: PilotManifest) -> str:
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
        f"# Pilot Guide — {manifest.organization_name}\n\n"
        f"## What this is\n\n"
        f"- Company: **{manifest.organization_name}**\n"
        f"- Archetype: **{manifest.archetype.replace('_', ' ')}**\n"
        f"- Current crisis: **{manifest.crisis_name}**\n"
        f"- Studio: `{manifest.studio_url}`\n"
        f"- Operator Console: `{manifest.pilot_console_url}`\n"
        f"- Gateway: `{manifest.gateway_url}`\n\n"
        f"## Supported surfaces\n\n"
        f"{surface_lines}\n\n"
        f"## Recommended first exercise\n\n"
        f"{manifest.recommended_first_exercise}\n\n"
        f"{orchestrator_lines}"
        f"## Sample client\n\n"
        f"`python {manifest.sample_client_path} --base-url {manifest.gateway_url} --token {manifest.bearer_token}`\n\n"
        f"## Connection snippets\n\n"
        f"{snippets}\n\n"
        "## Reset or finalize\n\n"
        "- Reset the twin to baseline: use `vei pilot down` then `vei pilot up`, or the reset control in the Operator Console.\n"
        "- Finalize the current run: use the Operator Console finalize control or `POST /api/twin/finalize` on the gateway.\n"
    )


def _build_activity(history_payload: Any) -> list[PilotActivityItem]:
    if not isinstance(history_payload, list):
        return []
    items: list[PilotActivityItem] = []
    for raw in history_payload:
        if not isinstance(raw, dict):
            continue
        if raw.get("kind") != "workflow_step":
            continue
        payload = raw.get("payload", {}) if isinstance(raw.get("payload"), dict) else {}
        agent = payload.get("agent", {}) if isinstance(payload, dict) else {}
        items.append(
            PilotActivityItem(
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
) -> list[PilotActivityItem]:
    if orchestrator_snapshot is None:
        return []
    return [
        PilotActivityItem(
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


def _attach_task_refs_to_activity(
    activity: list[PilotActivityItem],
    *,
    orchestrator_snapshot: OrchestratorSnapshot,
) -> list[PilotActivityItem]:
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
    enriched: list[PilotActivityItem] = []
    for item in activity:
        refs = list(item.object_refs)
        for ref in task_refs_by_agent.get(item.agent_id or "", []):
            if ref not in refs:
                refs.append(ref)
        enriched.append(item.model_copy(update={"object_refs": refs}, deep=True))
    return enriched


def _merge_activity(
    twin_activity: list[PilotActivityItem],
    orchestrator_activity: list[PilotActivityItem],
) -> list[PilotActivityItem]:
    combined = [*orchestrator_activity, *twin_activity]
    combined.sort(
        key=lambda item: item.timestamp or "",
        reverse=True,
    )
    return combined[:12]


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
    activity: list[PilotActivityItem],
    *,
    orchestrator_snapshot: OrchestratorSnapshot | None = None,
    workforce_commands: list[Any] | None = None,
) -> PilotOutcomeSummary:
    if gateway_payload is None:
        return PilotOutcomeSummary(
            status="stopped",
            summary="Pilot services are not fully up yet.",
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

    steering = {"comment_task", "approve", "reject", "request_revision", "pause", "resume"}
    cmds = workforce_commands or []
    vei_action_count = sum(1 for c in cmds if getattr(c, "action", None) in steering)
    downstream_response_count = 0
    if orchestrator_snapshot and vei_action_count:
        vei_times = sorted(
            c.created_at for c in cmds if getattr(c, "action", None) in steering and c.created_at
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
    return PilotOutcomeSummary(
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
    manifest: PilotManifest,
    services_ready: bool,
    force_sync: bool,
) -> tuple[OrchestratorSnapshot | None, OrchestratorSyncHealth | None]:
    config = manifest.orchestrator
    if config is None:
        return None, OrchestratorSyncHealth(status="disabled")

    now = _iso_now()
    cached_snapshot = _load_orchestrator_snapshot_cache(workspace_root)
    cached_health = _load_orchestrator_sync_cache(workspace_root)
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
            )
            message = f"Orchestrator snapshot refreshed and synced {synced_agent_count} routeable agents."
        else:
            message = "Orchestrator snapshot refreshed. Mirror registration will resume once the pilot services are live."
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
            workspace_root / PILOT_ORCHESTRATOR_CACHE_FILE,
            snapshot.model_dump(mode="json"),
        )
        _write_json(
            workspace_root / PILOT_ORCHESTRATOR_SYNC_FILE,
            health.model_dump(mode="json"),
        )
        return snapshot, health
    except Exception as exc:  # noqa: BLE001
        if cached_snapshot is not None:
            health.status = "stale"
            health.cache_used = True
            health.last_error = str(exc)
            if not health.message:
                health.message = "Using the last cached orchestrator snapshot."
            _write_json(
                workspace_root / PILOT_ORCHESTRATOR_SYNC_FILE,
                health.model_dump(mode="json"),
            )
            return cached_snapshot, health
        health.status = "error"
        health.cache_used = False
        health.last_error = str(exc)
        health.message = "The orchestrator could not be reached."
        _write_json(
            workspace_root / PILOT_ORCHESTRATOR_SYNC_FILE,
            health.model_dump(mode="json"),
        )
        return None, health


def _sync_orchestrator_agents_to_mirror(
    *,
    manifest: PilotManifest,
    snapshot: OrchestratorSnapshot,
) -> int:
    supported_surfaces = [item.name for item in manifest.supported_surfaces]
    auth_headers = {"Authorization": f"Bearer {manifest.bearer_token}"}
    synced_agent_count = 0
    for agent in snapshot.agents:
        if agent.integration_mode == "observe":
            continue
        allowed_surfaces = [
            surface
            for surface in (agent.allowed_surfaces or supported_surfaces)
            if surface in supported_surfaces
        ]
        payload = {
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
                "task_ids": list(agent.task_ids),
            },
        }
        response = _request_json(
            f"{manifest.gateway_url}/api/mirror/agents",
            method="POST",
            payload=payload,
            headers=auth_headers,
            timeout_s=4.0,
        )
        if response is None:
            raise RuntimeError(
                f"failed to sync orchestrator agent into mirror: {agent.agent_id}"
            )
        synced_agent_count += 1
    return synced_agent_count


def _resolve_orchestrator_config(
    *,
    provider: str | None,
    base_url: str | None,
    company_id: str | None,
    api_key_env: str | None,
) -> OrchestratorConfig | None:
    provider_value = str(provider or "").strip().lower()
    if not provider_value:
        return None
    if provider_value != "paperclip":
        raise ValueError(f"unsupported orchestrator provider: {provider}")
    if not base_url:
        raise ValueError("orchestrator_url is required when orchestrator is set")
    if not company_id:
        raise ValueError("orchestrator_company_id is required when orchestrator is set")
    return OrchestratorConfig(
        provider="paperclip",
        base_url=str(base_url).strip(),
        company_id=str(company_id).strip(),
        api_key_env=str(api_key_env or "PAPERCLIP_API_KEY").strip()
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
) -> PilotStatus:
    manifest = load_pilot_manifest(workspace_root)
    config = manifest.orchestrator
    if config is None:
        raise RuntimeError("pilot orchestrator is not configured")
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
    manifest: PilotManifest,
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


def _fetch_workforce_commands(manifest: PilotManifest) -> list[WorkforceCommandRecord]:
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
    manifest: PilotManifest,
    *,
    result: OrchestratorCommandResult,
    decision_note: str | None = None,
) -> None:
    command = workforce_command_from_result(
        result,
        decision_note=decision_note,
    )
    _post_json(
        f"{manifest.gateway_url}/api/workforce/commands",
        payload=command.model_dump(mode="json"),
        headers={"Authorization": f"Bearer {manifest.bearer_token}"},
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
    path = workspace_root / PILOT_ORCHESTRATOR_CACHE_FILE
    if not path.exists():
        return None
    return OrchestratorSnapshot.model_validate_json(path.read_text(encoding="utf-8"))


def _load_orchestrator_sync_cache(
    workspace_root: Path,
) -> OrchestratorSyncHealth | None:
    path = workspace_root / PILOT_ORCHESTRATOR_SYNC_FILE
    if not path.exists():
        return None
    return OrchestratorSyncHealth.model_validate_json(path.read_text(encoding="utf-8"))


def _default_pilot_snapshot(
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


def _service_alive(service: PilotServiceRecord) -> bool:
    if service.pid is None:
        return False
    try:
        os.kill(service.pid, 0)
    except OSError:
        return False
    return True


def _services_ready(runtime: PilotRuntime) -> bool:
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


def _service_by_name(runtime: PilotRuntime, name: str) -> PilotServiceRecord:
    for service in runtime.services:
        if service.name == name:
            return service
    raise KeyError(f"unknown pilot service: {name}")


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


def _clear_stale_pilot_listener(
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
            f"port {port} is still busy after stopping stale pilot services"
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


def _safe_load_runtime(workspace_root: Path) -> PilotRuntime | None:
    path = workspace_root / PILOT_RUNTIME_FILE
    if not path.exists():
        return None
    return PilotRuntime.model_validate_json(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _pilot_dir(workspace_root: Path) -> Path:
    return workspace_root / ".pilot"


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
