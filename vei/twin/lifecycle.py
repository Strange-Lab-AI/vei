from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from vei.context.models import ContextProviderConfig, ContextSnapshot
from vei.pilot.api import (
    activate_exercise,
    approve_pilot_orchestrator_approval as approve_twin_launch_orchestrator_approval,
    build_exercise_status,
    build_pilot_status as build_twin_launch_status,
    comment_on_pilot_orchestrator_task as comment_on_twin_launch_orchestrator_task,
    finalize_pilot_run as finalize_twin_launch_run,
    pause_pilot_orchestrator_agent as pause_twin_launch_orchestrator_agent,
    reject_pilot_orchestrator_approval as reject_twin_launch_orchestrator_approval,
    request_revision_pilot_orchestrator_approval as request_twin_launch_orchestrator_revision,
    reset_pilot_gateway as reset_twin_gateway,
    resume_pilot_orchestrator_agent as resume_twin_launch_orchestrator_agent,
    start_pilot as start_twin_launch,
    stop_pilot as stop_twin_launch,
    sync_pilot_orchestrator as sync_twin_orchestrator,
)
from vei.run.api import list_run_manifests

from .models import TwinArchetype, WorkspaceGovernorStatus


def start_twin(
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
    ui_skin: str = "studio",
    rebuild: bool = False,
    orchestrator: str | None = None,
    orchestrator_url: str | None = None,
    orchestrator_company_id: str | None = None,
    orchestrator_api_key_env: str | None = None,
):
    return start_twin_launch(
        root,
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
        host=host,
        gateway_port=gateway_port,
        studio_port=studio_port,
        ui_skin=ui_skin,
        rebuild=rebuild,
        orchestrator=orchestrator,
        orchestrator_url=orchestrator_url,
        orchestrator_company_id=orchestrator_company_id,
        orchestrator_api_key_env=orchestrator_api_key_env,
    )


def build_twin_status(root: str | Path):
    return build_twin_launch_status(root)


def stop_twin(root: str | Path):
    return stop_twin_launch(root)


def reset_twin(root: str | Path):
    return reset_twin_gateway(root)


def finalize_twin(root: str | Path):
    return finalize_twin_launch_run(root)


def sync_twin(root: str | Path):
    return sync_twin_orchestrator(root)


def pause_twin_orchestrator_agent(root: str | Path, agent_id: str):
    return pause_twin_launch_orchestrator_agent(root, agent_id)


def resume_twin_orchestrator_agent(root: str | Path, agent_id: str):
    return resume_twin_launch_orchestrator_agent(root, agent_id)


def comment_on_twin_orchestrator_task(
    root: str | Path,
    task_id: str,
    *,
    body: str,
):
    return comment_on_twin_launch_orchestrator_task(root, task_id, body=body)


def approve_twin_orchestrator_approval(
    root: str | Path,
    approval_id: str,
    *,
    decision_note: str | None = None,
):
    return approve_twin_launch_orchestrator_approval(
        root,
        approval_id,
        decision_note=decision_note,
    )


def reject_twin_orchestrator_approval(
    root: str | Path,
    approval_id: str,
    *,
    decision_note: str | None = None,
):
    return reject_twin_launch_orchestrator_approval(
        root,
        approval_id,
        decision_note=decision_note,
    )


def request_twin_orchestrator_revision(
    root: str | Path,
    approval_id: str,
    *,
    decision_note: str | None = None,
):
    return request_twin_launch_orchestrator_revision(
        root,
        approval_id,
        decision_note=decision_note,
    )


def activate_twin_exercise(
    root: str | Path,
    *,
    scenario_variant: str,
    contract_variant: str | None = None,
):
    return activate_exercise(
        root,
        scenario_variant=scenario_variant,
        contract_variant=contract_variant,
    )


def build_workspace_governor_status(
    root: str | Path,
    *,
    governor_payload: dict[str, Any] | None = None,
    workforce_payload: dict[str, Any] | None = None,
) -> WorkspaceGovernorStatus:
    workspace_root = Path(root).expanduser().resolve()
    launch_status = None
    exercise_status = None
    try:
        launch_status = build_twin_launch_status(workspace_root)
    except FileNotFoundError:
        pass
    try:
        exercise_status = build_exercise_status(workspace_root)
    except FileNotFoundError:
        pass

    payload = WorkspaceGovernorStatus(
        governor=governor_payload or _load_saved_governor_payload(workspace_root),
        workforce=workforce_payload or _load_saved_workforce_payload(workspace_root),
    )
    if launch_status is not None:
        payload.manifest = launch_status.manifest.model_dump(mode="json")
        payload.runtime = launch_status.runtime.model_dump(mode="json")
        payload.active_run = launch_status.active_run
        payload.twin_status = launch_status.twin_status
        payload.request_count = launch_status.request_count
        payload.services_ready = launch_status.services_ready
        payload.active_agents = [
            item.model_dump(mode="json") for item in launch_status.active_agents
        ]
        payload.activity = [
            item.model_dump(mode="json") for item in launch_status.activity
        ]
        payload.outcome = launch_status.outcome.model_dump(mode="json")
        if launch_status.orchestrator is not None:
            payload.orchestrator = launch_status.orchestrator.model_dump(mode="json")
        if launch_status.orchestrator_sync is not None:
            payload.orchestrator_sync = launch_status.orchestrator_sync.model_dump(
                mode="json"
            )
    if exercise_status is not None:
        payload.exercise = {
            "manifest": exercise_status.manifest.model_dump(mode="json"),
            "comparison": [
                item.model_dump(mode="json") for item in exercise_status.comparison
            ],
        }
    return payload


def _load_saved_governor_payload(workspace_root: Path) -> dict[str, Any]:
    twin_path = workspace_root / "twin_manifest.json"
    fallback: dict[str, Any] = {}
    if twin_path.exists():
        try:
            data = json.loads(twin_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            data = {}
        fallback = dict(data.get("metadata", {}).get("governor", {}) or {})
    if isinstance(fallback, dict) and (
        "config" in fallback
        or "agents" in fallback
        or "pending_approvals" in fallback
        or "pending_demo_steps" in fallback
    ):
        return fallback

    completed_governor: dict[str, Any] | None = None
    for manifest in list_run_manifests(workspace_root):
        if manifest.runner != "external":
            continue
        governor = manifest.metadata.get("governor", {})
        if not isinstance(governor, dict):
            continue
        if manifest.status == "running":
            return dict(governor)
        if completed_governor is None and manifest.status == "completed":
            completed_governor = dict(governor)
    return completed_governor if completed_governor is not None else fallback


def _load_saved_workforce_payload(workspace_root: Path) -> dict[str, Any]:
    completed_workforce: dict[str, Any] | None = None
    for manifest in list_run_manifests(workspace_root):
        if manifest.runner != "external":
            continue
        workforce = manifest.metadata.get("workforce", {})
        if not isinstance(workforce, dict):
            continue
        if manifest.status == "running":
            return dict(workforce)
        if completed_workforce is None and manifest.status == "completed":
            completed_workforce = dict(workforce)
    return completed_workforce or {}
