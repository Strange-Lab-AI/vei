from __future__ import annotations

import http.client
import json
import os
from pathlib import Path
from typing import Any, Mapping

from fastapi import HTTPException
from pydantic import BaseModel

from vei.whatif.models import WhatIfExperimentMode
from vei.twin import load_customer_twin
from vei.workspace.api import show_workspace
from vei.run.api import list_run_manifests


class RunLaunchRequest(BaseModel):
    runner: str = "workflow"
    scenario_name: str | None = None
    run_id: str | None = None
    seed: int = 42042
    branch: str | None = None
    model: str | None = None
    provider: str | None = None
    bc_model: str | None = None
    task: str | None = None
    max_steps: int = 12


class ScenarioActivateRequest(BaseModel):
    scenario_name: str | None = None
    variant: str | None = None
    bootstrap_contract: bool = False


class ContractActivateRequest(BaseModel):
    variant: str


class MissionActivateRequest(BaseModel):
    mission_name: str
    objective_variant: str | None = None


class MissionStartRequest(BaseModel):
    mission_name: str | None = None
    objective_variant: str | None = None
    run_id: str | None = None
    seed: int = 42042


class MissionBranchRequest(BaseModel):
    branch_name: str | None = None
    snapshot_id: int | None = None


class GovernorAgentUpdateRequest(BaseModel):
    name: str | None = None
    role: str | None = None
    team: str | None = None
    mode: str | None = None
    allowed_surfaces: list[str] | None = None
    policy_profile_id: str | None = None
    status: str | None = None


class GovernorApprovalResolveRequest(BaseModel):
    resolver_agent_id: str


class ServiceOpsPolicyReplayRequest(BaseModel):
    policy_delta: dict[str, Any]


class ContextCaptureRequest(BaseModel):
    providers: list[str]


class GovernorSituationActivateRequest(BaseModel):
    scenario_variant: str
    contract_variant: str | None = None


class OrchestratorTaskCommentRequest(BaseModel):
    body: str


class OrchestratorApprovalDecisionRequest(BaseModel):
    decision_note: str | None = None


class WhatIfSearchRequest(BaseModel):
    source: str = "enron"
    actor: str | None = None
    participant: str | None = None
    thread_id: str | None = None
    event_type: str | None = None
    query: str | None = None
    flagged_only: bool = False
    limit: int = 10
    max_events: int | None = None


class WhatIfOpenRequest(BaseModel):
    source: str = "enron"
    event_id: str | None = None
    thread_id: str | None = None
    label: str | None = None
    max_events: int | None = None


class WhatIfRunRequest(BaseModel):
    source: str = "enron"
    prompt: str
    label: str
    event_id: str | None = None
    thread_id: str | None = None
    mode: WhatIfExperimentMode = "both"
    max_events: int | None = None
    model: str = "gpt-5-mini"
    provider: str = "openai"
    ejepa_epochs: int = 4
    ejepa_batch_size: int = 64
    ejepa_force_retrain: bool = False
    ejepa_device: str | None = None


CONTEXT_PROVIDER_ENV_VARS = {
    "slack": "VEI_SLACK_TOKEN",
    "google": "VEI_GOOGLE_TOKEN",
    "jira": "VEI_JIRA_TOKEN",
    "okta": "VEI_OKTA_TOKEN",
    "gmail": "VEI_GMAIL_TOKEN",
    "teams": "VEI_TEAMS_TOKEN",
}

CONTEXT_PROVIDER_BASE_URL_ENV_VARS = {
    "jira": "VEI_JIRA_URL",
    "okta": "VEI_OKTA_ORG_URL",
}


def build_context_provider_status(
    provider: str,
    env: Mapping[str, str],
) -> dict[str, Any]:
    token_env = CONTEXT_PROVIDER_ENV_VARS[provider]
    if not env.get(token_env):
        return {
            "provider": provider,
            "configured": False,
            "env_var": token_env,
        }

    base_url_env = CONTEXT_PROVIDER_BASE_URL_ENV_VARS.get(provider)
    if base_url_env and not env.get(base_url_env):
        return {
            "provider": provider,
            "configured": False,
            "env_var": base_url_env,
        }

    return {
        "provider": provider,
        "configured": True,
        "env_var": token_env,
    }


def context_capture_org_name(workspace_root: Path) -> str:
    workspace = show_workspace(workspace_root)
    return workspace.manifest.title or workspace.manifest.name or "Unknown"


def resolve_whatif_rosetta_dir(workspace_root: Path) -> Path | None:
    candidates: list[Path] = []
    configured = os.environ.get("VEI_WHATIF_ROSETTA_DIR")
    if configured and configured.strip():
        candidates.append(Path(configured).expanduser())
    candidates.append(workspace_root / "rosetta")
    candidates.append(
        workspace_root.parent
        / "human_v_llm_messages_experiment"
        / "experiments"
        / "org_simulator"
        / "rosetta"
    )
    candidates.append(
        workspace_root.parent.parent
        / "human_v_llm_messages_experiment"
        / "experiments"
        / "org_simulator"
        / "rosetta"
    )
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if (resolved / "enron_rosetta_events_metadata.parquet").exists():
            return resolved
    return None


def load_workspace_governor_payload(root: Path) -> dict[str, Any]:
    twin_path = root / "twin_manifest.json"
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
    for manifest in list_run_manifests(root):
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


def load_workspace_workforce_payload(root: Path) -> dict[str, Any]:
    completed_workforce: dict[str, Any] | None = None
    for manifest in list_run_manifests(root):
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


def gateway_json_request(
    root: Path,
    *,
    path: str,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
) -> Any:
    try:
        bundle = load_customer_twin(root)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=404, detail="twin gateway is not configured"
        ) from exc

    body = None
    headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    connection = http.client.HTTPConnection(
        bundle.gateway.host,
        bundle.gateway.port,
        timeout=5,
    )
    try:
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        raw = response.read().decode("utf-8")
        if 200 <= response.status < 300:
            return json.loads(raw) if raw else {}
        try:
            parsed = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            parsed = raw or response.reason
        raise HTTPException(status_code=response.status, detail=parsed)
    except OSError as exc:
        raise HTTPException(
            status_code=503,
            detail="twin gateway is not reachable right now",
        ) from exc
    finally:
        connection.close()
