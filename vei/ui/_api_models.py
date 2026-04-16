from __future__ import annotations

import http.client
import json
from pathlib import Path
from typing import Any, Mapping

from fastapi import HTTPException
from pydantic import BaseModel, Field

from vei.project_settings import default_model_for_provider
from vei.whatif_filenames import EPISODE_MANIFEST_FILE
from vei.whatif.api import (
    load_episode_manifest,
    resolve_whatif_company_history_path as _resolve_whatif_company_history_path,
    resolve_whatif_mail_archive_path as _resolve_whatif_mail_archive_path,
    resolve_whatif_rosetta_dir as _resolve_whatif_rosetta_dir,
    resolve_whatif_source_path as _resolve_whatif_source_path,
)
from vei.whatif.models import (
    WhatIfEventReference,
    WhatIfExperimentMode,
    WhatIfJudgedPairwiseComparison,
    WhatIfObjectivePackId,
)
from vei.twin import (
    load_customer_twin,
    load_saved_governor_payload,
    load_saved_workforce_payload,
)
from vei.workspace.api import show_workspace


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
    source: str = "auto"
    actor: str | None = None
    participant: str | None = None
    thread_id: str | None = None
    event_type: str | None = None
    query: str | None = None
    flagged_only: bool = False
    limit: int = 10
    max_events: int | None = None


class WhatIfOpenRequest(BaseModel):
    source: str = "auto"
    event_id: str | None = None
    thread_id: str | None = None
    label: str | None = None
    max_events: int | None = None


class WhatIfSceneRequest(BaseModel):
    source: str = "auto"
    event_id: str | None = None
    thread_id: str | None = None
    max_events: int | None = None


class WhatIfRunRequest(BaseModel):
    source: str = "auto"
    prompt: str
    label: str
    event_id: str | None = None
    thread_id: str | None = None
    mode: WhatIfExperimentMode = "both"
    max_events: int | None = None
    model: str = Field(default_factory=lambda: default_model_for_provider("openai"))
    provider: str = "openai"
    ejepa_epochs: int = 4
    ejepa_batch_size: int = 64
    ejepa_force_retrain: bool = False
    ejepa_device: str | None = None


class WhatIfRankCandidateRequest(BaseModel):
    label: str | None = None
    prompt: str


class WhatIfRankRequest(BaseModel):
    source: str = "auto"
    label: str
    objective_pack_id: WhatIfObjectivePackId = "contain_exposure"
    candidates: list[WhatIfRankCandidateRequest]
    event_id: str | None = None
    thread_id: str | None = None
    rollout_count: int = 4
    max_events: int | None = None
    model: str = Field(default_factory=lambda: default_model_for_provider("openai"))
    provider: str = "openai"
    shadow_forecast_backend: str = "auto"
    ejepa_epochs: int = 4
    ejepa_batch_size: int = 64
    ejepa_force_retrain: bool = False
    ejepa_device: str | None = None


class AuditSubmitRequest(BaseModel):
    reviewer_id: str = ""
    ordered_candidate_ids: list[str]
    pairwise_comparisons: list[WhatIfJudgedPairwiseComparison] = Field(
        default_factory=list
    )
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    notes: str = ""


class WorkspaceHistoricalSummary(BaseModel):
    source: str
    organization_name: str
    organization_domain: str
    thread_id: str
    thread_subject: str
    branch_event_id: str
    branch_timestamp: str
    branch_event: WhatIfEventReference
    history_message_count: int = 0
    future_event_count: int = 0
    content_notice: str = ""


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
    return _resolve_whatif_rosetta_dir(workspace_root)


def resolve_whatif_mail_archive_path(workspace_root: Path) -> Path | None:
    return _resolve_whatif_mail_archive_path(workspace_root)


def resolve_whatif_company_history_path(workspace_root: Path) -> Path | None:
    return _resolve_whatif_company_history_path(workspace_root)


def resolve_whatif_source_path(
    workspace_root: Path,
    *,
    requested_source: str | None = None,
) -> tuple[str, Path] | None:
    return _resolve_whatif_source_path(
        workspace_root,
        requested_source=requested_source,
    )


def load_workspace_historical_summary(
    workspace_root: Path,
) -> WorkspaceHistoricalSummary | None:
    manifest_path = workspace_root / EPISODE_MANIFEST_FILE
    if not manifest_path.exists():
        return None
    manifest = load_episode_manifest(workspace_root)
    return WorkspaceHistoricalSummary(
        source=manifest.source,
        organization_name=manifest.organization_name,
        organization_domain=manifest.organization_domain,
        thread_id=manifest.thread_id,
        thread_subject=manifest.thread_subject,
        branch_event_id=manifest.branch_event_id,
        branch_timestamp=manifest.branch_timestamp,
        branch_event=manifest.branch_event,
        history_message_count=manifest.history_message_count,
        future_event_count=manifest.future_event_count,
        content_notice=manifest.content_notice,
    )


def load_workspace_governor_payload(root: Path) -> dict[str, Any]:
    return load_saved_governor_payload(root)


def load_workspace_workforce_payload(root: Path) -> dict[str, Any]:
    return load_saved_workforce_payload(root)


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
