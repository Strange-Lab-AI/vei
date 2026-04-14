from __future__ import annotations

import http.client
import json
import os
from pathlib import Path
from typing import Any, Mapping

from fastapi import HTTPException
from pydantic import BaseModel, Field

from vei.project_settings import default_model_for_provider
from vei.whatif import load_episode_manifest
from vei.whatif.models import (
    WhatIfEventReference,
    WhatIfExperimentMode,
    WhatIfJudgedPairwiseComparison,
    WhatIfObjectivePackId,
)
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
    candidates: list[Path] = []
    candidates.append(workspace_root / "rosetta")
    manifest_source_dir = _resolve_manifest_rosetta_dir(workspace_root)
    if manifest_source_dir is not None:
        candidates.append(manifest_source_dir)
    configured = os.environ.get("VEI_WHATIF_ROSETTA_DIR")
    if configured and configured.strip():
        candidates.append(Path(configured).expanduser())
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


def resolve_whatif_mail_archive_path(workspace_root: Path) -> Path | None:
    candidates: list[Path] = []
    manifest_source_dir = _resolve_manifest_mail_archive_source(workspace_root)
    if manifest_source_dir is not None:
        candidates.append(manifest_source_dir)
    saved_workspace_archive = _workspace_saved_mail_archive_path(workspace_root)
    if saved_workspace_archive is not None:
        candidates.append(saved_workspace_archive)
    configured = os.environ.get("VEI_WHATIF_SOURCE_DIR")
    if configured and configured.strip():
        candidates.append(Path(configured).expanduser())
    archive_override = os.environ.get("VEI_WHATIF_ARCHIVE_PATH")
    if archive_override and archive_override.strip():
        candidates.append(Path(archive_override).expanduser())
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if _looks_like_mail_archive_payload(resolved):
            return resolved
    return None


def resolve_whatif_company_history_path(workspace_root: Path) -> Path | None:
    candidates: list[Path] = []
    manifest_source_dir = _resolve_manifest_company_history_source(workspace_root)
    if manifest_source_dir is not None:
        candidates.append(manifest_source_dir)
    saved_workspace_bundle = _workspace_saved_company_history_path(workspace_root)
    if saved_workspace_bundle is not None:
        candidates.append(saved_workspace_bundle)
    configured = os.environ.get("VEI_WHATIF_SOURCE_DIR")
    if configured and configured.strip():
        candidates.append(Path(configured).expanduser())
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if _looks_like_non_mail_history_bundle_payload(resolved):
            return resolved
    return None


def resolve_whatif_source_path(
    workspace_root: Path,
    *,
    requested_source: str | None = None,
) -> tuple[str, Path] | None:
    normalized = (requested_source or "").strip().lower()
    if not normalized or normalized == "auto":
        normalized = (
            (
                _workspace_whatif_source_hint(workspace_root)
                or os.environ.get("VEI_WHATIF_SOURCE")
                or "auto"
            )
            .strip()
            .lower()
        )
    if normalized in {"", "auto", "company_history"}:
        company_history_path = resolve_whatif_company_history_path(workspace_root)
        if company_history_path is not None:
            return ("company_history", company_history_path)
    if normalized in {"", "auto", "mail_archive"}:
        archive_path = resolve_whatif_mail_archive_path(workspace_root)
        if archive_path is not None:
            return ("mail_archive", archive_path)
    if normalized in {"", "auto", "enron"}:
        rosetta_dir = resolve_whatif_rosetta_dir(workspace_root)
        if rosetta_dir is not None:
            return ("enron", rosetta_dir)
    return None


def _looks_like_mail_archive_payload(path: Path) -> bool:
    if not _looks_like_history_bundle_payload(path):
        return False
    if path.is_dir():
        return any(
            _looks_like_mail_archive_payload(path / filename)
            for filename in ("context_snapshot.json",)
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if isinstance(payload, dict) and isinstance(payload.get("threads"), list):
        return True
    if isinstance(payload, dict) and isinstance(payload.get("sources"), list):
        for source in payload.get("sources", []):
            if not isinstance(source, dict):
                continue
            provider = str(source.get("provider", "")).strip().lower()
            if provider in {"mail_archive", "gmail"}:
                return True
    return False


def _looks_like_history_bundle_payload(path: Path) -> bool:
    if not path.exists():
        return False
    if path.is_dir():
        return any(
            _looks_like_history_bundle_payload(path / filename)
            for filename in ("context_snapshot.json",)
        )
    if path.suffix.lower() != ".json":
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if isinstance(payload, dict) and isinstance(payload.get("threads"), list):
        return True
    if isinstance(payload, dict) and isinstance(payload.get("sources"), list):
        return True
    return False


def _looks_like_non_mail_history_bundle_payload(path: Path) -> bool:
    if not _looks_like_history_bundle_payload(path):
        return False
    if path.is_dir():
        return any(
            _looks_like_non_mail_history_bundle_payload(path / filename)
            for filename in ("context_snapshot.json",)
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    sources = payload.get("sources", []) if isinstance(payload, dict) else []
    if not isinstance(sources, list):
        return False
    for source in sources:
        if not isinstance(source, dict):
            continue
        provider = str(source.get("provider", "")).strip().lower()
        if provider not in {"mail_archive", "gmail"} and provider:
            return True
    return False


def _resolve_manifest_mail_archive_source(workspace_root: Path) -> Path | None:
    manifest_path = workspace_root / "episode_manifest.json"
    if not manifest_path.exists():
        return None
    try:
        manifest = load_episode_manifest(workspace_root)
    except ValueError:
        return None
    if manifest.source != "mail_archive":
        return None
    candidate = Path(manifest.source_dir).expanduser()
    if not candidate.exists():
        return None
    return candidate


def _resolve_manifest_company_history_source(workspace_root: Path) -> Path | None:
    manifest_path = workspace_root / "episode_manifest.json"
    if not manifest_path.exists():
        return None
    try:
        manifest = load_episode_manifest(workspace_root)
    except ValueError:
        return None
    if manifest.source != "company_history":
        return None
    candidate = Path(manifest.source_dir).expanduser()
    if not candidate.exists():
        return None
    return candidate


def _resolve_manifest_rosetta_dir(workspace_root: Path) -> Path | None:
    manifest_path = workspace_root / "episode_manifest.json"
    if not manifest_path.exists():
        return None
    try:
        manifest = load_episode_manifest(workspace_root)
    except ValueError:
        return None
    if manifest.source != "enron":
        return None
    candidate = Path(manifest.source_dir).expanduser()
    if not candidate.exists():
        return None
    if not (candidate / "enron_rosetta_events_metadata.parquet").exists():
        return None
    return candidate


def _workspace_saved_mail_archive_path(workspace_root: Path) -> Path | None:
    for path in (workspace_root / "context_snapshot.json",):
        if _looks_like_mail_archive_payload(path):
            return path
    return None


def _workspace_saved_company_history_path(workspace_root: Path) -> Path | None:
    for path in (workspace_root / "context_snapshot.json",):
        if _looks_like_non_mail_history_bundle_payload(path):
            return path
    return None


def _workspace_whatif_source_hint(workspace_root: Path) -> str | None:
    manifest_path = workspace_root / "episode_manifest.json"
    if manifest_path.exists():
        try:
            manifest = load_episode_manifest(workspace_root)
        except ValueError:
            manifest = None
        if manifest is not None and manifest.source:
            normalized_source = str(manifest.source).strip().lower()
            if normalized_source == "enron":
                # Saved repo examples scrub the original Rosetta path, so treat any
                # currently available Rosetta archive as enough to prefer live Enron
                # replay while still falling back to the saved snapshot when absent.
                if resolve_whatif_rosetta_dir(workspace_root) is not None:
                    return "enron"
                if _workspace_saved_mail_archive_path(workspace_root) is not None:
                    return "mail_archive"
            return normalized_source
    if _workspace_saved_mail_archive_path(workspace_root) is not None:
        return "mail_archive"
    if _workspace_saved_company_history_path(workspace_root) is not None:
        return "company_history"
    if (workspace_root / "rosetta" / "enron_rosetta_events_metadata.parquet").exists():
        return "enron"
    return None


def load_workspace_historical_summary(
    workspace_root: Path,
) -> WorkspaceHistoricalSummary | None:
    manifest_path = workspace_root / "episode_manifest.json"
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
