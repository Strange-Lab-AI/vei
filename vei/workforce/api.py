from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Iterable

from vei.orchestrators.api import (
    OrchestratorCommandResult,
    OrchestratorSnapshot,
    OrchestratorSyncHealth,
)

from .models import (
    WorkforceControlSummary,
    WorkforceState,
    WorkforceCommandRecord,
)

_BOUNDARY_EXPORTS = (WorkforceCommandRecord,)


def build_workforce_state(
    *,
    snapshot: OrchestratorSnapshot | None,
    sync: OrchestratorSyncHealth | None,
    commands: Iterable[WorkforceCommandRecord] = (),
) -> WorkforceState:
    command_list = _normalize_commands(commands)
    summary = _build_summary(snapshot=snapshot, sync=sync, commands=command_list)
    updated_at = _latest_timestamp(
        [summary.latest_activity_at] + [item.created_at for item in command_list]
    )
    return WorkforceState(
        updated_at=updated_at or _iso_now(),
        summary=summary,
        sync=sync,
        snapshot=snapshot,
        commands=command_list,
    )


def sync_workforce_state(
    current: WorkforceState | None,
    *,
    snapshot: OrchestratorSnapshot | None,
    sync: OrchestratorSyncHealth | None,
) -> WorkforceState:
    existing_commands = [] if current is None else current.commands
    return build_workforce_state(
        snapshot=snapshot,
        sync=sync,
        commands=existing_commands,
    )


def append_workforce_command(
    current: WorkforceState | None,
    command: WorkforceCommandRecord,
    *,
    max_commands: int = 30,
) -> WorkforceState:
    existing = [] if current is None else current.commands
    normalized = _normalize_command(command)
    commands = (existing + [normalized])[-max_commands:]
    snapshot = None if current is None else current.snapshot
    sync = None if current is None else current.sync
    return build_workforce_state(snapshot=snapshot, sync=sync, commands=commands)


def workforce_command_from_result(
    result: OrchestratorCommandResult,
    *,
    decision_note: str | None = None,
) -> WorkforceCommandRecord:
    return WorkforceCommandRecord(
        provider=result.provider,
        action=result.action,
        created_at=_iso_now(),
        message=result.message,
        agent_id=result.agent_id,
        external_agent_id=result.external_agent_id,
        task_id=result.task_id,
        external_task_id=result.external_task_id,
        approval_id=result.approval_id,
        external_approval_id=result.external_approval_id,
        comment_id=result.comment_id,
        decision_note=(decision_note or "").strip() or None,
    )


def workforce_state_fingerprint(state: WorkforceState | None) -> str:
    if state is None:
        return ""
    payload = state.model_dump(mode="json")
    payload["updated_at"] = ""
    if isinstance(payload.get("sync"), dict):
        payload["sync"]["last_attempt_at"] = ""
        payload["sync"]["last_success_at"] = ""
    if isinstance(payload.get("snapshot"), dict):
        payload["snapshot"]["fetched_at"] = ""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _build_summary(
    *,
    snapshot: OrchestratorSnapshot | None,
    sync: OrchestratorSyncHealth | None,
    commands: list[WorkforceCommandRecord],
) -> WorkforceControlSummary:
    if snapshot is None:
        return WorkforceControlSummary(
            provider=(None if sync is None else sync.provider),
            sync_status=("disabled" if sync is None else sync.status),
            latest_activity_at=_latest_timestamp(item.created_at for item in commands),
        )
    agents = list(snapshot.agents or [])
    tasks = list(snapshot.tasks or [])
    approvals = list(snapshot.approvals or [])
    capabilities = snapshot.capabilities
    latest_activity_at = _latest_timestamp(
        [item.created_at for item in snapshot.recent_activity]
        + [item.created_at for item in commands]
    )
    steering_actions = {
        "comment_task",
        "approve",
        "reject",
        "request_revision",
        "pause",
        "resume",
    }
    vei_action_count = sum(1 for c in commands if c.action in steering_actions)
    downstream_response_count = _count_downstream_responses(commands, snapshot)
    completed_task_count = sum(
        1
        for t in tasks
        if (t.status or "").strip().lower() in {"done", "completed", "closed"}
    )
    approved_count = sum(
        1
        for a in approvals
        if (a.status or "").strip().lower() in {"approved", "accepted"}
    )
    return WorkforceControlSummary(
        provider=snapshot.provider,
        company_name=snapshot.summary.company_name,
        sync_status=("disabled" if sync is None else sync.status),
        observed_agent_count=len(agents),
        governable_agent_count=sum(
            1 for item in agents if item.integration_mode in {"proxy", "ingest"}
        ),
        steerable_agent_count=(
            len(agents)
            if (
                capabilities.can_pause_agents
                or capabilities.can_resume_agents
                or capabilities.can_comment_on_tasks
                or capabilities.can_manage_approvals
            )
            else 0
        ),
        active_agent_count=sum(
            1
            for item in agents
            if (item.status or "").strip().lower() in {"active", "running", "busy"}
        ),
        task_count=len(tasks),
        pending_approval_count=sum(
            1
            for item in approvals
            if (item.status or "").strip().lower()
            in {"pending", "blocked", "review", "revision_requested"}
        ),
        routeable_surface_count=len(capabilities.routeable_surfaces),
        latest_activity_at=latest_activity_at,
        vei_action_count=vei_action_count,
        downstream_response_count=downstream_response_count,
        completed_task_count=completed_task_count,
        approved_count=approved_count,
    )


def _count_downstream_responses(
    commands: list[WorkforceCommandRecord],
    snapshot: OrchestratorSnapshot,
) -> int:
    if not commands:
        return 0
    steering = {
        "comment_task",
        "approve",
        "reject",
        "request_revision",
        "pause",
        "resume",
    }
    vei_times = sorted(
        c.created_at for c in commands if c.action in steering and c.created_at
    )
    if not vei_times:
        return 0
    first_vei = vei_times[0]
    return sum(
        1
        for a in (snapshot.recent_activity or [])
        if a.created_at and a.created_at > first_vei
    )


def _normalize_commands(
    commands: Iterable[WorkforceCommandRecord],
) -> list[WorkforceCommandRecord]:
    return [_normalize_command(item) for item in commands]


def _normalize_command(command: WorkforceCommandRecord) -> WorkforceCommandRecord:
    if command.created_at:
        return command
    return command.model_copy(update={"created_at": _iso_now()})


def _latest_timestamp(values: Iterable[str | None]) -> str | None:
    timestamps = [str(item).strip() for item in values if str(item or "").strip()]
    if not timestamps:
        return None
    return max(timestamps)


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


__all__ = [
    "WorkforceCommandRecord",
    "WorkforceControlSummary",
    "WorkforceState",
    "append_workforce_command",
    "build_workforce_state",
    "sync_workforce_state",
    "workforce_command_from_result",
    "workforce_state_fingerprint",
]
