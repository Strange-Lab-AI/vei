from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import Request

from vei.contract.models import ContractEvaluationResult
from vei.governor import GovernorAgentSpec
from vei.run.api import list_run_snapshots
from vei.run.models import RunContractSummary
from vei.workforce.api import WorkforceCommandRecord, WorkforceState
from vei.world.api import WorldSessionAPI

from .models import ExternalAgentIdentity


def channel_for_focus(focus: str) -> str:
    mapping = {
        "slack": "Communication",
        "mail": "Communication",
        "tickets": "Work",
        "calendar": "Work",
        "crm": "Revenue",
    }
    return mapping.get(focus, "World")


def object_refs(args: dict[str, Any], result: Any) -> list[str]:
    refs: list[str] = []
    for key in (
        "channel",
        "issue_id",
        "ticket_id",
        "id",
        "event_id",
        "deal_id",
        "company_id",
        "contact_id",
    ):
        value = args.get(key)
        if value:
            refs.append(str(value))
    if isinstance(result, dict):
        for key in ("id", "issue_id", "ticket_id", "event_id"):
            value = result.get(key)
            if value:
                refs.append(str(value))
    return sorted(set(refs))


def snapshot_path(root: Path, run_id: str, snapshot_id: int) -> str | None:
    for item in list_run_snapshots(root, run_id):
        if item.snapshot_id == snapshot_id:
            return item.path
    return None


def contract_summary(path: Path) -> RunContractSummary:
    if not path.exists():
        return RunContractSummary()
    payload = json.loads(path.read_text(encoding="utf-8"))
    evaluation = ContractEvaluationResult.model_validate(payload)
    issues = len(evaluation.dynamic_validation.issues) + len(
        evaluation.static_validation.issues
    )
    total = evaluation.success_predicate_count + evaluation.forbidden_predicate_count
    passed = evaluation.success_predicates_passed + max(
        0,
        evaluation.forbidden_predicate_count - evaluation.forbidden_predicates_failed,
    )
    return RunContractSummary(
        contract_name=evaluation.contract_name,
        ok=evaluation.ok,
        success_assertion_count=total,
        success_assertions_passed=passed,
        success_assertions_failed=max(0, total - passed),
        issue_count=issues,
        evaluation_path=str(path.name),
    )


def request_agent_identity(request: Request) -> ExternalAgentIdentity | None:
    agent_id = request.headers.get("x-vei-agent-id") or None
    name = request.headers.get("x-vei-agent-name") or None
    role = request.headers.get("x-vei-agent-role") or None
    team = request.headers.get("x-vei-agent-team") or None
    source = request.headers.get("user-agent") or None
    if not any([agent_id, name, role, team, source]):
        return None
    return ExternalAgentIdentity(
        agent_id=agent_id,
        name=name,
        role=role,
        team=team,
        source=source,
    )


def identity_from_mirror_agent(agent: GovernorAgentSpec) -> ExternalAgentIdentity:
    return ExternalAgentIdentity(
        agent_id=agent.agent_id,
        name=agent.name,
        role=agent.role,
        team=agent.team,
        source=agent.source,
    )


def merge_mirror_agent_identity(
    mirror_agent: GovernorAgentSpec,
    request_agent: ExternalAgentIdentity,
) -> GovernorAgentSpec:
    updates = {
        field: value
        for field in ("name", "role", "team", "source")
        if (value := getattr(request_agent, field))
    }
    if not updates:
        return mirror_agent
    return mirror_agent.model_copy(update=updates, deep=True)


def session_router(session: WorldSessionAPI) -> Any:
    router = getattr(session, "router", None)
    if router is None:
        raise RuntimeError("world session router is unavailable")
    return router


def workforce_object_refs(state: WorkforceState) -> list[str]:
    refs: list[str] = []
    snapshot = state.snapshot
    if snapshot is None:
        return refs
    refs.extend(agent.agent_id for agent in snapshot.agents[:4])
    refs.extend(task.task_id for task in snapshot.tasks[:4])
    refs.extend(item.approval_id for item in snapshot.approvals[:4])
    return refs


def workforce_command_label(command: WorkforceCommandRecord) -> str:
    target = (
        command.approval_id
        or command.task_id
        or command.agent_id
        or "outside workforce"
    )
    return f"VEI {command.action.replace('_', ' ')} on {target}"


def workforce_command_refs(command: WorkforceCommandRecord) -> list[str]:
    refs = [
        value
        for value in (command.agent_id, command.task_id, command.approval_id)
        if value
    ]
    if command.comment_id:
        refs.append(f"comment:{command.comment_id}")
    return refs


def iso_now() -> str:
    return datetime.now(UTC).isoformat()
