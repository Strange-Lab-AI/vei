from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from vei.benchmark.models import BenchmarkMetrics
from vei.blueprint.api import create_world_session_from_blueprint
from vei.connectors import TOOL_ROUTES
from vei.contract.models import ContractEvaluationResult
from vei.mirror import (
    MirrorActionPlan,
    MirrorAgentSpec,
    MirrorConnectorStatus,
    MirrorIngestEvent,
    MirrorRuntime,
    load_mirror_workspace_config,
)
from vei.router.errors import MCPError
from vei.run.api import (
    build_run_timeline,
    generate_run_id,
    get_run_surface_state,
    get_workspace_run_dir,
    list_run_snapshots,
    load_run_manifest,
    write_run_manifest,
)
from vei.run import append_run_event
from vei.run.models import (
    RunArtifactIndex,
    RunContractSummary,
    RunManifest,
    RunTimelineEvent,
)
from vei.workspace.api import (
    build_workspace_scenario_asset,
    evaluate_workspace_contract_against_state,
    load_workspace,
    load_workspace_blueprint_asset,
    resolve_workspace_scenario,
    temporary_env,
    upsert_workspace_run,
)
from vei.workspace.models import WorkspaceRunEntry
from vei.world.api import ActorState, WorldSessionAPI

from .api import load_customer_twin
from .models import CustomerTwinBundle, ExternalAgentIdentity, TwinRuntimeStatus

_MIRROR_OPERATION_CLASS_BY_TOOL: dict[str, str] = {
    "service_ops.list_overview": "read",
    "service_ops.assign_dispatch": "write_safe",
    "service_ops.reschedule_dispatch": "write_safe",
    "service_ops.hold_billing": "write_safe",
    "service_ops.clear_exception": "write_safe",
    "service_ops.update_policy": "write_risky",
    "jira.list_issues": "read",
    "jira.get_issue": "read",
    "jira.create_issue": "write_safe",
    "jira.update_issue": "write_safe",
    "jira.add_comment": "write_safe",
    "jira.transition_issue": "write_risky",
    "salesforce.opportunity.list": "read",
    "salesforce.opportunity.get": "read",
    "salesforce.opportunity.create": "write_safe",
    "salesforce.opportunity.update": "write_safe",
    "salesforce.contact.get": "read",
    "salesforce.contact.list": "read",
    "salesforce.account.get": "read",
    "salesforce.account.list": "read",
    "salesforce.activity.log": "write_safe",
}

_SURFACE_ALIASES: dict[str, set[str]] = {
    "slack": {"slack"},
    "mail": {"mail", "graph"},
    "calendar": {"calendar", "graph"},
    "tickets": {"tickets", "jira"},
    "crm": {"crm", "salesforce"},
    "service_ops": {"service_ops"},
}

_PROFILE_ACTIONS = {
    "observer": {
        "read": "allow",
        "write_safe": "deny",
        "write_risky": "deny",
    },
    "operator": {
        "read": "allow",
        "write_safe": "allow",
        "write_risky": "require_approval",
    },
    "approver": {
        "read": "allow",
        "write_safe": "allow",
        "write_risky": "require_approval",
    },
    "admin": {
        "read": "allow",
        "write_safe": "allow",
        "write_risky": "allow",
    },
}


class TwinRuntime:
    def __init__(self, workspace_root: Path, bundle: CustomerTwinBundle):
        self.workspace_root = workspace_root
        self.bundle = bundle
        self.mirror_config = load_mirror_workspace_config(bundle.metadata)
        self.workspace_manifest = load_workspace(workspace_root)
        self.scenario = resolve_workspace_scenario(
            workspace_root, self.workspace_manifest
        )
        self.run_id = generate_run_id(prefix="external")
        self.branch_name = f"{self.workspace_manifest.name}.{self.run_id}"
        self.run_dir = get_workspace_run_dir(workspace_root, self.run_id)
        self.events_path = self.run_dir / "events.jsonl"
        self.contract_path = self.run_dir / "workspace_contract_evaluation.json"
        self.state_dir = self.run_dir / "state"
        self.artifacts_dir = self.run_dir / "artifacts"
        self.started_at = _iso_now()
        self._lock = threading.RLock()
        self.status = TwinRuntimeStatus(
            run_id=self.run_id,
            branch_name=self.branch_name,
            started_at=self.started_at,
            metadata={"organization_name": bundle.organization_name},
        )
        self.mirror: MirrorRuntime | None = None

        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.session = self._build_session()
        snapshot = self.session.snapshot("gateway.start")
        contract_eval = self._evaluate(snapshot, result={"status": "started"})
        self.status.latest_snapshot_id = snapshot.snapshot_id
        self._update_contract_status(contract_eval)
        self._write_contract_eval(contract_eval)
        self._write_manifest(
            status="running", success=None, error=None, completed_at=None
        )
        self._upsert_run_index(status="running", success=None, completed_at=None)
        self._append_event(
            kind="run_started",
            label="external agent session started",
            channel="World",
            time_ms=snapshot.time_ms,
            status="running",
            payload={
                "organization_name": bundle.organization_name,
                "organization_domain": bundle.organization_domain,
            },
        )
        self._append_snapshot_event(
            "gateway.start", snapshot.snapshot_id, snapshot.time_ms
        )
        self._append_contract_event(contract_eval, snapshot.time_ms)
        self.mirror = MirrorRuntime(
            metadata=bundle.metadata,
            hero_world=bundle.mold.archetype,
            target=self,
        )
        self._refresh_mirror_status()
        self._write_manifest(
            status="running", success=None, error=None, completed_at=None
        )

    def finalize(self, *, error: str | None = None) -> None:
        if self.status.status != "running":
            return
        if self.mirror is not None:
            self.mirror.stop()
        completed_at = _iso_now()
        if error is not None:
            self.status.status = "error"
        else:
            self.status.status = "completed"
        self.status.completed_at = completed_at
        success = self.status.latest_contract_ok if error is None else False
        self._write_manifest(
            status="error" if error else "ok",
            success=success,
            error=error,
            completed_at=completed_at,
        )
        self._upsert_run_index(
            status="error" if error else "ok",
            success=success,
            completed_at=completed_at,
        )
        self._append_event(
            kind="run_completed" if error is None else "run_failed",
            label=(
                "external agent session completed"
                if error is None
                else "external agent session failed"
            ),
            channel="World",
            time_ms=0,
            status="ok" if error is None else "error",
            payload={"error": error},
        )

    def dispatch(
        self,
        *,
        external_tool: str,
        resolved_tool: str,
        args: dict[str, Any],
        focus_hint: str,
        agent: ExternalAgentIdentity | None = None,
    ) -> Any:
        with self._lock:
            try:
                result = self.session.call_tool(resolved_tool, args)
                observation = self.session.observe(focus_hint)
                snapshot = self.session.snapshot(f"gateway:{external_tool}")
                contract_eval = self._evaluate(
                    snapshot,
                    visible_observation=observation,
                    result=result,
                )
                self.status.latest_snapshot_id = snapshot.snapshot_id
                self.status.request_count += 1
                self._record_agent_identity(agent)
                self._update_contract_status(contract_eval)
                self._write_contract_eval(contract_eval)
                self._refresh_mirror_status()
                self._write_manifest(
                    status="running", success=None, error=None, completed_at=None
                )
                self._append_event(
                    kind="workflow_step",
                    label=external_tool,
                    channel=_channel_for_focus(focus_hint),
                    time_ms=snapshot.time_ms,
                    tool=external_tool,
                    resolved_tool=resolved_tool,
                    object_refs=_object_refs(args, result),
                    payload={
                        "args": args,
                        "result": result,
                        "agent": (
                            agent.model_dump(mode="json") if agent is not None else None
                        ),
                    },
                )
                self._append_snapshot_event(
                    f"gateway:{external_tool}",
                    snapshot.snapshot_id,
                    snapshot.time_ms,
                )
                self._append_contract_event(contract_eval, snapshot.time_ms)
                return result
            except Exception as exc:  # noqa: BLE001
                snapshot = self.session.snapshot(f"error:{external_tool}")
                contract_eval = self._evaluate(
                    snapshot,
                    result={"error": _error_payload(exc)},
                )
                self.status.latest_snapshot_id = snapshot.snapshot_id
                self.status.request_count += 1
                self._record_agent_identity(agent)
                self._update_contract_status(contract_eval)
                self._write_contract_eval(contract_eval)
                self._refresh_mirror_status()
                self._write_manifest(
                    status="running", success=None, error=None, completed_at=None
                )
                self._append_event(
                    kind="workflow_step",
                    label=external_tool,
                    channel=_channel_for_focus(focus_hint),
                    time_ms=snapshot.time_ms,
                    status="error",
                    tool=external_tool,
                    resolved_tool=resolved_tool,
                    object_refs=_object_refs(args, {}),
                    payload={
                        "args": args,
                        "error": _error_payload(exc),
                        "agent": (
                            agent.model_dump(mode="json") if agent is not None else None
                        ),
                    },
                )
                self._append_snapshot_event(
                    f"error:{external_tool}",
                    snapshot.snapshot_id,
                    snapshot.time_ms,
                )
                self._append_contract_event(contract_eval, snapshot.time_ms)
                raise

    def dispatch_proxy_request(
        self,
        *,
        external_tool: str,
        resolved_tool: str,
        args: dict[str, Any],
        focus_hint: str,
        agent: ExternalAgentIdentity,
    ) -> Any:
        if self.mirror is None:
            raise MCPError("mirror.unavailable", "mirror runtime unavailable")
        try:
            mirror_agent = self.mirror.require_agent(agent.agent_id or "")
        except ValueError as exc:
            raise MCPError(
                "mirror.agent_not_registered",
                str(exc),
            ) from exc
        merged_agent = _merge_mirror_agent_identity(mirror_agent, agent)
        if merged_agent != mirror_agent:
            mirror_agent = self.mirror.register_agent(merged_agent)
        event = MirrorIngestEvent(
            agent_id=mirror_agent.agent_id,
            external_tool=external_tool,
            resolved_tool=resolved_tool,
            focus_hint=focus_hint,
            args=dict(args),
            label=external_tool,
            source_mode="proxy",
        )
        result = self.mirror.ingest_event(event)
        if result.handled_by in {"denied", "pending_approval"}:
            reason = str(result.result.get("reason", "mirror request denied"))
            code = str(
                result.result.get(
                    "code",
                    (
                        "mirror.approval_required"
                        if result.handled_by == "pending_approval"
                        else "mirror.surface_denied"
                    ),
                )
            )
            raise MCPError(code, reason)
        return result.result.get("result")

    def peek(self, tool: str, args: dict[str, Any] | None = None) -> Any:
        with self._lock:
            return self.session.call_tool(tool, args or {})

    def status_payload(self) -> dict[str, Any]:
        return {
            "bundle": self.bundle.model_dump(mode="json"),
            "runtime": self.status.model_dump(mode="json"),
            "mirror": self._mirror_snapshot_payload(),
            "manifest": load_run_manifest(
                self.run_dir / "run_manifest.json"
            ).model_dump(mode="json"),
        }

    def _build_session(self) -> WorldSessionAPI:
        asset = build_workspace_scenario_asset(
            load_workspace_blueprint_asset(self.workspace_root),
            self.scenario,
        )
        with (
            temporary_env("VEI_STATE_DIR", str(self.state_dir)),
            temporary_env(
                "VEI_CRM_ALIAS_PACKS",
                "salesforce",
            ),
        ):
            return create_world_session_from_blueprint(
                asset,
                seed=42042,
                artifacts_dir=str(self.artifacts_dir),
                branch=self.branch_name,
                connector_mode=self.mirror_config.connector_mode,
            )

    def _evaluate(
        self,
        snapshot: Any,
        *,
        visible_observation: dict[str, Any] | None = None,
        result: object | None = None,
    ) -> ContractEvaluationResult:
        return evaluate_workspace_contract_against_state(
            root=self.workspace_root,
            scenario_name=self.scenario.name,
            oracle_state=snapshot.data.model_dump(),
            visible_observation=visible_observation or {},
            result=result,
            pending=self.session.pending(),
            time_ms=snapshot.time_ms,
        )

    def _write_contract_eval(self, contract_eval: ContractEvaluationResult) -> None:
        self.contract_path.write_text(
            contract_eval.model_dump_json(indent=2),
            encoding="utf-8",
        )

    def _write_manifest(
        self,
        *,
        status: str,
        success: bool | None,
        error: str | None,
        completed_at: str | None,
    ) -> None:
        manifest = RunManifest(
            run_id=self.run_id,
            workspace_name=self.workspace_manifest.name,
            scenario_name=self.scenario.name,
            runner="external",
            status=status,  # type: ignore[arg-type]
            started_at=self.started_at,
            completed_at=completed_at,
            seed=42042,
            branch=self.branch_name,
            success=success,
            metrics=BenchmarkMetrics(actions=self.status.request_count),
            contract=_contract_summary(self.contract_path),
            artifacts=RunArtifactIndex(
                run_dir=str(self.run_dir.relative_to(self.workspace_root)),
                artifacts_dir=str(self.artifacts_dir.relative_to(self.workspace_root)),
                state_dir=str(self.state_dir.relative_to(self.workspace_root)),
                events_path=str(self.events_path.relative_to(self.workspace_root)),
                contract_path=str(self.contract_path.relative_to(self.workspace_root)),
            ),
            snapshots=list_run_snapshots(self.workspace_root, self.run_id),
            error=error,
            metadata={
                "gateway_mode": "compatibility",
                "organization_name": self.bundle.organization_name,
                "surfaces": [item.name for item in self.bundle.gateway.surfaces],
                "agents": list(self.status.metadata.get("agents", [])),
                "last_agent": self.status.metadata.get("last_agent"),
                "mirror": self._mirror_snapshot_payload(),
            },
        )
        write_run_manifest(self.workspace_root, manifest)

    def _upsert_run_index(
        self,
        *,
        status: str,
        success: bool | None,
        completed_at: str | None,
    ) -> None:
        upsert_workspace_run(
            self.workspace_root,
            WorkspaceRunEntry(
                run_id=self.run_id,
                scenario_name=self.scenario.name,
                runner="external",
                status=status,  # type: ignore[arg-type]
                manifest_path=str(
                    (self.run_dir / "run_manifest.json").relative_to(
                        self.workspace_root
                    )
                ),
                started_at=self.started_at,
                completed_at=completed_at,
                success=success,
                branch=self.branch_name,
                metadata={"gateway_mode": "compatibility"},
            ),
        )

    def _update_contract_status(self, contract_eval: ContractEvaluationResult) -> None:
        issues = len(contract_eval.dynamic_validation.issues) + len(
            contract_eval.static_validation.issues
        )
        self.status.latest_contract_ok = contract_eval.ok
        self.status.contract_issue_count = issues

    def _record_agent_identity(self, agent: ExternalAgentIdentity | None) -> None:
        if agent is None:
            return
        metadata = dict(self.status.metadata)
        agents = list(metadata.get("agents", []))
        payload = agent.model_dump(mode="json")
        if payload not in agents:
            agents.append(payload)
        metadata["agents"] = agents
        metadata["last_agent"] = payload
        self.status.metadata = metadata

    def _refresh_mirror_status(self) -> None:
        if self.mirror is None:
            return
        metadata = dict(self.status.metadata)
        metadata["mirror"] = self._mirror_snapshot_payload()
        self.status.metadata = metadata

    def sync_mirror_runtime_state(self) -> None:
        with self._lock:
            self._refresh_mirror_status()
            self._write_manifest(
                status="running", success=None, error=None, completed_at=None
            )

    def _mirror_snapshot_payload(self) -> dict[str, Any]:
        if self.mirror is None:
            return {"config": self.mirror_config.model_dump(mode="json")}
        return self.mirror.snapshot().model_dump(mode="json")

    def start_mirror(self) -> None:
        if self.mirror is None:
            return
        self.mirror.start()
        self._refresh_mirror_status()
        self._write_manifest(
            status="running", success=None, error=None, completed_at=None
        )

    def _append_event(
        self,
        *,
        kind: str,
        label: str,
        channel: str,
        time_ms: int,
        status: str | None = None,
        tool: str | None = None,
        resolved_tool: str | None = None,
        object_refs: list[str] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        append_run_event(
            self.events_path,
            RunTimelineEvent(
                index=0,
                kind=kind,  # type: ignore[arg-type]
                label=label,
                channel=channel,
                time_ms=time_ms,
                runner="external",
                status=status,
                tool=tool,
                resolved_tool=resolved_tool,
                object_refs=list(object_refs or []),
                branch=self.branch_name,
                payload=dict(payload or {}),
            ),
        )

    def _append_snapshot_event(
        self, label: str, snapshot_id: int, time_ms: int
    ) -> None:
        append_run_event(
            self.events_path,
            RunTimelineEvent(
                index=0,
                kind="snapshot",
                label=label,
                channel="World",
                time_ms=time_ms,
                runner="external",
                branch=self.branch_name,
                snapshot_id=snapshot_id,
                payload={
                    "path": _snapshot_path(
                        self.workspace_root, self.run_id, snapshot_id
                    )
                },
            ),
        )

    def register_mirror_agent(self, agent: MirrorAgentSpec) -> None:
        actor = ActorState(
            actor_id=agent.agent_id,
            mode="scripted",
            status=agent.status,
            metadata={
                "name": agent.name,
                "role": agent.role,
                "team": agent.team,
                "allowed_surfaces": list(agent.allowed_surfaces),
                "policy_profile_id": agent.policy_profile_id,
                "source": agent.source,
            },
        )
        with self._lock:
            self.session.register_actor(actor)
            self._record_agent_identity(_identity_from_mirror_agent(agent))
            self._refresh_mirror_status()
            self._write_manifest(
                status="running", success=None, error=None, completed_at=None
            )

    def plan_mirror_action(
        self,
        *,
        event: MirrorIngestEvent,
        agent: MirrorAgentSpec,
        approval_granted: bool = False,
    ) -> MirrorActionPlan:
        action = "dispatch" if event.resolved_tool else "inject"
        tool_name = str(event.resolved_tool or event.external_tool or "")
        surface = _event_surface(event)
        operation_class = _mirror_operation_class(tool_name)
        if operation_class is None and action == "inject":
            operation_class = "write_safe"
        if operation_class is None:
            return MirrorActionPlan(
                action=action,
                surface=surface,
                resolved_tool=tool_name,
                operation_class="read",
                decision="deny",
                reason_code="mirror.unknown_operation_class",
                reason=(
                    f"mirror does not have an operation class for '{tool_name}' yet"
                ),
            )

        surface_denial = self._check_surface_access(agent, surface)
        if surface_denial is not None:
            return MirrorActionPlan(
                action=action,
                surface=surface,
                resolved_tool=tool_name,
                operation_class=operation_class,
                decision="deny",
                reason_code="mirror.surface_denied",
                reason=surface_denial,
            )

        profile_decision = self._check_policy_profile(
            agent=agent,
            operation_class=operation_class,
            approval_granted=approval_granted,
        )
        if profile_decision is not None:
            return MirrorActionPlan(
                action=action,
                surface=surface,
                resolved_tool=tool_name,
                operation_class=operation_class,
                decision=profile_decision["decision"],
                reason_code=profile_decision["code"],
                reason=profile_decision["reason"],
            )

        connector_decision = self._check_connector_safety(
            tool_name=tool_name,
            surface=surface,
            operation_class=operation_class,
            approval_granted=approval_granted,
        )
        if connector_decision is not None:
            return MirrorActionPlan(
                action=action,
                surface=surface,
                resolved_tool=tool_name,
                operation_class=operation_class,
                decision=connector_decision["decision"],
                reason_code=connector_decision["code"],
                reason=connector_decision["reason"],
            )

        return MirrorActionPlan(
            action=action,
            surface=surface,
            resolved_tool=tool_name,
            operation_class=operation_class,
        )

    def execute_mirror_action(
        self,
        *,
        plan: MirrorActionPlan,
        event: MirrorIngestEvent,
        agent: MirrorAgentSpec,
        approval_granted: bool = False,
    ) -> dict[str, Any]:
        if plan.action == "inject":
            return self._execute_mirror_inject(
                event=event,
                agent=agent,
            )
        return self._execute_mirror_dispatch(
            event=event,
            agent=agent,
            operation_class=plan.operation_class,
            approval_granted=approval_granted,
        )

    def mirror_connector_status(self) -> list[MirrorConnectorStatus]:
        mode = self.mirror_config.connector_mode
        checked_at = _iso_now()
        if mode != "live":
            return [
                MirrorConnectorStatus(
                    surface="slack",
                    source_mode="sim",
                    availability="healthy",
                    write_capability="interactive",
                    reason="Simulated Slack surface is interactive.",
                    last_checked_at=checked_at,
                ),
                MirrorConnectorStatus(
                    surface="jira",
                    source_mode="sim",
                    availability="healthy",
                    write_capability="interactive",
                    reason="Simulated Jira surface is interactive.",
                    last_checked_at=checked_at,
                ),
                MirrorConnectorStatus(
                    surface="graph",
                    source_mode="sim",
                    availability="healthy",
                    write_capability="interactive",
                    reason="Simulated Graph surface is interactive.",
                    last_checked_at=checked_at,
                ),
                MirrorConnectorStatus(
                    surface="salesforce",
                    source_mode="sim",
                    availability="healthy",
                    write_capability="interactive",
                    reason="Simulated Salesforce surface is interactive.",
                    last_checked_at=checked_at,
                ),
            ]

        slack_token = os.environ.get("VEI_LIVE_SLACK_TOKEN", "").strip()
        slack_status = MirrorConnectorStatus(
            surface="slack",
            source_mode="live",
            availability="healthy" if slack_token else "degraded",
            write_capability="interactive" if slack_token else "unsupported",
            reason=(
                "Live Slack passthrough is available."
                if slack_token
                else "Set VEI_LIVE_SLACK_TOKEN to enable live Slack passthrough."
            ),
            last_checked_at=checked_at,
        )
        return [
            slack_status,
            MirrorConnectorStatus(
                surface="jira",
                source_mode="live",
                availability="healthy",
                write_capability="read_only",
                reason="Live Jira compatibility stays read-only in this milestone.",
                last_checked_at=checked_at,
            ),
            MirrorConnectorStatus(
                surface="graph",
                source_mode="live",
                availability="healthy",
                write_capability="read_only",
                reason="Live Graph compatibility stays read-only in this milestone.",
                last_checked_at=checked_at,
            ),
            MirrorConnectorStatus(
                surface="salesforce",
                source_mode="live",
                availability="healthy",
                write_capability="read_only",
                reason=(
                    "Live Salesforce compatibility stays read-only in this milestone."
                ),
                last_checked_at=checked_at,
            ),
        ]

    def _check_surface_access(
        self,
        agent: MirrorAgentSpec,
        surface: str,
    ) -> str | None:
        if not agent.allowed_surfaces:
            return None
        normalized = _normalize_surface(surface)
        for allowed_surface in agent.allowed_surfaces:
            if normalized in _surface_alias_set(allowed_surface):
                return None
        return (
            f"agent '{agent.agent_id}' denied access to surface '{surface}' "
            f"(allowed: {', '.join(agent.allowed_surfaces)})"
        )

    def _check_policy_profile(
        self,
        *,
        agent: MirrorAgentSpec,
        operation_class: str,
        approval_granted: bool,
    ) -> dict[str, str] | None:
        if approval_granted:
            return None
        profile_id = str(agent.policy_profile_id or "admin").strip().lower() or "admin"
        profile_rules = _PROFILE_ACTIONS.get(profile_id, _PROFILE_ACTIONS["admin"])
        action = profile_rules.get(operation_class, "deny")
        if action == "allow":
            return None
        if action == "require_approval":
            return {
                "decision": "approval_required",
                "code": "mirror.approval_required",
                "reason": (
                    f"policy profile '{profile_id}' requires approval for "
                    f"{operation_class.replace('_', ' ')} actions"
                ),
            }
        return {
            "decision": "deny",
            "code": "mirror.profile_denied",
            "reason": (
                f"policy profile '{profile_id}' does not allow "
                f"{operation_class.replace('_', ' ')} actions"
            ),
        }

    def _check_connector_safety(
        self,
        *,
        tool_name: str,
        surface: str,
        operation_class: str,
        approval_granted: bool,
    ) -> dict[str, str] | None:
        if self.mirror_config.connector_mode != "live":
            return None
        if surface == "service_ops":
            return None
        if surface == "slack":
            if not os.environ.get("VEI_LIVE_SLACK_TOKEN", "").strip():
                return {
                    "decision": "deny",
                    "code": "mirror.connector_degraded",
                    "reason": "Live Slack passthrough is not configured in this environment.",
                }
            blocked = _blocked_live_operations()
            if tool_name in blocked:
                return {
                    "decision": "deny",
                    "code": "mirror.unsupported_live_write",
                    "reason": f"Live policy blocks the operation '{tool_name}'.",
                }
            if operation_class == "read":
                return None
            if operation_class == "write_safe":
                if approval_granted or _env_bool("VEI_LIVE_ALLOW_WRITE_SAFE"):
                    return None
                return {
                    "decision": "approval_required",
                    "code": "mirror.approval_required",
                    "reason": "Live safe-write requires approval in this workspace.",
                }
            if _env_bool("VEI_LIVE_ALLOW_WRITE_RISKY"):
                return None
            return {
                "decision": "deny",
                "code": "mirror.unsupported_live_write",
                "reason": "Live risky writes are blocked in this workspace.",
            }
        if operation_class == "read":
            return None
        return {
            "decision": "deny",
            "code": "mirror.unsupported_live_write",
            "reason": (
                f"Live writes are not enabled for the '{surface}' surface in this workspace."
            ),
        }

    def _record_denial(
        self,
        event: MirrorIngestEvent,
        agent: MirrorAgentSpec,
        reason: str,
    ) -> None:
        time_ms = self._current_time_ms()
        self._append_event(
            kind="mirror_denied",
            label=f"denied: {event.external_tool}",
            channel=_channel_for_focus(str(event.focus_hint or "browser")),
            time_ms=time_ms,
            status="denied",
            tool=event.external_tool,
            resolved_tool=event.resolved_tool,
            payload={
                "agent_id": agent.agent_id,
                "agent_name": agent.name,
                "reason": reason,
                "attempted_tool": event.resolved_tool or event.external_tool,
            },
        )

    def record_mirror_denial(
        self,
        *,
        event: MirrorIngestEvent,
        agent: MirrorAgentSpec,
        reason: str,
    ) -> None:
        with self._lock:
            self._record_denial(event, agent, reason)
            self._write_manifest(
                status="running", success=None, error=None, completed_at=None
            )

    def _current_time_ms(self) -> int:
        bus = getattr(getattr(self.session, "router", None), "bus", None)
        if bus is None:
            return 0
        return int(getattr(bus, "clock_ms", 0) or 0)

    def _execute_mirror_dispatch(
        self,
        *,
        event: MirrorIngestEvent,
        agent: MirrorAgentSpec,
        operation_class: str,
        approval_granted: bool,
    ) -> dict[str, Any]:
        with self._approval_override(
            tool_name=str(event.resolved_tool or event.external_tool),
            operation_class=operation_class,
            approval_granted=approval_granted,
        ):
            result = self.dispatch(
                external_tool=event.external_tool,
                resolved_tool=str(event.resolved_tool or ""),
                args=dict(event.args),
                focus_hint=str(event.focus_hint or "browser"),
                agent=_identity_from_mirror_agent(agent),
            )
        return {"result": result}

    def _execute_mirror_inject(
        self,
        *,
        event: MirrorIngestEvent,
        agent: MirrorAgentSpec,
    ) -> dict[str, Any]:
        target = str(event.target or "")
        if not target:
            raise ValueError("mirror inject events require a target")
        with self._lock:
            injected = self.session.inject(
                {
                    "target": target,
                    "payload": dict(event.payload),
                    "source": "mirror_ingest",
                    "actor_id": agent.agent_id,
                }
            )
            ticked = self.session.call_tool("vei.tick", {"dt_ms": 0})
            observation = self.session.observe(event.focus_hint or target)
            snapshot = self.session.snapshot(f"mirror:{event.external_tool}")
            contract_eval = self._evaluate(
                snapshot,
                visible_observation=observation,
                result={"inject": injected, "tick": ticked},
            )
            self.status.latest_snapshot_id = snapshot.snapshot_id
            self.status.request_count += 1
            self._record_agent_identity(_identity_from_mirror_agent(agent))
            self._update_contract_status(contract_eval)
            self._write_contract_eval(contract_eval)
            self._write_manifest(
                status="running", success=None, error=None, completed_at=None
            )
            self._append_event(
                kind="workflow_step",
                label=event.label or event.external_tool,
                channel=_channel_for_focus(event.focus_hint or target),
                time_ms=snapshot.time_ms,
                tool=event.external_tool,
                resolved_tool=f"inject:{target}",
                object_refs=_object_refs(event.payload, {}),
                payload={
                    "payload": dict(event.payload),
                    "inject": injected,
                    "tick": ticked,
                    "agent": _identity_from_mirror_agent(agent).model_dump(mode="json"),
                },
            )
            self._append_snapshot_event(
                f"mirror:{event.external_tool}",
                snapshot.snapshot_id,
                snapshot.time_ms,
            )
            self._append_contract_event(contract_eval, snapshot.time_ms)
            return {"inject": injected, "tick": ticked}

    @contextmanager
    def _approval_override(
        self,
        *,
        tool_name: str,
        operation_class: str,
        approval_granted: bool,
    ):
        if not approval_granted or self.mirror_config.connector_mode != "live":
            yield
            return
        route = TOOL_ROUTES.get(tool_name)
        connector_runtime = getattr(
            getattr(self.session, "router", None), "connector_runtime", None
        )
        policy_gate = getattr(connector_runtime, "policy_gate", None)
        if route is None or policy_gate is None:
            yield
            return

        safe_before = getattr(policy_gate, "live_allow_write_safe", None)
        risky_before = getattr(policy_gate, "live_allow_write_risky", None)
        try:
            if operation_class == "write_safe" and safe_before is not None:
                policy_gate.live_allow_write_safe = True
            if operation_class == "write_risky" and risky_before is not None:
                policy_gate.live_allow_write_risky = True
            yield
        finally:
            if safe_before is not None:
                policy_gate.live_allow_write_safe = safe_before
            if risky_before is not None:
                policy_gate.live_allow_write_risky = risky_before

    def record_mirror_event(
        self,
        *,
        event: MirrorIngestEvent,
        agent: MirrorAgentSpec,
    ) -> dict[str, Any]:
        with self._lock:
            snapshot = self.session.snapshot(f"mirror:{event.external_tool}")
            contract_eval = self._evaluate(
                snapshot,
                result={"payload": dict(event.payload)},
            )
            self.status.latest_snapshot_id = snapshot.snapshot_id
            self.status.request_count += 1
            self._record_agent_identity(_identity_from_mirror_agent(agent))
            self._update_contract_status(contract_eval)
            self._write_contract_eval(contract_eval)
            self._write_manifest(
                status="running", success=None, error=None, completed_at=None
            )
            self._append_event(
                kind="trace_event",
                label=event.label or event.external_tool,
                channel=_channel_for_focus(event.focus_hint or "browser"),
                time_ms=snapshot.time_ms,
                tool=event.external_tool,
                object_refs=_object_refs(event.payload, {}),
                payload={
                    "payload": dict(event.payload),
                    "agent": _identity_from_mirror_agent(agent).model_dump(mode="json"),
                },
            )
            self._append_snapshot_event(
                f"mirror:{event.external_tool}",
                snapshot.snapshot_id,
                snapshot.time_ms,
            )
            self._append_contract_event(contract_eval, snapshot.time_ms)
            return {"recorded": True}

    def _append_contract_event(
        self, contract_eval: ContractEvaluationResult, time_ms: int
    ) -> None:
        issues = len(contract_eval.dynamic_validation.issues) + len(
            contract_eval.static_validation.issues
        )
        append_run_event(
            self.events_path,
            RunTimelineEvent(
                index=0,
                kind="contract",
                label="external session contract updated",
                channel="World",
                time_ms=time_ms,
                runner="external",
                branch=self.branch_name,
                payload={
                    "ok": contract_eval.ok,
                    "issue_count": issues,
                },
            ),
        )


def create_twin_gateway_app(root: str | Path) -> FastAPI:
    workspace_root = Path(root).expanduser().resolve()
    bundle = load_customer_twin(workspace_root)
    runtime = TwinRuntime(workspace_root, bundle)

    @asynccontextmanager
    async def _lifespan(_: FastAPI):
        try:
            runtime.start_mirror()
            yield
        finally:
            runtime.finalize()

    app = FastAPI(
        title="VEI Customer Twin Gateway",
        version="1",
        lifespan=_lifespan,
    )
    app.state.runtime = runtime

    @app.get("/")
    def root_index() -> JSONResponse:
        return JSONResponse(
            {
                "organization_name": bundle.organization_name,
                "organization_domain": bundle.organization_domain,
                "surfaces": [
                    item.model_dump(mode="json") for item in bundle.gateway.surfaces
                ],
                "status_path": "/api/twin",
            }
        )

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        return JSONResponse({"ok": True, "run_id": runtime.run_id})

    @app.get("/api/twin")
    def api_twin() -> JSONResponse:
        return JSONResponse(runtime.status_payload())

    @app.get("/api/mirror")
    def api_mirror(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        return JSONResponse(runtime._mirror_snapshot_payload())

    @app.get("/api/mirror/agents")
    def api_mirror_agents(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            return JSONResponse({"agents": []})
        agents = [item.model_dump(mode="json") for item in runtime.mirror.list_agents()]
        return JSONResponse({"agents": agents})

    @app.post("/api/mirror/agents")
    async def api_mirror_register_agent(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            raise HTTPException(status_code=503, detail="mirror runtime unavailable")
        body = await _request_payload(request)
        agent = runtime.mirror.register_agent(MirrorAgentSpec.model_validate(body))
        return JSONResponse(agent.model_dump(mode="json"), status_code=201)

    @app.patch("/api/mirror/agents/{agent_id}")
    async def api_mirror_update_agent(agent_id: str, request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            raise HTTPException(status_code=503, detail="mirror runtime unavailable")
        body = await _request_payload(request)
        try:
            agent = runtime.mirror.update_agent(agent_id, dict(body))
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return JSONResponse(agent.model_dump(mode="json"))

    @app.delete("/api/mirror/agents/{agent_id}")
    def api_mirror_remove_agent(agent_id: str, request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            raise HTTPException(status_code=503, detail="mirror runtime unavailable")
        try:
            agent = runtime.mirror.remove_agent(agent_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return JSONResponse(agent.model_dump(mode="json"))

    @app.get("/api/mirror/approvals")
    def api_mirror_approvals(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            return JSONResponse({"approvals": []})
        approvals = [
            item.model_dump(mode="json")
            for item in runtime.mirror.list_pending_approvals()
        ]
        return JSONResponse({"approvals": approvals})

    @app.post("/api/mirror/approvals/{approval_id}/approve")
    async def api_mirror_approve(
        approval_id: str,
        request: Request,
    ) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            raise HTTPException(status_code=503, detail="mirror runtime unavailable")
        body = await _request_payload(request)
        resolver_agent_id = str(body.get("resolver_agent_id") or "").strip()
        if not resolver_agent_id:
            raise HTTPException(
                status_code=400,
                detail="resolver_agent_id is required to approve mirror actions",
            )
        try:
            approval = runtime.mirror.resolve_approval(
                approval_id=approval_id,
                resolver_agent_id=resolver_agent_id,
                action="approve",
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(approval.model_dump(mode="json"))

    @app.post("/api/mirror/approvals/{approval_id}/reject")
    async def api_mirror_reject(
        approval_id: str,
        request: Request,
    ) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            raise HTTPException(status_code=503, detail="mirror runtime unavailable")
        body = await _request_payload(request)
        resolver_agent_id = str(body.get("resolver_agent_id") or "").strip()
        if not resolver_agent_id:
            raise HTTPException(
                status_code=400,
                detail="resolver_agent_id is required to reject mirror actions",
            )
        try:
            approval = runtime.mirror.resolve_approval(
                approval_id=approval_id,
                resolver_agent_id=resolver_agent_id,
                action="reject",
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(approval.model_dump(mode="json"))

    @app.post("/api/mirror/events")
    async def api_mirror_ingest_event(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            raise HTTPException(status_code=503, detail="mirror runtime unavailable")
        body = await _request_payload(request)
        event = MirrorIngestEvent.model_validate(body).model_copy(
            update={"source_mode": "ingest"}
        )
        try:
            result = runtime.mirror.ingest_event(event)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "mirror.agent_not_registered",
                    "message": str(exc),
                },
            ) from exc
        return JSONResponse(result.model_dump(mode="json"), status_code=202)

    @app.post("/api/mirror/demo/tick")
    def api_mirror_demo_tick(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            raise HTTPException(status_code=503, detail="mirror runtime unavailable")
        result = runtime.mirror.demo_tick()
        if result is None:
            return JSONResponse({"ok": True, "remaining_demo_steps": 0})
        return JSONResponse(result.model_dump(mode="json"))

    @app.get("/api/twin/history")
    def api_twin_history() -> JSONResponse:
        payload = [
            item.model_dump(mode="json")
            for item in build_run_timeline(workspace_root, runtime.run_id)
        ]
        return JSONResponse(payload)

    @app.get("/api/twin/surfaces")
    def api_twin_surfaces() -> JSONResponse:
        payload = get_run_surface_state(workspace_root, runtime.run_id)
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/twin/finalize")
    def api_twin_finalize() -> JSONResponse:
        runtime.finalize()
        return JSONResponse(runtime.status_payload())

    @app.get("/slack/api/conversations.list")
    async def slack_conversations_list(request: Request) -> JSONResponse:
        if not _slack_auth_ok(request, bundle.gateway.auth_token):
            return JSONResponse({"ok": False, "error": "invalid_auth"})
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="slack.conversations.list",
                resolved_tool="slack.list_channels",
                args={},
                focus_hint="slack",
            )
        except Exception as exc:  # noqa: BLE001
            return _mirror_route_error_response(exc, surface="slack")
        channels = payload if isinstance(payload, list) else payload.get("channels", [])
        return JSONResponse(
            {"ok": True, "channels": [_slack_channel(channel) for channel in channels]}
        )

    @app.get("/slack/api/conversations.history")
    async def slack_conversations_history(request: Request) -> JSONResponse:
        if not _slack_auth_ok(request, bundle.gateway.auth_token):
            return JSONResponse({"ok": False, "error": "invalid_auth"})
        channel_arg = request.query_params.get("channel", "")
        channel_name = _resolve_slack_channel_name(runtime, channel_arg)
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="slack.conversations.history",
                resolved_tool="slack.open_channel",
                args={"channel": channel_name},
                focus_hint="slack",
            )
        except Exception as exc:  # noqa: BLE001
            return _mirror_route_error_response(exc, surface="slack")
        messages = payload.get("messages", []) if isinstance(payload, dict) else []
        return JSONResponse(
            {
                "ok": True,
                "messages": [
                    _slack_message(channel_name, message) for message in messages
                ],
                "has_more": False,
            }
        )

    @app.get("/slack/api/conversations.replies")
    async def slack_conversations_replies(request: Request) -> JSONResponse:
        if not _slack_auth_ok(request, bundle.gateway.auth_token):
            return JSONResponse({"ok": False, "error": "invalid_auth"})
        channel_name = _resolve_slack_channel_name(
            runtime, request.query_params.get("channel", "")
        )
        thread_ts = request.query_params.get("ts", "")
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="slack.conversations.replies",
                resolved_tool="slack.fetch_thread",
                args={"channel": channel_name, "thread_ts": thread_ts},
                focus_hint="slack",
            )
        except Exception as exc:  # noqa: BLE001
            return _mirror_route_error_response(exc, surface="slack")
        messages = payload.get("messages", []) if isinstance(payload, dict) else []
        return JSONResponse(
            {
                "ok": True,
                "messages": [
                    _slack_message(channel_name, message) for message in messages
                ],
            }
        )

    @app.post("/slack/api/chat.postMessage")
    async def slack_post_message(request: Request) -> JSONResponse:
        if not _slack_auth_ok(request, bundle.gateway.auth_token):
            return JSONResponse({"ok": False, "error": "invalid_auth"})
        body = await _request_payload(request)
        channel_name = _resolve_slack_channel_name(
            runtime, str(body.get("channel", ""))
        )
        args = {
            "channel": channel_name,
            "text": str(body.get("text", "")),
            "thread_ts": body.get("thread_ts"),
        }
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="slack.chat.postMessage",
                resolved_tool="slack.send_message",
                args=args,
                focus_hint="slack",
            )
        except Exception as exc:  # noqa: BLE001
            return _mirror_route_error_response(exc, surface="slack")
        ts = str(payload.get("ts", ""))
        return JSONResponse(
            {
                "ok": True,
                "channel": _slack_channel_id(channel_name),
                "ts": ts,
                "message": {
                    "type": "message",
                    "text": str(args["text"]),
                    "user": _slack_user_id("agent"),
                    "ts": ts,
                },
            }
        )

    @app.get("/jira/rest/api/3/project")
    async def jira_projects(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        project_key = _jira_project_key(runtime)
        return JSONResponse(
            [{"id": project_key, "key": project_key, "name": bundle.organization_name}]
        )

    @app.get("/jira/rest/api/3/search")
    async def jira_search_get(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        return JSONResponse(_jira_search(runtime, request, request.query_params))

    @app.post("/jira/rest/api/3/search")
    async def jira_search_post(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        body = await _request_payload(request)
        return JSONResponse(_jira_search(runtime, request, body))

    @app.get("/jira/rest/api/3/issue/{issue_id}")
    async def jira_issue(request: Request, issue_id: str) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="jira.issue.get",
                resolved_tool="jira.get_issue",
                args={"issue_id": issue_id},
                focus_hint="tickets",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        return JSONResponse(_jira_issue(payload))

    @app.get("/jira/rest/api/3/issue/{issue_id}/transitions")
    async def jira_issue_transitions(request: Request, issue_id: str) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="jira.issue.get",
                resolved_tool="jira.get_issue",
                args={"issue_id": issue_id},
                focus_hint="tickets",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        status = str(payload.get("status", "open"))
        return JSONResponse({"transitions": _jira_transitions(status)})

    @app.post("/jira/rest/api/3/issue/{issue_id}/comment")
    async def jira_add_comment(request: Request, issue_id: str) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        body = await _request_payload(request)
        args = {"issue_id": issue_id, "body": str(body.get("body", ""))}
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="jira.issue.comment",
                resolved_tool="jira.add_comment",
                args=args,
                focus_hint="tickets",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        return JSONResponse(
            {"id": payload.get("comment_id"), "body": body.get("body", "")},
            status_code=201,
        )

    @app.post("/jira/rest/api/3/issue/{issue_id}/transitions")
    async def jira_transition(request: Request, issue_id: str) -> Response:
        _require_bearer(request, bundle.gateway.auth_token)
        body = await _request_payload(request)
        transition = (
            body.get("transition", {})
            if isinstance(body.get("transition"), dict)
            else {}
        )
        status = transition.get("id") or transition.get("name") or body.get("status")
        try:
            _dispatch_request(
                runtime,
                request,
                external_tool="jira.issue.transition",
                resolved_tool="jira.transition_issue",
                args={"issue_id": issue_id, "status": str(status or "")},
                focus_hint="tickets",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        return Response(status_code=204)

    @app.get("/graph/v1.0/me/messages")
    async def graph_messages(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="graph.messages.list",
                resolved_tool="mail.list",
                args={"folder": "INBOX"},
                focus_hint="mail",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        messages = payload if isinstance(payload, list) else payload.get("messages", [])
        return JSONResponse(
            {"value": [_graph_message_summary(message) for message in messages]}
        )

    @app.get("/graph/v1.0/me/messages/{message_id}")
    async def graph_message(request: Request, message_id: str) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        try:
            summary = _dispatch_request(
                runtime,
                request,
                external_tool="graph.messages.get",
                resolved_tool="mail.open",
                args={"id": message_id},
                focus_hint="mail",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        listing = runtime.peek("mail.list", {"folder": "INBOX"})
        message = _find_mail_message(listing, message_id)
        return JSONResponse(_graph_message(message, summary))

    @app.post("/graph/v1.0/me/sendMail")
    async def graph_send_mail(request: Request) -> Response:
        _require_bearer(request, bundle.gateway.auth_token)
        body = await _request_payload(request)
        message = (
            body.get("message", {}) if isinstance(body.get("message"), dict) else {}
        )
        to_address = _graph_first_recipient(message.get("toRecipients"))
        subject = str(message.get("subject", ""))
        body_content = _graph_body_content(message.get("body"))
        try:
            _dispatch_request(
                runtime,
                request,
                external_tool="graph.messages.send",
                resolved_tool="mail.compose",
                args={"to": to_address, "subj": subject, "body_text": body_content},
                focus_hint="mail",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        return Response(status_code=202)

    @app.get("/graph/v1.0/me/events")
    async def graph_events(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="graph.events.list",
                resolved_tool="calendar.list_events",
                args={},
                focus_hint="calendar",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        events = payload if isinstance(payload, list) else payload.get("events", [])
        return JSONResponse({"value": [_graph_event(event) for event in events]})

    @app.post("/graph/v1.0/me/events")
    async def graph_create_event(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        body = await _request_payload(request)
        args = {
            "title": str(body.get("subject", "Untitled")),
            "start_ms": _graph_datetime_to_ms(
                (body.get("start") or {}).get("dateTime")
            ),
            "end_ms": _graph_datetime_to_ms((body.get("end") or {}).get("dateTime")),
            "attendees": _graph_attendees(body.get("attendees")),
            "location": ((body.get("location") or {}).get("displayName") or None),
            "description": _graph_body_content(body.get("body")),
            "organizer": _graph_email_address(
                (body.get("organizer") or {}).get("emailAddress")
            ),
        }
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="graph.events.create",
                resolved_tool="calendar.create_event",
                args=args,
                focus_hint="calendar",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        return JSONResponse({"id": payload.get("event_id")}, status_code=201)

    @app.get("/salesforce/services/data/v60.0/query")
    async def salesforce_query(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        query = request.query_params.get("q", "")
        return JSONResponse(_salesforce_query(runtime, request, query))

    @app.get("/salesforce/services/data/v60.0/sobjects/Opportunity/{record_id}")
    async def salesforce_opportunity_get(
        request: Request, record_id: str
    ) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="salesforce.opportunity.get",
                resolved_tool="salesforce.opportunity.get",
                args={"id": record_id},
                focus_hint="crm",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        return JSONResponse(_salesforce_opportunity(payload))

    @app.post("/salesforce/services/data/v60.0/sobjects/Opportunity")
    async def salesforce_opportunity_create(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        body = await _request_payload(request)
        args = {
            "name": str(body.get("Name", "")),
            "amount": float(body.get("Amount", 0) or 0),
            "stage": str(body.get("StageName", "New")),
            "contact_id": body.get("ContactId"),
            "company_id": body.get("AccountId"),
            "close_date": body.get("CloseDate"),
        }
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="salesforce.opportunity.create",
                resolved_tool="salesforce.opportunity.create",
                args=args,
                focus_hint="crm",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        return JSONResponse(
            {"id": payload.get("id"), "success": True, "errors": []}, status_code=201
        )

    @app.post("/salesforce/services/data/v60.0/sobjects/Task")
    async def salesforce_task_create(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        body = await _request_payload(request)
        args = {
            "kind": "task",
            "deal_id": body.get("WhatId"),
            "contact_id": body.get("WhoId"),
            "note": body.get("Description") or body.get("Subject") or "",
        }
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="salesforce.task.create",
                resolved_tool="salesforce.activity.log",
                args=args,
                focus_hint="crm",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        return JSONResponse(
            {"id": payload.get("id"), "success": True, "errors": []}, status_code=201
        )

    # --- Slack: reactions + users.list ------------------------------------------

    @app.post("/slack/api/reactions.add")
    async def slack_reactions_add(request: Request) -> JSONResponse:
        if not _slack_auth_ok(request, bundle.gateway.auth_token):
            return JSONResponse({"ok": False, "error": "invalid_auth"})
        body = await _request_payload(request)
        channel_name = _resolve_slack_channel_name(
            runtime, str(body.get("channel", ""))
        )
        args = {
            "channel": channel_name,
            "text": f":{body.get('name', 'thumbsup')}:",
            "thread_ts": body.get("timestamp"),
        }
        try:
            _dispatch_request(
                runtime,
                request,
                external_tool="slack.reactions.add",
                resolved_tool="slack.send_message",
                args=args,
                focus_hint="slack",
            )
        except Exception as exc:  # noqa: BLE001
            return _mirror_route_error_response(exc, surface="slack")
        return JSONResponse({"ok": True})

    @app.get("/slack/api/users.list")
    async def slack_users_list(request: Request) -> JSONResponse:
        if not _slack_auth_ok(request, bundle.gateway.auth_token):
            return JSONResponse({"ok": False, "error": "invalid_auth"})
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="slack.users.list",
                resolved_tool="okta.list_users",
                args={},
                focus_hint="slack",
            )
        except Exception as exc:  # noqa: BLE001
            return _mirror_route_error_response(exc, surface="slack")
        users = payload if isinstance(payload, list) else payload.get("users", [])
        members = [
            {
                "id": _slack_user_id(str(u.get("email", u.get("user_id", "")))),
                "name": str(u.get("login", u.get("email", ""))).split("@")[0],
                "real_name": u.get("display_name", u.get("first_name", "")),
                "profile": {
                    "email": u.get("email", ""),
                    "display_name": u.get("display_name", ""),
                    "title": u.get("title", ""),
                },
            }
            for u in users
        ]
        return JSONResponse({"ok": True, "members": members})

    # --- Jira: issue create + update -------------------------------------------

    @app.post("/jira/rest/api/3/issue")
    async def jira_create_issue(request: Request) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        body = await _request_payload(request)
        fields = body.get("fields", body)
        args = {
            "title": str(fields.get("summary", "")),
            "description": str(fields.get("description", "")),
            "assignee": (
                (fields.get("assignee") or {}).get("name", "")
                if isinstance(fields.get("assignee"), dict)
                else str(fields.get("assignee", ""))
            ),
            "priority": (
                (fields.get("priority") or {}).get("name", "P3")
                if isinstance(fields.get("priority"), dict)
                else str(fields.get("priority", "P3"))
            ),
            "labels": fields.get("labels", []),
        }
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="jira.issue.create",
                resolved_tool="jira.create_issue",
                args=args,
                focus_hint="tickets",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        issue_id = str(payload.get("issue_id", payload.get("ticket_id", "")))
        return JSONResponse(
            {"id": issue_id, "key": issue_id, "self": f"/rest/api/3/issue/{issue_id}"},
            status_code=201,
        )

    @app.put("/jira/rest/api/3/issue/{issue_id}")
    async def jira_update_issue(request: Request, issue_id: str) -> Response:
        _require_bearer(request, bundle.gateway.auth_token)
        body = await _request_payload(request)
        fields = body.get("fields", body)
        args: dict[str, Any] = {"issue_id": issue_id}
        if "summary" in fields:
            args["title"] = str(fields["summary"])
        if "description" in fields:
            args["description"] = str(fields["description"])
        if "assignee" in fields:
            assignee = fields["assignee"]
            args["assignee"] = (
                assignee.get("name", "")
                if isinstance(assignee, dict)
                else str(assignee)
            )
        if "priority" in fields:
            priority = fields["priority"]
            args["priority"] = (
                priority.get("name", "P3")
                if isinstance(priority, dict)
                else str(priority)
            )
        if "labels" in fields:
            args["labels"] = fields["labels"]
        try:
            _dispatch_request(
                runtime,
                request,
                external_tool="jira.issue.update",
                resolved_tool="jira.update_issue",
                args=args,
                focus_hint="tickets",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        return Response(status_code=204)

    # --- Salesforce: opportunity PATCH + contact endpoints ----------------------

    @app.patch("/salesforce/services/data/v60.0/sobjects/Opportunity/{record_id}")
    async def salesforce_opportunity_patch(
        request: Request, record_id: str
    ) -> Response:
        _require_bearer(request, bundle.gateway.auth_token)
        body = await _request_payload(request)
        args: dict[str, Any] = {"id": record_id}
        if "StageName" in body:
            args["stage"] = str(body["StageName"])
        if "Amount" in body:
            args["amount"] = float(body["Amount"] or 0)
        if "Name" in body:
            args["name"] = str(body["Name"])
        if "CloseDate" in body:
            args["close_date"] = body["CloseDate"]
        try:
            _dispatch_request(
                runtime,
                request,
                external_tool="salesforce.opportunity.update",
                resolved_tool="salesforce.opportunity.update",
                args=args,
                focus_hint="crm",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        return Response(status_code=204)

    @app.get("/salesforce/services/data/v60.0/sobjects/Contact/{record_id}")
    async def salesforce_contact_get(request: Request, record_id: str) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="salesforce.contact.get",
                resolved_tool="salesforce.contact.get",
                args={"id": record_id},
                focus_hint="crm",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        return JSONResponse(_salesforce_contact(payload))

    @app.get("/salesforce/services/data/v60.0/sobjects/Account/{record_id}")
    async def salesforce_account_get(request: Request, record_id: str) -> JSONResponse:
        _require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="salesforce.account.get",
                resolved_tool="salesforce.account.get",
                args={"id": record_id},
                focus_hint="crm",
            )
        except Exception as exc:  # noqa: BLE001
            raise _http_exception(exc) from exc
        return JSONResponse(_salesforce_account(payload))

    return app


def _slack_auth_ok(request: Request, token: str) -> bool:
    auth = request.headers.get("authorization", "")
    return auth == f"Bearer {token}"


def _require_bearer(request: Request, token: str) -> None:
    if request.headers.get("authorization", "") == f"Bearer {token}":
        return
    raise HTTPException(status_code=401, detail="invalid bearer token")


async def _request_payload(request: Request) -> dict[str, Any]:
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        payload = await request.json()
        return payload if isinstance(payload, dict) else {}
    if (
        "application/x-www-form-urlencoded" in content_type
        or "multipart/form-data" in content_type
    ):
        form = await request.form()
        return dict(form)
    return {}


def _slack_channel(channel: str) -> dict[str, Any]:
    name = channel[1:] if channel.startswith("#") else channel
    return {
        "id": _slack_channel_id(channel),
        "name": name,
        "is_channel": True,
        "is_member": True,
    }


def _slack_channel_id(channel: str) -> str:
    digest = hashlib.sha1(channel.encode("utf-8"), usedforsecurity=False)
    return "C" + digest.hexdigest()[:8].upper()


def _slack_user_id(user: str) -> str:
    digest = hashlib.sha1(user.encode("utf-8"), usedforsecurity=False)
    return "U" + digest.hexdigest()[:8].upper()


def _resolve_slack_channel_name(runtime: TwinRuntime, value: str) -> str:
    if value.startswith("#"):
        return value
    channels = runtime.peek("slack.list_channels", {})
    if isinstance(channels, list):
        for channel in channels:
            if _slack_channel_id(str(channel)) == value:
                return str(channel)
    if value:
        return f"#{value}"
    raise HTTPException(status_code=400, detail="channel is required")


def _slack_message(channel: str, message: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "message",
        "user": _slack_user_id(str(message.get("user", "unknown"))),
        "username": str(message.get("user", "unknown")),
        "text": str(message.get("text", "")),
        "ts": str(message.get("ts", "")),
        "thread_ts": message.get("thread_ts"),
        "channel": _slack_channel_id(channel),
    }


def _jira_search(
    runtime: TwinRuntime,
    request: Request,
    params: Any,
) -> dict[str, Any]:
    jql = str(params.get("jql", ""))
    max_results = int(params.get("maxResults", params.get("max_results", 25)) or 25)
    start_at = int(params.get("startAt", params.get("start_at", 0)) or 0)
    args: dict[str, Any] = {"limit": max_results}
    status = _extract_jql_value(jql, "status")
    assignee = _extract_jql_value(jql, "assignee")
    if status:
        args["status"] = status
    if assignee:
        args["assignee"] = assignee
    try:
        payload = _dispatch_request(
            runtime,
            request,
            external_tool="jira.search",
            resolved_tool="jira.list_issues",
            args=args,
            focus_hint="tickets",
        )
    except Exception as exc:  # noqa: BLE001
        raise _http_exception(exc) from exc
    issues = payload if isinstance(payload, list) else payload.get("issues", [])
    sliced = issues[start_at : start_at + max_results]
    return {
        "startAt": start_at,
        "maxResults": max_results,
        "total": len(issues),
        "issues": [_jira_issue(issue) for issue in sliced],
    }


def _jira_issue(issue: dict[str, Any]) -> dict[str, Any]:
    issue_id = str(issue.get("issue_id", issue.get("ticket_id", "")))
    return {
        "id": issue_id,
        "key": issue_id,
        "fields": {
            "summary": issue.get("title", ""),
            "description": issue.get("description", ""),
            "status": {"name": issue.get("status", "open")},
            "assignee": {"displayName": issue.get("assignee") or "unassigned"},
            "priority": {"name": issue.get("priority", "P3")},
            "labels": issue.get("labels", []),
            "comment": {"total": issue.get("comment_count", 0)},
        },
    }


def _jira_transitions(status: str) -> list[dict[str, Any]]:
    allowed = {
        "open": ["in_progress", "blocked", "resolved", "closed"],
        "in_progress": ["blocked", "resolved", "closed"],
        "blocked": ["open", "in_progress", "resolved", "closed"],
        "resolved": ["closed", "open", "in_progress"],
        "closed": ["open"],
    }
    return [{"id": item, "name": item} for item in allowed.get(status.lower(), [])]


def _extract_jql_value(jql: str, key: str) -> str | None:
    pattern = re.compile(rf"{key}\s*=\s*['\"]?([^'\"]+)['\"]?", re.IGNORECASE)
    match = pattern.search(jql)
    if not match:
        return None
    return match.group(1).strip()


def _graph_message_summary(message: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": message.get("id", ""),
        "subject": message.get("subj", ""),
        "from": {
            "emailAddress": {
                "address": message.get("from", ""),
                "name": message.get("from", ""),
            }
        },
        "toRecipients": [
            {
                "emailAddress": {
                    "address": message.get("to", ""),
                    "name": message.get("to", ""),
                }
            }
        ],
        "bodyPreview": message.get("body_text", ""),
        "isRead": not bool(message.get("unread", False)),
        "receivedDateTime": _ms_to_iso(int(message.get("time", 0) or 0)),
    }


def _graph_message(message: dict[str, Any], opened: dict[str, Any]) -> dict[str, Any]:
    summary = _graph_message_summary(message)
    summary["body"] = {
        "contentType": "text",
        "content": opened.get("body_text", ""),
    }
    return summary


def _find_mail_message(payload: Any, message_id: str) -> dict[str, Any]:
    messages = payload if isinstance(payload, list) else payload.get("messages", [])
    for message in messages:
        if str(message.get("id", "")) == message_id:
            return dict(message)
    return {"id": message_id}


def _graph_first_recipient(payload: Any) -> str:
    if not isinstance(payload, list) or not payload:
        return ""
    first = payload[0]
    if not isinstance(first, dict):
        return ""
    return _graph_email_address(first.get("emailAddress"))


def _graph_email_address(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("address", ""))


def _graph_body_content(payload: Any) -> str:
    if isinstance(payload, str):
        return payload
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("content", ""))


def _graph_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": event.get("event_id", ""),
        "subject": event.get("title", ""),
        "start": {"dateTime": _ms_to_iso(int(event.get("start_ms", 0) or 0))},
        "end": {"dateTime": _ms_to_iso(int(event.get("end_ms", 0) or 0))},
        "attendees": [
            {"emailAddress": {"address": item, "name": item}}
            for item in event.get("attendees", [])
        ],
        "organizer": {
            "emailAddress": {
                "address": event.get("organizer", ""),
                "name": event.get("organizer", ""),
            }
        },
        "location": {"displayName": event.get("location", "")},
        "bodyPreview": event.get("description", ""),
        "isCancelled": str(event.get("status", "")).upper() == "CANCELED",
    }


def _graph_datetime_to_ms(value: Any) -> int:
    if not value:
        return int(datetime.now(UTC).timestamp() * 1000)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def _graph_attendees(payload: Any) -> list[str]:
    if not isinstance(payload, list):
        return []
    result: list[str] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        address = _graph_email_address(item.get("emailAddress"))
        if address:
            result.append(address)
    return result


def _salesforce_query(
    runtime: TwinRuntime,
    request: Request,
    query: str,
) -> dict[str, Any]:
    lowered = query.lower()
    limit_match = re.search(r"limit\s+(\d+)", lowered)
    limit = int(limit_match.group(1)) if limit_match else 25
    try:
        if "from opportunity" in lowered:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="salesforce.query.opportunity",
                resolved_tool="salesforce.opportunity.list",
                args={"limit": limit},
                focus_hint="crm",
            )
            rows = payload if isinstance(payload, list) else payload.get("deals", [])
            records = [_salesforce_opportunity(item) for item in rows[:limit]]
            return {"totalSize": len(records), "done": True, "records": records}
        if "from contact" in lowered:
            payload = _dispatch_request(
                runtime,
                request,
                external_tool="salesforce.query.contact",
                resolved_tool="salesforce.contact.list",
                args={"limit": limit},
                focus_hint="crm",
            )
            rows = payload if isinstance(payload, list) else payload.get("contacts", [])
            records = [_salesforce_contact(item) for item in rows[:limit]]
            return {"totalSize": len(records), "done": True, "records": records}
        payload = _dispatch_request(
            runtime,
            request,
            external_tool="salesforce.query.account",
            resolved_tool="salesforce.account.list",
            args={"limit": limit},
            focus_hint="crm",
        )
    except Exception as exc:  # noqa: BLE001
        raise _http_exception(exc) from exc
    rows = payload if isinstance(payload, list) else payload.get("companies", [])
    records = [_salesforce_account(item) for item in rows[:limit]]
    return {"totalSize": len(records), "done": True, "records": records}


def _salesforce_opportunity(payload: dict[str, Any]) -> dict[str, Any]:
    record_id = str(payload.get("id", ""))
    return {
        "attributes": {
            "type": "Opportunity",
            "url": f"/services/data/v60.0/sobjects/Opportunity/{record_id}",
        },
        "Id": record_id,
        "Name": payload.get("name", ""),
        "StageName": payload.get("stage", ""),
        "Amount": payload.get("amount", 0),
        "AccountId": payload.get("company_id"),
        "ContactId": payload.get("contact_id"),
    }


def _salesforce_contact(payload: dict[str, Any]) -> dict[str, Any]:
    record_id = str(payload.get("id", ""))
    return {
        "attributes": {
            "type": "Contact",
            "url": f"/services/data/v60.0/sobjects/Contact/{record_id}",
        },
        "Id": record_id,
        "Email": payload.get("email", ""),
        "FirstName": payload.get("first_name", ""),
        "LastName": payload.get("last_name", ""),
        "AccountId": payload.get("company_id"),
    }


def _salesforce_account(payload: dict[str, Any]) -> dict[str, Any]:
    record_id = str(payload.get("id", ""))
    return {
        "attributes": {
            "type": "Account",
            "url": f"/services/data/v60.0/sobjects/Account/{record_id}",
        },
        "Id": record_id,
        "Name": payload.get("name", ""),
        "Domain__c": payload.get("domain", ""),
    }


def _http_exception(exc: Exception) -> HTTPException:
    if isinstance(exc, MCPError):
        return HTTPException(
            status_code=_status_code_for_error(exc.code),
            detail={"code": exc.code, "message": exc.message},
        )
    if isinstance(exc, ValueError):
        return HTTPException(
            status_code=400,
            detail={"code": "invalid_args", "message": str(exc)},
        )
    return HTTPException(
        status_code=500,
        detail={"code": "operation_failed", "message": str(exc)},
    )


def _provider_error_code(exc: Exception) -> str:
    if isinstance(exc, MCPError):
        return exc.code
    if isinstance(exc, ValueError):
        return "invalid_args"
    return "operation_failed"


def _error_payload(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, MCPError):
        return {"code": exc.code, "message": exc.message}
    return {"code": exc.__class__.__name__.lower(), "message": str(exc)}


def _channel_for_focus(focus: str) -> str:
    mapping = {
        "slack": "Communication",
        "mail": "Communication",
        "tickets": "Work",
        "calendar": "Work",
        "crm": "Revenue",
    }
    return mapping.get(focus, "World")


def _object_refs(args: dict[str, Any], result: Any) -> list[str]:
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


def _snapshot_path(root: Path, run_id: str, snapshot_id: int) -> str | None:
    for item in list_run_snapshots(root, run_id):
        if item.snapshot_id == snapshot_id:
            return item.path
    return None


def _contract_summary(path: Path) -> RunContractSummary:
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


def _jira_project_key(runtime: TwinRuntime) -> str:
    payload = runtime.peek("jira.list_issues", {"limit": 1})
    issues = payload if isinstance(payload, list) else payload.get("issues", [])
    if not issues:
        return "VEI"
    issue_id = str(issues[0].get("issue_id", "VEI-1"))
    return issue_id.split("-", 1)[0]


def _ms_to_iso(value: int) -> str:
    if value <= 0:
        return datetime.now(UTC).isoformat()
    return datetime.fromtimestamp(value / 1000, tz=UTC).isoformat()


def _dispatch_request(
    runtime: TwinRuntime,
    request: Request,
    *,
    external_tool: str,
    resolved_tool: str,
    args: dict[str, Any],
    focus_hint: str,
) -> Any:
    agent = _request_agent_identity(request)
    if runtime.mirror is not None:
        if agent is None or not agent.agent_id:
            raise MCPError(
                "mirror.agent_id_required",
                "proxy requests must include X-VEI-Agent-Id",
            )
        return runtime.dispatch_proxy_request(
            external_tool=external_tool,
            resolved_tool=resolved_tool,
            args=args,
            focus_hint=focus_hint,
            agent=agent,
        )
    return runtime.dispatch(
        external_tool=external_tool,
        resolved_tool=resolved_tool,
        args=args,
        focus_hint=focus_hint,
        agent=agent,
    )


def _status_code_for_error(code: str) -> int:
    if code in {
        "mirror.surface_denied",
        "mirror.profile_denied",
        "mirror.mode_denied",
        "mirror.agent_not_registered",
        "mirror.agent_inactive",
        "mirror.unknown_operation_class",
        "policy.denied",
    }:
        return 403
    if code in {"mirror.approval_required", "policy.approval_required"}:
        return 409
    if code == "mirror.rate_limited":
        return 429
    if code in {
        "mirror.unsupported_live_write",
        "mirror.connector_degraded",
        "service_unavailable",
        "slack.live_backend_unavailable",
        "mail.live_backend_unavailable",
        "calendar.live_backend_unavailable",
        "tickets.live_backend_unavailable",
        "crm.live_backend_unavailable",
    }:
        return 503
    if code == "mirror.agent_id_required":
        return 400
    return 400


def _mirror_route_error_response(
    exc: Exception,
    *,
    surface: str,
) -> JSONResponse:
    if surface == "slack":
        return JSONResponse(
            {"ok": False, "error": _provider_error_code(exc)},
            status_code=200,
        )
    raise _http_exception(exc)


def _mirror_operation_class(tool_name: str) -> str | None:
    route = TOOL_ROUTES.get(tool_name)
    if route is not None:
        return route.operation_class.value
    return _MIRROR_OPERATION_CLASS_BY_TOOL.get(tool_name)


def _event_surface(event: MirrorIngestEvent) -> str:
    if event.target:
        return _normalize_surface(str(event.target))
    tool_name = str(event.resolved_tool or event.external_tool or "")
    if tool_name.startswith("service_ops."):
        return "service_ops"
    if tool_name.startswith("jira."):
        return "tickets"
    if tool_name.startswith("salesforce."):
        return "crm"
    if tool_name.startswith("mail."):
        return "mail"
    if tool_name.startswith("calendar."):
        return "calendar"
    if tool_name.startswith("slack."):
        return "slack"
    return _normalize_surface(
        str(event.focus_hint or tool_name.split(".")[0] or "world")
    )


def _normalize_surface(surface: str) -> str:
    normalized = str(surface or "").strip().lower()
    if normalized in {"jira", "tickets"}:
        return "tickets"
    if normalized in {"salesforce", "crm"}:
        return "crm"
    return normalized


def _surface_alias_set(surface: str) -> set[str]:
    raw = str(surface or "").strip().lower()
    normalized = _normalize_surface(raw)
    if raw == "graph":
        return {"graph", "mail", "calendar"}
    if normalized == "mail":
        return {"mail", "graph"}
    if normalized == "calendar":
        return {"calendar", "graph"}
    if normalized == "tickets":
        return {"tickets", "jira"}
    if normalized == "crm":
        return {"crm", "salesforce"}
    if normalized == "slack":
        return {"slack"}
    if normalized == "service_ops":
        return {"service_ops"}
    return {normalized}


def _env_bool(name: str) -> bool:
    value = os.environ.get(name)
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _blocked_live_operations() -> set[str]:
    raw = os.environ.get("VEI_LIVE_BLOCK_OPS", "")
    return {item.strip() for item in raw.split(",") if item.strip()}


def _request_agent_identity(request: Request) -> ExternalAgentIdentity | None:
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


def _identity_from_mirror_agent(agent: MirrorAgentSpec) -> ExternalAgentIdentity:
    return ExternalAgentIdentity(
        agent_id=agent.agent_id,
        name=agent.name,
        role=agent.role,
        team=agent.team,
        source=agent.source,
    )


def _merge_mirror_agent_identity(
    mirror_agent: MirrorAgentSpec,
    request_agent: ExternalAgentIdentity,
) -> MirrorAgentSpec:
    updates = {
        field: value
        for field in ("name", "role", "team", "source")
        if (value := getattr(request_agent, field))
    }
    if not updates:
        return mirror_agent
    return mirror_agent.model_copy(update=updates, deep=True)


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()
