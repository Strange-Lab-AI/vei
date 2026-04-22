from __future__ import annotations

import threading
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI

from vei.benchmark.api import BenchmarkMetrics
from vei.blueprint.api import create_world_session_from_blueprint
from vei.connectors import TOOL_ROUTES
from vei.contract.api import ContractEvaluationResult
from vei.governor import (
    GovernorActionPlan,
    GovernorAgentSpec,
    GovernorConnectorStatus,
    GovernorIngestEvent,
    GovernorRuntime,
    load_governor_workspace_config,
)
from vei.router.api import MCPError
from vei.run.api import (
    generate_run_id,
    get_workspace_run_dir,
    list_run_snapshots,
    load_run_manifest,
    write_run_manifest,
)
from vei.run import append_run_event, merge_reproducibility_metadata
from vei.run.api import (
    RunArtifactIndex,
    RunManifest,
    RunTimelineEvent,
)
from vei.workforce.api import (
    WorkforceCommandRecord,
    WorkforceState,
    append_workforce_command,
    sync_workforce_state,
    workforce_state_fingerprint,
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
from vei.workspace.api import WorkspaceRunEntry
from vei.world.api import ActorState, WorldSessionAPI

from .api import load_customer_twin
from ._gateway_adapters import error_payload as _error_payload
from ._gateway_routes import register_gateway_routes
from ._governance import (
    check_approval_rules as _check_approval_rules,
    check_connector_safety as _check_connector_safety,
    check_policy_profile as _check_policy_profile,
    check_surface_access as _check_surface_access,
    connector_statuses as _connector_statuses,
    event_surface as _event_surface,
    mirror_operation_class as _mirror_operation_class,
)
from ._runtime_support import (
    channel_for_focus as _channel_for_focus,
    contract_summary as _contract_summary,
    identity_from_mirror_agent as _identity_from_mirror_agent,
    iso_now as _iso_now,
    merge_mirror_agent_identity as _merge_mirror_agent_identity,
    object_refs as _object_refs,
    session_router as _session_router,
    snapshot_path as _snapshot_path,
    workforce_command_label as _workforce_command_label,
    workforce_command_refs as _workforce_command_refs,
    workforce_object_refs as _workforce_object_refs,
)
from .models import CustomerTwinBundle, ExternalAgentIdentity, TwinRuntimeStatus


class TwinRuntime:
    def __init__(self, workspace_root: Path, bundle: CustomerTwinBundle):
        self.workspace_root = workspace_root
        self.bundle = bundle
        self.mirror_config = load_governor_workspace_config(bundle.metadata)
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
        self.seed = 42042
        self.started_at = _iso_now()
        self._lock = threading.RLock()
        self._pending_mirror_lock = threading.Lock()
        self._pending_mirror_agents: dict[
            str, tuple[ActorState, ExternalAgentIdentity | None]
        ] = {}
        self.status = TwinRuntimeStatus(
            run_id=self.run_id,
            branch_name=self.branch_name,
            started_at=self.started_at,
            metadata={"organization_name": bundle.organization_name},
        )
        self.mirror: GovernorRuntime | None = None
        self.workforce_state: WorkforceState | None = None
        self._workforce_sync_fingerprint = ""

        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.session = self._build_session()
        _session_router(self.session).workforce = {}
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
        self.mirror = GovernorRuntime(
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
            self._flush_pending_mirror_agents_locked()
            try:
                result = self.session.call_tool(resolved_tool, args)
                self._flush_pending_mirror_agents_locked()
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
                self._flush_pending_mirror_agents_locked()
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
            raise MCPError("mirror.unavailable", "governor runtime unavailable")
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
        event = GovernorIngestEvent(
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
            self._flush_pending_mirror_agents_locked()
            return self.session.call_tool(tool, args or {})

    def status_payload(self) -> dict[str, Any]:
        return {
            "bundle": self.bundle.model_dump(mode="json"),
            "runtime": self.status.model_dump(mode="json"),
            "governor": self._mirror_snapshot_payload(),
            "workforce": self._workforce_payload(),
            "manifest": load_run_manifest(
                self.run_dir / "run_manifest.json"
            ).model_dump(mode="json"),
        }

    def sync_workforce_state(self, state: WorkforceState) -> WorkforceState:
        with self._lock:
            next_state = sync_workforce_state(
                self.workforce_state,
                snapshot=state.snapshot,
                sync=state.sync,
            )
            fingerprint = workforce_state_fingerprint(next_state)
            self.workforce_state = next_state
            self._apply_workforce_state()
            if fingerprint == self._workforce_sync_fingerprint:
                return next_state
            self._workforce_sync_fingerprint = fingerprint
            snapshot = self.session.snapshot("workforce.sync")
            self.status.latest_snapshot_id = snapshot.snapshot_id
            self._append_event(
                kind="workforce_sync",
                label="outside workforce synced into the company world",
                channel="Control Room",
                time_ms=snapshot.time_ms,
                status=(
                    next_state.sync.status
                    if next_state.sync is not None
                    else "disabled"
                ),
                object_refs=_workforce_object_refs(next_state),
                payload=next_state.model_dump(mode="json"),
            )
            self._append_snapshot_event(
                "workforce.sync",
                snapshot.snapshot_id,
                snapshot.time_ms,
            )
            self._write_manifest(
                status="running",
                success=None,
                error=None,
                completed_at=None,
            )
            return next_state

    def record_workforce_command(
        self,
        command: WorkforceCommandRecord,
    ) -> WorkforceState:
        with self._lock:
            self.workforce_state = append_workforce_command(
                self.workforce_state,
                command,
            )
            self._apply_workforce_state()
            snapshot = self.session.snapshot(f"workforce.command:{command.action}")
            self.status.latest_snapshot_id = snapshot.snapshot_id
            self._append_event(
                kind="workforce_control",
                label=_workforce_command_label(command),
                channel="Control Room",
                time_ms=snapshot.time_ms,
                status="applied",
                object_refs=_workforce_command_refs(command),
                payload=command.model_dump(mode="json"),
            )
            self._append_snapshot_event(
                f"workforce.command:{command.action}",
                snapshot.snapshot_id,
                snapshot.time_ms,
            )
            self._write_manifest(
                status="running",
                success=None,
                error=None,
                completed_at=None,
            )
            return self.workforce_state

    def _apply_workforce_state(self) -> None:
        payload = self._workforce_payload()
        _session_router(self.session).workforce = payload
        metadata = dict(self.status.metadata)
        metadata["workforce"] = payload
        self.status.metadata = metadata

    def _workforce_payload(self) -> dict[str, Any]:
        if self.workforce_state is None:
            return {}
        return self.workforce_state.model_dump(mode="json")

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
                seed=self.seed,
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
            seed=self.seed,
            branch=self.branch_name,
            success=success,
            metrics=BenchmarkMetrics(actions=self.status.request_count),
            contract=_contract_summary(self.contract_path),
            artifacts=RunArtifactIndex(
                run_dir=str(self.run_dir.relative_to(self.workspace_root)),
                artifacts_dir=str(self.artifacts_dir.relative_to(self.workspace_root)),
                state_dir=str(self.state_dir.relative_to(self.workspace_root)),
                events_path=str(self.events_path.relative_to(self.workspace_root)),
                contract_path=str(
                    self._source_contract_path().relative_to(self.workspace_root)
                ),
            ),
            snapshots=list_run_snapshots(self.workspace_root, self.run_id),
            error=error,
            metadata=merge_reproducibility_metadata(
                {
                    "gateway_mode": "compatibility",
                    "organization_name": self.bundle.organization_name,
                    "surfaces": [item.name for item in self.bundle.gateway.surfaces],
                    "agents": list(self.status.metadata.get("agents", [])),
                    "last_agent": self.status.metadata.get("last_agent"),
                    "governor": self._mirror_snapshot_payload(),
                    "workforce": self._workforce_payload(),
                },
                seed=self.seed,
                blueprint_asset_path=self._source_blueprint_path(),
                contract_path=self._source_contract_path(),
            ),
        )
        write_run_manifest(self.workspace_root, manifest)

    def _source_blueprint_path(self) -> Path:
        return self.workspace_root / self.workspace_manifest.blueprint_asset_path

    def _source_contract_path(self) -> Path:
        contract_path = (
            self.scenario.contract_path
            or f"{self.workspace_manifest.contracts_dir}/{self.scenario.name}.contract.json"
        )
        return self.workspace_root / contract_path

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

    def _queue_mirror_agent(
        self,
        *,
        actor: ActorState,
        identity: ExternalAgentIdentity | None,
    ) -> None:
        with self._pending_mirror_lock:
            self._pending_mirror_agents[actor.actor_id] = (
                actor.model_copy(deep=True),
                identity.model_copy(deep=True) if identity is not None else None,
            )

    def _drain_pending_mirror_agents(
        self,
    ) -> list[tuple[ActorState, ExternalAgentIdentity | None]]:
        with self._pending_mirror_lock:
            pending = list(self._pending_mirror_agents.values())
            self._pending_mirror_agents.clear()
        return pending

    def _flush_pending_mirror_agents_locked(self) -> None:
        for actor, identity in self._drain_pending_mirror_agents():
            self.session.register_actor(actor)
            self._record_agent_identity(identity)

    def _refresh_mirror_status(self) -> None:
        if self.mirror is None:
            return
        metadata = dict(self.status.metadata)
        metadata["governor"] = self._mirror_snapshot_payload()
        self.status.metadata = metadata

    def sync_mirror_runtime_state(self) -> None:
        with self._lock:
            self._flush_pending_mirror_agents_locked()
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

    def register_mirror_agent(self, agent: GovernorAgentSpec) -> None:
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
        self._queue_mirror_agent(
            actor=actor,
            identity=_identity_from_mirror_agent(agent),
        )
        if not self._lock.acquire(blocking=False):
            return
        try:
            self._flush_pending_mirror_agents_locked()
            self._refresh_mirror_status()
            self._write_manifest(
                status="running", success=None, error=None, completed_at=None
            )
        finally:
            self._lock.release()

    def plan_mirror_action(
        self,
        *,
        event: GovernorIngestEvent,
        agent: GovernorAgentSpec,
        approval_granted: bool = False,
    ) -> GovernorActionPlan:
        action = "dispatch" if event.resolved_tool else "inject"
        tool_name = str(event.resolved_tool or event.external_tool or "")
        surface = _event_surface(event)
        operation_class = _mirror_operation_class(tool_name)
        if operation_class is None and action == "inject":
            operation_class = "write_safe"
        if operation_class is None:
            return GovernorActionPlan(
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
            return GovernorActionPlan(
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
            return GovernorActionPlan(
                action=action,
                surface=surface,
                resolved_tool=tool_name,
                operation_class=operation_class,
                decision=profile_decision["decision"],
                reason_code=profile_decision["code"],
                reason=profile_decision["reason"],
            )

        approval_rule_decision = self._check_approval_rules(
            tool_name=tool_name,
            surface=surface,
            operation_class=operation_class,
            approval_granted=approval_granted,
        )
        if approval_rule_decision is not None:
            return GovernorActionPlan(
                action=action,
                surface=surface,
                resolved_tool=tool_name,
                operation_class=operation_class,
                decision=approval_rule_decision["decision"],
                reason_code=approval_rule_decision["code"],
                reason=approval_rule_decision["reason"],
            )

        connector_decision = self._check_connector_safety(
            tool_name=tool_name,
            surface=surface,
            operation_class=operation_class,
            approval_granted=approval_granted,
        )
        if connector_decision is not None:
            return GovernorActionPlan(
                action=action,
                surface=surface,
                resolved_tool=tool_name,
                operation_class=operation_class,
                decision=connector_decision["decision"],
                reason_code=connector_decision["code"],
                reason=connector_decision["reason"],
            )

        return GovernorActionPlan(
            action=action,
            surface=surface,
            resolved_tool=tool_name,
            operation_class=operation_class,
        )

    def execute_mirror_action(
        self,
        *,
        plan: GovernorActionPlan,
        event: GovernorIngestEvent,
        agent: GovernorAgentSpec,
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

    def mirror_connector_status(self) -> list[GovernorConnectorStatus]:
        return _connector_statuses(
            self.mirror_config.connector_mode,
            checked_at=_iso_now(),
        )

    def _check_surface_access(
        self,
        agent: GovernorAgentSpec,
        surface: str,
    ) -> str | None:
        return _check_surface_access(agent, surface)

    def _check_policy_profile(
        self,
        *,
        agent: GovernorAgentSpec,
        operation_class: str,
        approval_granted: bool,
    ) -> dict[str, str] | None:
        return _check_policy_profile(
            agent=agent,
            operation_class=operation_class,
            approval_granted=approval_granted,
        )

    def _check_approval_rules(
        self,
        *,
        tool_name: str,
        surface: str,
        operation_class: str,
        approval_granted: bool,
    ) -> dict[str, str] | None:
        return _check_approval_rules(
            config=self.mirror_config,
            tool_name=tool_name,
            surface=surface,
            operation_class=operation_class,
            approval_granted=approval_granted,
        )

    def _check_connector_safety(
        self,
        *,
        tool_name: str,
        surface: str,
        operation_class: str,
        approval_granted: bool,
    ) -> dict[str, str] | None:
        return _check_connector_safety(
            connector_mode=self.mirror_config.connector_mode,
            tool_name=tool_name,
            surface=surface,
            operation_class=operation_class,
            approval_granted=approval_granted,
        )

    def _record_denial(
        self,
        event: GovernorIngestEvent,
        agent: GovernorAgentSpec,
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
        event: GovernorIngestEvent,
        agent: GovernorAgentSpec,
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
        event: GovernorIngestEvent,
        agent: GovernorAgentSpec,
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
        event: GovernorIngestEvent,
        agent: GovernorAgentSpec,
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
        event: GovernorIngestEvent,
        agent: GovernorAgentSpec,
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
    register_gateway_routes(app, runtime)
    return app
