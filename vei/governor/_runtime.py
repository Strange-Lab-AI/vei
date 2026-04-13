from __future__ import annotations

import logging
import threading
import time
from datetime import UTC, datetime
from typing import Any, Protocol

from ._config import (
    load_governor_workspace_config,
    governor_policy_profiles,
    resolve_governor_policy_profile,
)
from ._demo import (
    MirrorApprovalCommand,
    default_service_ops_demo_agents,
    default_service_ops_demo_steps,
)
from .models import (
    GovernorActionPlan,
    GovernorAgentSpec,
    GovernorConnectorStatus,
    GovernorEventResult,
    GovernorHandleMode,
    GovernorIngestEvent,
    GovernorPendingApproval,
    GovernorRecentEvent,
    GovernorRuntimeSnapshot,
)

logger = logging.getLogger(__name__)


class GovernorTarget(Protocol):
    def register_mirror_agent(self, agent: GovernorAgentSpec) -> None: ...

    def record_mirror_denial(
        self,
        *,
        event: GovernorIngestEvent,
        agent: GovernorAgentSpec,
        reason: str,
    ) -> None: ...

    def sync_mirror_runtime_state(self) -> None: ...

    def plan_mirror_action(
        self,
        *,
        event: GovernorIngestEvent,
        agent: GovernorAgentSpec,
        approval_granted: bool = False,
    ) -> GovernorActionPlan: ...

    def execute_mirror_action(
        self,
        *,
        plan: GovernorActionPlan,
        event: GovernorIngestEvent,
        agent: GovernorAgentSpec,
        approval_granted: bool = False,
    ) -> dict[str, Any]: ...

    def record_mirror_event(
        self,
        *,
        event: GovernorIngestEvent,
        agent: GovernorAgentSpec,
    ) -> dict[str, Any]: ...

    def mirror_connector_status(self) -> list[GovernorConnectorStatus]: ...


class GovernorRuntime:
    def __init__(
        self,
        *,
        metadata: dict[str, Any] | None,
        hero_world: str,
        target: GovernorTarget,
    ) -> None:
        self.config = load_governor_workspace_config(metadata)
        if self.config.hero_world is None:
            self.config.hero_world = hero_world
        self._target = target
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._autoplay_running = False
        self._event_count = 0
        self._denied_event_count = 0
        self._throttled_event_count = 0
        self._last_event_at: str | None = None
        self._agents: dict[str, GovernorAgentSpec] = {}
        self._demo_steps: list[GovernorIngestEvent | MirrorApprovalCommand] = []
        self._recent_events: list[GovernorRecentEvent] = []
        self._pending_approvals: list[GovernorPendingApproval] = []
        self._approval_seq = 0
        self._total_action_windows: dict[str, list[float]] = {}
        self._mutating_action_windows: dict[tuple[str, str], list[float]] = {}
        self._max_recent_events = 20
        self._seed_default_content()

    def snapshot(self) -> GovernorRuntimeSnapshot:
        with self._lock:
            return GovernorRuntimeSnapshot(
                config=self.config.model_copy(deep=True),
                agents=[
                    self._resolve_agent(agent).model_copy(deep=True)
                    for agent in self._agents.values()
                ],
                policy_profiles=governor_policy_profiles(),
                event_count=self._event_count,
                denied_event_count=self._denied_event_count,
                throttled_event_count=self._throttled_event_count,
                pending_demo_steps=len(self._demo_steps),
                last_event_at=self._last_event_at,
                autoplay_running=self._autoplay_running and bool(self._demo_steps),
                pending_approvals=[
                    approval.model_copy(deep=True)
                    for approval in self._pending_approvals
                ],
                connector_status=[
                    item.model_copy(deep=True)
                    for item in self._target.mirror_connector_status()
                ],
                recent_events=list(self._recent_events),
            )

    def list_agents(self) -> list[GovernorAgentSpec]:
        return self.snapshot().agents

    def list_pending_approvals(self) -> list[GovernorPendingApproval]:
        return self.snapshot().pending_approvals

    def get_agent(self, agent_id: str) -> GovernorAgentSpec | None:
        with self._lock:
            agent = self._agents.get(agent_id)
            if agent is None:
                return None
            return self._resolve_agent(agent)

    def register_agent(
        self, agent: GovernorAgentSpec | dict[str, Any]
    ) -> GovernorAgentSpec:
        payload = (
            agent
            if isinstance(agent, GovernorAgentSpec)
            else GovernorAgentSpec.model_validate(agent)
        )
        payload = self._resolve_agent(payload)
        with self._lock:
            existing = self._agents.get(payload.agent_id)
            merged = (
                payload
                if existing is None
                else self._resolve_agent(
                    existing.model_copy(
                        update=payload.model_dump(
                            exclude_unset=True,
                            exclude={"resolved_policy_profile"},
                        )
                    )
                )
            )
            self._agents[payload.agent_id] = merged.model_copy(deep=True)
            stored = self._agents[payload.agent_id].model_copy(deep=True)
        self._target.register_mirror_agent(stored.model_copy(deep=True))
        self._sync_runtime_state()
        return stored

    def update_agent(
        self,
        agent_id: str,
        fields: dict[str, Any],
    ) -> GovernorAgentSpec:
        current = self.require_agent(agent_id)
        payload = current.model_copy(
            update={k: v for k, v in fields.items() if k != "agent_id"},
            deep=True,
        )
        return self.register_agent(payload)

    def remove_agent(self, agent_id: str) -> GovernorAgentSpec:
        with self._lock:
            removed = self._agents.pop(agent_id, None)
        if removed is None:
            raise ValueError(f"governor agent not found: {agent_id}")
        self._sync_runtime_state()
        return removed

    def resolve_approval(
        self,
        *,
        approval_id: str,
        resolver_agent_id: str,
        action: str,
    ) -> GovernorPendingApproval:
        resolver = self.require_agent(resolver_agent_id)
        active_denial = _mirror_status_denial_reason(resolver)
        if active_denial is not None:
            raise ValueError(active_denial)
        if (
            not resolver.resolved_policy_profile
            or not resolver.resolved_policy_profile.can_approve
        ):
            raise ValueError(
                f"governor agent '{resolver.agent_id}' cannot approve held actions"
            )

        with self._lock:
            approval = next(
                (
                    item
                    for item in self._pending_approvals
                    if item.approval_id == approval_id
                ),
                None,
            )
            if approval is None:
                raise ValueError(f"mirror approval not found: {approval_id}")
            if approval.status != "pending":
                raise ValueError(
                    f"mirror approval '{approval_id}' is already {approval.status}"
                )

        if str(action).strip().lower() == "reject":
            updated = approval.model_copy(
                update={
                    "status": "rejected",
                    "resolved_by": resolver.agent_id,
                    "resolved_at": _iso_now(),
                },
                deep=True,
            )
            self._store_resolved_approval(updated)
            self._record_resolution_event(
                agent_id=resolver.agent_id,
                label=f"Rejected {approval.resolved_tool}",
                handled_by="denied",
                surface=approval.surface,
                resolved_tool=approval.resolved_tool,
                reason_code="mirror.approval_rejected",
                reason=f"{resolver.name} rejected the held action.",
            )
            self._touch_agent(
                resolver.agent_id,
                last_action=f"Rejected {approval.resolved_tool}",
            )
            self._sync_runtime_state()
            return updated

        event = GovernorIngestEvent(
            agent_id=approval.agent_id,
            external_tool=approval.external_tool or approval.resolved_tool,
            resolved_tool=approval.resolved_tool,
            focus_hint=approval.focus_hint,
            target=approval.target,
            args=dict(approval.args),
            payload=dict(approval.payload),
            label=approval.external_tool or approval.resolved_tool,
            source_mode=approval.source_mode,
        )
        actor = self.require_agent(approval.agent_id)
        plan = self._target.plan_mirror_action(
            event=event,
            agent=actor,
            approval_granted=True,
        )
        if plan.decision != "allow":
            updated = approval.model_copy(
                update={
                    "status": "failed",
                    "resolved_by": resolver.agent_id,
                    "resolved_at": _iso_now(),
                    "execution_result": {
                        "denied": True,
                        "code": plan.reason_code or "mirror.approval_execution_denied",
                        "reason": plan.reason or "approval could not be executed",
                    },
                },
                deep=True,
            )
            self._store_resolved_approval(updated)
            self._record_resolution_event(
                agent_id=approval.agent_id,
                label=approval.external_tool or approval.resolved_tool,
                handled_by="denied",
                surface=approval.surface,
                resolved_tool=approval.resolved_tool,
                reason_code=plan.reason_code,
                reason=plan.reason,
            )
            self._touch_agent(
                resolver.agent_id,
                last_action=f"Attempted approval for {approval.resolved_tool}",
            )
            self._sync_runtime_state()
            return updated

        rate_limited = self._consume_rate_limit(
            agent_id=approval.agent_id,
            surface=plan.surface,
            operation_class=plan.operation_class,
            source_mode=approval.source_mode,
        )
        if rate_limited is not None:
            updated = approval.model_copy(
                update={
                    "status": "failed",
                    "resolved_by": resolver.agent_id,
                    "resolved_at": _iso_now(),
                    "execution_result": {
                        "denied": True,
                        "code": "mirror.rate_limited",
                        "reason": rate_limited,
                    },
                },
                deep=True,
            )
            self._store_resolved_approval(updated)
            self._record_resolution_event(
                agent_id=approval.agent_id,
                label=approval.external_tool or approval.resolved_tool,
                handled_by="denied",
                surface=approval.surface,
                resolved_tool=approval.resolved_tool,
                reason_code="mirror.rate_limited",
                reason=rate_limited,
            )
            self._touch_agent(
                approval.agent_id,
                denied_delta=1,
                throttled_delta=1,
                last_action=approval.external_tool or approval.resolved_tool,
            )
            self._touch_agent(
                resolver.agent_id,
                last_action=f"Attempted approval for {approval.resolved_tool}",
            )
            self._throttled_event_count += 1
            self._sync_runtime_state()
            return updated

        try:
            result = self._target.execute_mirror_action(
                plan=plan,
                event=event,
                agent=actor,
                approval_granted=True,
            )
        except Exception as exc:  # noqa: BLE001
            updated = approval.model_copy(
                update={
                    "status": "failed",
                    "resolved_by": resolver.agent_id,
                    "resolved_at": _iso_now(),
                    "execution_result": {
                        "code": exc.__class__.__name__.lower(),
                        "message": str(exc),
                    },
                },
                deep=True,
            )
            self._store_resolved_approval(updated)
            self._record_resolution_event(
                agent_id=approval.agent_id,
                label=approval.external_tool or approval.resolved_tool,
                handled_by="denied",
                surface=approval.surface,
                resolved_tool=approval.resolved_tool,
                reason_code="mirror.approval_execution_failed",
                reason=str(exc),
            )
            self._touch_agent(
                resolver.agent_id,
                last_action=f"Attempted approval for {approval.resolved_tool}",
            )
            self._sync_runtime_state()
            return updated

        updated = approval.model_copy(
            update={
                "status": "executed",
                "resolved_by": resolver.agent_id,
                "resolved_at": _iso_now(),
                "execution_result": dict(result),
            },
            deep=True,
        )
        self._store_resolved_approval(updated)
        self._record_resolution_event(
            agent_id=approval.agent_id,
            label=approval.external_tool or approval.resolved_tool,
            handled_by=plan.action,
            surface=plan.surface,
            resolved_tool=plan.resolved_tool,
            reason_code="mirror.approval_executed",
            reason=f"Approved by {resolver.name or resolver.agent_id}.",
        )
        self._touch_agent(
            approval.agent_id,
            last_action=approval.external_tool or approval.resolved_tool,
        )
        self._touch_agent(
            resolver.agent_id,
            last_action=f"Approved {approval.resolved_tool}",
        )
        self._sync_runtime_state()
        return updated

    def ingest_event(
        self, event: GovernorIngestEvent | dict[str, Any]
    ) -> GovernorEventResult:
        payload = (
            event
            if isinstance(event, GovernorIngestEvent)
            else GovernorIngestEvent.model_validate(event)
        )
        agent = self.require_agent(payload.agent_id)

        status_denial = _mirror_status_denial_reason(agent)
        if status_denial is not None:
            return self._deny_event(
                payload,
                agent,
                reason=status_denial,
                code="mirror.agent_inactive",
            )

        mode_denial = _mirror_mode_denial_reason(agent, payload)
        if mode_denial is not None:
            return self._deny_event(
                payload,
                agent,
                reason=mode_denial,
                code="mirror.mode_denied",
            )

        if not payload.resolved_tool and not payload.target:
            result = self._target.record_mirror_event(event=payload, agent=agent)
            return self._finalize_event(
                payload,
                agent,
                handled_by="record_only",
                result=result,
                surface=str(payload.focus_hint or "world"),
                resolved_tool=payload.external_tool,
            )

        plan = self._target.plan_mirror_action(
            event=payload,
            agent=agent,
            approval_granted=False,
        )
        if plan.decision == "deny":
            return self._deny_event(
                payload,
                agent,
                reason=plan.reason or "mirror request denied",
                code=plan.reason_code or "mirror.surface_denied",
                surface=plan.surface,
                resolved_tool=plan.resolved_tool,
            )

        if plan.decision == "approval_required":
            approval = self._create_pending_approval(payload, plan)
            return self._finalize_event(
                payload,
                agent,
                handled_by="pending_approval",
                result={
                    "approval_required": True,
                    "approval_id": approval.approval_id,
                    "reason": approval.reason,
                    "code": approval.reason_code,
                },
                surface=plan.surface,
                resolved_tool=plan.resolved_tool,
                reason_code=approval.reason_code,
                reason=approval.reason,
            )

        rate_denial = self._consume_rate_limit(
            agent_id=agent.agent_id,
            surface=plan.surface,
            operation_class=plan.operation_class,
            source_mode=payload.source_mode,
        )
        if rate_denial is not None:
            return self._deny_event(
                payload,
                agent,
                reason=rate_denial,
                code="mirror.rate_limited",
                surface=plan.surface,
                resolved_tool=plan.resolved_tool,
                throttled=True,
            )

        result = self._target.execute_mirror_action(
            plan=plan,
            event=payload,
            agent=agent,
            approval_granted=False,
        )
        return self._finalize_event(
            payload,
            agent,
            handled_by=plan.action,
            result=result,
            surface=plan.surface,
            resolved_tool=plan.resolved_tool,
        )

    def demo_tick(self) -> GovernorEventResult | None:
        while True:
            with self._lock:
                if not self._demo_steps:
                    return None
                next_step = self._demo_steps.pop(0)

            if not isinstance(next_step, MirrorApprovalCommand):
                return self.ingest_event(next_step)

            approval_id = next_step.approval_id
            if approval_id is None:
                pending = [
                    item
                    for item in self.list_pending_approvals()
                    if item.status == "pending"
                ]
                if not pending:
                    continue
                approval_id = pending[-1].approval_id

            try:
                resolved = self.resolve_approval(
                    approval_id=approval_id,
                    resolver_agent_id=next_step.agent_id,
                    action=next_step.action,
                )
            except ValueError as exc:
                if "already" in str(exc):
                    continue
                raise

            return GovernorEventResult(
                ok=resolved.status == "executed",
                handled_by="record_only" if resolved.status == "executed" else "denied",
                agent_id=next_step.agent_id,
                remaining_demo_steps=len(self._demo_steps),
                result=resolved.model_dump(mode="json"),
            )

    def start(self) -> None:
        if not self.config.demo_mode or not self.config.autoplay:
            return
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        with self._lock:
            self._autoplay_running = True
        self._thread = threading.Thread(
            target=self._autoplay_loop,
            name="vei-governor-demo",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)

    def require_agent(self, agent_id: str) -> GovernorAgentSpec:
        with self._lock:
            existing = self._agents.get(agent_id)
            if existing is not None:
                return self._resolve_agent(existing)
        raise ValueError(f"governor agent not registered: {agent_id}")

    def _seed_default_content(self) -> None:
        if self.config.hero_world != "service_ops" or not self.config.demo_mode:
            return
        for agent in default_service_ops_demo_agents():
            self.register_agent(agent)
        self._demo_steps = default_service_ops_demo_steps()

    def _resolve_agent(self, agent: GovernorAgentSpec) -> GovernorAgentSpec:
        return agent.model_copy(
            update={
                "resolved_policy_profile": resolve_governor_policy_profile(
                    agent.policy_profile_id
                )
            },
            deep=True,
        )

    def _create_pending_approval(
        self,
        event: GovernorIngestEvent,
        plan: GovernorActionPlan,
    ) -> GovernorPendingApproval:
        with self._lock:
            self._approval_seq += 1
            approval = GovernorPendingApproval(
                approval_id=f"approval-{self._approval_seq:04d}",
                agent_id=event.agent_id,
                surface=plan.surface,
                resolved_tool=plan.resolved_tool,
                operation_class=plan.operation_class,
                args=dict(event.args),
                reason_code=plan.reason_code or "mirror.approval_required",
                reason=plan.reason or "action requires human approval",
                created_at=_iso_now(),
                external_tool=event.external_tool,
                focus_hint=event.focus_hint,
                target=event.target,
                payload=dict(event.payload),
                source_mode=event.source_mode,
            )
            self._pending_approvals.append(approval)
            return approval

    def _store_resolved_approval(self, updated: GovernorPendingApproval) -> None:
        with self._lock:
            self._pending_approvals = [
                updated if item.approval_id == updated.approval_id else item
                for item in self._pending_approvals
            ]

    def _consume_rate_limit(
        self,
        *,
        agent_id: str,
        surface: str,
        operation_class: str,
        source_mode: str,
    ) -> str | None:
        if source_mode == "demo":
            return None
        now = time.monotonic()
        total_window = self._total_action_windows.setdefault(agent_id, [])
        surface_window = self._mutating_action_windows.setdefault(
            (agent_id, surface), []
        )
        self._prune_window(total_window, now)
        self._prune_window(surface_window, now)
        if len(total_window) >= 60:
            return f"agent '{agent_id}' exceeded the mirror action rate limit"
        if operation_class != "read" and len(surface_window) >= 20:
            return (
                f"agent '{agent_id}' exceeded the mutating rate limit for surface "
                f"'{surface}'"
            )
        total_window.append(now)
        if operation_class != "read":
            surface_window.append(now)
        return None

    def _prune_window(self, window: list[float], now: float) -> None:
        cutoff = now - 60.0
        while window and window[0] < cutoff:
            window.pop(0)

    def _deny_event(
        self,
        event: GovernorIngestEvent,
        agent: GovernorAgentSpec,
        *,
        reason: str,
        code: str,
        surface: str | None = None,
        resolved_tool: str | None = None,
        throttled: bool = False,
    ) -> GovernorEventResult:
        self._target.record_mirror_denial(event=event, agent=agent, reason=reason)
        return self._finalize_event(
            event,
            agent,
            handled_by="denied",
            result={"denied": True, "reason": reason, "code": code},
            surface=surface or str(event.focus_hint or event.target or "world"),
            resolved_tool=resolved_tool
            or str(event.resolved_tool or event.external_tool),
            reason_code=code,
            reason=reason,
            throttled=throttled,
        )

    def _finalize_event(
        self,
        event: GovernorIngestEvent,
        agent: GovernorAgentSpec,
        *,
        handled_by: GovernorHandleMode,
        result: dict[str, Any],
        surface: str,
        resolved_tool: str,
        reason_code: str | None = None,
        reason: str | None = None,
        throttled: bool = False,
    ) -> GovernorEventResult:
        action_label = event.label or event.external_tool
        with self._lock:
            self._event_count += 1
            self._last_event_at = _iso_now()
            denied_delta = 1 if handled_by == "denied" else 0
            throttled_delta = 1 if throttled else 0
            if handled_by == "denied":
                if throttled:
                    self._throttled_event_count += 1
                else:
                    self._denied_event_count += 1
            updated_agent = self._touch_agent(
                agent.agent_id,
                denied_delta=denied_delta,
                throttled_delta=throttled_delta,
                last_action=action_label,
            )
            self._recent_events.append(
                GovernorRecentEvent(
                    event_id=event.event_id,
                    agent_id=updated_agent.agent_id,
                    tool=event.external_tool,
                    handled_by=handled_by,
                    resolved_tool=resolved_tool,
                    surface=surface,
                    label=action_label,
                    reason_code=reason_code,
                    reason=reason,
                    timestamp=self._last_event_at,
                )
            )
            if len(self._recent_events) > self._max_recent_events:
                self._recent_events = self._recent_events[-self._max_recent_events :]
            event_result = GovernorEventResult(
                ok=handled_by not in {"denied", "pending_approval"},
                handled_by=handled_by,
                agent_id=updated_agent.agent_id,
                remaining_demo_steps=len(self._demo_steps),
                result=result,
            )
        self._sync_runtime_state()
        return event_result

    def _touch_agent(
        self,
        agent_id: str,
        *,
        denied_delta: int = 0,
        throttled_delta: int = 0,
        last_action: str | None = None,
    ) -> GovernorAgentSpec:
        now = self._last_event_at or _iso_now()
        current = self._agents.get(agent_id)
        if current is None:
            raise ValueError(f"governor agent not registered: {agent_id}")
        updated = self._resolve_agent(
            current.model_copy(
                update={
                    "status": "active",
                    "last_seen_at": now,
                    "last_action": last_action or current.last_action,
                    "denied_count": current.denied_count + denied_delta,
                    "throttled_count": current.throttled_count + throttled_delta,
                },
                deep=True,
            )
        )
        self._agents[agent_id] = updated
        return updated

    def _record_resolution_event(
        self,
        *,
        agent_id: str,
        label: str,
        handled_by: GovernorHandleMode,
        surface: str,
        resolved_tool: str,
        reason_code: str | None,
        reason: str | None,
    ) -> None:
        with self._lock:
            self._event_count += 1
            self._last_event_at = _iso_now()
            if handled_by == "denied":
                self._denied_event_count += 1
            self._recent_events.append(
                GovernorRecentEvent(
                    agent_id=agent_id,
                    tool=label,
                    handled_by=handled_by,
                    resolved_tool=resolved_tool,
                    surface=surface,
                    label=label,
                    reason_code=reason_code,
                    reason=reason,
                    timestamp=self._last_event_at,
                )
            )
            if len(self._recent_events) > self._max_recent_events:
                self._recent_events = self._recent_events[-self._max_recent_events :]

    def _sync_runtime_state(self) -> None:
        try:
            self._target.sync_mirror_runtime_state()
        except Exception:
            logger.warning("governor runtime state sync failed", exc_info=True)

    def _autoplay_loop(self) -> None:
        interval_s = max(0.25, self.config.demo_interval_ms / 1000.0)
        next_tick_at = time.monotonic()
        try:
            while not self._stop.is_set():
                if not self._demo_steps:
                    return
                wait_s = max(0.0, next_tick_at - time.monotonic())
                if self._stop.wait(wait_s):
                    return
                try:
                    self.demo_tick()
                except Exception:
                    logger.warning("mirror autoplay tick failed", exc_info=True)
                    return
                next_tick_at += interval_s
                finished_at = time.monotonic()
                if next_tick_at < finished_at:
                    next_tick_at = finished_at
        finally:
            with self._lock:
                self._autoplay_running = False
            self._sync_runtime_state()


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _mirror_status_denial_reason(agent: GovernorAgentSpec) -> str | None:
    if agent.status in {"registered", "active"}:
        return None
    return (
        f"agent '{agent.agent_id}' is in status '{agent.status}' and cannot act "
        "through mirror controls"
    )


def _mirror_mode_denial_reason(
    agent: GovernorAgentSpec,
    event: GovernorIngestEvent,
) -> str | None:
    if event.source_mode == "proxy":
        if agent.mode in {"proxy", "demo"}:
            return None
        return (
            f"agent '{agent.agent_id}' is registered for {agent.mode} mode and "
            "cannot use proxy compatibility routes"
        )
    if agent.mode in {"ingest", "demo"}:
        return None
    return (
        f"agent '{agent.agent_id}' is registered for {agent.mode} mode and "
        "cannot use mirror ingest events"
    )
