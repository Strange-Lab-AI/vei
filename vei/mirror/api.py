from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime
from typing import Any, Mapping, Protocol

from .models import (
    MirrorAgentSpec,
    MirrorEventResult,
    MirrorHandleMode,
    MirrorIngestEvent,
    MirrorRecentEvent,
    MirrorRuntimeSnapshot,
    MirrorWorkspaceConfig,
)

logger = logging.getLogger(__name__)


class MirrorTarget(Protocol):
    def register_mirror_agent(self, agent: MirrorAgentSpec) -> None: ...

    def sync_mirror_runtime_state(self) -> None: ...

    def dispatch_mirror_tool(
        self,
        *,
        event: MirrorIngestEvent,
        agent: MirrorAgentSpec,
    ) -> dict[str, Any]: ...

    def inject_mirror_event(
        self,
        *,
        event: MirrorIngestEvent,
        agent: MirrorAgentSpec,
    ) -> dict[str, Any]: ...

    def record_mirror_event(
        self,
        *,
        event: MirrorIngestEvent,
        agent: MirrorAgentSpec,
    ) -> dict[str, Any]: ...


def default_mirror_workspace_config(
    *,
    connector_mode: str = "sim",
    demo_mode: bool = False,
    autoplay: bool = False,
    demo_interval_ms: int = 1500,
    hero_world: str | None = None,
) -> MirrorWorkspaceConfig:
    return MirrorWorkspaceConfig(
        connector_mode=(
            "live" if str(connector_mode).strip().lower() == "live" else "sim"
        ),
        demo_mode=bool(demo_mode),
        autoplay=bool(autoplay),
        demo_interval_ms=max(250, int(demo_interval_ms)),
        hero_world=hero_world,
    )


def mirror_metadata_payload(
    config: MirrorWorkspaceConfig | None = None,
    *,
    connector_mode: str = "sim",
    demo_mode: bool = False,
    autoplay: bool = False,
    demo_interval_ms: int = 1500,
    hero_world: str | None = None,
) -> dict[str, Any]:
    resolved = config or default_mirror_workspace_config(
        connector_mode=connector_mode,
        demo_mode=demo_mode,
        autoplay=autoplay,
        demo_interval_ms=demo_interval_ms,
        hero_world=hero_world,
    )
    return resolved.model_dump(mode="json")


def load_mirror_workspace_config(
    metadata: Mapping[str, Any] | None,
) -> MirrorWorkspaceConfig:
    if not isinstance(metadata, Mapping):
        return default_mirror_workspace_config()
    payload = metadata.get("mirror")
    if not isinstance(payload, Mapping):
        return default_mirror_workspace_config()
    return MirrorWorkspaceConfig.model_validate(dict(payload))


def default_service_ops_demo_agents() -> list[MirrorAgentSpec]:
    return [
        MirrorAgentSpec(
            agent_id="dispatch-bot",
            name="Dispatch Bot",
            mode="demo",
            role="dispatch_orchestrator",
            team="dispatch",
            allowed_surfaces=["slack", "service_ops"],
            policy_profile="dispatch_safe",
            source="mirror-demo",
        ),
        MirrorAgentSpec(
            agent_id="billing-bot",
            name="Billing Bot",
            mode="demo",
            role="billing_coordinator",
            team="finance",
            allowed_surfaces=["slack", "service_ops", "mail"],
            policy_profile="billing_safe",
            source="mirror-demo",
        ),
    ]


def default_service_ops_demo_steps() -> list[MirrorIngestEvent]:
    return [
        MirrorIngestEvent(
            event_id="mirror-demo-001",
            agent_id="dispatch-bot",
            external_tool="slack.chat.postMessage",
            resolved_tool="slack.send_message",
            focus_hint="slack",
            args={
                "channel": "#clearwater-dispatch",
                "text": (
                    "Dispatch Bot: rerouting Clearwater Medical to standby controls tech "
                    "while we keep billing aligned."
                ),
            },
            label="Dispatch bot posts the reroute into Clearwater dispatch",
            source_mode="demo",
        ),
        MirrorIngestEvent(
            event_id="mirror-demo-002",
            agent_id="dispatch-bot",
            external_tool="service_ops.assign_dispatch",
            resolved_tool="service_ops.assign_dispatch",
            focus_hint="service_ops",
            args={
                "work_order_id": "WO-CFS-100",
                "technician_id": "TECH-CFS-02",
                "appointment_id": "APT-CFS-100",
                "note": "Mirror demo reroute to preserve VIP same-day coverage.",
            },
            label="Dispatch bot assigns the backup controls technician",
            source_mode="demo",
        ),
        MirrorIngestEvent(
            event_id="mirror-demo-003",
            agent_id="billing-bot",
            external_tool="service_ops.hold_billing",
            resolved_tool="service_ops.hold_billing",
            focus_hint="service_ops",
            args={
                "billing_case_id": "BILL-CFS-100",
                "reason": "Mirror demo: keep VIP dispute contained while dispatch recovers.",
                "hold": True,
            },
            label="Billing bot pauses the disputed VIP billing case",
            source_mode="demo",
        ),
        MirrorIngestEvent(
            event_id="mirror-demo-004",
            agent_id="billing-bot",
            external_tool="slack.chat.postMessage",
            resolved_tool="slack.send_message",
            focus_hint="slack",
            args={
                "channel": "#billing-ops",
                "text": (
                    "Billing Bot: dispute follow-up is paused until the Clearwater dispatch "
                    "reroute is fully confirmed."
                ),
            },
            label="Billing bot posts the containment update into billing ops",
            source_mode="demo",
        ),
    ]


class MirrorRuntime:
    def __init__(
        self,
        *,
        metadata: Mapping[str, Any] | None,
        hero_world: str,
        target: MirrorTarget,
    ) -> None:
        self.config = load_mirror_workspace_config(metadata)
        if self.config.hero_world is None:
            self.config.hero_world = hero_world
        self._target = target
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._event_count = 0
        self._last_event_at: str | None = None
        self._agents: dict[str, MirrorAgentSpec] = {}
        self._demo_steps: list[MirrorIngestEvent] = []
        self._recent_events: list[MirrorRecentEvent] = []
        self._max_recent_events = 20
        self._seed_default_content()

    def snapshot(self) -> MirrorRuntimeSnapshot:
        with self._lock:
            return MirrorRuntimeSnapshot(
                config=self.config.model_copy(deep=True),
                agents=[agent.model_copy(deep=True) for agent in self._agents.values()],
                event_count=self._event_count,
                pending_demo_steps=len(self._demo_steps),
                last_event_at=self._last_event_at,
                autoplay_running=bool(self._thread and self._thread.is_alive()),
                recent_events=list(self._recent_events),
            )

    def list_agents(self) -> list[MirrorAgentSpec]:
        return self.snapshot().agents

    def get_agent(self, agent_id: str) -> MirrorAgentSpec | None:
        with self._lock:
            agent = self._agents.get(agent_id)
            if agent is None:
                return None
            return agent.model_copy(deep=True)

    def register_agent(
        self, agent: MirrorAgentSpec | dict[str, Any]
    ) -> MirrorAgentSpec:
        payload = (
            agent
            if isinstance(agent, MirrorAgentSpec)
            else MirrorAgentSpec.model_validate(agent)
        )
        with self._lock:
            existing = self._agents.get(payload.agent_id)
            merged = (
                payload
                if existing is None
                else existing.model_copy(update=payload.model_dump(exclude_unset=True))
            )
        with self._lock:
            self._agents[payload.agent_id] = merged.model_copy(deep=True)
            stored = self._agents[payload.agent_id].model_copy(deep=True)
        self._target.register_mirror_agent(stored.model_copy(deep=True))
        return stored

    def ingest_event(
        self, event: MirrorIngestEvent | dict[str, Any]
    ) -> MirrorEventResult:
        payload = (
            event
            if isinstance(event, MirrorIngestEvent)
            else MirrorIngestEvent.model_validate(event)
        )
        agent = self._ensure_agent(payload)
        handled_by: MirrorHandleMode
        if payload.resolved_tool:
            result = self._target.dispatch_mirror_tool(event=payload, agent=agent)
            handled_by = "denied" if result.get("denied") else "dispatch"
        elif payload.target:
            result = self._target.inject_mirror_event(event=payload, agent=agent)
            handled_by = "denied" if result.get("denied") else "inject"
        else:
            result = self._target.record_mirror_event(event=payload, agent=agent)
            handled_by = "record_only"
        action_label = payload.label or payload.external_tool
        with self._lock:
            self._event_count += 1
            self._last_event_at = _iso_now()
            update_fields: dict[str, Any] = {
                "status": "active",
                "last_seen_at": self._last_event_at,
                "last_action": action_label,
            }
            if handled_by == "denied":
                update_fields["denied_count"] = agent.denied_count + 1
            updated_agent = agent.model_copy(
                update=update_fields,
                deep=True,
            )
            self._agents[updated_agent.agent_id] = updated_agent
            self._recent_events.append(
                MirrorRecentEvent(
                    event_id=payload.event_id,
                    agent_id=payload.agent_id,
                    tool=payload.external_tool,
                    handled_by=handled_by,
                    label=action_label,
                    timestamp=self._last_event_at,
                )
            )
            if len(self._recent_events) > self._max_recent_events:
                self._recent_events = self._recent_events[-self._max_recent_events :]
            event_result = MirrorEventResult(
                ok=handled_by != "denied",
                handled_by=handled_by,
                agent_id=updated_agent.agent_id,
                remaining_demo_steps=len(self._demo_steps),
                result=result,
            )
        try:
            self._target.sync_mirror_runtime_state()
        except Exception:
            logger.warning("mirror runtime state sync failed", exc_info=True)
        return event_result

    def demo_tick(self) -> MirrorEventResult | None:
        with self._lock:
            if not self._demo_steps:
                return None
            next_event = self._demo_steps.pop(0)
        return self.ingest_event(next_event)

    def start(self) -> None:
        if not self.config.demo_mode or not self.config.autoplay:
            return
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._autoplay_loop,
            name="vei-mirror-demo",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)

    def _seed_default_content(self) -> None:
        if self.config.hero_world != "service_ops" or not self.config.demo_mode:
            return
        for agent in default_service_ops_demo_agents():
            self.register_agent(agent)
        self._demo_steps = default_service_ops_demo_steps()

    def _ensure_agent(self, event: MirrorIngestEvent) -> MirrorAgentSpec:
        with self._lock:
            existing = self._agents.get(event.agent_id)
            if existing is not None:
                return existing.model_copy(deep=True)
        created = MirrorAgentSpec(
            agent_id=event.agent_id,
            name=event.agent_id.replace("-", " ").title(),
            mode=event.source_mode,
            status="registered",
        )
        with self._lock:
            self._agents[created.agent_id] = created.model_copy(deep=True)
            current = self._agents[created.agent_id].model_copy(deep=True)
        self._target.register_mirror_agent(current.model_copy(deep=True))
        return current

    def _autoplay_loop(self) -> None:
        interval_s = max(0.25, self.config.demo_interval_ms / 1000.0)
        while not self._stop.is_set():
            if not self._demo_steps:
                return
            if self._stop.wait(interval_s):
                return
            try:
                self.demo_tick()
            except Exception:
                logger.warning("mirror autoplay tick failed", exc_info=True)
                return


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()
