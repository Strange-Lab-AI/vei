from __future__ import annotations

import heapq
import json
import logging
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING

from vei.blueprint import list_runtime_facade_plugins
from vei.capability_graph.api import (
    build_graph_action_plan,
    build_runtime_capability_graphs,
    infer_graph_action_object_refs,
    get_runtime_capability_graph,
    resolve_graph_action,
)
from vei.capability_graph.models import (
    CapabilityGraphActionInput,
    CapabilityGraphActionResult,
    CapabilityGraphPlan,
    RuntimeCapabilityGraphs,
)
from vei.connectors.models import ConnectorReceipt
from vei.monitors.models import MonitorFinding
from vei.orientation.api import build_world_orientation
from vei.orientation.models import WorldOrientation
from vei.world.scenario import Scenario
from vei.world.models import (
    ActorState,
    InjectedEvent,
    ScheduledEvent,
    WorldSnapshot,
    WorldState,
)
from vei.world.replay import materialize_overlay_event

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from vei.router import Router


_MATERIALIZER_TARGET_ALIASES = {
    "calendar": "calendar",
    "chat": "slack",
    "db": "db",
    "database": "db",
    "doc": "docs",
    "docs": "docs",
    "mail": "mail",
    "slack": "slack",
    "ticket": "tickets",
    "tickets": "tickets",
    "tool": "tool",
}


def _materializer_event_payload(event: Any) -> Dict[str, Any]:
    delta = getattr(event, "delta", None)
    data = getattr(delta, "data", None)
    if not isinstance(data, dict):
        return {}
    return dict(data)


def _materializer_target_from_kind(kind: str) -> Optional[str]:
    lowered = str(kind or "").strip().lower().replace("-", "_")
    if not lowered:
        return None
    for part in lowered.split("."):
        for token in part.split("_"):
            target = _MATERIALIZER_TARGET_ALIASES.get(token)
            if target:
                return target
    return None


def _materializer_target_for_event(
    event: Any, payload: Dict[str, Any]
) -> Optional[str]:
    for key in ("target", "surface"):
        raw_value = payload.get(key)
        if not isinstance(raw_value, str):
            continue
        target = _MATERIALIZER_TARGET_ALIASES.get(raw_value.strip().lower())
        if target:
            return target
    if isinstance(payload.get("tool"), str):
        return "tool"
    target = _materializer_target_from_kind(str(getattr(event, "kind", "")))
    if target:
        return target
    domain_value = str(getattr(getattr(event, "domain", None), "value", "")).strip()
    if domain_value == "doc_graph":
        return "docs"
    if domain_value == "work_graph":
        return "tickets"
    if domain_value == "data_graph" and isinstance(payload.get("row"), dict):
        return "db"
    if domain_value == "comm_graph":
        if "channel" in payload or "thread_ts" in payload:
            return "slack"
        if "start_ms" in payload or "attendees" in payload:
            return "calendar"
        return "mail"
    return None


def _materializer_delivery_for_event(
    event: Any,
) -> Optional[tuple[str, Dict[str, Any]]]:
    payload = _materializer_event_payload(event)
    target = _materializer_target_for_event(event, payload)
    if target is None:
        return None

    actor_id = str(
        getattr(getattr(event, "actor_ref", None), "actor_id", "") or "system"
    )
    subject = str(
        payload.get("subj")
        or payload.get("subject")
        or payload.get("title")
        or payload.get("text")
        or getattr(event, "kind", "ingest_event")
    )
    body_text = str(
        payload.get("body_text")
        or payload.get("body")
        or payload.get("text")
        or payload.get("comment")
        or subject
    )

    if target == "mail":
        normalized = dict(payload)
        normalized["from"] = str(payload.get("from") or actor_id)
        normalized["to"] = str(payload.get("to") or "me@example")
        normalized["subj"] = subject
        normalized["body_text"] = body_text
        normalized["thread_id"] = str(
            payload.get("thread_id") or getattr(event, "case_id", "") or event.event_id
        )
        return target, normalized

    if target == "slack":
        normalized = dict(payload)
        normalized["channel"] = str(payload.get("channel") or "#procurement")
        normalized["text"] = str(payload.get("text") or body_text)
        normalized["user"] = str(payload.get("user") or actor_id)
        if payload.get("thread_ts") is not None:
            normalized["thread_ts"] = str(payload["thread_ts"])
        return target, normalized

    if target == "docs":
        normalized = dict(payload)
        normalized["title"] = subject
        normalized["body"] = str(
            payload.get("body") or payload.get("body_text") or body_text
        )
        return target, normalized

    if target == "tickets":
        normalized = dict(payload)
        if isinstance(payload.get("ticket_id"), str):
            if "comment" not in normalized and "body_text" in payload:
                normalized["comment"] = body_text
            normalized["author"] = str(payload.get("author") or actor_id)
            return target, normalized
        normalized["title"] = subject
        if "description" not in normalized:
            normalized["description"] = body_text
        normalized["assignee"] = payload.get("assignee")
        return target, normalized

    if target == "calendar":
        normalized = dict(payload)
        start_ms = int(payload.get("start_ms", getattr(event, "ts_ms", 0)))
        end_ms = int(payload.get("end_ms", start_ms + 1_800_000))
        normalized["title"] = subject
        normalized["start_ms"] = start_ms
        normalized["end_ms"] = end_ms
        if "organizer" not in normalized:
            normalized["organizer"] = actor_id
        return target, normalized

    if target == "db":
        normalized = dict(payload)
        normalized["op"] = str(payload.get("op") or "upsert")
        return target, normalized

    if target == "tool":
        normalized = dict(payload)
        normalized["args"] = (
            dict(payload.get("args", {}))
            if isinstance(payload.get("args"), dict)
            else {}
        )
        if not isinstance(normalized.get("tool"), str):
            return None
        return target, normalized

    return None


def _jsonable(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def _component_state(router: "Router") -> Dict[str, Dict[str, Any]]:
    components: Dict[str, Dict[str, Any]] = {}
    seen_component_names: set[str] = set()
    for plugin in list_runtime_facade_plugins():
        component_name = plugin.state_component_name()
        if component_name in seen_component_names or plugin.state_dump is None:
            continue
        component = getattr(router, plugin.component_attr or "", None)
        if component is None:
            continue
        components[component_name] = plugin.state_dump(component)
        seen_component_names.add(component_name)
    workforce_state = getattr(router, "workforce", None)
    if isinstance(workforce_state, dict) and workforce_state:
        components["workforce"] = _jsonable(workforce_state)
    return components


def serialize_router_state(router: "Router") -> WorldState:
    pending_events = [
        ScheduledEvent(
            event_id=getattr(event, "event_id", f"evt-{seq:08d}"),
            target=event.target,
            payload=_jsonable(event.payload),
            due_ms=int(event.t_due_ms),
            source=getattr(event, "source", "system"),
            actor_id=getattr(event, "actor_id", None),
            kind=getattr(event, "kind", "scheduled"),
        )
        for _, seq, event in sorted(router.bus._heap)
    ]
    actor_states = {
        actor_id: (
            state
            if isinstance(state, ActorState)
            else ActorState.model_validate(_jsonable(state))
        )
        for actor_id, state in getattr(router, "actor_states", {}).items()
    }
    return WorldState(
        branch=router.state_store.branch,
        clock_ms=int(router.bus.clock_ms),
        rng_state=int(router.bus.rng.state),
        queue_seq=int(router.bus._seq),
        seed=int(getattr(router, "seed", 0)),
        scenario=_jsonable(getattr(router, "scenario", None)),
        pending_events=pending_events,
        components=_component_state(router),
        trace_entries=_jsonable(router.trace.entries),
        receipts=_jsonable(router._receipts),
        connector_runtime={
            "mode": getattr(router.connector_mode, "value", str(router.connector_mode)),
            "request_seq": int(getattr(router.connector_runtime, "_request_seq", 0)),
            "receipts": _jsonable(
                [
                    receipt.model_dump()
                    for receipt in getattr(router.connector_runtime, "_receipts", [])
                ]
            ),
        },
        actor_states=actor_states,
        audit_state={
            "state_head": int(router.state_store.head),
            "state": _jsonable(router.state_store.materialised_state()),
            "policy_findings": _jsonable(router._policy_findings),
            "monitor_findings": _jsonable(
                [asdict(item) for item in router.monitor_manager.findings_tail(200)]
            ),
        },
        replay=_jsonable(getattr(router, "_replay_state", {})),
    )


def restore_router_state(router: "Router", state: WorldState) -> None:
    router.state_store.branch = state.branch
    if isinstance(state.scenario, dict):
        router.scenario = Scenario(**_jsonable(state.scenario))
    else:
        router.scenario = _jsonable(state.scenario)
    components = state.components
    workforce_state = components.get("workforce", {})
    router.workforce = (
        _jsonable(workforce_state) if isinstance(workforce_state, dict) else {}
    )
    seen_component_names: set[str] = set()
    for plugin in list_runtime_facade_plugins():
        component_name = plugin.state_component_name()
        if (
            component_name in seen_component_names
            or plugin.state_restore is None
            or plugin.component_attr is None
        ):
            continue
        component = getattr(router, plugin.component_attr, None)
        plugin_state = components.get(component_name)
        if component is not None and isinstance(plugin_state, dict):
            plugin.state_restore(component, plugin_state)
            seen_component_names.add(component_name)

    router.bus.clock_ms = int(state.clock_ms)
    router.bus.rng.state = int(state.rng_state)
    router.bus._seq = int(state.queue_seq)
    heap: list[tuple[int, int, Any]] = []
    seq = 0
    from vei.router import BusEvent as RuntimeEvent

    for item in state.pending_events:
        seq += 1
        heap.append(
            (
                int(item.due_ms),
                seq,
                RuntimeEvent(
                    t_due_ms=int(item.due_ms),
                    target=item.target,
                    payload=_jsonable(item.payload),
                    event_id=item.event_id,
                    source=item.source,
                    actor_id=item.actor_id,
                    kind=item.kind,
                ),
            )
        )
    heapq.heapify(heap)
    router.bus._heap = heap
    router.trace.entries = _jsonable(state.trace_entries)
    router.trace._flush_idx = len(router.trace.entries)
    router._receipts = _jsonable(state.receipts)
    restored_audit_state = _jsonable(state.audit_state.get("state", {}))
    router.state_store._state = (
        restored_audit_state if isinstance(restored_audit_state, dict) else {}
    )
    restored_state_head = int(state.audit_state.get("state_head", -1))
    if restored_state_head >= 0:
        from vei.world.state import Event as StateStoreEvent

        router.state_store._events = [
            StateStoreEvent.create(
                restored_state_head,
                kind="state.restore",
                payload={},
                clock_ms=int(state.clock_ms),
                event_id=f"state.restore.{restored_state_head}",
            )
        ]
    else:
        router.state_store._events = []
    router._policy_findings = _jsonable(state.audit_state.get("policy_findings", []))
    router.monitor_manager._findings = [
        MonitorFinding(**payload)
        for payload in _jsonable(state.audit_state.get("monitor_findings", []))
    ]
    router.actor_states = {
        actor_id: (
            value if isinstance(value, ActorState) else ActorState.model_validate(value)
        )
        for actor_id, value in state.actor_states.items()
    }
    router._replay_state = _jsonable(state.replay)
    router.connector_runtime._request_seq = int(
        state.connector_runtime.get(
            "request_seq", router.connector_runtime._request_seq
        )
    )
    mode_value = state.connector_runtime.get("mode")
    if mode_value:
        try:
            router.connector_runtime.mode = type(router.connector_runtime.mode)(
                mode_value
            )
        except Exception:
            logger.warning(
                "Failed to restore connector mode '%s'", mode_value, exc_info=True
            )
    router.connector_runtime._receipts = [
        ConnectorReceipt.model_validate(receipt)
        for receipt in _jsonable(state.connector_runtime.get("receipts", []))
    ]


class WorldSession:
    def __init__(self, router: "Router") -> None:
        self.router = router
        self.actor_registry: Optional[Any] = None
        if not hasattr(self.router, "actor_states"):
            self.router.actor_states = {}
        if not hasattr(self.router, "_replay_state"):
            self.router._replay_state = {}
        self.router._actor_dispatch = self._dispatch_actor_event

    @classmethod
    def attach_router(cls, router: "Router") -> "WorldSession":
        return cls(router)

    @classmethod
    def from_session_materializer(
        cls,
        sm: Any,
        case_id: str,
        *,
        seed: int = 42042,
        tenant_id: str = "",
    ) -> "WorldSession":
        """Create a WorldSession from an ingest SessionMaterializer slice.

        This is the opt-in constructor for the four-layer ingest path.
        Existing constructors remain the default.
        """
        from vei.world.api import create_world_session

        session = create_world_session(seed=seed, artifacts_dir=None)
        try:
            from vei.ingest.api import SessionMaterializer

            if isinstance(sm, SessionMaterializer):
                slice_data = sm.materialize(tenant_id, case_id)
                events = sorted(
                    list(getattr(slice_data, "events", [])),
                    key=lambda item: (
                        int(getattr(item, "ts_ms", 0)),
                        str(item.event_id),
                    ),
                )
                if not events:
                    return session
                base_ts = int(getattr(events[0], "ts_ms", 0))
                max_dt = 0
                scheduled = 0
                for event in events:
                    delivery = _materializer_delivery_for_event(event)
                    if delivery is None:
                        continue
                    target, payload = delivery
                    if target == "slack":
                        channel = str(payload.get("channel") or "#procurement")
                        session.router.slack.channels.setdefault(
                            channel,
                            {"messages": [], "unread": 0},
                        )
                        payload["channel"] = channel
                    dt_ms = max(0, int(getattr(event, "ts_ms", 0)) - base_ts)
                    max_dt = max(max_dt, dt_ms)
                    scheduled += 1
                    session.router.bus.schedule(
                        dt_ms=dt_ms,
                        target=target,
                        payload=payload,
                        event_id=str(getattr(event, "event_id", "")) or None,
                        source="ingest_materializer",
                        actor_id=getattr(
                            getattr(event, "actor_ref", None),
                            "actor_id",
                            None,
                        ),
                        kind=str(getattr(event, "kind", "scheduled")),
                    )
                if scheduled:
                    session.router.tick(dt_ms=max_dt)
        except ImportError:
            pass
        return session

    def attach_actor_registry(self, registry: Any) -> None:
        """Attach an actor-response backend so NPC events can generate replies."""
        self.actor_registry = registry

    def _dispatch_actor_event(
        self, actor_id: str, target: str, payload: Dict[str, Any]
    ) -> Optional[str]:
        """Called by Router when delivering an event tagged with actor_id.

        Returns the actor's response text, or None if no registry/actor.
        """
        if self.actor_registry is None:
            return None
        message = payload.get("text") or payload.get("body_text") or ""
        if not message:
            return None
        channel = payload.get("channel") or target
        return self.actor_registry.route_message(actor_id, message, channel=channel)

    def observe(self, focus_hint: Optional[str] = None) -> Dict[str, Any]:
        return self.router.observe(focus_hint=focus_hint).model_dump()

    def call_tool(
        self, tool: str, args: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        return self.router.call_and_step(tool, dict(args or {}))

    def act_and_observe(
        self, tool: str, args: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        return self.router.act_and_observe(tool, dict(args or {}))

    def pending(self) -> Dict[str, int]:
        return self.router.pending()

    def capability_graphs(self) -> RuntimeCapabilityGraphs:
        state = serialize_router_state(self.router)
        return build_runtime_capability_graphs(state)

    def graph_plan(
        self, *, domain: Optional[str] = None, limit: int = 12
    ) -> CapabilityGraphPlan:
        state = serialize_router_state(self.router)
        return build_graph_action_plan(state, domain=domain, limit=limit)

    def graph_action(
        self, action: CapabilityGraphActionInput | Dict[str, Any]
    ) -> CapabilityGraphActionResult:
        payload = (
            action
            if isinstance(action, CapabilityGraphActionInput)
            else CapabilityGraphActionInput.model_validate(action)
        )
        state = serialize_router_state(self.router)
        resolved = resolve_graph_action(state, payload)
        result = self.router.call_and_step(resolved.tool, dict(resolved.args))
        post_state = serialize_router_state(self.router)
        post_graph = get_runtime_capability_graph(post_state, resolved.domain)
        next_plan = build_graph_action_plan(post_state, limit=8)
        executed_focus_map = {
            "comm_graph": "slack",
            "doc_graph": "docs",
            "work_graph": "tickets",
            "identity_graph": "identity",
            "revenue_graph": "crm",
            "data_graph": "spreadsheet",
            "obs_graph": "pagerduty",
            "property_graph": "property",
            "campaign_graph": "campaign",
            "inventory_graph": "inventory",
        }
        next_focuses = list(next_plan.next_focuses)
        if resolved.domain == "ops_graph":
            tool_prefix = (resolved.tool or "").split(".")[0]
            executed_focus = tool_prefix if tool_prefix else "feature_flags"
        else:
            executed_focus = executed_focus_map.get(resolved.domain)
        if executed_focus and executed_focus not in next_focuses:
            next_focuses.insert(0, executed_focus)
        object_refs = infer_graph_action_object_refs(
            domain=resolved.domain,
            action=resolved.action,
            args=resolved.args,
            result=result,
        )
        graph_intent = f"{resolved.domain}.{resolved.action}"
        return CapabilityGraphActionResult(
            ok="error" not in result,
            branch=post_state.branch,
            clock_ms=post_state.clock_ms,
            domain=resolved.domain,
            action=resolved.action,
            tool=resolved.tool,
            tool_args=dict(resolved.args),
            step_id=resolved.step_id,
            result=result,
            graph=(
                post_graph.model_dump(mode="json")
                if hasattr(post_graph, "model_dump")
                else {}
            ),
            next_focuses=next_focuses,
            metadata={
                "graph_domain": resolved.domain,
                "graph_action": resolved.action,
                "graph_intent": graph_intent,
                "requested_args": dict(resolved.args),
                "affected_object_refs": object_refs,
                "scenario_name": (
                    str((post_state.scenario or {}).get("name"))
                    if (post_state.scenario or {}).get("name") is not None
                    else None
                ),
                "step_title": resolved.title,
                "remaining_suggested_steps": len(next_plan.suggested_steps),
            },
        )

    def orientation(self) -> WorldOrientation:
        state = serialize_router_state(self.router)
        return build_world_orientation(state)

    def current_state(self) -> WorldState:
        return serialize_router_state(self.router)

    def snapshot(self, label: Optional[str] = None) -> WorldSnapshot:
        state = self.current_state()
        path = self._persist_snapshot(state, label=label)
        raw = json.loads(path.read_text(encoding="utf-8"))
        return WorldSnapshot(
            snapshot_id=int(raw.get("index", path.stem)),
            branch=str(raw.get("branch", state.branch)),
            time_ms=int(raw.get("clock_ms", state.clock_ms)),
            data=WorldState.model_validate(raw.get("data", {})),
            label=raw.get("label"),
        )

    def restore(self, snapshot_id: int) -> WorldSnapshot:
        snapshot = self._load_snapshot(snapshot_id)
        restore_router_state(self.router, snapshot.data)
        return snapshot

    def branch(self, snapshot_id: int, branch_name: str) -> "WorldSession":
        snapshot = self._load_snapshot(snapshot_id)
        from vei.world.api import create_world_session

        branched = create_world_session(
            seed=snapshot.data.seed,
            artifacts_dir=self.router.trace.out_dir,
            connector_mode=self.router.connector_mode.value,
            scenario=self.router.scenario,
            branch=branch_name,
        )
        restore_router_state(
            branched.router, snapshot.data.model_copy(update={"branch": branch_name})
        )
        branched.router.state_store.branch = branch_name
        return branched

    def inject(self, event: InjectedEvent | Dict[str, Any]) -> Dict[str, Any]:
        payload = (
            event
            if isinstance(event, InjectedEvent)
            else InjectedEvent.model_validate(event)
        )
        event_id = self.router.bus.schedule(
            dt_ms=int(payload.dt_ms),
            target=payload.target,
            payload=dict(payload.payload),
            source=payload.source,
            actor_id=payload.actor_id,
            kind=payload.kind,
        )
        self.router._sync_world_snapshot(label=f"injected:{event_id}")
        return {"ok": True, "event_id": event_id}

    def list_events(self) -> List[Dict[str, Any]]:
        return [item.model_dump() for item in self.current_state().pending_events]

    def cancel_event(self, event_id: str) -> Dict[str, Any]:
        cancelled = self.router.bus.cancel(event_id)
        if cancelled:
            self.router._sync_world_snapshot(label=f"cancelled:{event_id}")
        return {"ok": cancelled, "event_id": event_id}

    def replay(
        self,
        *,
        mode: str,
        dataset_events: Optional[Iterable[Any]] = None,
    ) -> Dict[str, Any]:
        normalized = mode.strip().lower()
        if normalized not in {"strict", "overlay"}:
            raise ValueError(f"unsupported replay mode: {mode}")
        scheduled = 0
        if normalized == "overlay":
            for raw in dataset_events or []:
                payload = materialize_overlay_event(raw)
                if payload is None:
                    continue
                scheduled += 1
                self.router.bus.schedule(
                    dt_ms=max(
                        0, int(payload.get("time_ms", 0)) - self.router.bus.clock_ms
                    ),
                    target=str(payload.get("target")),
                    payload=_jsonable(payload.get("payload", {})),
                    source=str(payload.get("source", "replay_overlay")),
                    actor_id=payload.get("actor_id"),
                    kind="scheduled",
                )
        else:
            self.router.bus.clear()
            for actor_state in self.router.actor_states.values():
                for event in actor_state.recorded_events:
                    scheduled += 1
                    self.router.bus.schedule(
                        dt_ms=max(0, int(event.due_ms) - self.router.bus.clock_ms),
                        target=event.target,
                        payload=_jsonable(event.payload),
                        event_id=event.event_id,
                        source=event.source,
                        actor_id=event.actor_id or actor_state.actor_id,
                        kind="actor_recorded",
                    )
        self.router._replay_state = {"mode": normalized, "scheduled": scheduled}
        self.router._sync_world_snapshot(label=f"replay:{normalized}")
        return {"ok": True, "mode": normalized, "scheduled": scheduled}

    def register_actor(self, actor: ActorState | Dict[str, Any]) -> ActorState:
        payload = (
            actor if isinstance(actor, ActorState) else ActorState.model_validate(actor)
        )
        self.router.actor_states[payload.actor_id] = payload
        self.router._sync_world_snapshot(label=f"actor:{payload.actor_id}")
        return payload

    def _snapshot_dir(self) -> Optional[Path]:
        if not self.router.state_store.storage_dir:
            return None
        path = self.router.state_store.storage_dir / "snapshots"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _next_snapshot_id(self) -> int:
        directory = self._snapshot_dir()
        if directory is None:
            return int(self.router.state_store.head + 1)
        ids = []
        for path in directory.glob("*.json"):
            try:
                ids.append(int(path.stem))
            except ValueError:
                continue
        fallback = max(int(self.router.state_store.head), 0)
        return max(ids, default=fallback) + 1

    def _persist_snapshot(self, state: WorldState, label: Optional[str] = None) -> Path:
        directory = self._snapshot_dir()
        snapshot_id = self._next_snapshot_id()
        snapshot = WorldSnapshot(
            snapshot_id=snapshot_id,
            branch=state.branch,
            time_ms=state.clock_ms,
            data=state,
            label=label,
        )
        payload = {
            "index": snapshot.snapshot_id,
            "clock_ms": snapshot.time_ms,
            "branch": snapshot.branch,
            "label": snapshot.label,
            "data": snapshot.data.model_dump(),
        }
        if directory is None:
            fallback = (
                Path(self.router.trace.out_dir or ".") / ".artifacts" / "snapshots"
            )
            fallback.mkdir(parents=True, exist_ok=True)
            path = fallback / f"{snapshot.snapshot_id:09d}.json"
        else:
            path = directory / f"{snapshot.snapshot_id:09d}.json"
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def _load_snapshot(self, snapshot_id: int) -> WorldSnapshot:
        directory = self._snapshot_dir()
        if directory is None:
            raise ValueError(
                "snapshot restore requires VEI_STATE_DIR or router storage"
            )
        path = directory / f"{int(snapshot_id):09d}.json"
        if not path.exists():
            raise ValueError(f"snapshot not found: {snapshot_id}")
        raw = json.loads(path.read_text(encoding="utf-8"))
        return WorldSnapshot(
            snapshot_id=int(raw.get("index", snapshot_id)),
            branch=str(raw.get("branch", self.router.state_store.branch)),
            time_ms=int(raw.get("clock_ms", 0)),
            data=WorldState.model_validate(raw.get("data", {})),
            label=raw.get("label"),
        )
