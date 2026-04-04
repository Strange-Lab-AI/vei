from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from pydantic import BaseModel
from vei.blueprint import FacadePlugin, list_runtime_facade_plugins
from vei.connectors import (
    ConnectorInvocationError,
    create_default_runtime,
    parse_adapter_mode,
)
from vei.monitors import MonitorManager
from vei.router._policy import DEFAULT_RULES, PolicyEngine, PromoteMonitorRule
from vei.world import (
    DriftEngine,
    Event as StateEvent,
    ReplayAdapter,
    Scenario,
    StateStore,
    load_from_env,
)
from ._catalog import build_alias_map, build_builtin_tool_specs, build_help_payload
from ._dispatch import GUARDED_PREFIXES, build_dispatch_table
from .calendar import CalendarSim
from .datadog import DatadogSim, DatadogToolProvider
from .database import DatabaseSim
from .docs import DocsSim
from .feature_flags import FeatureFlagSim, FeatureFlagToolProvider
from .google_admin import GoogleAdminSim, GoogleAdminToolProvider
from .hris import HrisSim, HrisToolProvider
from .jira import JiraToolProvider
from .pagerduty import PagerDutySim, PagerDutyToolProvider
from .siem import SiemSim, SiemToolProvider
from .tickets import TicketsSim
from .errors import MCPError
from .tool_providers import ToolProvider
from .servicedesk import ServiceDeskSim, ServiceDeskToolProvider
from .tool_registry import ToolRegistry, ToolSpec
from .sims import SlackSim, MailSim, BrowserVirtual
from ._event_bus import Event, EventBus, LinearCongruentialGenerator  # noqa: F401
from ._observation import (
    build_action_menu,
    build_focus_summary,
    resolve_focus_for_tool,
)
from ._trace import TraceLogger  # noqa: F401
from ._reducers import (  # noqa: F401
    FAULT_PROFILES,
    _reduce_drift_delivered,
    _reduce_drift_schedule,
    _reduce_event_delivery,
    _reduce_monitor_finding,
    _reduce_policy_finding,
    _reduce_router_init,
    _reduce_tool_call,
)

logger = logging.getLogger(__name__)


class Observation(BaseModel):
    time_ms: int
    focus: str
    summary: str
    screenshot_ref: Optional[str] = None
    action_menu: List[Dict[str, Any]]
    pending_events: Dict[str, int]


class Router:
    def __init__(
        self,
        seed: int,
        artifacts_dir: Optional[str] = None,
        scenario: Optional[Scenario] = None,
        connector_mode: Optional[str] = None,
        branch: str = "main",
        surface_fidelity: Optional[Dict[str, Any]] = None,
    ):
        self.seed = int(seed)
        self.world_session = None
        self._surface_fidelity = surface_fidelity or {}
        self._l2_store: Optional[Any] = None
        if any(
            (v.level if hasattr(v, "level") else v.get("level")) == "L2"
            for v in self._surface_fidelity.values()
        ):
            from vei.blueprint import L2Store

            self._l2_store = L2Store()
        self.bus = EventBus(seed)

        state_dir_env = os.environ.get("VEI_STATE_DIR")
        base_dir = Path(state_dir_env).expanduser() if state_dir_env else None
        self.state_store = StateStore(base_dir=base_dir, branch=branch)
        self.state_store.register_reducer("router.init", _reduce_router_init)
        self.state_store.register_reducer("tool.call", _reduce_tool_call)
        self.state_store.register_reducer("event.delivery", _reduce_event_delivery)
        self.state_store.register_reducer("drift.schedule", _reduce_drift_schedule)
        self.state_store.register_reducer("drift.delivered", _reduce_drift_delivered)
        self.state_store.register_reducer("monitor.finding", _reduce_monitor_finding)
        self.state_store.register_reducer("policy.finding", _reduce_policy_finding)
        self._snapshot_interval = 25 if base_dir else None
        self._receipts: List[Dict[str, Any]] = []
        self._receipts_path: Optional[Path] = None
        if self.state_store.storage_dir:
            self._receipts_path = self.state_store.storage_dir / "receipts.jsonl"
            self._load_receipts()

        self.registry = ToolRegistry()
        self.tool_providers: List[ToolProvider] = []
        self.facade_plugins: Dict[str, Dict[str, Any]] = {}
        self._seed_tool_registry()
        self.alias_map = self._build_alias_map()
        self._register_alias_specs(self.alias_map)
        fault_profile_env = os.environ.get("VEI_FAULT_PROFILE", "off").strip().lower()
        if fault_profile_env not in FAULT_PROFILES:
            fault_profile_env = "off"
        self.fault_profile = fault_profile_env
        self._fault_overrides = dict(FAULT_PROFILES.get(fault_profile_env, {}))
        monitors_env = os.environ.get("VEI_MONITORS", "").strip()
        monitor_names = [
            m.strip()
            for m in (monitors_env.split(",") if monitors_env else [])
            if m.strip()
        ]
        self.monitor_manager = MonitorManager(self.registry, monitor_names)
        rules = list(DEFAULT_RULES)
        policy_promote_env = os.environ.get("VEI_POLICY_PROMOTE", "").strip()
        if policy_promote_env:
            for item in policy_promote_env.split(","):
                token = item.strip()
                if not token:
                    continue
                if ":" in token:
                    code, severity = token.split(":", 1)
                    rules.append(
                        PromoteMonitorRule(
                            code.strip(), severity=severity.strip() or "warning"
                        )
                    )
                else:
                    rules.append(PromoteMonitorRule(token, severity="warning"))
        self.policy_engine = PolicyEngine(rules)
        self._policy_findings: List[Dict[str, Any]] = []
        self.actor_states: Dict[str, Any] = {}
        self._replay_state: Dict[str, Any] = {}
        self._actor_dispatch: Optional[Any] = None

        self.trace = TraceLogger(artifacts_dir)
        self.scenario = scenario or load_from_env(seed)
        self.slack = SlackSim(self.bus, self.scenario)
        self.mail = MailSim(self.bus, self.scenario)
        self.browser = BrowserVirtual(self.bus, self.scenario)
        dataset_path = os.environ.get("VEI_DATASET")
        if dataset_path:
            try:
                from vei.data.models import VEIDataset
                import json

                data = json.loads(Path(dataset_path).read_text(encoding="utf-8"))
                dataset = VEIDataset.model_validate(data)
                adapter = ReplayAdapter(self.bus, dataset.events)
                adapter.prime()
                self.replay_adapter = adapter
            except Exception:
                logger.warning(
                    "Failed to load replay dataset from %s", dataset_path, exc_info=True
                )
                self.replay_adapter = None
        else:
            self.replay_adapter = None
        self.docs = DocsSim(self.scenario)
        self.calendar = CalendarSim(self.scenario)
        self.tickets = TicketsSim(self.scenario)
        self.database = DatabaseSim(self.scenario)
        # Optional ERP twin
        try:
            from .erp import ErpSim  # local import to avoid import-time failures

            self.erp = ErpSim(self.bus, self.scenario)
        except Exception:
            logger.warning("ERP twin failed to initialise", exc_info=True)
            self.erp = None  # type: ignore[attr-defined]
        # Optional CRM twin
        try:
            from .crm import CrmSim

            self.crm = CrmSim(self.bus, self.scenario)
        except Exception:
            logger.warning("CRM twin failed to initialise", exc_info=True)
            self.crm = None  # type: ignore[attr-defined]

        # Optional identity twin
        self.okta = None  # type: ignore[attr-defined]
        try:
            from .identity import OktaSim, OktaToolProvider

            self.okta = OktaSim(self.scenario)
            self.register_tool_provider(OktaToolProvider(self.okta))
        except Exception:
            logger.warning("Okta twin failed to initialise", exc_info=True)
            self.okta = None  # type: ignore[attr-defined]

        # ServiceDesk twin
        self.servicedesk = ServiceDeskSim(self.scenario)
        self.register_tool_provider(ServiceDeskToolProvider(self.servicedesk))

        # Admin / control-plane twins
        self.google_admin = GoogleAdminSim(self.scenario)
        self.register_tool_provider(GoogleAdminToolProvider(self.google_admin))
        self.siem = SiemSim(self.scenario)
        self.register_tool_provider(SiemToolProvider(self.siem))
        self.datadog = DatadogSim(self.scenario)
        self.register_tool_provider(DatadogToolProvider(self.datadog))
        self.pagerduty = PagerDutySim(self.scenario)
        self.register_tool_provider(PagerDutyToolProvider(self.pagerduty))
        self.feature_flags = FeatureFlagSim(self.scenario)
        self.register_tool_provider(FeatureFlagToolProvider(self.feature_flags))
        self.hris = HrisSim(self.scenario)
        self.register_tool_provider(HrisToolProvider(self.hris))
        self.register_tool_provider(JiraToolProvider(self.tickets))
        self._bootstrap_facade_plugins()

        self.connector_mode = parse_adapter_mode(
            connector_mode or os.environ.get("VEI_CONNECTOR_MODE")
        )
        connector_receipts_path: Optional[Path] = None
        if self.state_store.storage_dir:
            connector_receipts_path = (
                self.state_store.storage_dir / "connector_receipts.jsonl"
            )
        elif artifacts_dir:
            connector_receipts_path = Path(artifacts_dir) / "connector_receipts.jsonl"
        self.connector_runtime = create_default_runtime(
            mode=self.connector_mode,
            slack=self.slack,
            mail=self.mail,
            calendar=self.calendar,
            docs=self.docs,
            tickets=self.tickets,
            database=self.database,
            erp=getattr(self, "erp", None),
            crm=getattr(self, "crm", None),
            okta=getattr(self, "okta", None),
            servicedesk=self.servicedesk,
            receipts_path=connector_receipts_path,
        )

        for evt in self.scenario.derail_events or []:
            try:
                dt = int(evt.get("dt_ms", 0))
                target = evt.get("target")
                payload = evt.get("payload", {})
                if target:
                    self.bus.schedule(dt_ms=dt, target=target, payload=payload)
            except Exception:
                continue

        drift_seed_env = os.environ.get("VEI_DRIFT_SEED")
        try:
            drift_seed = (
                int(drift_seed_env) if drift_seed_env is not None else (seed ^ 0xD1F7)
            )
        except ValueError:
            drift_seed = seed ^ 0xD1F7
        drift_mode = (
            os.environ.get("VEI_DRIFT_MODE")
            or os.environ.get("VEI_DRIFT_RATE")
            or "off"
        )
        self.drift = DriftEngine(
            state_store=self.state_store, bus=self.bus, seed=drift_seed, mode=drift_mode
        )
        self.drift.prime()

        existing_policy = self.state_store.materialised_state().get("policy", {})
        if isinstance(existing_policy, dict):
            findings = existing_policy.get("findings", [])
            if isinstance(findings, list):
                self._policy_findings.extend(findings)

        self._dispatch = self._build_dispatch_table()
        self._record_router_init(seed)
        self._sync_world_snapshot(label="router.init")

    @staticmethod
    def _jsonable(value: Any) -> Any:
        try:
            json.dumps(value)
            return value
        except TypeError:
            if isinstance(value, dict):
                return {k: Router._jsonable(v) for k, v in value.items()}
            if isinstance(value, list):
                return [Router._jsonable(v) for v in value]
            if isinstance(value, tuple):
                return [Router._jsonable(v) for v in value]
            if isinstance(value, set):
                return [Router._jsonable(v) for v in sorted(value)]
            return repr(value)

    def _append_state(
        self,
        kind: str,
        payload: Optional[Dict[str, Any]] = None,
        *,
        clock_ms: Optional[int] = None,
    ) -> Optional[StateEvent]:
        payload_map = {k: Router._jsonable(v) for k, v in dict(payload or {}).items()}
        event_clock = self.bus.clock_ms if clock_ms is None else int(clock_ms)
        event = self.state_store.append(kind, payload_map, clock_ms=event_clock)
        if self._snapshot_interval and event.index % self._snapshot_interval == 0:
            self._sync_world_snapshot(label=kind)
        return event

    def _record_router_init(self, seed: int) -> None:
        scenario_name = getattr(self.scenario, "name", None)
        payload = {
            "seed": seed,
            "scenario": scenario_name,
            "branch": self.state_store.branch,
        }
        self._append_state("router.init", payload)
        if self._snapshot_interval:
            self._sync_world_snapshot(label="router.init")

    def _sync_world_snapshot(self, label: Optional[str] = None) -> None:
        try:
            from vei.world.api import ensure_world_session

            ensure_world_session(self).snapshot(label=label)
        except Exception:
            # Snapshotting is best-effort to preserve runtime continuity.
            pass

    def _record_tool_call(self, tool: str, args: Dict[str, Any], result: Any) -> None:
        payload = {
            "tool": tool,
            "args": {k: Router._jsonable(v) for k, v in dict(args or {}).items()},
            "time_ms": self.bus.clock_ms,
        }
        event = self._append_state("tool.call", payload)
        receipt = {
            "tool": tool,
            "time_ms": self.bus.clock_ms,
            "state_head": self.state_store.head,
            "event_index": event.index if event else None,
        }
        try:
            receipt["result_preview"] = Router._jsonable(result)
        except Exception:
            receipt["result_preview"] = repr(result)
        self._receipts.append(receipt)
        if len(self._receipts) > 50:
            self._receipts.pop(0)
        self._write_receipt(receipt)

        findings: List[Any] = []
        if self.monitor_manager.monitors():
            snapshot = self.state_snapshot(
                include_state=False, tool_tail=0, include_receipts=False
            )
            findings = self.monitor_manager.after_tool_call(
                tool=tool,
                args=args,
                result=result,
                snapshot=snapshot,
            )
            for finding in findings:
                payload = {
                    "monitor": finding.monitor,
                    "code": finding.code,
                    "message": finding.message,
                    "severity": finding.severity,
                    "time_ms": finding.time_ms,
                    "tool": finding.tool,
                    "metadata": Router._jsonable(finding.metadata),
                }
                self._append_state("monitor.finding", payload)
        if findings:
            policy_findings = self.policy_engine.evaluate(findings)
            for pf in policy_findings:
                payload = {
                    "code": pf.code,
                    "message": pf.message,
                    "severity": pf.severity,
                    "time_ms": pf.time_ms,
                    "tool": pf.tool,
                    "metadata": Router._jsonable(pf.metadata),
                }
                self._policy_findings.append(payload)
                self._append_state("policy.finding", payload)
        if len(self._policy_findings) > 200:
            self._policy_findings = self._policy_findings[-200:]

    def _record_event_delivery(self, target: str, payload: Dict[str, Any]) -> None:
        st_payload = {
            "target": target,
            "payload": Router._jsonable(payload),
            "time_ms": self.bus.clock_ms,
        }
        self._append_state("event.delivery", st_payload)
        if getattr(self, "drift", None) is not None:
            try:
                self.drift.handle_delivery(target, payload)
            except Exception:
                # Drift is best-effort; never break the main loop.
                pass

    def _deliver_event(self, target: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if target == "slack":
            return self.slack.deliver(payload)
        if target == "mail":
            return self.mail.deliver(payload)
        if target == "docs":
            return self.docs.deliver(payload)
        if target == "calendar":
            return self.calendar.deliver(payload)
        if target == "tickets":
            return self.tickets.deliver(payload)
        if target in {"db", "database"}:
            return self.database.deliver(payload)
        if target in {
            "erp",
            "crm",
            "servicedesk",
            "okta",
            "google_admin",
            "siem",
            "datadog",
            "pagerduty",
            "feature_flags",
            "hris",
            "jira",
            "tool",
        }:
            tool = payload.get("tool")
            args = payload.get("args", {})
            if not isinstance(tool, str):
                raise MCPError(
                    "invalid_event",
                    f"{target} event payload must include string 'tool'",
                )
            if not isinstance(args, dict):
                raise MCPError(
                    "invalid_event", f"{target} event payload args must be an object"
                )
            result = self._execute(tool, args)
            return {"tool": tool, "result": Router._jsonable(result)}
        plugin_delivery = self._deliver_plugin_event(target, payload)
        if plugin_delivery is not None:
            return plugin_delivery
        # Unknown targets are intentionally ignored but surfaced in trace/state.
        return {"ignored": True, "reason": f"unsupported target '{target}'"}

    def _deliver_due_event(self, evt: Event) -> Dict[str, Any]:
        try:
            emitted = self._deliver_event(evt.target, evt.payload)
        except Exception as exc:
            emitted = {
                "error": {
                    "type": type(exc).__name__,
                    "message": str(exc),
                }
            }
        self.trace.record_event(
            evt.target, evt.payload, emitted, time_ms=self.bus.clock_ms
        )
        self._record_event_delivery(evt.target, evt.payload)
        if evt.actor_id and self._actor_dispatch:
            response = self._actor_dispatch(evt.actor_id, evt.target, evt.payload)
            if response:
                self.bus.schedule(
                    dt_ms=2000,
                    target=evt.target,
                    payload={
                        "text": response,
                        "user": evt.actor_id,
                        "channel": evt.payload.get("channel", ""),
                    },
                    source=f"actor:{evt.actor_id}",
                )
        return emitted

    def _load_receipts(self) -> None:
        if not self._receipts_path or not self._receipts_path.exists():
            return
        try:
            with self._receipts_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    data = json.loads(line)
                    self._receipts.append(data)
        except Exception:
            logger.warning(
                "Failed to load receipts from %s", self._receipts_path, exc_info=True
            )
            self._receipts = []

    def _write_receipt(self, receipt: Dict[str, Any]) -> None:
        if not self._receipts_path:
            return
        try:
            with self._receipts_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(receipt, sort_keys=True) + "\n")
        except Exception:
            logger.warning(
                "Failed to persist receipt to %s", self._receipts_path, exc_info=True
            )

    def state_snapshot(
        self,
        *,
        include_state: bool = False,
        tool_tail: int = 20,
        include_receipts: bool = True,
    ) -> Dict[str, Any]:
        state = self.state_store.materialised_state()
        tool_calls: List[Dict[str, Any]] = list(state.get("tool_calls", []))
        tail = tool_calls[-tool_tail:] if tool_tail and tool_tail > 0 else tool_calls
        deliveries = dict(state.get("deliveries", {}))
        drift_state = state.get("drift", {})
        drift_summary = {
            "scheduled_count": len(drift_state.get("scheduled", [])),
            "delivered": dict(drift_state.get("delivered", {})),
        }
        snapshot: Dict[str, Any] = {
            "head": self.state_store.head,
            "branch": self.state_store.branch,
            "time_ms": self.bus.clock_ms,
            "meta": dict(state.get("meta", {})),
            "tool_tail": tail,
            "deliveries": deliveries,
            "drift": drift_summary,
        }
        monitor_tail = [
            asdict(f) for f in self.monitor_manager.findings_tail(tool_tail or 20)
        ]
        snapshot["monitor_findings"] = monitor_tail
        policy_tail: List[Dict[str, Any]] = []
        for item in self._policy_findings[-(tool_tail or 20) :]:
            policy_tail.append(
                {
                    "code": item.get("code"),
                    "message": item.get("message"),
                    "severity": item.get("severity"),
                    "time_ms": item.get("time_ms"),
                    "tool": item.get("tool"),
                    "metadata": item.get("metadata", {}),
                }
            )
        snapshot["policy_findings"] = policy_tail
        snapshot["connectors"] = {
            "mode": self.connector_mode.value,
            "last_receipt": self.connector_runtime.last_receipt(),
        }
        snapshot["scheduled_events"] = [
            {
                "event_id": getattr(event, "event_id", None),
                "target": event.target,
                "due_ms": event.t_due_ms,
                "source": getattr(event, "source", "system"),
                "actor_id": getattr(event, "actor_id", None),
                "kind": getattr(event, "kind", "scheduled"),
            }
            for event in self.bus.list_events()
        ]
        snapshot["actors"] = {
            actor_id: (
                state.model_dump() if hasattr(state, "model_dump") else dict(state)
            )
            for actor_id, state in self.actor_states.items()
        }
        snapshot["replay"] = dict(self._replay_state)
        if include_receipts:
            snapshot["receipts"] = (
                list(self._receipts[-tool_tail:]) if tool_tail else list(self._receipts)
            )
        if include_state:
            snapshot["state"] = state
        return snapshot

    def register_tool_provider(self, provider: ToolProvider) -> None:
        """Register a provider and copy its specs into the registry."""
        self.tool_providers.append(provider)
        self._register_tool_specs(provider.specs())

    def _bootstrap_facade_plugins(self) -> None:
        for plugin in list_runtime_facade_plugins():
            component = (
                getattr(self, plugin.component_attr, None)
                if plugin.component_attr
                else None
            )
            if component is None and plugin.component_factory and plugin.component_attr:
                component = plugin.component_factory(self, self.scenario)
                setattr(self, plugin.component_attr, component)
            if plugin.component_attr and component is not None:
                self.facade_plugins[plugin.manifest.name] = {
                    "plugin": plugin,
                    "component": component,
                }
                if plugin.provider_factory is not None:
                    self.register_tool_provider(plugin.provider_factory(component))

    def _event_targets(self) -> List[str]:
        targets = [
            "slack",
            "mail",
            "calendar",
            "docs",
            "tickets",
            "db",
            "erp",
            "crm",
            "okta",
            "servicedesk",
            "google_admin",
            "siem",
            "datadog",
            "pagerduty",
            "feature_flags",
            "hris",
            "jira",
            "tool",
        ]
        for entry in self.facade_plugins.values():
            plugin: FacadePlugin = entry["plugin"]
            for target in plugin.event_targets:
                if target not in targets:
                    targets.append(target)
        return targets

    def _plugin_focus_for_tool(self, tool: str) -> Optional[str]:
        for entry in self.facade_plugins.values():
            plugin: FacadePlugin = entry["plugin"]
            for prefix in plugin.tool_prefixes:
                if tool.startswith(prefix):
                    return plugin.focuses[0] if plugin.focuses else plugin.manifest.name
        return None

    def _plugin_summary(self, focus: str) -> Optional[str]:
        for entry in self.facade_plugins.values():
            plugin: FacadePlugin = entry["plugin"]
            if plugin.matches_focus(focus) and plugin.summary_builder is not None:
                return plugin.summary_builder(self, entry["component"])
        return None

    def _plugin_action_menu(self, focus: str) -> Optional[List[Dict[str, Any]]]:
        for entry in self.facade_plugins.values():
            plugin: FacadePlugin = entry["plugin"]
            if plugin.matches_focus(focus) and plugin.action_menu_builder is not None:
                return plugin.action_menu_builder(self, entry["component"])
        return None

    def _deliver_plugin_event(
        self, target: str, payload: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        for entry in self.facade_plugins.values():
            plugin: FacadePlugin = entry["plugin"]
            if target not in plugin.event_targets:
                continue
            component = entry["component"]
            if plugin.event_handler is not None:
                return plugin.event_handler(self, component, payload)
            tool = payload.get("tool")
            args = payload.get("args", {})
            if not isinstance(tool, str):
                raise MCPError(
                    "invalid_event",
                    f"{target} event payload must include string 'tool'",
                )
            if not isinstance(args, dict):
                raise MCPError(
                    "invalid_event", f"{target} event payload args must be an object"
                )
            result = self._execute(tool, args)
            return {"tool": tool, "result": Router._jsonable(result)}
        return None

    def _register_tool_specs(self, specs: Iterable[ToolSpec]) -> None:
        for spec in specs:
            try:
                self.registry.register(spec)
            except ValueError:
                continue

    def _build_alias_map(self) -> Dict[str, str]:
        return build_alias_map()

    def _register_alias_specs(self, alias_map: Dict[str, str]) -> None:
        specs: List[ToolSpec] = []
        for alias_name, base_tool in alias_map.items():
            base = self.registry.get(base_tool)
            if base:
                specs.append(
                    ToolSpec(
                        name=alias_name,
                        description=f"Alias -> {base_tool}. {base.description}",
                        side_effects=base.side_effects,
                        permissions=base.permissions,
                        default_latency_ms=base.default_latency_ms,
                        latency_jitter_ms=base.latency_jitter_ms,
                        nominal_cost=base.nominal_cost,
                        returns=base.returns,
                        fault_probability=base.fault_probability,
                    )
                )
            else:
                specs.append(
                    ToolSpec(name=alias_name, description=f"Alias -> {base_tool}")
                )
        self._register_tool_specs(specs)

    def _seed_tool_registry(self) -> None:
        self._register_tool_specs(build_builtin_tool_specs())

    def last_receipt(self) -> Optional[Dict[str, Any]]:
        return self._receipts[-1] if self._receipts else None

    def search_tools(self, query: str, top_k: int = 10) -> Dict[str, Any]:
        matches = self.registry.search(query, top_k=top_k)
        results = [
            {
                "name": spec.name,
                "description": spec.description,
                "score": round(score, 3),
                "permissions": list(spec.permissions),
                "side_effects": list(spec.side_effects),
                "default_latency_ms": spec.default_latency_ms,
                "latency_jitter_ms": spec.latency_jitter_ms,
                "fault_probability": spec.fault_probability,
                "returns": spec.returns,
            }
            for spec, score in matches
        ]
        return {
            "query": query,
            "top_k": top_k,
            "results": results,
        }

    def help_payload(self) -> Dict[str, Any]:
        return build_help_payload(self)

    def call_and_step(self, tool: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a tool call, deliver any due event, advance time, and persist trace.

        This keeps the simulation deterministic and ensures artifacts are flushed
        so downstream scoring can consume trace.jsonl during tests.
        """
        result = self._execute(tool, args)
        self._record_tool_call(tool, args, result)
        self.trace.record_call(tool, args, result, time_ms=self.bus.clock_ms)
        evt = self.bus.next_if_due()
        if evt:
            self._deliver_due_event(evt)
        self.bus.advance(self._tool_latency_ms(tool))
        # Persist after each step when artifacts directory is configured
        self.trace.flush()
        return result

    def _build_dispatch_table(self) -> Dict[str, Any]:
        return build_dispatch_table(self)

    _GUARDED_PREFIXES = GUARDED_PREFIXES

    def _execute(self, tool: str, args: Dict[str, Any]) -> Any:
        if tool == "vei.observe":
            focus = args.get("focus") if isinstance(args, dict) else None
            return self.observe(focus_hint=focus).model_dump()
        if tool == "vei.tick":
            return self.tick(**args)
        if tool == "vei.state":
            return self.state_snapshot(**args)
        if tool == "vei.act_and_observe":
            target_tool = args.get("tool")
            target_args = args.get("args", {})
            if not target_tool:
                raise MCPError("invalid_args", "act_and_observe requires tool")
            return self.act_and_observe(target_tool, target_args)
        if tool == "vei.inject":
            return self.inject(**args)

        if not tool.startswith("vei."):
            self._maybe_fault(tool)
        tool = self.alias_map.get(tool, tool)
        intercepted = self._maybe_fidelity_intercept(tool, args)
        if intercepted is not None:
            return intercepted
        if self.connector_runtime.managed_tool(tool):
            try:
                return self.connector_runtime.invoke_tool(
                    tool,
                    args,
                    time_ms=self.bus.clock_ms,
                    metadata={"router_branch": self.state_store.branch},
                )
            except ConnectorInvocationError as exc:
                raise MCPError(exc.code, exc.message) from exc

        handler = self._dispatch.get(tool)
        if handler is not None:
            return handler(args)

        for prefix, label in self._GUARDED_PREFIXES.items():
            if tool.startswith(prefix):
                if not getattr(self, prefix.rstrip("."), None):
                    raise MCPError("unsupported_tool", f"{label} twin not available")
                raise MCPError("unknown_tool", f"No such tool: {tool}")

        for provider in self.tool_providers:
            if provider.handles(tool):
                return provider.call(tool, args)

        raise MCPError("unknown_tool", f"No such tool: {tool}")

    def snapshot_observation(self, focus_hint: Optional[str] = None) -> Observation:
        """Build an Observation without advancing time or delivering events.

        Useful for server adapters that need to return an observation after a
        call_and_step without mutating simulator state a second time.
        """
        focus = focus_hint or "browser"
        return Observation(
            time_ms=self.bus.clock_ms,
            focus=focus,
            summary=self._summary(focus),
            screenshot_ref=None,
            action_menu=self._action_menu(focus),
            pending_events=self._pending_counts(),
        )

    def step_and_observe(self, tool: str, args: Dict[str, Any]) -> Observation:
        """Execute a tool call with deterministic step, then return an observation snapshot."""
        self.call_and_step(tool, args)
        focus = self._focus_for_tool(tool)
        return self.snapshot_observation(focus)

    def act_and_observe(self, tool: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a tool call, advance deterministic time, and return both result and observation.

        This is a convenience for clients that want a single call semantics.
        """
        result = self.call_and_step(tool, args)
        focus = self._focus_for_tool(tool)
        obs = self.snapshot_observation(focus)
        return {"result": result, "observation": obs.model_dump()}

    def _focus_for_tool(self, tool: str) -> str:
        return resolve_focus_for_tool(self, tool)

    def inject(
        self, target: str, payload: Dict[str, Any], dt_ms: int = 0
    ) -> Dict[str, Any]:
        """Inject an external event into the bus."""
        event_id = self.bus.schedule(
            dt_ms=dt_ms,
            target=target,
            payload=payload,
            source="legacy_inject",
            kind="injected",
        )
        return {"ok": True, "event_id": event_id}

    def pending(self) -> Dict[str, int]:
        """Return pending event counts per target without advancing time."""
        return self._pending_counts()

    def tick(self, dt_ms: int = 1000) -> Dict[str, Any]:
        """Advance logical time by dt_ms and deliver all due events deterministically.

        Returns the number of delivered events per target and the new time.
        """
        delivered = {target: 0 for target in self._event_targets()}
        target_time = self.bus.clock_ms + max(0, int(dt_ms))
        # Deliver in order at due timestamps
        while (self.bus.peek_due_time() is not None) and (
            self.bus.peek_due_time() <= target_time
        ):
            next_due = int(self.bus.peek_due_time() or self.bus.clock_ms)
            # advance clock to the event due time
            self.bus.clock_ms = next_due
            evt = self.bus.next_if_due()
            if evt:
                delivered[evt.target] = delivered.get(evt.target, 0) + 1
                self._deliver_due_event(evt)
        # Advance remaining time to target_time
        self.bus.clock_ms = target_time
        self.trace.flush()
        return {
            "delivered": delivered,
            "time_ms": self.bus.clock_ms,
            "pending": self.pending(),
        }

    def observe(self, focus_hint: Optional[str] = None) -> Observation:
        """Produce an observation and drain time/event queue incrementally.

        Unlike a pure read, observation advances logical time and delivers at
        most one due event to allow tests to "tick" the simulation forward
        without invoking a side-effecting tool.
        """
        # Deliver one due event if any
        evt = self.bus.next_if_due()
        if evt:
            self._deliver_due_event(evt)
        # Advance time per observation to make future events become due
        self.bus.advance(1000)
        focus = focus_hint or "browser"
        obs = Observation(
            time_ms=self.bus.clock_ms,
            focus=focus,
            summary=self._summary(focus),
            screenshot_ref=None,
            action_menu=self._action_menu(focus),
            pending_events=self._pending_counts(),
        )
        # Persist observations/events so trace is available while running
        self.trace.flush()
        return obs

    def _summary(self, focus: str) -> str:
        return build_focus_summary(self, focus)

    def _pending_counts(self) -> Dict[str, int]:
        counts = {target: 0 for target in self._event_targets()}
        for _, _, event in self.bus._heap:
            counts[event.target] = counts.get(event.target, 0) + 1
        counts["total"] = self.bus.pending_count()
        return counts

    def _maybe_fidelity_intercept(
        self, tool: str, args: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Return an intercepted response for L1/L2 surfaces, or None for L3."""
        if not self._surface_fidelity or tool.startswith("vei."):
            return None
        from vei.blueprint import resolve_surface

        surface = resolve_surface(tool)
        spec = self._surface_fidelity.get(surface)
        if spec is None:
            return None
        level = spec.level if hasattr(spec, "level") else spec.get("level", "L3")
        if level == "L3":
            return None
        if level == "L1":
            from vei.blueprint import l1_response

            return l1_response(spec, tool)
        if level == "L2" and self._l2_store is not None:
            return self._l2_store.handle(surface, tool, args)
        return None

    def _maybe_fault(self, tool: str) -> None:
        prob = self._fault_overrides.get(tool)
        if prob is None:
            spec = self.registry.get(tool)
            use_spec_faults = os.environ.get(
                "VEI_USE_SPEC_FAULTS", ""
            ).strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
            if self.fault_profile != "off" or use_spec_faults:
                prob = spec.fault_probability if spec else 0.0
            else:
                prob = 0.0
        if prob and prob > 0:
            if self.bus.rng.next_float() < prob:
                raise MCPError("fault.injected", f"Injected fault for {tool}")

    def _tool_latency_ms(self, tool: str) -> int:
        if tool.startswith("vei."):
            return 0
        base = 1000
        spec = self.registry.get(tool)
        if spec:
            base = max(base, spec.default_latency_ms or base)
            if spec.latency_jitter_ms > 0:
                base += self.bus.rng.randint(0, spec.latency_jitter_ms)
        return base

    def _action_menu(self, focus: str) -> List[Dict[str, Any]]:
        return build_action_menu(self, focus)
