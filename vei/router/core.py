from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
import heapq
import threading
import queue
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from pydantic import BaseModel
from vei.blueprint.plugins import FacadePlugin, list_runtime_facade_plugins
from vei.connectors import (
    ConnectorInvocationError,
    create_default_runtime,
    parse_adapter_mode,
)
from vei.world.scenario import Scenario
from vei.world.scenarios import load_from_env
from vei.monitors.manager import MonitorManager
from vei.policy import DEFAULT_RULES, PolicyEngine, PromoteMonitorRule
from vei.world.drift import DriftEngine
from vei.world.state import Event as StateEvent, StateStore
from .alias_packs import CRM_ALIAS_PACKS, ERP_ALIAS_PACKS
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


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


class Observation(BaseModel):
    time_ms: int
    focus: str
    summary: str
    screenshot_ref: Optional[str] = None
    action_menu: List[Dict[str, Any]]
    pending_events: Dict[str, int]


@dataclass
class Event:
    t_due_ms: int
    target: str
    payload: Dict[str, Any]
    event_id: str
    source: str = "system"
    actor_id: Optional[str] = None
    kind: str = "scheduled"


class LinearCongruentialGenerator:
    def __init__(self, seed: int):
        self.state = seed & 0xFFFFFFFF

    def next_u32(self) -> int:
        self.state = (1664525 * self.state + 1013904223) & 0xFFFFFFFF
        return self.state

    def next_float(self) -> float:
        return self.next_u32() / 0x100000000

    def randint(self, a: int, b: int) -> int:
        return a + int(self.next_float() * (b - a + 1))


class EventBus:
    def __init__(self, seed: int):
        self.rng = LinearCongruentialGenerator(seed)
        self.clock_ms = 0
        self._heap: list[tuple[int, int, Event]] = []
        self._seq = 0

    def schedule(
        self,
        dt_ms: int,
        target: str,
        payload: Dict[str, Any],
        *,
        event_id: Optional[str] = None,
        source: str = "system",
        actor_id: Optional[str] = None,
        kind: str = "scheduled",
    ) -> str:
        self._seq += 1
        evt = Event(
            self.clock_ms + dt_ms,
            target,
            payload,
            event_id=event_id or f"evt-{self._seq:08d}",
            source=source,
            actor_id=actor_id,
            kind=kind,
        )
        heapq.heappush(self._heap, (evt.t_due_ms, self._seq, evt))
        return evt.event_id

    def next_if_due(self) -> Optional[Event]:
        if self._heap and self._heap[0][0] <= self.clock_ms:
            _, _, evt = heapq.heappop(self._heap)
            return evt
        return None

    def advance(self, dt_ms: int) -> None:
        self.clock_ms += dt_ms

    def peek_due_time(self) -> Optional[int]:
        return self._heap[0][0] if self._heap else None

    def pending_count(self, target: Optional[str] = None) -> int:
        if target is None:
            return len(self._heap)
        return sum(1 for _, _, e in self._heap if e.target == target)

    def cancel(self, event_id: str) -> bool:
        remaining = [
            item
            for item in self._heap
            if getattr(item[2], "event_id", None) != event_id
        ]
        if len(remaining) == len(self._heap):
            return False
        self._heap = remaining
        heapq.heapify(self._heap)
        return True

    def clear(self) -> None:
        self._heap = []

    def list_events(self) -> List[Event]:
        return [event for _, _, event in sorted(self._heap)]


class TraceLogger:
    def __init__(self, out_dir: Optional[str]):
        self.out_dir = out_dir
        self.entries: List[Dict[str, Any]] = []
        # Optional: stream each entry to an external collector
        self.post_url: Optional[str] = self._validated_post_url(
            os.environ.get("VEI_TRACE_POST_URL")
        )
        self._flush_idx = 0
        self.append_mode = os.environ.get("VEI_TRACE_APPEND", "1") == "1"
        # Background poster for streaming to avoid inline latency
        self._q: queue.Queue[Dict[str, Any]] | None = None
        self._poster_thread: threading.Thread | None = None
        if self.post_url:
            self._q = queue.Queue(maxsize=256)
            self._poster_thread = threading.Thread(
                target=self._poster_loop, name="vei-trace-poster", daemon=True
            )
            self._poster_thread.start()

    @staticmethod
    def _validated_post_url(raw: Optional[str]) -> Optional[str]:
        """Allow https endpoints by default and localhost http for dev."""
        if not raw:
            return None
        try:
            parsed = urlparse(raw)
        except Exception:
            return None
        host = (parsed.hostname or "").lower()
        if parsed.scheme == "https":
            return raw
        if parsed.scheme == "http" and host in {"127.0.0.1", "localhost", "::1"}:
            return raw
        return None

    def _try_stream(self, entry: Dict[str, Any]) -> None:
        if not self._q:
            return
        try:
            self._q.put_nowait(entry)
        except queue.Full:
            # Drop when saturated to preserve determinism and low latency
            pass

    def _poster_loop(self) -> None:
        # Use stdlib to avoid extra deps
        import urllib.request

        while True:
            try:
                item = self._q.get(timeout=1.0) if self._q else None
            except queue.Empty:
                continue
            if not item:
                continue
            try:
                data = json.dumps(item).encode("utf-8")
                req = urllib.request.Request(
                    self.post_url,
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=1.0) as _:  # nosec B310
                    pass
            except Exception:
                # Best-effort only
                pass

    def record_call(
        self, tool: str, args: Dict[str, Any], response: Any, time_ms: int
    ) -> None:
        entry = {
            "trace_version": 1,
            "type": "call",
            "tool": tool,
            "args": args,
            "response": response,
            "time_ms": time_ms,
        }
        self.entries.append(entry)
        self._try_stream(entry)

    def record_event(
        self, target: str, payload: Dict[str, Any], emitted: Any, time_ms: int
    ) -> None:
        entry = {
            "trace_version": 1,
            "type": "event",
            "target": target,
            "payload": payload,
            "emitted": emitted,
            "time_ms": time_ms,
        }
        self.entries.append(entry)
        self._try_stream(entry)

    def flush(self) -> None:
        # Allow late-binding of artifacts dir via environment during tests
        if not self.out_dir:
            env_dir = os.environ.get("VEI_ARTIFACTS_DIR")
            if env_dir:
                self.out_dir = env_dir
            else:
                return
        os.makedirs(self.out_dir, exist_ok=True)
        path = os.path.join(self.out_dir, "trace.jsonl")
        if self.append_mode and os.path.exists(path):
            mode = "a"
        else:
            mode = "w"
            self._flush_idx = 0
        with open(path, mode, encoding="utf-8") as f:
            for entry in self.entries[self._flush_idx :]:
                f.write(json.dumps(entry, separators=(",", ":")) + "\n")
        self._flush_idx = len(self.entries)


FAULT_PROFILES: Dict[str, Dict[str, float]] = {
    "off": {},
    "light": {
        "mail.compose": 0.05,
        "mail.reply": 0.04,
        "slack.send_message": 0.02,
        "calendar.create_event": 0.03,
        "tickets.create": 0.03,
    },
    "spiky": {
        "mail.compose": 0.12,
        "mail.reply": 0.1,
        "slack.send_message": 0.08,
        "calendar.create_event": 0.1,
        "tickets.create": 0.12,
        "docs.update": 0.05,
    },
}


def _reduce_router_init(state: Dict[str, Any], event: StateEvent) -> None:
    meta = state.setdefault("meta", {})
    meta.update(
        {
            "seed": event.payload.get("seed"),
            "scenario": event.payload.get("scenario"),
            "branch": event.payload.get("branch"),
        }
    )


def _reduce_tool_call(state: Dict[str, Any], event: StateEvent) -> None:
    calls = state.setdefault("tool_calls", [])
    calls.append(
        {
            "index": event.index,
            "tool": event.payload.get("tool"),
            "time_ms": event.payload.get("time_ms"),
        }
    )
    if len(calls) > 200:
        del calls[: len(calls) - 200]


def _reduce_event_delivery(state: Dict[str, Any], event: StateEvent) -> None:
    deliveries = state.setdefault("deliveries", {})
    target = str(event.payload.get("target"))
    deliveries[target] = deliveries.get(target, 0) + 1


def _reduce_drift_schedule(state: Dict[str, Any], event: StateEvent) -> None:
    drift_state = state.setdefault("drift", {})
    scheduled = drift_state.setdefault("scheduled", [])
    scheduled.append(
        {
            "job": event.payload.get("job"),
            "target": event.payload.get("target"),
            "dt_ms": event.payload.get("dt_ms"),
        }
    )
    if len(scheduled) > 100:
        del scheduled[: len(scheduled) - 100]


def _reduce_drift_delivered(state: Dict[str, Any], event: StateEvent) -> None:
    drift_state = state.setdefault("drift", {})
    delivered = drift_state.setdefault("delivered", {})
    job = event.payload.get("job")
    if job is None:
        return
    delivered[job] = delivered.get(job, 0) + 1


def _reduce_monitor_finding(state: Dict[str, Any], event: StateEvent) -> None:
    monitor_state = state.setdefault("monitors", {})
    findings = monitor_state.setdefault("findings", [])
    findings.append(event.payload)
    if len(findings) > 100:
        del findings[: len(findings) - 100]


def _reduce_policy_finding(state: Dict[str, Any], event: StateEvent) -> None:
    policy_state = state.setdefault("policy", {})
    findings = policy_state.setdefault("findings", [])
    findings.append(event.payload)
    if len(findings) > 200:
        del findings[: len(findings) - 200]


class SlackSim:
    def __init__(self, bus: EventBus, scenario: Optional[Scenario] = None):
        self.bus = bus
        # Configurable behavior
        if scenario and scenario.budget_cap_usd is not None:
            self.budget_cap_usd = int(scenario.budget_cap_usd)
        else:
            self.budget_cap_usd = int(os.environ.get("VEI_BUDGET_CAP", "3500"))
        try:
            if scenario and scenario.derail_prob is not None:
                self.derail_prob = float(scenario.derail_prob)
            else:
                self.derail_prob = float(os.environ.get("VEI_SLACK_DERAIL_PCT", "0.1"))
        except ValueError:
            self.derail_prob = 0.1

        initial_text = (
            scenario.slack_initial_message
            if scenario and scenario.slack_initial_message is not None
            else "Reminder: citations required for any request over $2k."
        )
        seeded_channels = (
            dict(scenario.slack_channels)
            if scenario and scenario.slack_channels
            else {}
        )
        self.channels = {}
        if seeded_channels:
            for channel, payload in seeded_channels.items():
                base = dict(payload or {})
                messages = list(base.get("messages", []))
                if not messages:
                    messages = [
                        {
                            "ts": "1",
                            "user": "itops",
                            "text": initial_text,
                            "thread_ts": None,
                        }
                    ]
                self.channels[str(channel)] = {
                    "messages": messages,
                    "unread": int(base.get("unread", 0)),
                }
        else:
            self.channels = {
                "#procurement": {
                    "messages": [
                        {
                            "ts": "1",
                            "user": "itops",
                            "text": initial_text,
                            "thread_ts": None,
                        }
                    ],
                    "unread": 0,
                }
            }

    # MCP tools
    def list_channels(self) -> List[str]:
        return list(self.channels.keys())

    def open_channel(self, channel: str) -> Dict[str, Any]:
        ch = self.channels.get(channel)
        if not ch:
            raise MCPError("unknown_channel", f"Unknown Slack channel: {channel}")
        return {"messages": ch["messages"], "unread_count": ch["unread"]}

    def send_message(
        self, channel: str, text: str, thread_ts: Optional[str] = None
    ) -> Dict[str, Any]:
        ch = self.channels.get(channel)
        if not ch:
            raise MCPError("unknown_channel", f"Unknown Slack channel: {channel}")
        # Monotonic 1-based timestamps to avoid duplicate ids
        ts = str(len(ch["messages"]) + 1)
        msg = {"ts": ts, "user": "agent", "text": text, "thread_ts": thread_ts}
        ch["messages"].append(msg)
        lower = text.lower()
        # Occasionally derail/off-topic before anything else
        if self.bus.rng.next_float() < self.derail_prob:
            self.bus.schedule(
                dt_ms=7000,
                target="slack",
                payload={
                    "channel": channel,
                    "text": "Could someone update the Q3 KPI sheet?",
                    "thread_ts": ts,
                },
            )
        # Approval policy: must include a budget number and be <= budget cap
        if "approve" in lower or "summary" in lower or "budget" in lower:
            m = re.search(r"\$?([0-9]{3,6})", text.replace(",", ""))
            if m:
                amount = int(m.group(1))
                if amount <= self.budget_cap_usd:
                    self.bus.schedule(
                        dt_ms=12000,
                        target="slack",
                        payload={
                            "channel": channel,
                            "text": ":white_check_mark: Approved",
                            "thread_ts": ts,
                        },
                    )
                else:
                    self.bus.schedule(
                        dt_ms=10000,
                        target="slack",
                        payload={
                            "channel": channel,
                            "text": "Need clearer budget justification (over cap).",
                            "thread_ts": ts,
                        },
                    )
            else:
                self.bus.schedule(
                    dt_ms=9000,
                    target="slack",
                    payload={
                        "channel": channel,
                        "text": "What is the budget amount?",
                        "thread_ts": ts,
                    },
                )
        return {"ts": ts}

    def react(self, channel: str, ts: str, emoji: str) -> Dict[str, Any]:
        return {"ok": True}

    def fetch_thread(self, channel: str, thread_ts: str) -> Dict[str, Any]:
        ch = self.channels.get(channel)
        if not ch:
            raise MCPError("unknown_channel", f"Unknown Slack channel: {channel}")
        base = _safe_int(thread_ts, 0)
        msgs = [
            m
            for m in ch["messages"]
            if m.get("thread_ts") in (thread_ts, None)
            and _safe_int(m.get("ts"), 0) >= base
        ]
        return {"messages": msgs}

    # Event delivery
    def deliver(self, event: Dict[str, Any]) -> Dict[str, Any]:
        channel = event["channel"]
        ch = self.channels.get(channel)
        if not ch:
            raise MCPError("unknown_channel")
        ts = str(len(ch["messages"]) + 1)
        ch["messages"].append(
            {
                "ts": ts,
                "user": event.get("user", "cfo"),
                "text": event["text"],
                "thread_ts": event.get("thread_ts"),
            }
        )
        ch["unread"] += 1
        return {"ok": True}


class MailSim:
    def __init__(self, bus: EventBus, scenario: Optional[Scenario] = None):
        self.bus = bus
        self.messages: Dict[str, Dict[str, Any]] = {}
        self.inbox: List[str] = []
        self.counter = 1
        self._variants_override = (
            scenario.vendor_reply_variants
            if scenario and scenario.vendor_reply_variants
            else None
        )

    def list(self, folder: str = "INBOX") -> List[Dict[str, Any]]:
        return [self.messages[mid] for mid in self.inbox]

    def open(self, id: str) -> Dict[str, Any]:
        m = self.messages.get(id)
        if not m:
            raise MCPError("unknown_message", f"Unknown mail id: {id}")
        return {
            "headers": m["headers"],
            "body_text": m["body_text"],
            "parts": m.get("parts", []),
        }

    def compose(
        self,
        to: str,
        subj: str,
        body_text: str,
        attachments: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        mid = f"m{self.counter}"
        self.counter += 1
        self.messages[mid] = {
            "id": mid,
            "from": "me@example",
            "to": to,
            "subj": subj,
            "time": self.bus.clock_ms,
            "unread": False,
            "headers": {"From": "me@example", "To": to, "Subject": subj},
            "body_text": body_text,
        }
        # Schedule vendor reply with varied templates (scenario override if provided)
        variants = self._variants_override or [
            "Thanks — Price: $3199, ETA: 5-7 business days.",
            "> On Mon, we received your request\nPRICE: USD 3,199\nEta: within 5-7 business days\n--\nBest, MacroCompute",
            "quote attached (inline): total: $3,199.00, ETA: 5 business days. Regards, Sales",
            "PRICE - $3199; eta: approx. 1 week\n\n\nJohn Doe\nSales Representative\nMacroCompute",
        ]
        idx = 0 if not variants else self.bus.rng.randint(0, max(0, len(variants) - 1))
        body = variants[idx] if variants else ""

        self.bus.schedule(
            dt_ms=15000,
            target="mail",
            payload={
                "in_reply_to": mid,
                "from": to,
                "subj": f"Re: {subj}",
                "body_text": body,
            },
        )
        return {"id": mid}

    def reply(self, id: str, body_text: str) -> Dict[str, Any]:
        message = self.messages.get(id)
        if not message:
            raise MCPError("unknown_message", f"Unknown mail id: {id}")
        return self.compose(
            to=message["from"], subj=f"Re: {message['subj']}", body_text=body_text
        )

    def deliver(self, event: Dict[str, Any]) -> Dict[str, Any]:
        mid = f"m{self.counter}"
        self.counter += 1
        msg = {
            "id": mid,
            "from": event["from"],
            "to": "me@example",
            "subj": event["subj"],
            "time": self.bus.clock_ms,
            "unread": True,
            "headers": {
                "From": event["from"],
                "To": "me@example",
                "Subject": event["subj"],
            },
            "body_text": event["body_text"],
        }
        self.messages[mid] = msg
        self.inbox.insert(0, mid)
        return {"id": mid}


class BrowserVirtual:
    def __init__(self, bus: EventBus, scenario: Optional[Scenario] = None):
        self.bus = bus
        # Very small virtual site with two pages and one affordance
        default_nodes = {
            "home": {
                "url": "https://vweb.local/home",
                "title": "MacroCompute — Home",
                "excerpt": "Welcome to MacroCompute. Find laptops and specs.",
                "affordances": [
                    {
                        "tool": "browser.click",
                        "args": {"node_id": "CLICK:open_pdp#0"},
                        "name": "Open product page",
                    },
                ],
                "next": {"CLICK:open_pdp#0": "pdp"},
            },
            "pdp": {
                "url": "https://vweb.local/pdp/macrobook-pro-16",
                "title": "MacroBook Pro 16 — Product",
                "excerpt": "Powerful 16-inch laptop. Price $3199. See specifications.",
                "affordances": [
                    {
                        "tool": "browser.click",
                        "args": {"node_id": "CLICK:open_specs#0"},
                        "name": "See specifications",
                    },
                    {"tool": "browser.back", "args": {}, "name": "Back to home"},
                ],
                "next": {"CLICK:open_specs#0": "specs", "BACK": "home"},
            },
            "specs": {
                "url": "https://vweb.local/pdp/macrobook-pro-16/specs",
                "title": "MacroBook Pro 16 — Specifications",
                "excerpt": "16-core CPU, 32GB RAM, 1TB SSD",
                "affordances": [
                    {"tool": "browser.back", "args": {}, "name": "Back to product"},
                ],
                "next": {"BACK": "pdp"},
            },
        }
        self.nodes = (
            scenario.browser_nodes
            if scenario and scenario.browser_nodes
            else default_nodes
        )
        self.state = "home"

    def open(self, url: str) -> Dict[str, Any]:
        if "pdp" in url:
            self.state = "pdp"
        else:
            self.state = "home"
        return {
            "url": self.nodes[self.state]["url"],
            "title": self.nodes[self.state]["title"],
        }

    def find(self, query: str, top_k: int = 10) -> Dict[str, Any]:
        node = self.nodes[self.state]
        hits = []
        for a in node["affordances"]:
            name = a.get("name") or a["args"].get("node_id", "")
            # Only include affordances that have a concrete click target
            args = a.get("args", {})
            node_id = args.get("node_id")
            if node_id is None:
                # Skip generic affordances like browser.back that lack node_id
                continue
            hits.append(
                {
                    "node_id": node_id,
                    "role": a.get("role", "button"),
                    "name": name,
                }
            )
        return {"hits": hits[:top_k]}

    def click(self, node_id: str) -> Dict[str, Any]:
        node = self.nodes[self.state]
        nxt = node["next"].get(node_id)
        if not nxt:
            raise MCPError("invalid_action", f"Invalid click target: {node_id}")
        self.state = nxt
        return {"url": self.nodes[self.state]["url"]}

    def type(self, node_id: str, text: str) -> Dict[str, Any]:
        return {"ok": True}

    def submit(self, form_id: str) -> Dict[str, Any]:
        return {"url": self.nodes[self.state]["url"]}

    def read(self) -> Dict[str, Any]:
        node = self.nodes[self.state]
        return {
            "url": node["url"],
            "title": node["title"],
            "excerpt": node["excerpt"],
        }

    def back(self) -> Dict[str, Any]:
        node = self.nodes[self.state]
        nxt = node["next"].get("BACK")
        if not nxt:
            return {"url": node["url"]}
        self.state = nxt
        return {"url": self.nodes[self.state]["url"]}


class Router:
    def __init__(
        self,
        seed: int,
        artifacts_dir: Optional[str] = None,
        scenario: Optional[Scenario] = None,
        connector_mode: Optional[str] = None,
        branch: str = "main",
    ):
        self.seed = int(seed)
        self.world_session = None
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

        self.trace = TraceLogger(artifacts_dir)
        self.scenario = scenario or load_from_env(seed)
        self.slack = SlackSim(self.bus, self.scenario)
        self.mail = MailSim(self.bus, self.scenario)
        self.browser = BrowserVirtual(self.bus, self.scenario)
        dataset_path = os.environ.get("VEI_DATASET")
        if dataset_path:
            try:
                from vei.data.models import VEIDataset
                from vei.world.replay import ReplayAdapter
                import json

                data = json.loads(Path(dataset_path).read_text(encoding="utf-8"))
                dataset = VEIDataset.model_validate(data)
                adapter = ReplayAdapter(self.bus, dataset.events)
                adapter.prime()
                self.replay_adapter = adapter
            except Exception:
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
            self.erp = None  # type: ignore[attr-defined]
        # Optional CRM twin
        try:
            from .crm import CrmSim

            self.crm = CrmSim(self.bus, self.scenario)
        except Exception:
            self.crm = None  # type: ignore[attr-defined]

        # Optional identity twin
        self.okta = None  # type: ignore[attr-defined]
        try:
            from .identity import OktaSim, OktaToolProvider

            self.okta = OktaSim(self.scenario)
            self.register_tool_provider(OktaToolProvider(self.okta))
        except Exception:
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
            from vei.world.session import WorldSession

            if self.world_session is None:
                self.world_session = WorldSession.attach_router(self)
            self.world_session.snapshot(label=label)
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
            self._receipts = []

    def _write_receipt(self, receipt: Dict[str, Any]) -> None:
        if not self._receipts_path:
            return
        try:
            with self._receipts_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(receipt, sort_keys=True) + "\n")
        except Exception:
            pass

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
        alias_map: Dict[str, str] = {}
        erp_packs_env = os.environ.get("VEI_ALIAS_PACKS", "xero").strip()
        erp_packs = [pack.strip() for pack in erp_packs_env.split(",") if pack.strip()]
        for pack in erp_packs:
            for alias, base in ERP_ALIAS_PACKS.get(pack, []):
                alias_map[alias] = base

        crm_packs_env = os.environ.get(
            "VEI_CRM_ALIAS_PACKS", "hubspot,salesforce"
        ).strip()
        crm_packs = [pack.strip() for pack in crm_packs_env.split(",") if pack.strip()]
        for pack in crm_packs:
            for alias, base in CRM_ALIAS_PACKS.get(pack, []):
                alias_map[alias] = base
        return alias_map

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
        specs = [
            ToolSpec(
                name="vei.observe",
                description="Obtain the current observation (advances time).",
                side_effects=("time_advance",),
                default_latency_ms=1000,
            ),
            ToolSpec(
                name="vei.tick",
                description="Advance logical time and deliver due events.",
                side_effects=("time_advance", "event_delivery"),
            ),
            ToolSpec(
                name="vei.act_and_observe",
                description="Execute a tool then fetch the next observation.",
                side_effects=("time_advance",),
            ),
            ToolSpec(
                name="vei.tools.search",
                description="Search the MCP tool catalog for relevant entries.",
            ),
            ToolSpec(
                name="vei.state",
                description="Inspect state head, receipts, and recent tool calls.",
                side_effects=(),
            ),
            ToolSpec(
                name="vei.inject",
                description="Inject an external event (e.g. human message) into the simulation.",
                side_effects=("event_schedule",),
            ),
            ToolSpec(
                name="slack.send_message",
                description="Post a message into a Slack channel thread.",
                side_effects=("slack_outbound",),
                permissions=("slack:write",),
                default_latency_ms=500,
                latency_jitter_ms=200,
                fault_probability=0.01,
            ),
            ToolSpec(
                name="slack.open_channel",
                description="Open a Slack channel view.",
                side_effects=(),
                permissions=("slack:read",),
            ),
            ToolSpec(
                name="slack.fetch_thread",
                description="Fetch a Slack thread for review.",
                side_effects=(),
                permissions=("slack:read",),
            ),
            ToolSpec(
                name="slack.list_channels",
                description="List available Slack channels.",
                permissions=("slack:read",),
            ),
            ToolSpec(
                name="slack.react",
                description="Add a reaction to a Slack message.",
                side_effects=("slack_outbound",),
                permissions=("slack:write",),
            ),
            ToolSpec(
                name="mail.compose",
                description="Send an email to a recipient.",
                side_effects=("mail_outbound", "event_schedule"),
                permissions=("mail:write",),
                default_latency_ms=800,
                latency_jitter_ms=300,
                fault_probability=0.02,
            ),
            ToolSpec(
                name="mail.list",
                description="List newest messages in the inbox.",
                permissions=("mail:read",),
            ),
            ToolSpec(
                name="mail.open",
                description="Open a specific email body.",
                permissions=("mail:read",),
            ),
            ToolSpec(
                name="mail.reply",
                description="Reply to an existing email thread.",
                side_effects=("mail_outbound", "event_schedule"),
                permissions=("mail:write",),
                default_latency_ms=800,
                latency_jitter_ms=300,
                fault_probability=0.02,
            ),
            ToolSpec(
                name="browser.read",
                description="Read current browser node.",
                permissions=("browser:read",),
            ),
            ToolSpec(
                name="browser.click",
                description="Click a UI element and navigate.",
                side_effects=("browser_navigation",),
                permissions=("browser:write",),
            ),
            ToolSpec(
                name="browser.find",
                description="Search current document for affordances.",
                permissions=("browser:read",),
            ),
            ToolSpec(
                name="browser.open",
                description="Open a URL inside the virtual browser.",
                side_effects=("browser_navigation",),
                permissions=("browser:write",),
            ),
            ToolSpec(
                name="browser.back",
                description="Navigate back to the previous page.",
                side_effects=("browser_navigation",),
                permissions=("browser:write",),
            ),
            ToolSpec(
                name="browser.type",
                description="Type text into a field.",
                side_effects=("browser_input",),
                permissions=("browser:write",),
            ),
            ToolSpec(
                name="browser.submit",
                description="Submit a form.",
                side_effects=("browser_navigation",),
                permissions=("browser:write",),
            ),
        ]
        docs_specs = [
            ToolSpec(
                name="docs.list",
                description="List documents in the knowledge base with optional filtering/pagination.",
                permissions=("docs:read",),
            ),
            ToolSpec(
                name="docs.read",
                description="Read a document by id.",
                permissions=("docs:read",),
            ),
            ToolSpec(
                name="docs.search",
                description="Search documents for a query.",
                permissions=("docs:read",),
            ),
            ToolSpec(
                name="docs.create",
                description="Create a new document entry.",
                permissions=("docs:write",),
                side_effects=("docs_mutation",),
                default_latency_ms=400,
                latency_jitter_ms=150,
            ),
            ToolSpec(
                name="docs.update",
                description="Update an existing document.",
                permissions=("docs:write",),
                side_effects=("docs_mutation",),
                default_latency_ms=350,
                latency_jitter_ms=120,
            ),
        ]
        calendar_specs = [
            ToolSpec(
                name="calendar.list_events",
                description="List calendar events with optional filtering/pagination.",
                permissions=("calendar:read",),
            ),
            ToolSpec(
                name="calendar.create_event",
                description="Create a new calendar event.",
                permissions=("calendar:write",),
                side_effects=("calendar_mutation",),
                default_latency_ms=600,
                latency_jitter_ms=200,
            ),
            ToolSpec(
                name="calendar.accept",
                description="Accept a calendar invite.",
                permissions=("calendar:write",),
                side_effects=("calendar_response",),
                default_latency_ms=300,
                latency_jitter_ms=150,
            ),
            ToolSpec(
                name="calendar.decline",
                description="Decline a calendar invite.",
                permissions=("calendar:write",),
                side_effects=("calendar_response",),
                default_latency_ms=300,
                latency_jitter_ms=150,
            ),
            ToolSpec(
                name="calendar.update_event",
                description="Update event fields (time, attendees, description, status).",
                permissions=("calendar:write",),
                side_effects=("calendar_mutation",),
                default_latency_ms=500,
                latency_jitter_ms=180,
            ),
            ToolSpec(
                name="calendar.cancel_event",
                description="Cancel a calendar event with optional reason.",
                permissions=("calendar:write",),
                side_effects=("calendar_mutation",),
                default_latency_ms=420,
                latency_jitter_ms=140,
            ),
        ]
        ticket_specs = [
            ToolSpec(
                name="tickets.list",
                description="List tickets in the queue with optional filtering/pagination.",
                permissions=("tickets:read",),
            ),
            ToolSpec(
                name="tickets.get",
                description="Fetch ticket details.",
                permissions=("tickets:read",),
            ),
            ToolSpec(
                name="tickets.create",
                description="Create a new ticket.",
                permissions=("tickets:write",),
                side_effects=("tickets_mutation",),
                default_latency_ms=500,
                latency_jitter_ms=200,
                fault_probability=0.03,
            ),
            ToolSpec(
                name="tickets.update",
                description="Update ticket fields.",
                permissions=("tickets:write",),
                side_effects=("tickets_mutation",),
                default_latency_ms=400,
                latency_jitter_ms=150,
            ),
            ToolSpec(
                name="tickets.transition",
                description="Transition a ticket to a new status.",
                permissions=("tickets:write",),
                side_effects=("tickets_mutation",),
                default_latency_ms=420,
                latency_jitter_ms=160,
                fault_probability=0.02,
            ),
            ToolSpec(
                name="tickets.add_comment",
                description="Add a comment to a ticket.",
                permissions=("tickets:write",),
                side_effects=("tickets_mutation",),
                default_latency_ms=350,
                latency_jitter_ms=130,
            ),
        ]
        db_specs = [
            ToolSpec(
                name="db.list_tables",
                description="List available enterprise database tables.",
                permissions=("db:read",),
            ),
            ToolSpec(
                name="db.describe_table",
                description="Describe columns and row counts for a database table.",
                permissions=("db:read",),
            ),
            ToolSpec(
                name="db.query",
                description="Run a structured query over a database table.",
                permissions=("db:read",),
            ),
            ToolSpec(
                name="db.upsert",
                description="Insert or update a row in a database table.",
                permissions=("db:write",),
                side_effects=("db_mutation",),
                default_latency_ms=450,
                latency_jitter_ms=150,
                fault_probability=0.01,
            ),
        ]
        # ERP and CRM specs are registered lazily to avoid importing optional twins here.
        erp_specs = [
            ToolSpec(
                name="erp.create_po",
                description="Create a purchase order.",
                permissions=("erp:write",),
            ),
            ToolSpec(
                name="erp.get_po",
                description="Retrieve a purchase order.",
                permissions=("erp:read",),
            ),
            ToolSpec(
                name="erp.list_pos",
                description="List purchase orders.",
                permissions=("erp:read",),
            ),
            ToolSpec(
                name="erp.receive_goods",
                description="Record goods receipt.",
                permissions=("erp:write",),
            ),
            ToolSpec(
                name="erp.submit_invoice",
                description="Submit a vendor invoice.",
                permissions=("erp:write",),
            ),
            ToolSpec(
                name="erp.get_invoice",
                description="Retrieve invoice detail.",
                permissions=("erp:read",),
            ),
            ToolSpec(
                name="erp.list_invoices",
                description="List invoices.",
                permissions=("erp:read",),
            ),
            ToolSpec(
                name="erp.match_three_way",
                description="Run three-way match.",
                permissions=("erp:write",),
            ),
            ToolSpec(
                name="erp.post_payment",
                description="Post a payment.",
                permissions=("erp:write",),
            ),
        ]
        crm_specs = [
            ToolSpec(
                name="crm.create_contact",
                description="Create a CRM contact.",
                permissions=("crm:write",),
            ),
            ToolSpec(
                name="crm.get_contact",
                description="Fetch CRM contact details.",
                permissions=("crm:read",),
            ),
            ToolSpec(
                name="crm.list_contacts",
                description="List contacts.",
                permissions=("crm:read",),
            ),
            ToolSpec(
                name="crm.create_company",
                description="Create a company record.",
                permissions=("crm:write",),
            ),
            ToolSpec(
                name="crm.get_company",
                description="Fetch company details.",
                permissions=("crm:read",),
            ),
            ToolSpec(
                name="crm.list_companies",
                description="List company records.",
                permissions=("crm:read",),
            ),
            ToolSpec(
                name="crm.associate_contact_company",
                description="Link contact to company.",
                permissions=("crm:write",),
            ),
            ToolSpec(
                name="crm.create_deal",
                description="Create a deal/opportunity.",
                permissions=("crm:write",),
            ),
            ToolSpec(
                name="crm.get_deal",
                description="Fetch deal details.",
                permissions=("crm:read",),
            ),
            ToolSpec(
                name="crm.list_deals",
                description="List deals.",
                permissions=("crm:read",),
            ),
            ToolSpec(
                name="crm.update_deal_stage",
                description="Update deal stage.",
                permissions=("crm:write",),
            ),
            ToolSpec(
                name="crm.reassign_deal_owner",
                description="Transfer deal ownership.",
                permissions=("crm:write",),
            ),
            ToolSpec(
                name="crm.log_activity",
                description="Log an activity.",
                permissions=("crm:write",),
            ),
        ]
        specs.extend(docs_specs)
        specs.extend(calendar_specs)
        specs.extend(ticket_specs)
        specs.extend(db_specs)
        specs.extend(erp_specs)
        specs.extend(crm_specs)
        self._register_tool_specs(specs)

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
        focuses = [
            "browser",
            "slack",
            "mail",
            "docs",
            "calendar",
            "tickets",
            "db",
            "erp",
            "crm",
            "okta",
            "servicedesk",
        ]
        for entry in self.facade_plugins.values():
            plugin: FacadePlugin = entry["plugin"]
            for focus in plugin.focuses:
                if focus not in focuses:
                    focuses.append(focus)
        focus_menus: Dict[str, List[Dict[str, Any]]] = {}
        for focus in focuses:
            menu = self._action_menu(focus)
            if menu:
                focus_menus[focus] = menu
        tools = [
            {
                "tool": spec.name,
                "description": spec.description,
                "permissions": list(spec.permissions),
                "side_effects": list(spec.side_effects),
            }
            for spec in sorted(self.registry.list(), key=lambda item: item.name)
        ]
        software = [
            "slack",
            "mail",
            "browser",
            "docs",
            "calendar",
            "tickets",
            "db",
            "erp",
            "crm",
            "okta",
            "servicedesk",
        ]
        for entry in self.facade_plugins.values():
            plugin: FacadePlugin = entry["plugin"]
            if plugin.manifest.name not in software:
                software.append(plugin.manifest.name)
        return {
            "instructions": (
                "Use MCP tools against the virtual enterprise. "
                "Typical loop: observe -> call one tool -> observe again."
            ),
            "software": software,
            "tools": tools,
            "focus_action_menus": focus_menus,
            "examples": [
                {"tool": "vei.observe", "args": {"focus": "browser"}},
                {
                    "tool": "mail.compose",
                    "args": {
                        "to": "sales@macrocompute.example",
                        "subj": "Quote request",
                        "body_text": "Please send quote and ETA.",
                    },
                },
                {
                    "tool": "slack.send_message",
                    "args": {
                        "channel": "#procurement",
                        "text": "Approval request with budget and evidence.",
                    },
                },
                {"tool": "db.query", "args": {"table": "approval_audit", "limit": 5}},
                {
                    "tool": "servicedesk.list_requests",
                    "args": {"status": "PENDING_APPROVAL", "limit": 5},
                },
                {"tool": "okta.list_users", "args": {"status": "ACTIVE", "limit": 5}},
            ],
        }

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
        if tool == "slack.list_channels":
            return self.slack.list_channels()
        if tool == "slack.open_channel":
            return self.slack.open_channel(**args)
        if tool == "slack.send_message":
            return self.slack.send_message(**args)
        if tool == "slack.react":
            return self.slack.react(**args)
        if tool == "slack.fetch_thread":
            return self.slack.fetch_thread(**args)

        if tool == "mail.list":
            return self.mail.list(**args)
        if tool == "mail.open":
            return self.mail.open(**args)
        if tool == "mail.compose":
            return self.mail.compose(**args)
        if tool == "mail.reply":
            return self.mail.reply(**args)

        if tool == "browser.open":
            return self.browser.open(**args)
        if tool == "browser.find":
            return self.browser.find(**args)
        if tool == "browser.click":
            return self.browser.click(**args)
        if tool == "browser.type":
            return self.browser.type(**args)
        if tool == "browser.submit":
            return self.browser.submit(**args)
        if tool == "browser.read":
            return self.browser.read()
        if tool == "browser.back":
            return self.browser.back()

        if tool.startswith("docs."):
            if tool == "docs.list":
                return self.docs.list(**args)
            if tool == "docs.read":
                return self.docs.read(**args)
            if tool == "docs.search":
                return self.docs.search(**args)
            if tool == "docs.create":
                return self.docs.create(**args)
            if tool == "docs.update":
                return self.docs.update(**args)
            raise MCPError("unknown_tool", f"No such tool: {tool}")

        if tool.startswith("calendar."):
            if tool == "calendar.list_events":
                return self.calendar.list_events(**args)
            if tool == "calendar.create_event":
                return self.calendar.create_event(**args)
            if tool == "calendar.accept":
                return self.calendar.accept(**args)
            if tool == "calendar.decline":
                return self.calendar.decline(**args)
            if tool == "calendar.update_event":
                return self.calendar.update_event(**args)
            if tool == "calendar.cancel_event":
                return self.calendar.cancel_event(**args)
            raise MCPError("unknown_tool", f"No such tool: {tool}")

        if tool.startswith("tickets."):
            if tool == "tickets.list":
                return self.tickets.list(**args)
            if tool == "tickets.get":
                return self.tickets.get(**args)
            if tool == "tickets.create":
                return self.tickets.create(**args)
            if tool == "tickets.update":
                return self.tickets.update(**args)
            if tool == "tickets.transition":
                return self.tickets.transition(**args)
            if tool == "tickets.add_comment":
                return self.tickets.add_comment(**args)
            raise MCPError("unknown_tool", f"No such tool: {tool}")

        # ERP tools
        if tool.startswith("erp."):
            if not getattr(self, "erp", None):
                raise MCPError("unsupported_tool", "ERP twin not available")
            erp = getattr(self, "erp")
            if tool == "erp.create_po":
                return erp.create_po(**args)
            if tool == "erp.get_po":
                return erp.get_po(**args)
            if tool == "erp.list_pos":
                return erp.list_pos(**args)
            if tool == "erp.receive_goods":
                return erp.receive_goods(**args)
            if tool == "erp.submit_invoice":
                return erp.submit_invoice(**args)
            if tool == "erp.get_invoice":
                return erp.get_invoice(**args)
            if tool == "erp.list_invoices":
                return erp.list_invoices(**args)
            if tool == "erp.match_three_way":
                return erp.match_three_way(**args)
            if tool == "erp.post_payment":
                return erp.post_payment(**args)
            raise MCPError("unknown_tool", f"No such tool: {tool}")

        # CRM tools
        if tool.startswith("crm."):
            if not getattr(self, "crm", None):
                raise MCPError("unsupported_tool", "CRM twin not available")
            crm = getattr(self, "crm")
            if tool == "crm.create_contact":
                return crm.create_contact(**args)
            if tool == "crm.get_contact":
                return crm.get_contact(**args)
            if tool == "crm.list_contacts":
                return crm.list_contacts(**args)
            if tool == "crm.create_company":
                return crm.create_company(**args)
            if tool == "crm.get_company":
                return crm.get_company(**args)
            if tool == "crm.list_companies":
                return crm.list_companies(**args)
            if tool == "crm.associate_contact_company":
                return crm.associate_contact_company(**args)
            if tool == "crm.create_deal":
                return crm.create_deal(**args)
            if tool == "crm.get_deal":
                return crm.get_deal(**args)
            if tool == "crm.list_deals":
                return crm.list_deals(**args)
            if tool == "crm.update_deal_stage":
                return crm.update_deal_stage(**args)
            if tool == "crm.reassign_deal_owner":
                return crm.reassign_deal_owner(**args)
            if tool == "crm.log_activity":
                return crm.log_activity(**args)
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
        resolved = self.alias_map.get(tool, tool)
        for prefix in (
            "slack",
            "mail",
            "docs",
            "calendar",
            "tickets",
            "erp",
            "crm",
            "db",
            "browser",
            "okta",
            "servicedesk",
            "google_admin",
            "siem",
            "datadog",
            "pagerduty",
            "feature_flags",
            "hris",
            "jira",
        ):
            if resolved.startswith(f"{prefix}."):
                return prefix
        if tool.startswith("salesforce.") or tool.startswith("hubspot."):
            return "crm"
        plugin_focus = self._plugin_focus_for_tool(resolved)
        if plugin_focus:
            return plugin_focus
        return "browser"

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
        if focus == "browser":
            r = self.browser.read()
            return f"Browser: {r['title']} — {r['excerpt']}"
        if focus == "slack":
            channels = self.slack.list_channels()
            if not channels:
                return "Slack: no channels"
            channel = channels[0]
            ch = self.slack.open_channel(channel)
            latest = ch["messages"][-1]["text"] if ch["messages"] else ""
            return f"Slack {channel} latest: {latest}"
        if focus == "mail":
            lst = self.mail.list()
            if lst:
                return f"Mail: {lst[0]['subj']} from {lst[0]['from']}"
            return "Mail: INBOX empty"
        if focus == "docs":
            docs = self.docs.list()
            if not docs:
                return "Docs: empty library"
            return f"Docs: {len(docs)} available (latest: {docs[-1]['title']})"
        if focus == "calendar":
            events = self.calendar.list_events()
            if not events:
                return "Calendar: no scheduled events"
            soon = events[0]
            return f"Calendar: next {soon['title']} at {soon['start_ms']}"
        if focus == "tickets":
            tickets = self.tickets.list()
            if not tickets:
                return "Tickets: queue empty"
            open_count = sum(1 for t in tickets if t["status"].lower() != "closed")
            return f"Tickets: {open_count} open of {len(tickets)}"
        if focus == "erp":
            # Surface a short state summary for agents
            pos = len(getattr(self, "erp").pos) if getattr(self, "erp", None) else 0
            invs = (
                len(getattr(self, "erp").invoices) if getattr(self, "erp", None) else 0
            )
            return f"ERP: {pos} POs, {invs} invoices"
        if focus == "crm":
            cs = len(getattr(self, "crm").contacts) if getattr(self, "crm", None) else 0
            ds = len(getattr(self, "crm").deals) if getattr(self, "crm", None) else 0
            return f"CRM: {cs} contacts, {ds} deals"
        if focus == "db":
            tables = self.database.list_tables()
            if not tables:
                return "DB: no tables"
            largest = max(tables, key=lambda item: int(item.get("row_count", 0)))
            return f"DB: {len(tables)} tables (largest: {largest['table']})"
        if focus == "okta":
            if not getattr(self, "okta", None):
                return "Okta: unavailable"
            users = self.okta.list_users(limit=1)
            total = int(users.get("total", users.get("count", 0)))
            suspended = self.okta.list_users(status="SUSPENDED", limit=1)
            suspended_total = int(suspended.get("total", suspended.get("count", 0)))
            return f"Okta: {total} users ({suspended_total} suspended)"
        if focus == "servicedesk":
            incidents = self.servicedesk.list_incidents(limit=1)
            request_rows = self.servicedesk.list_requests(limit=1)
            return (
                "ServiceDesk: "
                f"{incidents.get('total', incidents.get('count', 0))} incidents, "
                f"{request_rows.get('total', request_rows.get('count', 0))} requests"
            )
        if focus == "google_admin":
            apps = self.google_admin.list_oauth_apps(limit=1)
            shares = self.google_admin.list_drive_shares(limit=1)
            return (
                "Google Admin: "
                f"{apps.get('total', apps.get('count', 0))} OAuth apps, "
                f"{shares.get('total', shares.get('count', 0))} drive shares"
            )
        if focus == "siem":
            alerts = self.siem.list_alerts(limit=1)
            cases = self.siem.list_cases(limit=1)
            return (
                "SIEM: "
                f"{alerts.get('total', alerts.get('count', 0))} alerts, "
                f"{cases.get('total', cases.get('count', 0))} cases"
            )
        if focus == "datadog":
            services = self.datadog.list_services(limit=1)
            monitors = self.datadog.list_monitors(limit=1)
            return (
                "Datadog: "
                f"{services.get('total', services.get('count', 0))} services, "
                f"{monitors.get('total', monitors.get('count', 0))} monitors"
            )
        if focus == "pagerduty":
            incidents = self.pagerduty.list_incidents(limit=1)
            return (
                "PagerDuty: "
                f"{incidents.get('total', incidents.get('count', 0))} incidents"
            )
        if focus == "feature_flags":
            flags = self.feature_flags.list_flags(limit=1)
            return (
                "Feature Flags: " f"{flags.get('total', flags.get('count', 0))} flags"
            )
        if focus == "hris":
            employees = self.hris.list_employees(limit=1)
            return (
                "HRIS: "
                f"{employees.get('total', employees.get('count', 0))} employees"
            )
        if focus == "jira":
            issues = self.tickets.list(limit=1)
            total = (
                issues.get("total", issues.get("count", 0))
                if isinstance(issues, dict)
                else len(issues)
            )
            return f"Jira: {total} issues"
        plugin_summary = self._plugin_summary(focus)
        if plugin_summary is not None:
            return plugin_summary
        return ""

    def _pending_counts(self) -> Dict[str, int]:
        counts = {target: 0 for target in self._event_targets()}
        for _, _, event in self.bus._heap:
            counts[event.target] = counts.get(event.target, 0) + 1
        counts["total"] = self.bus.pending_count()
        return counts

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
        if focus == "browser":
            node_aff = self.browser.nodes[self.browser.state]["affordances"]
            # Provide both concrete affordances and generic actions with schemas for LLMs
            generic: List[Dict[str, Any]] = [
                {"tool": "browser.read", "args_schema": {}},
                {
                    "tool": "browser.find",
                    "args_schema": {"query": "str", "top_k": "int?"},
                },
                {"tool": "browser.open", "args_schema": {"url": "str"}},
                {"tool": "browser.back", "args_schema": {}},
            ]
            return [*node_aff, *generic]
        if focus == "slack":
            return [
                {
                    "tool": "slack.send_message",
                    "args_schema": {
                        "channel": "str",
                        "text": "str",
                        "thread_ts": "str?",
                    },
                },
            ]
        if focus == "mail":
            return [
                {
                    "tool": "mail.compose",
                    "args_schema": {"to": "str", "subj": "str", "body_text": "str"},
                },
            ]
        if focus == "docs":
            return [
                {
                    "tool": "docs.list",
                    "args_schema": {
                        "query": "str?",
                        "tag": "str?",
                        "status": "str?",
                        "owner": "str?",
                        "limit": "int?",
                        "cursor": "str?",
                        "sort_by": "str?",
                        "sort_dir": "asc|desc?",
                    },
                },
                {
                    "tool": "docs.search",
                    "args_schema": {"query": "str", "limit": "int?", "cursor": "str?"},
                },
                {"tool": "docs.read", "args_schema": {"doc_id": "str"}},
                {
                    "tool": "docs.create",
                    "args_schema": {
                        "title": "str",
                        "body": "str",
                        "tags": "[str]?",
                        "owner": "str?",
                        "status": "str?",
                    },
                },
                {
                    "tool": "docs.update",
                    "args_schema": {
                        "doc_id": "str",
                        "title": "str?",
                        "body": "str?",
                        "tags": "[str]?",
                        "status": "str?",
                    },
                },
            ]
        if focus == "calendar":
            return [
                {
                    "tool": "calendar.list_events",
                    "args_schema": {
                        "attendee": "str?",
                        "status": "str?",
                        "starts_after_ms": "int?",
                        "ends_before_ms": "int?",
                        "limit": "int?",
                        "cursor": "str?",
                        "sort_dir": "asc|desc?",
                    },
                },
                {
                    "tool": "calendar.create_event",
                    "args_schema": {
                        "title": "str",
                        "start_ms": "int",
                        "end_ms": "int",
                        "attendees": "[str]?",
                        "location": "str?",
                        "description": "str?",
                        "organizer": "str?",
                        "status": "str?",
                    },
                },
                {
                    "tool": "calendar.accept",
                    "args_schema": {"event_id": "str", "attendee": "str"},
                },
                {
                    "tool": "calendar.decline",
                    "args_schema": {"event_id": "str", "attendee": "str"},
                },
                {
                    "tool": "calendar.update_event",
                    "args_schema": {
                        "event_id": "str",
                        "title": "str?",
                        "start_ms": "int?",
                        "end_ms": "int?",
                        "attendees": "[str]?",
                        "location": "str?",
                        "description": "str?",
                        "status": "str?",
                    },
                },
                {
                    "tool": "calendar.cancel_event",
                    "args_schema": {"event_id": "str", "reason": "str?"},
                },
            ]
        if focus == "tickets":
            return [
                {
                    "tool": "tickets.list",
                    "args_schema": {
                        "status": "str?",
                        "assignee": "str?",
                        "priority": "str?",
                        "query": "str?",
                        "limit": "int?",
                        "cursor": "str?",
                        "sort_by": "str?",
                        "sort_dir": "asc|desc?",
                    },
                },
                {"tool": "tickets.get", "args_schema": {"ticket_id": "str"}},
                {
                    "tool": "tickets.create",
                    "args_schema": {
                        "title": "str",
                        "description": "str?",
                        "assignee": "str?",
                        "priority": "str?",
                        "severity": "str?",
                        "labels": "[str]?",
                    },
                },
                {
                    "tool": "tickets.update",
                    "args_schema": {
                        "ticket_id": "str",
                        "description": "str?",
                        "assignee": "str?",
                        "priority": "str?",
                        "severity": "str?",
                        "labels": "[str]?",
                    },
                },
                {
                    "tool": "tickets.transition",
                    "args_schema": {"ticket_id": "str", "status": "str"},
                },
                {
                    "tool": "tickets.add_comment",
                    "args_schema": {
                        "ticket_id": "str",
                        "body": "str",
                        "author": "str?",
                    },
                },
            ]
        if focus == "erp" and getattr(self, "erp", None):
            return [
                {
                    "tool": "erp.create_po",
                    "args_schema": {
                        "vendor": "str",
                        "currency": "str",
                        "lines": "[{item_id,desc,qty,unit_price}]",
                    },
                },
                {
                    "tool": "erp.list_pos",
                    "args_schema": {
                        "vendor": "str?",
                        "status": "str?",
                        "currency": "str?",
                        "limit": "int?",
                        "cursor": "str?",
                        "sort_by": "str?",
                        "sort_dir": "asc|desc?",
                    },
                },
                {
                    "tool": "erp.submit_invoice",
                    "args_schema": {
                        "vendor": "str",
                        "po_id": "str",
                        "lines": "[{item_id,qty,unit_price}]",
                    },
                },
                {
                    "tool": "erp.match_three_way",
                    "args_schema": {
                        "po_id": "str",
                        "invoice_id": "str",
                        "receipt_id": "str?",
                    },
                },
            ]
        if focus == "crm" and getattr(self, "crm", None):
            return [
                {
                    "tool": "crm.create_contact",
                    "args_schema": {
                        "email": "str",
                        "first_name": "str?",
                        "last_name": "str?",
                        "do_not_contact": "bool?",
                    },
                },
                {
                    "tool": "crm.create_company",
                    "args_schema": {"name": "str", "domain": "str?"},
                },
                {
                    "tool": "crm.list_contacts",
                    "args_schema": {
                        "query": "str?",
                        "company_id": "str?",
                        "do_not_contact": "bool?",
                        "limit": "int?",
                        "cursor": "str?",
                    },
                },
                {
                    "tool": "crm.list_companies",
                    "args_schema": {
                        "query": "str?",
                        "domain": "str?",
                        "limit": "int?",
                        "cursor": "str?",
                    },
                },
                {
                    "tool": "crm.associate_contact_company",
                    "args_schema": {"contact_id": "str", "company_id": "str"},
                },
                {
                    "tool": "crm.create_deal",
                    "args_schema": {
                        "name": "str",
                        "amount": "number",
                        "stage": "str?",
                        "contact_id": "str?",
                        "company_id": "str?",
                        "close_date": "str?",
                    },
                },
                {
                    "tool": "crm.update_deal_stage",
                    "args_schema": {"id": "str", "stage": "str"},
                },
                {
                    "tool": "crm.list_deals",
                    "args_schema": {
                        "stage": "str?",
                        "company_id": "str?",
                        "min_amount": "number?",
                        "max_amount": "number?",
                        "limit": "int?",
                        "cursor": "str?",
                    },
                },
                {
                    "tool": "crm.reassign_deal_owner",
                    "args_schema": {"id": "str", "owner": "str"},
                },
                {
                    "tool": "crm.log_activity",
                    "args_schema": {
                        "kind": "str",
                        "contact_id": "str?",
                        "deal_id": "str?",
                        "note": "str?",
                    },
                },
            ]
        if focus == "db":
            return [
                {
                    "tool": "db.list_tables",
                    "args_schema": {
                        "query": "str?",
                        "limit": "int?",
                        "cursor": "str?",
                        "sort_by": "str?",
                        "sort_dir": "asc|desc?",
                    },
                },
                {"tool": "db.describe_table", "args_schema": {"table": "str"}},
                {
                    "tool": "db.query",
                    "args_schema": {
                        "table": "str",
                        "filters": "object?",
                        "columns": "[str]?",
                        "limit": "int?",
                        "offset": "int?",
                        "cursor": "str?",
                        "sort_by": "str?",
                        "descending": "bool?",
                    },
                },
                {
                    "tool": "db.upsert",
                    "args_schema": {"table": "str", "row": "object", "key": "str?"},
                },
            ]
        if focus == "okta":
            return [
                {
                    "tool": "okta.list_users",
                    "args_schema": {
                        "status": "str?",
                        "query": "str?",
                        "include_groups": "bool?",
                        "limit": "int?",
                        "cursor": "str?",
                    },
                },
                {"tool": "okta.get_user", "args_schema": {"user_id": "str"}},
                {"tool": "okta.suspend_user", "args_schema": {"user_id": "str"}},
                {"tool": "okta.unsuspend_user", "args_schema": {"user_id": "str"}},
                {"tool": "okta.list_groups", "args_schema": {"query": "str?"}},
                {
                    "tool": "okta.assign_group",
                    "args_schema": {"user_id": "str", "group_id": "str"},
                },
            ]
        if focus == "servicedesk":
            return [
                {
                    "tool": "servicedesk.list_incidents",
                    "args_schema": {
                        "status": "str?",
                        "priority": "str?",
                        "query": "str?",
                        "limit": "int?",
                        "cursor": "str?",
                    },
                },
                {
                    "tool": "servicedesk.get_incident",
                    "args_schema": {"incident_id": "str"},
                },
                {
                    "tool": "servicedesk.update_incident",
                    "args_schema": {
                        "incident_id": "str",
                        "status": "str?",
                        "assignee": "str?",
                        "comment": "str?",
                    },
                },
                {
                    "tool": "servicedesk.list_requests",
                    "args_schema": {"status": "str?", "query": "str?"},
                },
                {
                    "tool": "servicedesk.update_request",
                    "args_schema": {
                        "request_id": "str",
                        "status": "str?",
                        "approval_stage": "str?",
                        "approval_status": "str?",
                        "comment": "str?",
                    },
                },
            ]
        if focus == "google_admin":
            return [
                {
                    "tool": "google_admin.list_oauth_apps",
                    "args_schema": {
                        "status": "str?",
                        "risk_level": "str?",
                        "query": "str?",
                        "limit": "int?",
                        "cursor": "str?",
                    },
                },
                {
                    "tool": "google_admin.get_oauth_app",
                    "args_schema": {"app_id": "str"},
                },
                {
                    "tool": "google_admin.suspend_oauth_app",
                    "args_schema": {"app_id": "str", "reason": "str?"},
                },
                {
                    "tool": "google_admin.preserve_oauth_evidence",
                    "args_schema": {"app_id": "str", "note": "str?"},
                },
                {
                    "tool": "google_admin.list_drive_shares",
                    "args_schema": {
                        "visibility": "str?",
                        "owner": "str?",
                        "query": "str?",
                        "limit": "int?",
                        "cursor": "str?",
                    },
                },
                {
                    "tool": "google_admin.restrict_drive_share",
                    "args_schema": {
                        "doc_id": "str",
                        "visibility": "str?",
                        "note": "str?",
                    },
                },
            ]
        if focus == "siem":
            return [
                {
                    "tool": "siem.list_alerts",
                    "args_schema": {
                        "status": "str?",
                        "severity": "str?",
                        "query": "str?",
                        "limit": "int?",
                        "cursor": "str?",
                    },
                },
                {"tool": "siem.get_alert", "args_schema": {"alert_id": "str"}},
                {
                    "tool": "siem.create_case",
                    "args_schema": {
                        "title": "str",
                        "alert_id": "str?",
                        "severity": "str?",
                        "owner": "str?",
                    },
                },
                {
                    "tool": "siem.list_cases",
                    "args_schema": {"status": "str?", "owner": "str?"},
                },
                {"tool": "siem.get_case", "args_schema": {"case_id": "str"}},
                {
                    "tool": "siem.preserve_evidence",
                    "args_schema": {
                        "alert_id": "str",
                        "case_id": "str?",
                        "note": "str?",
                    },
                },
                {
                    "tool": "siem.update_case",
                    "args_schema": {
                        "case_id": "str",
                        "status": "str?",
                        "owner": "str?",
                        "customer_notification_required": "bool?",
                        "note": "str?",
                    },
                },
            ]
        if focus == "datadog":
            return [
                {
                    "tool": "datadog.list_services",
                    "args_schema": {"status": "str?", "query": "str?"},
                },
                {"tool": "datadog.get_service", "args_schema": {"service_id": "str"}},
                {
                    "tool": "datadog.update_service",
                    "args_schema": {
                        "service_id": "str",
                        "status": "str?",
                        "note": "str?",
                    },
                },
                {
                    "tool": "datadog.list_monitors",
                    "args_schema": {
                        "status": "str?",
                        "severity": "str?",
                        "service_id": "str?",
                    },
                },
                {"tool": "datadog.get_monitor", "args_schema": {"monitor_id": "str"}},
                {
                    "tool": "datadog.mute_monitor",
                    "args_schema": {"monitor_id": "str", "reason": "str?"},
                },
            ]
        if focus == "pagerduty":
            return [
                {
                    "tool": "pagerduty.list_incidents",
                    "args_schema": {
                        "status": "str?",
                        "urgency": "str?",
                        "service_id": "str?",
                    },
                },
                {
                    "tool": "pagerduty.get_incident",
                    "args_schema": {"incident_id": "str"},
                },
                {
                    "tool": "pagerduty.ack_incident",
                    "args_schema": {"incident_id": "str", "assignee": "str?"},
                },
                {
                    "tool": "pagerduty.escalate_incident",
                    "args_schema": {"incident_id": "str", "assignee": "str"},
                },
                {
                    "tool": "pagerduty.resolve_incident",
                    "args_schema": {"incident_id": "str", "note": "str?"},
                },
            ]
        if focus == "feature_flags":
            return [
                {
                    "tool": "feature_flags.list_flags",
                    "args_schema": {"service": "str?", "env": "str?", "limit": "int?"},
                },
                {"tool": "feature_flags.get_flag", "args_schema": {"flag_key": "str"}},
                {
                    "tool": "feature_flags.set_flag",
                    "args_schema": {
                        "flag_key": "str",
                        "enabled": "bool",
                        "env": "str?",
                        "reason": "str?",
                    },
                },
                {
                    "tool": "feature_flags.update_rollout",
                    "args_schema": {
                        "flag_key": "str",
                        "rollout_pct": "int",
                        "env": "str?",
                        "reason": "str?",
                    },
                },
            ]
        if focus == "hris":
            return [
                {
                    "tool": "hris.list_employees",
                    "args_schema": {
                        "status": "str?",
                        "cohort": "str?",
                        "query": "str?",
                        "limit": "int?",
                        "cursor": "str?",
                    },
                },
                {"tool": "hris.get_employee", "args_schema": {"employee_id": "str"}},
                {
                    "tool": "hris.resolve_identity",
                    "args_schema": {
                        "employee_id": "str",
                        "corporate_email": "str?",
                        "manager": "str?",
                        "note": "str?",
                    },
                },
                {
                    "tool": "hris.mark_onboarded",
                    "args_schema": {"employee_id": "str", "note": "str?"},
                },
            ]
        if focus == "jira":
            return [
                {
                    "tool": "jira.list_issues",
                    "args_schema": {"status": "str?", "assignee": "str?"},
                },
                {"tool": "jira.get_issue", "args_schema": {"issue_id": "str"}},
                {
                    "tool": "jira.create_issue",
                    "args_schema": {
                        "title": "str",
                        "description": "str?",
                        "assignee": "str?",
                    },
                },
                {
                    "tool": "jira.transition_issue",
                    "args_schema": {"issue_id": "str", "status": "str"},
                },
                {
                    "tool": "jira.add_comment",
                    "args_schema": {"issue_id": "str", "body": "str", "author": "str?"},
                },
            ]
        plugin_action_menu = self._plugin_action_menu(focus)
        if plugin_action_menu is not None:
            return plugin_action_menu
        return []
