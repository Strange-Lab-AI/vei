"""Surface simulations: Slack, Mail, and Browser.

These were extracted from core.py to keep the Router class focused on
orchestration and dispatch while each surface lives in its own class.
"""

from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from vei.world.scenario import Scenario

from .errors import MCPError

if TYPE_CHECKING:
    from .core import EventBus


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


class SlackSim:
    def __init__(self, bus: "EventBus", scenario: Optional[Scenario] = None):
        self.bus = bus
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
        ts = str(len(ch["messages"]) + 1)
        msg = {"ts": ts, "user": "agent", "text": text, "thread_ts": thread_ts}
        ch["messages"].append(msg)
        lower = text.lower()
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
        ch = self.channels.get(channel)
        if not ch:
            raise MCPError("unknown_channel", f"Unknown Slack channel: {channel}")
        for msg in ch["messages"]:
            if msg.get("ts") == ts:
                reactions = msg.setdefault("reactions", [])
                for r in reactions:
                    if r["name"] == emoji:
                        r["count"] += 1
                        return {"ok": True}
                reactions.append({"name": emoji, "count": 1, "users": ["agent"]})
                return {"ok": True}
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
    def __init__(self, bus: "EventBus", scenario: Optional[Scenario] = None):
        self.bus = bus
        self.messages: Dict[str, Dict[str, Any]] = {}
        self.inbox: List[str] = []
        self.counter = 1
        self.local_domains = self._local_domains(scenario)
        self.local_mailbox = "me@example"
        self._variants_override = (
            scenario.vendor_reply_variants
            if scenario and scenario.vendor_reply_variants
            else None
        )
        seeded_threads = (
            list(scenario.mail_threads) if scenario and scenario.mail_threads else []
        )
        if seeded_threads:
            seeded_inbox: List[tuple[int, str]] = []
            for thread in seeded_threads:
                thread_id = str(thread.get("thread_id") or f"thread-{self.counter}")
                category = str(thread.get("category") or "external")
                title = thread.get("title")
                for index, message in enumerate(thread.get("messages", [])):
                    mid = f"m{self.counter}"
                    self.counter += 1
                    time_ms = int(message.get("time_ms") or (self.bus.clock_ms + index))
                    sender = str(message.get("from") or "unknown@example")
                    recipient = str(message.get("to") or "me@example")
                    if (
                        self._is_local_recipient(recipient)
                        and self.local_mailbox == "me@example"
                    ):
                        self.local_mailbox = recipient
                    unread = bool(
                        message.get("unread", self._is_local_recipient(recipient))
                    )
                    record = {
                        "id": mid,
                        "from": sender,
                        "to": recipient,
                        "subj": str(message.get("subj") or title or "Untitled thread"),
                        "time": time_ms,
                        "unread": unread,
                        "headers": {
                            "From": sender,
                            "To": recipient,
                            "Subject": str(
                                message.get("subj") or title or "Untitled thread"
                            ),
                        },
                        "body_text": str(message.get("body_text") or ""),
                        "thread_id": thread_id,
                        "category": category,
                    }
                    self.messages[mid] = record
                    if self._is_local_recipient(recipient):
                        seeded_inbox.append((time_ms, mid))
            seeded_inbox.sort(key=lambda item: item[0], reverse=True)
            self.inbox = [message_id for _, message_id in seeded_inbox]

    def list(self, folder: str = "INBOX") -> List[Dict[str, Any]]:
        return [self.messages[mid] for mid in self.inbox]

    def open(self, id: str) -> Dict[str, Any]:
        m = self.messages.get(id)
        if not m:
            raise MCPError("unknown_message", f"Unknown mail id: {id}")
        m["unread"] = False
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
            "from": self.local_mailbox,
            "to": to,
            "subj": subj,
            "time": self.bus.clock_ms,
            "unread": False,
            "headers": {"From": self.local_mailbox, "To": to, "Subject": subj},
            "body_text": body_text,
        }
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
            "to": self.local_mailbox,
            "subj": event["subj"],
            "time": self.bus.clock_ms,
            "unread": True,
            "headers": {
                "From": event["from"],
                "To": self.local_mailbox,
                "Subject": event["subj"],
            },
            "body_text": event["body_text"],
        }
        self.messages[mid] = msg
        self.inbox.insert(0, mid)
        return {"id": mid}

    def _is_local_recipient(self, address: str) -> bool:
        if address == "me@example":
            return True
        if "@" not in address:
            return False
        _, domain = address.rsplit("@", 1)
        return domain.strip().lower() in self.local_domains

    def _local_domains(self, scenario: Optional[Scenario]) -> set[str]:
        metadata = dict(scenario.metadata or {}) if scenario is not None else {}
        domain = str(metadata.get("builder_organization_domain", "")).strip().lower()
        result = {"example", "example.com"}
        if domain:
            result.add(domain)
        return result


class BrowserVirtual:
    def __init__(self, bus: "EventBus", scenario: Optional[Scenario] = None):
        self.bus = bus
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
            args = a.get("args", {})
            node_id = args.get("node_id")
            if node_id is None:
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
