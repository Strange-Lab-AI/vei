from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable

from .models import FlowDataset, FlowStep

FLOW_CHANNEL_LAYOUT: tuple[dict[str, object], ...] = (
    {"id": "Plan", "label": "Plan", "x": 80, "y": 240, "color": "#7B5BFF"},
    {"id": "Slack", "label": "Slack", "x": 260, "y": 150, "color": "#36C5F0"},
    {"id": "Mail", "label": "Mail", "x": 260, "y": 330, "color": "#FFB347"},
    {"id": "Browser", "label": "Browser", "x": 420, "y": 90, "color": "#B57EDC"},
    {"id": "Docs", "label": "Docs", "x": 420, "y": 210, "color": "#66BB6A"},
    {"id": "Tickets", "label": "Tickets", "x": 420, "y": 330, "color": "#FF7043"},
    {"id": "CRM", "label": "CRM", "x": 580, "y": 150, "color": "#42A5F5"},
    {"id": "World", "label": "World", "x": 580, "y": 270, "color": "#8D6E63"},
    {"id": "Help", "label": "Help", "x": 740, "y": 150, "color": "#F06292"},
    {"id": "Misc", "label": "Misc", "x": 740, "y": 330, "color": "#9E9E9E"},
)


FLOW_TOOL_PREFIX_MAP: tuple[tuple[str, str], ...] = (
    ("slack.", "Slack"),
    ("mail.", "Mail"),
    ("browser.", "Browser"),
    ("docs.", "Docs"),
    ("doc.", "Docs"),
    ("drive.", "Docs"),
    ("tickets.", "Tickets"),
    ("ticket.", "Tickets"),
    ("crm.", "CRM"),
    ("okta.", "World"),
    ("google_admin.", "World"),
    ("datadog.", "World"),
    ("pagerduty.", "World"),
    ("feature_flags.", "World"),
    ("service_ops.", "World"),
    ("spreadsheet.", "World"),
    ("vei.", "World"),
    ("help.", "Help"),
    ("support.", "Tickets"),
)


def load_transcript(path: Path) -> list[Dict[str, Any]]:
    if path.suffix == ".jsonl":
        records: list[Dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                records.append(json.loads(line))
        return records
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, list) else []


def load_trace(path: Path) -> list[Dict[str, Any]]:
    return load_transcript(path)


def discover_question(start: Path) -> str | None:
    search_dirs = [start]
    for _ in range(4):
        parent = search_dirs[-1].parent
        if parent == search_dirs[-1]:
            break
        search_dirs.append(parent)
    for directory in search_dirs:
        summary = directory / "summary.txt"
        if not summary.exists():
            continue
        with summary.open("r", encoding="utf-8") as fh:
            for line in fh:
                if line.lower().startswith("task:"):
                    return line.split(":", 1)[1].strip()
    return None


def flow_channel_from_tool(tool: str) -> str:
    normalized = tool.lower()
    for prefix, channel in FLOW_TOOL_PREFIX_MAP:
        if normalized.startswith(prefix):
            return channel
    return "Misc"


def flow_channel_from_focus(focus: str | None) -> str:
    if not focus:
        return "Misc"
    normalized = focus.lower()
    if normalized in {"slack", "slack_thread"}:
        return "Slack"
    if normalized in {"mail", "inbox"}:
        return "Mail"
    if normalized in {"browser", "web"}:
        return "Browser"
    if normalized in {"docs", "doc", "drive"}:
        return "Docs"
    if normalized in {"tickets", "ticket"}:
        return "Tickets"
    if normalized in {"crm", "salesforce"}:
        return "CRM"
    if normalized in {
        "world",
        "router",
        "identity",
        "spreadsheet",
        "pagerduty",
        "service_ops",
    }:
        return "World"
    if normalized == "help":
        return "Help"
    return "Misc"


def flow_events_from_transcript_entry(entry: Dict[str, Any]) -> list[Dict[str, Any]]:
    events: list[Dict[str, Any]] = []
    meta = entry.get("meta")
    meta_time = meta.get("time_ms") if isinstance(meta, dict) else None

    if "llm_plan" in entry:
        raw = entry["llm_plan"]
        label = _shorten(str(raw), 72)
        return [
            {
                "channel": "Plan",
                "label": label,
                "tool": "llm_plan",
                "time_ms": meta_time,
            }
        ]

    if "action" in entry and isinstance(entry["action"], dict):
        tool = str(entry["action"].get("tool", ""))
        return [
            {
                "channel": flow_channel_from_tool(tool),
                "label": _shorten(_format_action(tool, entry["action"]), 90),
                "tool": tool,
                "time_ms": meta_time,
            }
        ]

    if "observation" in entry and isinstance(entry["observation"], dict):
        obs = entry["observation"]
        focus = obs.get("focus") if isinstance(obs.get("focus"), str) else None
        summary = obs.get("summary")
        label = _shorten(str(summary or f"Observed {focus or 'world'}"), 90)
        events.append(
            {
                "channel": flow_channel_from_focus(focus),
                "label": label,
                "tool": f"observe:{focus or 'summary'}",
                "time_ms": obs.get("time_ms", meta_time),
            }
        )
    return events


def flow_events_from_trace_record(record: Dict[str, Any]) -> list[Dict[str, Any]]:
    record_type = str(record.get("type", "")).lower()
    time_ms = int(record.get("time_ms", 0))
    if record_type == "call":
        tool = str(record.get("tool", ""))
        return [
            {
                "channel": flow_channel_from_tool(tool),
                "label": _shorten(_format_action(tool, record.get("args", {})), 90),
                "tool": tool,
                "time_ms": time_ms,
            }
        ]
    if record_type == "event":
        target = str(record.get("target", "world"))
        payload = record.get("payload", {})
        label = _shorten(f"{target}: {payload}", 90)
        return [
            {
                "channel": flow_channel_from_focus(target),
                "label": label,
                "tool": f"event:{target}",
                "time_ms": time_ms,
            }
        ]
    return []


def build_flow_steps(events: Iterable[Dict[str, Any]]) -> list[FlowStep]:
    steps: list[FlowStep] = []
    prev_channel = "Plan"
    for index, item in enumerate(events, start=1):
        channel = str(item.get("channel", "Misc"))
        step = FlowStep(
            index=index,
            channel=channel,
            label=str(item.get("label", "")),
            tool=(str(item.get("tool")) if item.get("tool") else None),
            prev_channel=prev_channel,
            time_ms=(
                int(item.get("time_ms", 0)) if item.get("time_ms") is not None else None
            ),
        )
        steps.append(step)
        prev_channel = channel
    return steps


def load_flow_dataset(path: Path) -> FlowDataset:
    trace_path = path / "trace.jsonl"
    transcript_path = path / "transcript.json"
    events: list[Dict[str, Any]] = []
    source = "trace"
    if trace_path.exists():
        for record in load_trace(trace_path):
            events.extend(flow_events_from_trace_record(record))
    elif transcript_path.exists():
        source = "transcript"
        for record in load_transcript(transcript_path):
            events.extend(flow_events_from_transcript_entry(record))
    return FlowDataset(
        key=path.name,
        label=path.name.replace("_", " ").title(),
        steps=build_flow_steps(events),
        source=source,
        question=discover_question(path),
    )


def _format_action(tool: str, payload: Dict[str, Any]) -> str:
    if not payload:
        return tool
    return f"{tool} {json.dumps(payload, sort_keys=True)}"


def _shorten(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"
