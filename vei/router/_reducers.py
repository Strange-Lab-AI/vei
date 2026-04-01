"""State reducers and fault profiles for the event-sourced router."""

from __future__ import annotations

from typing import Any, Dict

from vei.world import Event as StateEvent

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
