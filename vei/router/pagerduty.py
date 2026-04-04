from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from vei.world.api import Scenario

from ._pagination import decode_cursor, encode_cursor, normalize_limit, sortable
from .errors import MCPError
from .tool_providers import PrefixToolProvider
from .tool_registry import ToolSpec


def _default_incidents() -> Dict[str, Dict[str, Any]]:
    return {
        "PD-9001": {
            "incident_id": "PD-9001",
            "title": "Checkout latency and error spike",
            "status": "triggered",
            "urgency": "high",
            "service_id": "svc-checkout",
            "assignee": "oncall-commerce",
            "notes": [],
        }
    }


class PagerDutySim:
    """Deterministic PagerDuty-style incident response twin."""

    _DEFAULT_LIMIT = 25
    _MAX_LIMIT = 200

    def __init__(self, scenario: Optional[Scenario] = None):
        seed = (scenario.pagerduty or {}) if scenario else {}
        incidents = seed["incidents"] if "incidents" in seed else _default_incidents()
        self.incidents: Dict[str, Dict[str, Any]] = {
            incident_id: dict(payload) for incident_id, payload in incidents.items()
        }

    def list_incidents(
        self,
        status: Optional[str] = None,
        urgency: Optional[str] = None,
        service_id: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_by: str = "incident_id",
        sort_dir: str = "asc",
    ) -> Dict[str, Any]:
        rows = []
        for incident in self.incidents.values():
            if status and str(incident.get("status", "")).lower() != status.lower():
                continue
            if urgency and str(incident.get("urgency", "")).lower() != urgency.lower():
                continue
            if service_id and incident.get("service_id") != service_id:
                continue
            rows.append(
                {
                    "id": incident["incident_id"],
                    "title": incident["title"],
                    "status": incident.get("status"),
                    "urgency": incident.get("urgency"),
                    "service_id": incident.get("service_id"),
                    "assignee": incident.get("assignee"),
                }
            )
        return _page(
            rows,
            limit=limit,
            cursor=cursor,
            key="incidents",
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    def get_incident(self, incident_id: str) -> Dict[str, Any]:
        incident = self.incidents.get(incident_id)
        if not incident:
            raise MCPError(
                "pagerduty.incident_not_found", f"Unknown incident: {incident_id}"
            )
        return dict(incident)

    def ack_incident(
        self, incident_id: str, assignee: Optional[str] = None
    ) -> Dict[str, Any]:
        incident = self._require_incident(incident_id)
        incident["status"] = "acknowledged"
        if assignee is not None:
            incident["assignee"] = assignee
        return {
            "incident_id": incident_id,
            "status": incident["status"],
            "assignee": incident.get("assignee"),
        }

    def resolve_incident(
        self, incident_id: str, note: Optional[str] = None
    ) -> Dict[str, Any]:
        incident = self._require_incident(incident_id)
        incident["status"] = "resolved"
        if note:
            incident.setdefault("notes", []).append(note)
        return {"incident_id": incident_id, "status": incident["status"]}

    def escalate_incident(self, incident_id: str, assignee: str) -> Dict[str, Any]:
        incident = self._require_incident(incident_id)
        incident["assignee"] = assignee
        incident.setdefault("notes", []).append(f"Escalated to {assignee}")
        return {
            "incident_id": incident_id,
            "status": incident.get("status"),
            "assignee": assignee,
        }

    def _require_incident(self, incident_id: str) -> Dict[str, Any]:
        incident = self.incidents.get(incident_id)
        if not incident:
            raise MCPError(
                "pagerduty.incident_not_found", f"Unknown incident: {incident_id}"
            )
        return incident


class PagerDutyToolProvider(PrefixToolProvider):
    def __init__(self, sim: PagerDutySim):
        super().__init__("pagerduty", prefixes=("pagerduty.",))
        self.sim = sim
        self._specs: List[ToolSpec] = [
            ToolSpec(
                name="pagerduty.list_incidents",
                description="List PagerDuty-style incidents.",
                permissions=("pagerduty:read",),
                default_latency_ms=260,
                latency_jitter_ms=80,
            ),
            ToolSpec(
                name="pagerduty.get_incident",
                description="Fetch a PagerDuty-style incident by id.",
                permissions=("pagerduty:read",),
                default_latency_ms=230,
                latency_jitter_ms=60,
            ),
            ToolSpec(
                name="pagerduty.ack_incident",
                description="Acknowledge a PagerDuty-style incident.",
                permissions=("pagerduty:write",),
                side_effects=("pagerduty_mutation",),
                default_latency_ms=320,
                latency_jitter_ms=100,
            ),
            ToolSpec(
                name="pagerduty.resolve_incident",
                description="Resolve a PagerDuty-style incident.",
                permissions=("pagerduty:write",),
                side_effects=("pagerduty_mutation",),
                default_latency_ms=330,
                latency_jitter_ms=100,
            ),
            ToolSpec(
                name="pagerduty.escalate_incident",
                description="Reassign or escalate a PagerDuty-style incident.",
                permissions=("pagerduty:write",),
                side_effects=("pagerduty_mutation",),
                default_latency_ms=340,
                latency_jitter_ms=110,
            ),
        ]
        self._handlers: Dict[str, Callable[..., Any]] = {
            "pagerduty.list_incidents": self.sim.list_incidents,
            "pagerduty.get_incident": self.sim.get_incident,
            "pagerduty.ack_incident": self.sim.ack_incident,
            "pagerduty.resolve_incident": self.sim.resolve_incident,
            "pagerduty.escalate_incident": self.sim.escalate_incident,
        }

    def specs(self) -> List[ToolSpec]:
        return list(self._specs)

    def call(self, tool: str, args: Dict[str, Any]) -> Any:
        handler = self._handlers.get(tool)
        if not handler:
            raise MCPError("unknown_tool", f"No such tool: {tool}")
        try:
            return handler(**(args or {}))
        except TypeError as exc:
            raise MCPError("invalid_args", str(exc)) from exc


def _page(
    rows: List[Dict[str, Any]],
    *,
    limit: Optional[int],
    cursor: Optional[str],
    key: str,
    sort_by: str,
    sort_dir: str,
) -> Dict[str, Any]:
    if rows:
        sort_field = sort_by if sort_by in rows[0] else next(iter(rows[0]))
        rows.sort(
            key=lambda row: sortable(row.get(sort_field), lowercase=True),
            reverse=sort_dir.lower() != "asc",
        )
    start = decode_cursor(cursor, prefix="idx", error_code="pagerduty.invalid_cursor")
    page_limit = normalize_limit(
        limit, default=PagerDutySim._DEFAULT_LIMIT, max_limit=PagerDutySim._MAX_LIMIT
    )
    sliced = rows[start : start + page_limit]
    next_cursor = (
        encode_cursor(start + page_limit, prefix="idx")
        if (start + page_limit) < len(rows)
        else None
    )
    return {
        key: sliced,
        "count": len(sliced),
        "total": len(rows),
        "next_cursor": next_cursor,
        "has_more": next_cursor is not None,
    }
