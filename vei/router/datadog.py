from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from vei.world.api import Scenario

from .errors import MCPError
from .tool_providers import PrefixToolProvider
from .tool_registry import ToolSpec


def _default_services() -> Dict[str, Dict[str, Any]]:
    return {
        "svc-checkout": {
            "service_id": "svc-checkout",
            "name": "checkout-api",
            "status": "degraded",
            "error_rate_pct": 18.4,
            "latency_p95_ms": 2240,
            "revenue_tier": "critical",
            "notes": [],
        }
    }


def _default_monitors() -> Dict[str, Dict[str, Any]]:
    return {
        "mon-5001": {
            "monitor_id": "mon-5001",
            "title": "Checkout 5xx spike",
            "service_id": "svc-checkout",
            "status": "alert",
            "severity": "critical",
            "threshold": "5xx > 3%",
            "current_value": "18.4%",
            "muted": False,
            "history": [],
        }
    }


class DatadogSim:
    """Deterministic Datadog-style monitoring twin."""

    _DEFAULT_LIMIT = 25
    _MAX_LIMIT = 200

    def __init__(self, scenario: Optional[Scenario] = None):
        seed = (scenario.datadog or {}) if scenario else {}
        services = seed["services"] if "services" in seed else _default_services()
        monitors = seed["monitors"] if "monitors" in seed else _default_monitors()
        self.services: Dict[str, Dict[str, Any]] = {
            service_id: dict(payload) for service_id, payload in services.items()
        }
        self.monitors: Dict[str, Dict[str, Any]] = {
            monitor_id: dict(payload) for monitor_id, payload in monitors.items()
        }

    def list_services(
        self,
        status: Optional[str] = None,
        query: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_by: str = "name",
        sort_dir: str = "asc",
    ) -> Dict[str, Any]:
        rows = []
        needle = (query or "").strip().lower()
        for service in self.services.values():
            if status and str(service.get("status", "")).lower() != status.lower():
                continue
            haystack = " ".join(
                [str(service.get("name", "")), str(service.get("revenue_tier", ""))]
            ).lower()
            if needle and needle not in haystack:
                continue
            rows.append(
                {
                    "id": service["service_id"],
                    "name": service["name"],
                    "status": service.get("status"),
                    "error_rate_pct": service.get("error_rate_pct"),
                    "latency_p95_ms": service.get("latency_p95_ms"),
                    "revenue_tier": service.get("revenue_tier"),
                }
            )
        return _page(
            rows,
            limit=limit,
            cursor=cursor,
            key="services",
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    def get_service(self, service_id: str) -> Dict[str, Any]:
        service = self.services.get(service_id)
        if not service:
            raise MCPError(
                "datadog.service_not_found", f"Unknown service: {service_id}"
            )
        return dict(service)

    def update_service(
        self, service_id: str, status: Optional[str] = None, note: Optional[str] = None
    ) -> Dict[str, Any]:
        service = self._require_service(service_id)
        if status:
            service["status"] = status
        if note:
            service.setdefault("notes", []).append(note)
        return {"service_id": service_id, "status": service.get("status")}

    def list_monitors(
        self,
        status: Optional[str] = None,
        severity: Optional[str] = None,
        service_id: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_by: str = "severity",
        sort_dir: str = "desc",
    ) -> Dict[str, Any]:
        rows = []
        for monitor in self.monitors.values():
            if status and str(monitor.get("status", "")).lower() != status.lower():
                continue
            if (
                severity
                and str(monitor.get("severity", "")).lower() != severity.lower()
            ):
                continue
            if service_id and monitor.get("service_id") != service_id:
                continue
            rows.append(
                {
                    "id": monitor["monitor_id"],
                    "title": monitor["title"],
                    "service_id": monitor.get("service_id"),
                    "status": monitor.get("status"),
                    "severity": monitor.get("severity"),
                    "muted": bool(monitor.get("muted", False)),
                }
            )
        return _page(
            rows,
            limit=limit,
            cursor=cursor,
            key="monitors",
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    def get_monitor(self, monitor_id: str) -> Dict[str, Any]:
        monitor = self.monitors.get(monitor_id)
        if not monitor:
            raise MCPError(
                "datadog.monitor_not_found", f"Unknown monitor: {monitor_id}"
            )
        return dict(monitor)

    def mute_monitor(
        self, monitor_id: str, reason: Optional[str] = None
    ) -> Dict[str, Any]:
        monitor = self._require_monitor(monitor_id)
        monitor["muted"] = True
        monitor.setdefault("history", []).append(
            {"action": "mute", "reason": reason or ""}
        )
        return {"monitor_id": monitor_id, "muted": True}

    def _require_service(self, service_id: str) -> Dict[str, Any]:
        service = self.services.get(service_id)
        if not service:
            raise MCPError(
                "datadog.service_not_found", f"Unknown service: {service_id}"
            )
        return service

    def _require_monitor(self, monitor_id: str) -> Dict[str, Any]:
        monitor = self.monitors.get(monitor_id)
        if not monitor:
            raise MCPError(
                "datadog.monitor_not_found", f"Unknown monitor: {monitor_id}"
            )
        return monitor


class DatadogToolProvider(PrefixToolProvider):
    def __init__(self, sim: DatadogSim):
        super().__init__("datadog", prefixes=("datadog.",))
        self.sim = sim
        self._specs: List[ToolSpec] = [
            ToolSpec(
                name="datadog.list_services",
                description="List monitored services and current health status.",
                permissions=("datadog:read",),
                default_latency_ms=250,
                latency_jitter_ms=70,
            ),
            ToolSpec(
                name="datadog.get_service",
                description="Fetch a service health view by id.",
                permissions=("datadog:read",),
                default_latency_ms=230,
                latency_jitter_ms=60,
            ),
            ToolSpec(
                name="datadog.update_service",
                description="Update a Datadog-style service incident annotation or status.",
                permissions=("datadog:write",),
                side_effects=("datadog_mutation",),
                default_latency_ms=330,
                latency_jitter_ms=100,
            ),
            ToolSpec(
                name="datadog.list_monitors",
                description="List monitors by service, status, or severity.",
                permissions=("datadog:read",),
                default_latency_ms=250,
                latency_jitter_ms=70,
            ),
            ToolSpec(
                name="datadog.get_monitor",
                description="Fetch a Datadog-style monitor by id.",
                permissions=("datadog:read",),
                default_latency_ms=220,
                latency_jitter_ms=60,
            ),
            ToolSpec(
                name="datadog.mute_monitor",
                description="Mute a noisy Datadog-style monitor.",
                permissions=("datadog:write",),
                side_effects=("datadog_mutation",),
                default_latency_ms=320,
                latency_jitter_ms=100,
            ),
        ]
        self._handlers: Dict[str, Callable[..., Any]] = {
            "datadog.list_services": self.sim.list_services,
            "datadog.get_service": self.sim.get_service,
            "datadog.update_service": self.sim.update_service,
            "datadog.list_monitors": self.sim.list_monitors,
            "datadog.get_monitor": self.sim.get_monitor,
            "datadog.mute_monitor": self.sim.mute_monitor,
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
            key=lambda row: _sortable(row.get(sort_field)),
            reverse=sort_dir.lower() != "asc",
        )
    start = _decode_cursor(cursor)
    page_limit = _normalize_limit(limit)
    sliced = rows[start : start + page_limit]
    next_cursor = (
        _encode_cursor(start + page_limit) if (start + page_limit) < len(rows) else None
    )
    return {
        key: sliced,
        "count": len(sliced),
        "total": len(rows),
        "next_cursor": next_cursor,
        "has_more": next_cursor is not None,
    }


def _normalize_limit(limit: Optional[int]) -> int:
    if limit is None:
        return DatadogSim._DEFAULT_LIMIT
    return max(1, min(int(limit), DatadogSim._MAX_LIMIT))


def _encode_cursor(index: int) -> str:
    return f"idx:{index}"


def _decode_cursor(cursor: Optional[str]) -> int:
    if not cursor:
        return 0
    if not cursor.startswith("idx:"):
        raise MCPError("datadog.invalid_cursor", f"Invalid cursor: {cursor}")
    try:
        return max(0, int(cursor.split(":", 1)[1]))
    except ValueError as exc:
        raise MCPError("datadog.invalid_cursor", f"Invalid cursor: {cursor}") from exc


def _sortable(value: Any) -> Any:
    if value is None:
        return ""
    return str(value).lower()
