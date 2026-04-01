from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from vei.world import Scenario, ServiceDeskIncident, ServiceDeskRequest

from .errors import MCPError
from .tool_providers import PrefixToolProvider
from .tool_registry import ToolSpec


def _ensure_list(container: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    value = container.get(key)
    if isinstance(value, list):
        return value
    new_list: List[Dict[str, Any]] = []
    container[key] = new_list
    return new_list


def _incident_to_dict(incident: ServiceDeskIncident | Dict[str, Any]) -> Dict[str, Any]:
    data = (
        incident.__dict__.copy()
        if isinstance(incident, ServiceDeskIncident)
        else dict(incident)
    )
    _ensure_list(data, "history")
    _ensure_list(data, "comments")
    return data


def _request_to_dict(request: ServiceDeskRequest | Dict[str, Any]) -> Dict[str, Any]:
    data = (
        request.__dict__.copy()
        if isinstance(request, ServiceDeskRequest)
        else dict(request)
    )
    _ensure_list(data, "history")
    _ensure_list(data, "comments")
    approvals = data.get("approvals")
    if not isinstance(approvals, list):
        data["approvals"] = []
    return data


def _default_incidents() -> Dict[str, Dict[str, Any]]:
    return {
        "INC-4201": _incident_to_dict(
            ServiceDeskIncident(
                incident_id="INC-4201",
                title="Procurement portal outage",
                status="IN_PROGRESS",
                priority="P2",
                assignee="maya.ops",
                description="Procurement UI throws 500 when approving requests.",
                history=[
                    {"status": "NEW"},
                    {"status": "IN_PROGRESS", "assignee": "maya.ops"},
                ],
            )
        ),
    }


def _default_requests() -> Dict[str, Dict[str, Any]]:
    return {
        "REQ-8801": _request_to_dict(
            ServiceDeskRequest(
                request_id="REQ-8801",
                title="Access: Procurement Admin",
                status="PENDING_APPROVAL",
                requester="amy@macrocompute.example",
                description="Need elevated rights to review MacroBook vendor contract.",
                approvals=[
                    {"stage": "manager", "status": "APPROVED"},
                    {"stage": "security", "status": "PENDING"},
                ],
            )
        )
    }


class ServiceDeskSim:
    """Deterministic ServiceDesk twin (akin to ServiceNow)."""

    _DEFAULT_LIMIT = 25
    _MAX_LIMIT = 200

    def __init__(self, scenario: Optional[Scenario] = None):
        incs = (scenario.service_incidents if scenario else None) or {}
        reqs = (scenario.service_requests if scenario else None) or {}
        self.incidents: Dict[str, Dict[str, Any]] = (
            {inc_id: _incident_to_dict(inc) for inc_id, inc in incs.items()}
            if incs
            else _default_incidents()
        )
        self.requests: Dict[str, Dict[str, Any]] = (
            {req_id: _request_to_dict(req) for req_id, req in reqs.items()}
            if reqs
            else _default_requests()
        )

    def list_incidents(
        self,
        status: Optional[str] = None,
        priority: Optional[str] = None,
        query: Optional[str] = None,
        assignee: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_by: str = "id",
        sort_dir: str = "asc",
    ) -> Dict[str, Any]:
        rows = []
        for inc in self.incidents.values():
            if status and inc.get("status") != status:
                continue
            if priority and inc.get("priority") != priority:
                continue
            if assignee and inc.get("assignee") != assignee:
                continue
            if query:
                needle = query.lower()
                if (
                    needle not in str(inc.get("title", "")).lower()
                    and needle not in str(inc.get("description", "")).lower()
                ):
                    continue
            rows.append(
                {
                    "id": inc["incident_id"],
                    "title": inc["title"],
                    "status": inc["status"],
                    "priority": inc.get("priority"),
                    "assignee": inc.get("assignee"),
                }
            )
        sort_field = (
            sort_by if sort_by in {"id", "status", "priority", "assignee"} else "id"
        )
        rows.sort(
            key=lambda row: _sortable(row.get(sort_field)),
            reverse=sort_dir.lower() != "asc",
        )
        start = _decode_cursor(cursor)
        page_limit = _normalize_limit(
            limit, default=self._DEFAULT_LIMIT, max_limit=self._MAX_LIMIT
        )
        sliced = rows[start : start + page_limit]
        next_cursor = (
            _encode_cursor(start + page_limit)
            if (start + page_limit) < len(rows)
            else None
        )
        return {
            "incidents": sliced,
            "count": len(sliced),
            "total": len(rows),
            "next_cursor": next_cursor,
            "has_more": next_cursor is not None,
        }

    def get_incident(self, incident_id: str) -> Dict[str, Any]:
        incident = self.incidents.get(incident_id)
        if not incident:
            raise MCPError(
                "servicedesk.incident_not_found", f"Unknown incident: {incident_id}"
            )
        return incident

    def update_incident(
        self,
        incident_id: str,
        status: Optional[str] = None,
        assignee: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> Dict[str, Any]:
        incident = self.incidents.get(incident_id)
        if not incident:
            raise MCPError(
                "servicedesk.incident_not_found", f"Unknown incident: {incident_id}"
            )
        if status:
            incident["status"] = status
            _ensure_list(incident, "history").append({"status": status})
        if assignee:
            incident["assignee"] = assignee
            _ensure_list(incident, "history").append({"assignee": assignee})
        if comment:
            _ensure_list(incident, "comments").append(
                {"author": "agent", "body": comment}
            )
        return {
            "incident_id": incident_id,
            "status": incident["status"],
            "assignee": incident.get("assignee"),
        }

    def list_requests(
        self,
        status: Optional[str] = None,
        requester: Optional[str] = None,
        query: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_by: str = "id",
        sort_dir: str = "asc",
    ) -> Dict[str, Any]:
        rows = []
        for req in self.requests.values():
            if status and req.get("status") != status:
                continue
            if requester and req.get("requester") != requester:
                continue
            if query:
                needle = query.lower()
                if (
                    needle not in str(req.get("title", "")).lower()
                    and needle not in str(req.get("description", "")).lower()
                ):
                    continue
            rows.append(
                {
                    "id": req["request_id"],
                    "title": req["title"],
                    "status": req["status"],
                    "requester": req.get("requester"),
                }
            )
        sort_field = sort_by if sort_by in {"id", "status", "requester"} else "id"
        rows.sort(
            key=lambda row: _sortable(row.get(sort_field)),
            reverse=sort_dir.lower() != "asc",
        )
        start = _decode_cursor(cursor)
        page_limit = _normalize_limit(
            limit, default=self._DEFAULT_LIMIT, max_limit=self._MAX_LIMIT
        )
        sliced = rows[start : start + page_limit]
        next_cursor = (
            _encode_cursor(start + page_limit)
            if (start + page_limit) < len(rows)
            else None
        )
        return {
            "requests": sliced,
            "count": len(sliced),
            "total": len(rows),
            "next_cursor": next_cursor,
            "has_more": next_cursor is not None,
        }

    def get_request(self, request_id: str) -> Dict[str, Any]:
        request = self.requests.get(request_id)
        if not request:
            raise MCPError(
                "servicedesk.request_not_found", f"Unknown request: {request_id}"
            )
        return request

    def update_request(
        self,
        request_id: str,
        status: Optional[str] = None,
        approval_stage: Optional[str] = None,
        approval_status: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> Dict[str, Any]:
        request = self.requests.get(request_id)
        if not request:
            raise MCPError(
                "servicedesk.request_not_found", f"Unknown request: {request_id}"
            )
        if status:
            request["status"] = status
            _ensure_list(request, "history").append({"status": status})
        if approval_stage and approval_status:
            updated = False
            approvals: List[Dict[str, Any]] = request.setdefault("approvals", [])
            for approval in approvals:
                if approval.get("stage") == approval_stage:
                    approval["status"] = approval_status
                    updated = True
                    break
            if not updated:
                approvals.append({"stage": approval_stage, "status": approval_status})
            _ensure_list(request, "history").append(
                {"approval_stage": approval_stage, "approval_status": approval_status}
            )
        if comment:
            _ensure_list(request, "comments").append(
                {"author": "agent", "body": comment}
            )
        return {"request_id": request_id, "status": request["status"]}


def _normalize_limit(limit: Optional[int], *, default: int, max_limit: int) -> int:
    if limit is None:
        return default
    if limit < 1:
        return 1
    return min(max_limit, int(limit))


def _decode_cursor(cursor: Optional[str]) -> int:
    if not cursor:
        return 0
    if not cursor.startswith("ofs:"):
        raise MCPError("servicedesk.invalid_cursor", f"Invalid cursor: {cursor}")
    try:
        value = int(cursor.split(":", 1)[1])
    except ValueError as exc:
        raise MCPError(
            "servicedesk.invalid_cursor", f"Invalid cursor: {cursor}"
        ) from exc
    return max(0, value)


def _encode_cursor(offset: int) -> str:
    return f"ofs:{max(0, int(offset))}"


def _sortable(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, (int, float, str)):
        return value
    return str(value)


class ServiceDeskToolProvider(PrefixToolProvider):
    """Tool provider exposing ServiceDesk operations."""

    def __init__(self, sim: ServiceDeskSim):
        super().__init__("servicedesk", prefixes=("servicedesk.",))
        self.sim = sim
        self._specs: List[ToolSpec] = [
            ToolSpec(
                name="servicedesk.list_incidents",
                description="List incidents filtered by status or priority.",
                permissions=("servicedesk:read",),
                default_latency_ms=350,
                latency_jitter_ms=120,
            ),
            ToolSpec(
                name="servicedesk.get_incident",
                description="Fetch a single incident detail.",
                permissions=("servicedesk:read",),
                default_latency_ms=320,
                latency_jitter_ms=110,
            ),
            ToolSpec(
                name="servicedesk.update_incident",
                description="Update incident status, assignee, or leave a comment.",
                permissions=("servicedesk:write",),
                side_effects=("servicedesk_mutation",),
                default_latency_ms=420,
                latency_jitter_ms=140,
            ),
            ToolSpec(
                name="servicedesk.list_requests",
                description="List access/service requests.",
                permissions=("servicedesk:read",),
                default_latency_ms=330,
                latency_jitter_ms=110,
            ),
            ToolSpec(
                name="servicedesk.get_request",
                description="Fetch details for a request.",
                permissions=("servicedesk:read",),
                default_latency_ms=320,
                latency_jitter_ms=100,
            ),
            ToolSpec(
                name="servicedesk.update_request",
                description="Update request status or approval stages with optional comments.",
                permissions=("servicedesk:write",),
                side_effects=("servicedesk_mutation",),
                default_latency_ms=430,
                latency_jitter_ms=150,
            ),
        ]
        self._handlers: Dict[str, Callable[..., Any]] = {
            "servicedesk.list_incidents": self.sim.list_incidents,
            "servicedesk.get_incident": self.sim.get_incident,
            "servicedesk.update_incident": self.sim.update_incident,
            "servicedesk.list_requests": self.sim.list_requests,
            "servicedesk.get_request": self.sim.get_request,
            "servicedesk.update_request": self.sim.update_request,
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
