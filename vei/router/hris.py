from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from vei.world.api import Scenario

from .errors import MCPError
from .tool_providers import PrefixToolProvider
from .tool_registry import ToolSpec


def _default_employees() -> Dict[str, Dict[str, Any]]:
    return {
        "EMP-1001": {
            "employee_id": "EMP-1001",
            "email": "jordan.sellers@acquired.example.com",
            "display_name": "Jordan Sellers",
            "department": "Sales",
            "manager": "maria.vp@example.com",
            "status": "pre_start",
            "cohort": "acquisition-wave-1",
            "identity_conflict": True,
            "onboarded": False,
            "notes": [],
        }
    }


class HrisSim:
    """Deterministic HRIS twin for onboarding and identity conflict resolution."""

    _DEFAULT_LIMIT = 25
    _MAX_LIMIT = 500

    def __init__(self, scenario: Optional[Scenario] = None):
        seed = (scenario.hris or {}) if scenario else {}
        employees = seed["employees"] if "employees" in seed else _default_employees()
        self.employees: Dict[str, Dict[str, Any]] = {
            employee_id: dict(payload) for employee_id, payload in employees.items()
        }

    def list_employees(
        self,
        status: Optional[str] = None,
        cohort: Optional[str] = None,
        query: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_by: str = "employee_id",
        sort_dir: str = "asc",
    ) -> Dict[str, Any]:
        rows = []
        needle = (query or "").strip().lower()
        for employee in self.employees.values():
            if status and str(employee.get("status", "")).lower() != status.lower():
                continue
            if cohort and employee.get("cohort") != cohort:
                continue
            haystack = " ".join(
                [
                    str(employee.get("display_name", "")),
                    str(employee.get("email", "")),
                    str(employee.get("department", "")),
                ]
            ).lower()
            if needle and needle not in haystack:
                continue
            rows.append(
                {
                    "id": employee["employee_id"],
                    "email": employee.get("email"),
                    "display_name": employee.get("display_name"),
                    "status": employee.get("status"),
                    "cohort": employee.get("cohort"),
                    "identity_conflict": bool(employee.get("identity_conflict", False)),
                    "onboarded": bool(employee.get("onboarded", False)),
                }
            )
        return _page(
            rows,
            limit=limit,
            cursor=cursor,
            key="employees",
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    def get_employee(self, employee_id: str) -> Dict[str, Any]:
        employee = self.employees.get(employee_id)
        if not employee:
            raise MCPError(
                "hris.employee_not_found", f"Unknown employee: {employee_id}"
            )
        return dict(employee)

    def resolve_identity(
        self,
        employee_id: str,
        corporate_email: Optional[str] = None,
        manager: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        employee = self._require_employee(employee_id)
        if corporate_email is not None:
            employee["email"] = corporate_email
        if manager is not None:
            employee["manager"] = manager
        employee["identity_conflict"] = False
        if note:
            employee.setdefault("notes", []).append(note)
        return {
            "employee_id": employee_id,
            "email": employee.get("email"),
            "identity_conflict": False,
        }

    def mark_onboarded(
        self, employee_id: str, note: Optional[str] = None
    ) -> Dict[str, Any]:
        employee = self._require_employee(employee_id)
        employee["onboarded"] = True
        employee["status"] = "active"
        if note:
            employee.setdefault("notes", []).append(note)
        return {
            "employee_id": employee_id,
            "status": employee.get("status"),
            "onboarded": True,
        }

    def _require_employee(self, employee_id: str) -> Dict[str, Any]:
        employee = self.employees.get(employee_id)
        if not employee:
            raise MCPError(
                "hris.employee_not_found", f"Unknown employee: {employee_id}"
            )
        return employee


class HrisToolProvider(PrefixToolProvider):
    def __init__(self, sim: HrisSim):
        super().__init__("hris", prefixes=("hris.",))
        self.sim = sim
        self._specs: List[ToolSpec] = [
            ToolSpec(
                name="hris.list_employees",
                description="List HRIS employees by status or cohort.",
                permissions=("hris:read",),
                default_latency_ms=240,
                latency_jitter_ms=70,
            ),
            ToolSpec(
                name="hris.get_employee",
                description="Fetch an HRIS employee record by id.",
                permissions=("hris:read",),
                default_latency_ms=220,
                latency_jitter_ms=60,
            ),
            ToolSpec(
                name="hris.resolve_identity",
                description="Resolve an employee identity conflict before onboarding.",
                permissions=("hris:write",),
                side_effects=("hris_mutation",),
                default_latency_ms=320,
                latency_jitter_ms=100,
            ),
            ToolSpec(
                name="hris.mark_onboarded",
                description="Mark an employee as onboarded in the HRIS.",
                permissions=("hris:write",),
                side_effects=("hris_mutation",),
                default_latency_ms=320,
                latency_jitter_ms=100,
            ),
        ]
        self._handlers: Dict[str, Callable[..., Any]] = {
            "hris.list_employees": self.sim.list_employees,
            "hris.get_employee": self.sim.get_employee,
            "hris.resolve_identity": self.sim.resolve_identity,
            "hris.mark_onboarded": self.sim.mark_onboarded,
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
        return HrisSim._DEFAULT_LIMIT
    return max(1, min(int(limit), HrisSim._MAX_LIMIT))


def _encode_cursor(index: int) -> str:
    return f"idx:{index}"


def _decode_cursor(cursor: Optional[str]) -> int:
    if not cursor:
        return 0
    if not cursor.startswith("idx:"):
        raise MCPError("hris.invalid_cursor", f"Invalid cursor: {cursor}")
    try:
        return max(0, int(cursor.split(":", 1)[1]))
    except ValueError as exc:
        raise MCPError("hris.invalid_cursor", f"Invalid cursor: {cursor}") from exc


def _sortable(value: Any) -> Any:
    if value is None:
        return ""
    return str(value).lower()
