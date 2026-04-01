from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from vei.world.api import Scenario

from .errors import MCPError
from .tool_providers import PrefixToolProvider
from .tool_registry import ToolSpec


def _default_alerts() -> Dict[str, Dict[str, Any]]:
    return {
        "ALT-7001": {
            "alert_id": "ALT-7001",
            "title": "Suspicious OAuth grant spike",
            "status": "OPEN",
            "severity": "high",
            "source": "google_workspace",
            "artifact_refs": ["OAUTH-4201"],
            "evidence_preserved": False,
            "history": [],
        }
    }


class SiemSim:
    """Deterministic SIEM twin for alert triage, casework, and evidence preservation."""

    _DEFAULT_LIMIT = 25
    _MAX_LIMIT = 200

    def __init__(self, scenario: Optional[Scenario] = None):
        seed = (scenario.siem or {}) if scenario else {}
        alerts = seed["alerts"] if "alerts" in seed else _default_alerts()
        cases = seed["cases"] if "cases" in seed else {}
        self.alerts: Dict[str, Dict[str, Any]] = {
            alert_id: dict(payload) for alert_id, payload in alerts.items()
        }
        self.cases: Dict[str, Dict[str, Any]] = {
            case_id: dict(payload) for case_id, payload in cases.items()
        }
        self._case_seq = (
            max((self._extract_seq(case_id) for case_id in self.cases), default=0) + 1
        )

    def list_alerts(
        self,
        status: Optional[str] = None,
        severity: Optional[str] = None,
        query: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_by: str = "severity",
        sort_dir: str = "desc",
    ) -> Dict[str, Any]:
        rows: List[Dict[str, Any]] = []
        needle = (query or "").strip().lower()
        for alert in self.alerts.values():
            if status and str(alert.get("status", "")).upper() != status.upper():
                continue
            if severity and str(alert.get("severity", "")).lower() != severity.lower():
                continue
            haystack = " ".join(
                [
                    str(alert.get("title", "")),
                    str(alert.get("source", "")),
                    " ".join(str(item) for item in alert.get("artifact_refs", [])),
                ]
            ).lower()
            if needle and needle not in haystack:
                continue
            rows.append(
                {
                    "id": alert["alert_id"],
                    "title": alert["title"],
                    "status": alert.get("status"),
                    "severity": alert.get("severity"),
                    "source": alert.get("source"),
                    "evidence_preserved": bool(alert.get("evidence_preserved", False)),
                }
            )
        return _page(
            rows,
            limit=limit,
            cursor=cursor,
            key="alerts",
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    def get_alert(self, alert_id: str) -> Dict[str, Any]:
        alert = self.alerts.get(alert_id)
        if not alert:
            raise MCPError("siem.alert_not_found", f"Unknown alert: {alert_id}")
        return dict(alert)

    def create_case(
        self,
        title: str,
        alert_id: Optional[str] = None,
        severity: Optional[str] = None,
        owner: Optional[str] = None,
    ) -> Dict[str, Any]:
        case_id = f"CASE-{self._case_seq:04d}"
        self._case_seq += 1
        case = {
            "case_id": case_id,
            "title": title,
            "status": "OPEN",
            "severity": severity or "medium",
            "owner": owner,
            "alert_id": alert_id,
            "customer_notification_required": None,
            "evidence_refs": [alert_id] if alert_id else [],
            "notes": [],
        }
        self.cases[case_id] = case
        return {"case_id": case_id, "status": case["status"], "alert_id": alert_id}

    def list_cases(
        self,
        status: Optional[str] = None,
        owner: Optional[str] = None,
        severity: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_by: str = "case_id",
        sort_dir: str = "asc",
    ) -> Dict[str, Any]:
        rows: List[Dict[str, Any]] = []
        for case in self.cases.values():
            if status and str(case.get("status", "")).upper() != status.upper():
                continue
            if owner and case.get("owner") != owner:
                continue
            if severity and str(case.get("severity", "")).lower() != severity.lower():
                continue
            rows.append(
                {
                    "id": case["case_id"],
                    "title": case["title"],
                    "status": case.get("status"),
                    "severity": case.get("severity"),
                    "owner": case.get("owner"),
                    "customer_notification_required": case.get(
                        "customer_notification_required"
                    ),
                }
            )
        return _page(
            rows,
            limit=limit,
            cursor=cursor,
            key="cases",
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    def get_case(self, case_id: str) -> Dict[str, Any]:
        case = self.cases.get(case_id)
        if not case:
            raise MCPError("siem.case_not_found", f"Unknown case: {case_id}")
        return dict(case)

    def preserve_evidence(
        self,
        alert_id: str,
        case_id: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        alert = self._require_alert(alert_id)
        alert["evidence_preserved"] = True
        alert.setdefault("history", []).append(
            {"action": "preserve_evidence", "case_id": case_id, "note": note or ""}
        )
        if case_id:
            case = self._require_case(case_id)
            refs = case.setdefault("evidence_refs", [])
            if alert_id not in refs:
                refs.append(alert_id)
            if note:
                case.setdefault("notes", []).append(note)
        return {
            "alert_id": alert_id,
            "evidence_preserved": True,
            "case_id": case_id,
        }

    def update_case(
        self,
        case_id: str,
        status: Optional[str] = None,
        owner: Optional[str] = None,
        customer_notification_required: Optional[bool] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        case = self._require_case(case_id)
        if status:
            case["status"] = status
        if owner is not None:
            case["owner"] = owner
        if customer_notification_required is not None:
            case["customer_notification_required"] = bool(
                customer_notification_required
            )
        if note:
            case.setdefault("notes", []).append(note)
        return {
            "case_id": case_id,
            "status": case.get("status"),
            "owner": case.get("owner"),
            "customer_notification_required": case.get(
                "customer_notification_required"
            ),
        }

    def _require_alert(self, alert_id: str) -> Dict[str, Any]:
        alert = self.alerts.get(alert_id)
        if not alert:
            raise MCPError("siem.alert_not_found", f"Unknown alert: {alert_id}")
        return alert

    def _require_case(self, case_id: str) -> Dict[str, Any]:
        case = self.cases.get(case_id)
        if not case:
            raise MCPError("siem.case_not_found", f"Unknown case: {case_id}")
        return case

    @staticmethod
    def _extract_seq(case_id: str) -> int:
        try:
            return int(case_id.split("-", 1)[1])
        except Exception:
            return 0


class SiemToolProvider(PrefixToolProvider):
    def __init__(self, sim: SiemSim):
        super().__init__("siem", prefixes=("siem.",))
        self.sim = sim
        self._specs: List[ToolSpec] = [
            ToolSpec(
                name="siem.list_alerts",
                description="List SIEM alerts by severity or status.",
                permissions=("siem:read",),
                default_latency_ms=260,
                latency_jitter_ms=80,
            ),
            ToolSpec(
                name="siem.get_alert",
                description="Fetch a SIEM alert by id.",
                permissions=("siem:read",),
                default_latency_ms=240,
                latency_jitter_ms=70,
            ),
            ToolSpec(
                name="siem.create_case",
                description="Create a SIEM investigation case from an alert.",
                permissions=("siem:write",),
                side_effects=("siem_mutation",),
                default_latency_ms=330,
                latency_jitter_ms=110,
            ),
            ToolSpec(
                name="siem.list_cases",
                description="List SIEM investigation cases.",
                permissions=("siem:read",),
                default_latency_ms=250,
                latency_jitter_ms=70,
            ),
            ToolSpec(
                name="siem.get_case",
                description="Fetch a SIEM investigation case by id.",
                permissions=("siem:read",),
                default_latency_ms=230,
                latency_jitter_ms=60,
            ),
            ToolSpec(
                name="siem.preserve_evidence",
                description="Preserve evidence for an alert and optionally attach it to a case.",
                permissions=("siem:write",),
                side_effects=("siem_mutation",),
                default_latency_ms=360,
                latency_jitter_ms=120,
            ),
            ToolSpec(
                name="siem.update_case",
                description="Update SIEM case status, owner, or notification decision.",
                permissions=("siem:write",),
                side_effects=("siem_mutation",),
                default_latency_ms=340,
                latency_jitter_ms=110,
            ),
        ]
        self._handlers: Dict[str, Callable[..., Any]] = {
            "siem.list_alerts": self.sim.list_alerts,
            "siem.get_alert": self.sim.get_alert,
            "siem.create_case": self.sim.create_case,
            "siem.list_cases": self.sim.list_cases,
            "siem.get_case": self.sim.get_case,
            "siem.preserve_evidence": self.sim.preserve_evidence,
            "siem.update_case": self.sim.update_case,
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
    sort_field = (
        sort_by if sort_by in rows[0] else next(iter(rows[0]), "") if rows else ""
    )
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
        return SiemSim._DEFAULT_LIMIT
    return max(1, min(int(limit), SiemSim._MAX_LIMIT))


def _encode_cursor(index: int) -> str:
    return f"idx:{index}"


def _decode_cursor(cursor: Optional[str]) -> int:
    if not cursor:
        return 0
    if not cursor.startswith("idx:"):
        raise MCPError("siem.invalid_cursor", f"Invalid cursor: {cursor}")
    try:
        return max(0, int(cursor.split(":", 1)[1]))
    except ValueError as exc:
        raise MCPError("siem.invalid_cursor", f"Invalid cursor: {cursor}") from exc


def _sortable(value: Any) -> Any:
    if value is None:
        return ""
    return str(value).lower()
