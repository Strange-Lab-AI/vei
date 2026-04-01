from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from vei.world.api import Scenario

from .errors import MCPError
from .tool_providers import PrefixToolProvider
from .tool_registry import ToolSpec


def _default_oauth_apps() -> Dict[str, Dict[str, Any]]:
    return {
        "OAUTH-4201": {
            "app_id": "OAUTH-4201",
            "name": "Calendar Sync Helper",
            "publisher": "MacroCloud Labs",
            "status": "ACTIVE",
            "risk_level": "high",
            "verified": False,
            "scopes": [
                "gmail.readonly",
                "drive.readonly",
                "admin.directory.user.readonly",
            ],
            "affected_users": [
                "finance.ops@example.com",
                "revops@example.com",
            ],
            "evidence_hold": False,
            "history": [],
        }
    }


def _default_drive_shares() -> Dict[str, Dict[str, Any]]:
    return {
        "GDRIVE-1001": {
            "doc_id": "GDRIVE-1001",
            "title": "Q2 Territory Plan",
            "owner": "sales.ops@example.com",
            "visibility": "external_link",
            "classification": "internal",
            "shared_with": ["integration.partner@example.net"],
            "history": [],
        }
    }


class GoogleAdminSim:
    """Deterministic Google Admin / Workspace security control-plane twin."""

    _DEFAULT_LIMIT = 25
    _MAX_LIMIT = 200

    def __init__(self, scenario: Optional[Scenario] = None):
        seed = (scenario.google_admin or {}) if scenario else {}
        oauth_apps = (
            seed["oauth_apps"] if "oauth_apps" in seed else _default_oauth_apps()
        )
        drive_shares = (
            seed["drive_shares"] if "drive_shares" in seed else _default_drive_shares()
        )
        self.oauth_apps: Dict[str, Dict[str, Any]] = {
            app_id: dict(payload) for app_id, payload in oauth_apps.items()
        }
        self.drive_shares: Dict[str, Dict[str, Any]] = {
            doc_id: dict(payload) for doc_id, payload in drive_shares.items()
        }

    def list_oauth_apps(
        self,
        status: Optional[str] = None,
        risk_level: Optional[str] = None,
        query: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_by: str = "name",
        sort_dir: str = "asc",
    ) -> Dict[str, Any]:
        rows: List[Dict[str, Any]] = []
        needle = (query or "").strip().lower()
        for app in self.oauth_apps.values():
            if status and str(app.get("status", "")).upper() != status.upper():
                continue
            if (
                risk_level
                and str(app.get("risk_level", "")).lower() != risk_level.lower()
            ):
                continue
            haystack = " ".join(
                [
                    str(app.get("name", "")),
                    str(app.get("publisher", "")),
                    " ".join(str(item) for item in app.get("scopes", [])),
                ]
            ).lower()
            if needle and needle not in haystack:
                continue
            rows.append(
                {
                    "id": app["app_id"],
                    "name": app["name"],
                    "publisher": app.get("publisher"),
                    "status": app.get("status"),
                    "risk_level": app.get("risk_level"),
                    "verified": bool(app.get("verified", False)),
                    "affected_users": len(app.get("affected_users", [])),
                }
            )
        sort_field = (
            sort_by
            if sort_by in {"name", "status", "risk_level", "publisher"}
            else "name"
        )
        rows.sort(
            key=lambda row: _sortable(row.get(sort_field)),
            reverse=sort_dir.lower() != "asc",
        )
        return _page(rows, limit=limit, cursor=cursor, key="apps")

    def get_oauth_app(self, app_id: str) -> Dict[str, Any]:
        app = self.oauth_apps.get(app_id)
        if not app:
            raise MCPError("google_admin.app_not_found", f"Unknown OAuth app: {app_id}")
        return dict(app)

    def suspend_oauth_app(
        self, app_id: str, reason: Optional[str] = None
    ) -> Dict[str, Any]:
        app = self._require_app(app_id)
        if app.get("status") == "SUSPENDED":
            return {"app_id": app_id, "status": "SUSPENDED", "changed": False}
        app["status"] = "SUSPENDED"
        app.setdefault("history", []).append(
            {"action": "suspend", "reason": reason or "manual"}
        )
        return {
            "app_id": app_id,
            "status": app["status"],
            "changed": True,
            "reason": reason or "manual",
        }

    def preserve_oauth_evidence(
        self, app_id: str, note: Optional[str] = None
    ) -> Dict[str, Any]:
        app = self._require_app(app_id)
        app["evidence_hold"] = True
        app.setdefault("history", []).append(
            {"action": "preserve_evidence", "note": note or ""}
        )
        return {
            "app_id": app_id,
            "evidence_hold": True,
            "history_count": len(app.get("history", [])),
        }

    def list_drive_shares(
        self,
        visibility: Optional[str] = None,
        owner: Optional[str] = None,
        query: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_by: str = "title",
        sort_dir: str = "asc",
    ) -> Dict[str, Any]:
        rows: List[Dict[str, Any]] = []
        needle = (query or "").strip().lower()
        for share in self.drive_shares.values():
            if visibility and share.get("visibility") != visibility:
                continue
            if owner and share.get("owner") != owner:
                continue
            haystack = " ".join(
                [
                    str(share.get("title", "")),
                    str(share.get("owner", "")),
                    str(share.get("classification", "")),
                    " ".join(str(item) for item in share.get("shared_with", [])),
                ]
            ).lower()
            if needle and needle not in haystack:
                continue
            rows.append(
                {
                    "id": share["doc_id"],
                    "title": share["title"],
                    "owner": share.get("owner"),
                    "visibility": share.get("visibility"),
                    "classification": share.get("classification"),
                    "shared_with_count": len(share.get("shared_with", [])),
                }
            )
        sort_field = sort_by if sort_by in {"title", "owner", "visibility"} else "title"
        rows.sort(
            key=lambda row: _sortable(row.get(sort_field)),
            reverse=sort_dir.lower() != "asc",
        )
        return _page(rows, limit=limit, cursor=cursor, key="shares")

    def restrict_drive_share(
        self,
        doc_id: str,
        visibility: str = "internal",
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        share = self._require_share(doc_id)
        share["visibility"] = visibility
        if visibility != "external_link":
            share["shared_with"] = [
                principal
                for principal in share.get("shared_with", [])
                if principal.endswith("@example.com")
            ]
        share.setdefault("history", []).append(
            {"action": "restrict_share", "visibility": visibility, "note": note or ""}
        )
        return {
            "doc_id": doc_id,
            "visibility": visibility,
            "shared_with_count": len(share.get("shared_with", [])),
        }

    def transfer_drive_ownership(
        self, doc_id: str, owner: str, note: Optional[str] = None
    ) -> Dict[str, Any]:
        share = self._require_share(doc_id)
        share["owner"] = owner
        share.setdefault("history", []).append(
            {"action": "transfer_ownership", "owner": owner, "note": note or ""}
        )
        return {"doc_id": doc_id, "owner": owner}

    def _require_app(self, app_id: str) -> Dict[str, Any]:
        app = self.oauth_apps.get(app_id)
        if not app:
            raise MCPError("google_admin.app_not_found", f"Unknown OAuth app: {app_id}")
        return app

    def _require_share(self, doc_id: str) -> Dict[str, Any]:
        share = self.drive_shares.get(doc_id)
        if not share:
            raise MCPError("google_admin.doc_not_found", f"Unknown drive doc: {doc_id}")
        return share


class GoogleAdminToolProvider(PrefixToolProvider):
    def __init__(self, sim: GoogleAdminSim):
        super().__init__("google_admin", prefixes=("google_admin.",))
        self.sim = sim
        self._specs: List[ToolSpec] = [
            ToolSpec(
                name="google_admin.list_oauth_apps",
                description="List Google Workspace OAuth apps and their risk posture.",
                permissions=("google_admin:read",),
                default_latency_ms=320,
                latency_jitter_ms=90,
            ),
            ToolSpec(
                name="google_admin.get_oauth_app",
                description="Fetch a Google Workspace OAuth app by id.",
                permissions=("google_admin:read",),
                default_latency_ms=280,
                latency_jitter_ms=80,
            ),
            ToolSpec(
                name="google_admin.suspend_oauth_app",
                description="Suspend a suspicious OAuth app without deleting evidence.",
                permissions=("google_admin:write",),
                side_effects=("google_admin_mutation",),
                default_latency_ms=420,
                latency_jitter_ms=140,
            ),
            ToolSpec(
                name="google_admin.preserve_oauth_evidence",
                description="Place a Google OAuth app under evidence hold.",
                permissions=("google_admin:write",),
                side_effects=("google_admin_mutation",),
                default_latency_ms=350,
                latency_jitter_ms=110,
            ),
            ToolSpec(
                name="google_admin.list_drive_shares",
                description="List Google Drive documents and their sharing posture.",
                permissions=("google_admin:read",),
                default_latency_ms=310,
                latency_jitter_ms=100,
            ),
            ToolSpec(
                name="google_admin.restrict_drive_share",
                description="Restrict a Drive document sharing policy.",
                permissions=("google_admin:write",),
                side_effects=("google_admin_mutation",),
                default_latency_ms=430,
                latency_jitter_ms=150,
            ),
            ToolSpec(
                name="google_admin.transfer_drive_ownership",
                description="Transfer Drive document ownership to a new user.",
                permissions=("google_admin:write",),
                side_effects=("google_admin_mutation",),
                default_latency_ms=440,
                latency_jitter_ms=150,
            ),
        ]
        self._handlers: Dict[str, Callable[..., Any]] = {
            "google_admin.list_oauth_apps": self.sim.list_oauth_apps,
            "google_admin.get_oauth_app": self.sim.get_oauth_app,
            "google_admin.suspend_oauth_app": self.sim.suspend_oauth_app,
            "google_admin.preserve_oauth_evidence": self.sim.preserve_oauth_evidence,
            "google_admin.list_drive_shares": self.sim.list_drive_shares,
            "google_admin.restrict_drive_share": self.sim.restrict_drive_share,
            "google_admin.transfer_drive_ownership": self.sim.transfer_drive_ownership,
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
) -> Dict[str, Any]:
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
        return GoogleAdminSim._DEFAULT_LIMIT
    return max(1, min(int(limit), GoogleAdminSim._MAX_LIMIT))


def _encode_cursor(index: int) -> str:
    return f"idx:{index}"


def _decode_cursor(cursor: Optional[str]) -> int:
    if not cursor:
        return 0
    if not cursor.startswith("idx:"):
        raise MCPError("google_admin.invalid_cursor", f"Invalid cursor: {cursor}")
    try:
        return max(0, int(cursor.split(":", 1)[1]))
    except ValueError as exc:
        raise MCPError(
            "google_admin.invalid_cursor", f"Invalid cursor: {cursor}"
        ) from exc


def _sortable(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return int(value)
    return str(value).lower()
