from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from vei.world.api import Scenario

from ._pagination import decode_cursor, encode_cursor, normalize_limit, sortable
from .errors import MCPError
from .tool_providers import PrefixToolProvider
from .tool_registry import ToolSpec


def _default_flags() -> Dict[str, Dict[str, Any]]:
    return {
        "checkout_v2": {
            "flag_key": "checkout_v2",
            "service": "checkout-api",
            "env": "prod",
            "enabled": True,
            "rollout_pct": 100,
            "history": [],
        },
        "checkout_kill_switch": {
            "flag_key": "checkout_kill_switch",
            "service": "checkout-api",
            "env": "prod",
            "enabled": False,
            "rollout_pct": 0,
            "history": [],
        },
    }


class FeatureFlagSim:
    """Deterministic feature flag / control-plane twin."""

    _DEFAULT_LIMIT = 25
    _MAX_LIMIT = 200

    def __init__(self, scenario: Optional[Scenario] = None):
        seed = (scenario.feature_flags or {}) if scenario else {}
        flags = seed["flags"] if "flags" in seed else _default_flags()
        self.flags: Dict[str, Dict[str, Any]] = {
            flag_key: dict(payload) for flag_key, payload in flags.items()
        }

    def list_flags(
        self,
        service: Optional[str] = None,
        env: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_by: str = "flag_key",
        sort_dir: str = "asc",
    ) -> Dict[str, Any]:
        rows = []
        for flag in self.flags.values():
            if service and flag.get("service") != service:
                continue
            if env and flag.get("env") != env:
                continue
            rows.append(
                {
                    "id": flag["flag_key"],
                    "service": flag.get("service"),
                    "env": flag.get("env"),
                    "enabled": bool(flag.get("enabled", False)),
                    "rollout_pct": int(flag.get("rollout_pct", 0)),
                }
            )
        return _page(
            rows,
            limit=limit,
            cursor=cursor,
            key="flags",
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    def get_flag(self, flag_key: str) -> Dict[str, Any]:
        flag = self.flags.get(flag_key)
        if not flag:
            raise MCPError("feature_flags.flag_not_found", f"Unknown flag: {flag_key}")
        return dict(flag)

    def set_flag(
        self,
        flag_key: str,
        enabled: bool,
        env: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        flag = self._require_flag(flag_key)
        if env and flag.get("env") != env:
            raise MCPError(
                "feature_flags.env_mismatch",
                f"Flag {flag_key} does not belong to env {env}",
            )
        flag["enabled"] = bool(enabled)
        flag.setdefault("history", []).append(
            {"action": "set_flag", "enabled": bool(enabled), "reason": reason or ""}
        )
        return {
            "flag_key": flag_key,
            "enabled": flag["enabled"],
            "rollout_pct": flag.get("rollout_pct"),
        }

    def update_rollout(
        self,
        flag_key: str,
        rollout_pct: int,
        env: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        flag = self._require_flag(flag_key)
        if env and flag.get("env") != env:
            raise MCPError(
                "feature_flags.env_mismatch",
                f"Flag {flag_key} does not belong to env {env}",
            )
        pct = max(0, min(int(rollout_pct), 100))
        flag["rollout_pct"] = pct
        flag.setdefault("history", []).append(
            {"action": "update_rollout", "rollout_pct": pct, "reason": reason or ""}
        )
        return {
            "flag_key": flag_key,
            "enabled": bool(flag.get("enabled", False)),
            "rollout_pct": pct,
        }

    def _require_flag(self, flag_key: str) -> Dict[str, Any]:
        flag = self.flags.get(flag_key)
        if not flag:
            raise MCPError("feature_flags.flag_not_found", f"Unknown flag: {flag_key}")
        return flag


class FeatureFlagToolProvider(PrefixToolProvider):
    def __init__(self, sim: FeatureFlagSim):
        super().__init__("feature_flags", prefixes=("feature_flags.",))
        self.sim = sim
        self._specs: List[ToolSpec] = [
            ToolSpec(
                name="feature_flags.list_flags",
                description="List feature flags and rollout percentages.",
                permissions=("feature_flags:read",),
                default_latency_ms=220,
                latency_jitter_ms=60,
            ),
            ToolSpec(
                name="feature_flags.get_flag",
                description="Fetch a feature flag by key.",
                permissions=("feature_flags:read",),
                default_latency_ms=210,
                latency_jitter_ms=60,
            ),
            ToolSpec(
                name="feature_flags.set_flag",
                description="Enable or disable a feature flag.",
                permissions=("feature_flags:write",),
                side_effects=("feature_flags_mutation",),
                default_latency_ms=300,
                latency_jitter_ms=90,
            ),
            ToolSpec(
                name="feature_flags.update_rollout",
                description="Update the rollout percentage for a feature flag.",
                permissions=("feature_flags:write",),
                side_effects=("feature_flags_mutation",),
                default_latency_ms=300,
                latency_jitter_ms=90,
            ),
        ]
        self._handlers: Dict[str, Callable[..., Any]] = {
            "feature_flags.list_flags": self.sim.list_flags,
            "feature_flags.get_flag": self.sim.get_flag,
            "feature_flags.set_flag": self.sim.set_flag,
            "feature_flags.update_rollout": self.sim.update_rollout,
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
    start = decode_cursor(
        cursor, prefix="idx", error_code="feature_flags.invalid_cursor"
    )
    page_limit = normalize_limit(
        limit,
        default=FeatureFlagSim._DEFAULT_LIMIT,
        max_limit=FeatureFlagSim._MAX_LIMIT,
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
