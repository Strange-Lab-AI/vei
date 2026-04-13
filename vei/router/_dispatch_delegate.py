from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

from vei.connectors import ConnectorInvocationError

from ._dispatch import GUARDED_PREFIXES, build_dispatch_table
from .errors import MCPError

if TYPE_CHECKING:
    from .core import Router


class RouterDispatch:
    GUARDED_PREFIXES = GUARDED_PREFIXES

    @staticmethod
    def build_dispatch_table(router: Router) -> Dict[str, Any]:
        return build_dispatch_table(router)

    @staticmethod
    def deliver_plugin_event(
        router: Router,
        target: str,
        payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        for entry in router.facade_plugins.values():
            plugin = entry.plugin
            if target not in plugin.event_targets:
                continue
            component = entry.component
            if plugin.event_handler is not None:
                return plugin.event_handler(router, component, payload)
            tool = payload.get("tool")
            args = payload.get("args", {})
            if not isinstance(tool, str):
                raise MCPError(
                    "invalid_event",
                    f"{target} event payload must include string 'tool'",
                )
            if not isinstance(args, dict):
                raise MCPError(
                    "invalid_event",
                    f"{target} event payload args must be an object",
                )
            result = RouterDispatch.execute(router, tool, args)
            return {"tool": tool, "result": router._jsonable(result)}
        return None

    @staticmethod
    def deliver_event(
        router: Router, target: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        if target == "slack":
            return router.slack.deliver(payload)
        if target == "mail":
            return router.mail.deliver(payload)
        if target == "docs":
            return router.docs.deliver(payload)
        if target == "calendar":
            return router.calendar.deliver(payload)
        if target == "tickets":
            return router.tickets.deliver(payload)
        if target in {"db", "database"}:
            return router.database.deliver(payload)
        if target in {
            "erp",
            "crm",
            "servicedesk",
            "okta",
            "google_admin",
            "siem",
            "datadog",
            "pagerduty",
            "feature_flags",
            "hris",
            "jira",
            "tool",
        }:
            tool = payload.get("tool")
            args = payload.get("args", {})
            if not isinstance(tool, str):
                raise MCPError(
                    "invalid_event",
                    f"{target} event payload must include string 'tool'",
                )
            if not isinstance(args, dict):
                raise MCPError(
                    "invalid_event",
                    f"{target} event payload args must be an object",
                )
            result = RouterDispatch.execute(router, tool, args)
            return {"tool": tool, "result": router._jsonable(result)}
        plugin_delivery = RouterDispatch.deliver_plugin_event(router, target, payload)
        if plugin_delivery is not None:
            return plugin_delivery
        return {"ignored": True, "reason": f"unsupported target '{target}'"}

    @staticmethod
    def execute(router: Router, tool: str, args: Dict[str, Any]) -> Any:
        if tool == "vei.observe":
            focus = args.get("focus") if isinstance(args, dict) else None
            return router.observe(focus_hint=focus).model_dump()
        if tool == "vei.tick":
            return router.tick(**args)
        if tool == "vei.state":
            return router.state_snapshot(**args)
        if tool == "vei.act_and_observe":
            target_tool = args.get("tool")
            target_args = args.get("args", {})
            if not target_tool:
                raise MCPError("invalid_args", "act_and_observe requires tool")
            return router.act_and_observe(target_tool, target_args)
        if tool == "vei.inject":
            return router.inject(**args)

        if not tool.startswith("vei."):
            router._maybe_fault(tool)
        tool = router.alias_map.get(tool, tool)
        intercepted = router._maybe_fidelity_intercept(tool, args)
        if intercepted is not None:
            return intercepted
        if router.connector_runtime.managed_tool(tool):
            try:
                return router.connector_runtime.invoke_tool(
                    tool,
                    args,
                    time_ms=router.bus.clock_ms,
                    metadata={"router_branch": router.state_store.branch},
                )
            except ConnectorInvocationError as exc:
                raise MCPError(exc.code, exc.message) from exc

        handler = router._dispatch.get(tool)
        if handler is not None:
            return handler(args)

        for prefix, label in RouterDispatch.GUARDED_PREFIXES.items():
            if tool.startswith(prefix):
                if not getattr(router, prefix.rstrip("."), None):
                    raise MCPError("unsupported_tool", f"{label} twin not available")
                raise MCPError("unknown_tool", f"No such tool: {tool}")

        for provider in router.tool_providers:
            if provider.handles(tool):
                return provider.call(tool, args)

        raise MCPError("unknown_tool", f"No such tool: {tool}")
