from __future__ import annotations

import os
from typing import Any, Callable

from mcp.server.fastmcp import server as fserver
from pydantic import Field

from vei.blueprint import list_runtime_facade_plugins
from vei.router.api import RouterServerAPI
from vei.world.api import WorldSessionAPI

from .alias_packs import CRM_ALIAS_PACKS, ERP_ALIAS_PACKS
from .tool_registry import ToolSpec

RouterGetter = Callable[[], RouterServerAPI]
SessionGetter = Callable[[], WorldSessionAPI]
SafeCall = Callable[[str, dict[str, Any]], dict[str, Any]]


def register_alias_and_provider_tools(
    srv: fserver.FastMCP,
    *,
    get_router: RouterGetter,
    safe_call: SafeCall,
) -> None:
    packs_env = os.environ.get("VEI_ALIAS_PACKS", "xero").strip()
    packs = [pack.strip() for pack in packs_env.split(",") if pack.strip()]

    def _register_alias(alias_name: str, base_tool: str) -> None:
        @srv.tool(name=alias_name, description=f"Alias → {base_tool}")
        def _alias_passthrough(**kwargs: Any) -> dict[str, Any]:  # type: ignore[no-redef]
            return safe_call(base_tool, dict(kwargs))

        base_spec = get_router().registry.get(base_tool)
        if base_spec:
            alias_spec = ToolSpec(
                name=alias_name,
                description=f"Alias → {base_tool}. {base_spec.description}",
                side_effects=base_spec.side_effects,
                permissions=base_spec.permissions,
                default_latency_ms=base_spec.default_latency_ms,
                latency_jitter_ms=base_spec.latency_jitter_ms,
                nominal_cost=base_spec.nominal_cost,
                returns=base_spec.returns,
                fault_probability=base_spec.fault_probability,
            )
        else:
            alias_spec = ToolSpec(name=alias_name, description=f"Alias → {base_tool}")
        try:
            get_router().registry.register(alias_spec)
        except ValueError:
            pass

    for pack in packs:
        for alias, base in ERP_ALIAS_PACKS.get(pack, []):
            _register_alias(alias, base)

    crm_packs_env = os.environ.get("VEI_CRM_ALIAS_PACKS", "hubspot,salesforce").strip()
    crm_packs = [pack.strip() for pack in crm_packs_env.split(",") if pack.strip()]
    for pack in crm_packs:
        for alias, base in CRM_ALIAS_PACKS.get(pack, []):
            _register_alias(alias, base)

    def _register_passthrough(tool_name: str, description: str) -> None:
        @srv.tool(name=tool_name, description=description)
        def _provider_passthrough(**kwargs: Any) -> dict[str, Any]:  # type: ignore[no-redef]
            return safe_call(tool_name, dict(kwargs))

    dynamic_prefixes = (
        "google_admin.",
        "siem.",
        "datadog.",
        "pagerduty.",
        "feature_flags.",
        "hris.",
        "jira.",
    )
    plugin_prefixes: list[str] = list(dynamic_prefixes)
    for plugin in list_runtime_facade_plugins():
        if plugin.provider_factory is None:
            continue
        for prefix in plugin.tool_prefixes:
            if prefix not in plugin_prefixes:
                plugin_prefixes.append(prefix)

    for spec in sorted(get_router().registry.list(), key=lambda item: item.name):
        if spec.name.startswith(tuple(plugin_prefixes)):
            _register_passthrough(spec.name, spec.description)


def register_vei_tools(
    srv: fserver.FastMCP,
    *,
    get_router: RouterGetter,
    get_session: SessionGetter,
    safe_call: SafeCall,
) -> None:
    @srv.tool(
        name="vei.observe", description="Get current observation summary + action menu"
    )
    def vei_observe(focus: str = None) -> dict[str, Any]:
        return get_router().observe(focus_hint=focus).model_dump()

    @srv.tool(
        name="vei.orientation",
        description="Get an agent-facing summary of visible surfaces, policy hints, key objects, and next questions",
    )
    def vei_orientation() -> dict[str, Any]:
        return get_session().orientation().model_dump(mode="json")

    @srv.tool(
        name="vei.capability_graphs",
        description="Inspect runtime capability graphs for shared identity, doc, work, comm, and revenue state",
    )
    def vei_capability_graphs(domain: str = None) -> dict[str, Any]:
        graphs = get_session().capability_graphs().model_dump(mode="json")
        if domain is None:
            return graphs
        normalized = domain.strip().lower()
        if normalized not in graphs.get("available_domains", []):
            return {
                "error": {
                    "code": "unknown_domain",
                    "message": f"Unknown capability graph domain: {domain}",
                }
            }
        return {
            "branch": graphs["branch"],
            "clock_ms": graphs["clock_ms"],
            "domain": normalized,
            "graph": graphs.get(normalized),
        }

    @srv.tool(
        name="vei.graph_plan",
        description="Get suggested next graph-native mutations across identity, docs, work, revenue, spreadsheet, observability, and rollout state",
    )
    def vei_graph_plan(domain: str = None, limit: int = 12) -> dict[str, Any]:
        return (
            get_session().graph_plan(domain=domain, limit=limit).model_dump(mode="json")
        )

    @srv.tool(
        name="vei.graph_action",
        description="Apply a graph-native mutation step, either by explicit domain/action or by a suggested step_id from vei.graph_plan",
    )
    def vei_graph_action(
        domain: str = None,
        action: str = None,
        args: dict[str, Any] = Field(default_factory=dict),
        step_id: str = None,
    ) -> dict[str, Any]:
        try:
            return (
                get_session()
                .graph_action(
                    {
                        "domain": domain,
                        "action": action,
                        "args": dict(args or {}),
                        "step_id": step_id,
                    }
                )
                .model_dump(mode="json")
            )
        except (KeyError, ValueError, TypeError) as exc:
            return {
                "error": {
                    "code": "invalid_graph_action",
                    "message": str(exc),
                }
            }

    @srv.tool(name="vei.ping", description="Health check and current logical time")
    def vei_ping() -> dict[str, Any]:
        return {"ok": True, "time_ms": get_router().bus.clock_ms}

    @srv.tool(
        name="vei.act_and_observe",
        description="Execute a tool and return its result and a post-action observation",
    )
    def vei_act_and_observe(
        tool: str, args: dict[str, Any] = Field(default_factory=dict)
    ) -> dict[str, Any]:
        return get_router().act_and_observe(tool, args)

    @srv.tool(
        name="vei.call", description="Call any tool name with args via the VEI router"
    )
    def vei_call(
        tool: str, args: dict[str, Any] = Field(default_factory=dict)
    ) -> dict[str, Any]:
        return safe_call(tool, args)

    @srv.tool(
        name="vei.tools.search",
        description="Search the tool catalog for relevant entries",
    )
    def vei_tools_search(query: str, top_k: int = 10) -> dict[str, Any]:
        limit = top_k if isinstance(top_k, int) else 10
        if limit < 0:
            limit = 0
        return get_router().search_tools(query, top_k=limit)

    @srv.tool(
        name="vei.tick",
        description="Advance logical time by dt_ms and deliver due events",
    )
    def vei_tick(dt_ms: int = 1000) -> dict[str, Any]:
        return get_router().tick(dt_ms)

    @srv.tool(
        name="vei.pending",
        description="Return pending event counts without advancing time",
    )
    def vei_pending() -> dict[str, int]:
        return get_router().pending()

    @srv.tool(
        name="vei.state",
        description="Inspect state head, receipts, and recent tool calls",
    )
    def vei_state(
        include_state: bool = False, tool_tail: int = 20, include_receipts: bool = True
    ) -> dict[str, Any]:
        return get_router().state_snapshot(
            include_state=include_state,
            tool_tail=tool_tail,
            include_receipts=include_receipts,
        )

    @srv.tool(
        name="vei.help",
        description="Usage help: how to interact via MCP and example actions",
    )
    def vei_help() -> dict[str, Any]:
        payload = get_router().help_payload()
        payload["examples"].extend(
            [
                {"tool": "vei.orientation", "args": {}},
                {"tool": "vei.capability_graphs", "args": {"domain": "identity_graph"}},
                {"tool": "vei.graph_plan", "args": {"domain": "identity_graph"}},
                {
                    "tool": "vei.graph_action",
                    "args": {
                        "domain": "identity_graph",
                        "action": "assign_application",
                        "args": {"user_id": "USR-ACQ-1", "app_id": "APP-crm"},
                    },
                },
            ]
        )
        return payload
