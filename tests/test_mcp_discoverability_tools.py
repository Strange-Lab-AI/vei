from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


def _normalize_result(res: Any) -> dict[str, Any]:
    structured = getattr(res, "structuredContent", None)
    if structured is not None:
        return structured
    content = getattr(res, "content", None)
    if content and isinstance(content, list) and getattr(content[0], "text", None):
        import json

        return json.loads(content[0].text)
    raise AssertionError(f"Unexpected MCP result shape: {res!r}")


def test_mcp_tools_expose_orientation_and_capability_graphs() -> None:
    asyncio.run(_exercise_mcp_discovery_tools())


async def _exercise_mcp_discovery_tools() -> None:
    params = StdioServerParameters(
        command=sys.executable or "python3",
        args=["-m", "vei.router"],
        env={
            **os.environ,
            "VEI_DISABLE_AUTOSTART": "1",
            "VEI_SCENARIO": "checkout_spike_mitigation",
            "FASTMCP_LOG_LEVEL": "ERROR",
        },
    )

    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools_info = await session.list_tools()
            tool_names = {tool.name for tool in getattr(tools_info, "tools", []) or []}
            assert "vei.orientation" in tool_names
            assert "vei.capability_graphs" in tool_names
            assert "vei.graph_plan" in tool_names
            assert "vei.graph_action" in tool_names

            orientation = _normalize_result(
                await session.call_tool("vei.orientation", {})
            )
            assert orientation["scenario_name"] == "checkout_spike_mitigation"
            assert orientation["available_surfaces"]
            assert orientation["suggested_focuses"]
            assert orientation["next_questions"]

            graphs = _normalize_result(
                await session.call_tool(
                    "vei.capability_graphs", {"domain": "identity_graph"}
                )
            )
            assert graphs["domain"] == "identity_graph"
            assert graphs["graph"] is not None

            graph_plan = _normalize_result(
                await session.call_tool("vei.graph_plan", {"domain": "ops_graph"})
            )
            assert graph_plan["scenario_name"] == "checkout_spike_mitigation"
            assert any(
                step["action"] == "update_rollout"
                for step in graph_plan["suggested_steps"]
            )

            graph_action = _normalize_result(
                await session.call_tool(
                    "vei.graph_action",
                    {
                        "domain": "ops_graph",
                        "action": "update_rollout",
                        "args": {
                            "flag_key": "checkout_v2",
                            "rollout_pct": 0,
                            "env": "prod",
                            "reason": "MCP graph-action regression",
                        },
                    },
                )
            )
            assert graph_action["tool"] == "feature_flags.update_rollout"
            assert any(
                flag["flag_key"] == "checkout_v2" and flag["rollout_pct"] == 0
                for flag in graph_action["graph"]["flags"]
            )

            help_payload = _normalize_result(await session.call_tool("vei.help", {}))
            example_tools = {item["tool"] for item in help_payload["examples"]}
            assert "vei.orientation" in example_tools
            assert "vei.capability_graphs" in example_tools
            assert "vei.graph_plan" in example_tools
            assert "vei.graph_action" in example_tools
