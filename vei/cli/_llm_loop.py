from __future__ import annotations

import json
from typing import Any, List, Dict

from mcp.client.session import ClientSession
from openai import AsyncOpenAI

SYSTEM_PROMPT_BASE = (
    "You are an assistant controlling tools via MCP in a synthetic enterprise world. "
    "Each step: first call 'vei.observe' to see the action_menu, then pick exactly one tool to call. "
)


def extract_plan(raw: str, default_tool: str = "vei.observe") -> Dict[str, Any]:
    try:
        if "```json" in raw:
            return json.loads(raw.strip().split("```json")[-1].split("```")[-2])
        return json.loads(raw)
    except Exception:
        return {"tool": default_tool, "args": {}}


async def call_mcp_tool(session: ClientSession, tool: str, args: dict) -> dict:
    return await session.call_tool(tool, args)


async def observe_plan_act(
    session: ClientSession,
    client: AsyncOpenAI,
    messages: List[Dict[str, Any]],
    system_prompt: str,
) -> Dict[str, Any]:
    obs = await call_mcp_tool(session, "vei.observe", {})
    menu = obs.get("action_menu", [])
    menu_text = "\n".join(
        f"- {m.get('tool')} {json.dumps(m.get('args', m.get('args_schema', {})))}"
        for m in menu
    )
    user = (
        f"Time: {obs.get('time_ms')}\n"
        f"Focus: {obs.get('focus')}\n"
        f"Summary: {obs.get('summary')}\n"
        f"Pending: {json.dumps(obs.get('pending_events'))}\n"
        f"Action menu (choose ONE):\n{menu_text}\n"
        'Reply strictly as JSON {"tool": str, "args": object}.'
    )
    if not messages:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": user})

    chat = await client.chat.completions.create(
        model="gpt-5", messages=messages, temperature=0
    )
    raw = chat.choices[0].message.content or "{}"
    plan = extract_plan(raw)
    tool = plan.get("tool", "vei.observe")
    args = plan.get("args", {})
    res = await call_mcp_tool(session, tool, args)
    messages.append({"role": "assistant", "content": json.dumps(plan)})
    return {"observation": obs, "action": {"tool": tool, "args": args, "result": res}}
