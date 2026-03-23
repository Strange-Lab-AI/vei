from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from vei.run.api import (
    get_run_capability_graphs,
    get_run_orientation,
    load_run_contract_evaluation,
    load_run_manifest,
    get_workspace_run_manifest_path,
)

from .models import AgentConfig


def build_agent_config(workspace_root: Path, run_id: str) -> AgentConfig:
    manifest = load_run_manifest(
        get_workspace_run_manifest_path(workspace_root, run_id)
    )
    orientation = get_run_orientation(workspace_root, run_id)
    graphs = get_run_capability_graphs(workspace_root, run_id)
    contract_eval = load_run_contract_evaluation(workspace_root, run_id)

    org_name = orientation.get("organization_name", manifest.workspace_name)
    scenario = manifest.scenario_name
    summary = orientation.get("summary", "")

    tools = _extract_tool_specs(graphs)
    guardrails = _extract_guardrails(contract_eval)
    success_criteria = _extract_success_criteria(contract_eval)
    context = _build_context_summary(orientation, graphs)

    system_prompt = (
        f"You are an operations agent for {org_name}.\n\n"
        f"Current situation: {summary}\n\n"
        f"Scenario: {scenario}\n\n"
    )
    if guardrails:
        system_prompt += "Guardrails (never violate these):\n"
        for g in guardrails:
            system_prompt += f"- {g}\n"
        system_prompt += "\n"
    if success_criteria:
        system_prompt += "Success criteria:\n"
        for c in success_criteria:
            system_prompt += f"- {c}\n"
        system_prompt += "\n"
    system_prompt += (
        "Use the available tools to assess the situation and take "
        "the actions needed to meet the success criteria while "
        "respecting all guardrails."
    )

    return AgentConfig(
        system_prompt=system_prompt,
        tools=tools,
        guardrails=guardrails,
        success_criteria=success_criteria,
        context_summary=context,
        metadata={
            "run_id": run_id,
            "organization_name": org_name,
            "scenario_name": scenario,
        },
    )


def _extract_tool_specs(
    graphs: Dict[str, Any],
) -> List[Dict[str, Any]]:
    tool_specs: list[dict[str, Any]] = []
    resolved_tools = graphs.get("resolved_tools", [])
    if isinstance(resolved_tools, list):
        for tool_name in resolved_tools:
            tool_specs.append(
                {
                    "name": str(tool_name),
                    "description": f"Enterprise tool: {tool_name}",
                }
            )
    return tool_specs


def _extract_guardrails(
    contract_eval: Dict[str, Any] | None,
) -> List[str]:
    if not contract_eval:
        return []
    guardrails: list[str] = []

    static = contract_eval.get("static_validation", {})
    if isinstance(static, dict):
        for issue in static.get("issues", []):
            if isinstance(issue, dict):
                guardrails.append(str(issue.get("message", "")))

    dynamic = contract_eval.get("dynamic_validation", {})
    if isinstance(dynamic, dict):
        for issue in dynamic.get("issues", []):
            if isinstance(issue, dict) and issue.get("severity") in (
                "error",
                "forbidden",
            ):
                guardrails.append(str(issue.get("message", "")))

    return guardrails


def _extract_success_criteria(
    contract_eval: Dict[str, Any] | None,
) -> List[str]:
    if not contract_eval:
        return []
    criteria: list[str] = []
    metadata = contract_eval.get("metadata", {})
    if isinstance(metadata, dict):
        for pred in metadata.get("success_predicates", []):
            if isinstance(pred, dict):
                name = pred.get("name", "")
                desc = pred.get("description", "")
                criteria.append(f"{name}: {desc}" if desc else str(name))
            elif isinstance(pred, str):
                criteria.append(pred)
    return criteria


def _build_context_summary(
    orientation: Dict[str, Any],
    graphs: Dict[str, Any],
) -> str:
    parts: list[str] = []
    summary = orientation.get("summary", "")
    if summary:
        parts.append(summary)

    domains = graphs.get("domains", [])
    if isinstance(domains, list) and domains:
        parts.append(f"Active domains: {', '.join(str(d) for d in domains)}")

    tools = graphs.get("resolved_tools", [])
    if isinstance(tools, list) and tools:
        parts.append(f"Available tools: {', '.join(str(t) for t in tools[:10])}")

    return " | ".join(parts)
