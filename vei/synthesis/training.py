from __future__ import annotations

from pathlib import Path
from typing import Any, List

from vei.run.api import (
    get_workspace_run_dir,
    get_workspace_run_manifest_path,
    get_run_knowledge_state,
    load_run_contract_evaluation,
    load_run_manifest,
)
from vei.run import load_run_events
from vei.run.models import RunTimelineEvent
from vei.knowledge.api import latest_composed_asset_payload

from .models import TrainingExample, TrainingFormat, TrainingSet


def build_training_set(
    workspace_root: Path,
    run_ids: List[str],
    *,
    format: TrainingFormat = "conversations",
) -> TrainingSet:
    all_examples: list[TrainingExample] = []
    scenario_name = ""

    for run_id in run_ids:
        manifest = load_run_manifest(
            get_workspace_run_manifest_path(workspace_root, run_id)
        )
        if not scenario_name:
            scenario_name = manifest.scenario_name

        run_dir = get_workspace_run_dir(workspace_root, run_id)
        events_path = run_dir / "events.jsonl"
        events: list[RunTimelineEvent] = []
        if events_path.exists():
            events = load_run_events(events_path)

        if format == "conversations":
            all_examples.extend(_to_conversations(run_id, events, manifest))
        elif format == "trajectories":
            all_examples.extend(_to_trajectories(run_id, events))
        elif format == "demonstrations":
            all_examples.extend(_to_demonstrations(run_id, events))
        elif format == "authoring":
            knowledge_state = get_run_knowledge_state(workspace_root, run_id)
            contract = load_run_contract_evaluation(workspace_root, run_id) or {}
            all_examples.extend(
                _to_authoring_examples(
                    run_id,
                    events,
                    manifest,
                    knowledge_state=knowledge_state,
                    contract=contract,
                )
            )

    return TrainingSet(
        format=format,
        scenario_name=scenario_name,
        example_count=len(all_examples),
        examples=all_examples,
        metadata={
            "run_ids": run_ids,
            "format": format,
        },
    )


def _to_conversations(
    run_id: str,
    events: list[RunTimelineEvent],
    manifest: Any,
) -> list[TrainingExample]:
    tool_events = [e for e in events if e.kind in ("trace_call", "workflow_step")]
    if not tool_events:
        return []

    system_msg = (
        f"You are an operations agent for {manifest.workspace_name}. "
        f"Scenario: {manifest.scenario_name}. "
        f"Use the available tools to complete the mission."
    )

    messages: list[dict[str, str]] = [{"role": "system", "content": system_msg}]
    for event in tool_events:
        tool = event.tool or event.resolved_tool or "unknown"
        args = {
            k: v
            for k, v in (event.payload or {}).items()
            if k not in ("result", "observation", "emitted")
        }
        observation = (event.payload or {}).get(
            "observation", (event.payload or {}).get("result", "")
        )
        messages.append(
            {
                "role": "assistant",
                "content": f"[tool_call] {tool}({_compact_args(args)})",
            }
        )
        if observation:
            messages.append(
                {
                    "role": "tool",
                    "content": str(observation)[:500],
                }
            )

    return [
        TrainingExample(
            format="conversations",
            run_id=run_id,
            sequence_index=0,
            data={"messages": messages},
        )
    ]


def _to_trajectories(
    run_id: str,
    events: list[RunTimelineEvent],
) -> list[TrainingExample]:
    examples: list[TrainingExample] = []
    snapshot_events = [e for e in events if e.kind == "snapshot"]
    tool_events = [e for e in events if e.kind in ("trace_call", "workflow_step")]

    tool_idx = 0
    for i, snap in enumerate(snapshot_events):
        action = ""
        reward = 0.0
        if tool_idx < len(tool_events):
            te = tool_events[tool_idx]
            action = te.tool or te.resolved_tool or ""
            reward = 1.0 if te.status in ("ok", "success", None) else -0.5
            tool_idx += 1

        examples.append(
            TrainingExample(
                format="trajectories",
                run_id=run_id,
                sequence_index=i,
                data={
                    "state_id": f"{run_id}:snap:{snap.snapshot_id or i}",
                    "action": action,
                    "reward": reward,
                    "next_state_id": (
                        f"{run_id}:snap:{snapshot_events[i + 1].snapshot_id or i + 1}"
                        if i + 1 < len(snapshot_events)
                        else f"{run_id}:terminal"
                    ),
                    "time_ms": snap.time_ms,
                },
            )
        )
    return examples


def _to_demonstrations(
    run_id: str,
    events: list[RunTimelineEvent],
) -> list[TrainingExample]:
    tool_events = [e for e in events if e.kind in ("trace_call", "workflow_step")]
    return [
        TrainingExample(
            format="demonstrations",
            run_id=run_id,
            sequence_index=i,
            data={
                "tool": event.tool or event.resolved_tool or "",
                "resolved_tool": event.resolved_tool or "",
                "domain": event.graph_domain or "",
                "action": event.graph_action or event.label or "",
                "args": {
                    k: v
                    for k, v in (event.payload or {}).items()
                    if k not in ("result", "observation", "emitted")
                },
                "result": (event.payload or {}).get("result"),
                "status": event.status or "ok",
                "time_ms": event.time_ms,
                "object_refs": event.object_refs,
            },
        )
        for i, event in enumerate(tool_events)
    ]


def _to_authoring_examples(
    run_id: str,
    events: list[RunTimelineEvent],
    manifest: Any,
    *,
    knowledge_state: dict[str, Any],
    contract: dict[str, Any],
) -> list[TrainingExample]:
    compose_events = [
        event
        for event in events
        if (
            (event.graph_action_ref or "") == "knowledge_graph.compose_artifact"
            or (event.resolved_tool or event.tool or "") == "knowledge.compose_artifact"
        )
    ]
    examples: list[TrainingExample] = []
    for index, event in enumerate(compose_events):
        result = (event.payload or {}).get("result", {})
        artifact = result.get("artifact")
        retrieved_assets = result.get("retrieved_assets", [])
        if not isinstance(artifact, dict):
            continue
        examples.append(
            TrainingExample(
                format="authoring",
                run_id=run_id,
                sequence_index=index,
                data={
                    "scenario_name": manifest.scenario_name,
                    "runner": manifest.runner,
                    "prompt": artifact.get("composition", {}).get("prompt", ""),
                    "target": artifact.get("composition", {}).get("target", ""),
                    "template_id": artifact.get("composition", {}).get(
                        "template_id", ""
                    ),
                    "subject_object_ref": artifact.get("composition", {}).get(
                        "subject_object_ref", ""
                    ),
                    "retrieved_asset_ids": [
                        item.get("asset_id")
                        for item in retrieved_assets
                        if isinstance(item, dict) and item.get("asset_id")
                    ],
                    "retrieved_assets": [
                        _compact_knowledge_asset(item)
                        for item in retrieved_assets
                        if isinstance(item, dict)
                    ],
                    "artifact": _compact_knowledge_asset(artifact),
                    "validation": artifact.get("composition", {}).get("validation", {}),
                    "reviewer_feedback": artifact.get("composition", {}).get(
                        "reviewer_feedback", []
                    ),
                    "contract": _compact_contract(contract),
                },
            )
        )
    if examples:
        return examples

    assets = knowledge_state.get("assets", {})
    if not isinstance(assets, dict):
        return []
    artifact = latest_composed_asset_payload(assets)
    if artifact is None:
        return []
    composition = artifact.get("composition", {})
    cited_asset_ids = [
        span.get("asset_id")
        for span in composition.get("citation_spans", [])
        if isinstance(span, dict) and span.get("asset_id")
    ]
    retrieved_assets = [
        assets[asset_id]
        for asset_id in cited_asset_ids
        if isinstance(assets.get(asset_id), dict)
    ]
    return [
        TrainingExample(
            format="authoring",
            run_id=run_id,
            sequence_index=0,
            data={
                "scenario_name": manifest.scenario_name,
                "runner": manifest.runner,
                "prompt": composition.get("prompt", ""),
                "target": composition.get("target", ""),
                "template_id": composition.get("template_id", ""),
                "subject_object_ref": composition.get("subject_object_ref", ""),
                "retrieved_asset_ids": cited_asset_ids,
                "retrieved_assets": [
                    _compact_knowledge_asset(item) for item in retrieved_assets
                ],
                "artifact": _compact_knowledge_asset(artifact),
                "validation": composition.get("validation", {}),
                "reviewer_feedback": composition.get("reviewer_feedback", []),
                "contract": _compact_contract(contract),
            },
        )
    ]


def _compact_knowledge_asset(payload: dict[str, Any]) -> dict[str, Any]:
    raw_composition = (
        payload.get("composition", {}) if isinstance(payload, dict) else {}
    )
    composition = raw_composition if isinstance(raw_composition, dict) else {}
    return {
        "asset_id": payload.get("asset_id", ""),
        "kind": payload.get("kind", ""),
        "title": payload.get("title", ""),
        "summary": payload.get("summary", ""),
        "body": payload.get("body", ""),
        "tags": list(payload.get("tags", []))[:8],
        "linked_object_refs": list(payload.get("linked_object_refs", []))[:8],
        "metrics": dict(payload.get("metrics", {})),
        "citations": [
            span.get("asset_id")
            for span in composition.get("citation_spans", [])
            if isinstance(span, dict) and span.get("asset_id")
        ],
        "sections": list(composition.get("sections", [])),
        "claims": [
            {
                "text": item.get("text", ""),
                "citation_asset_ids": list(item.get("citation_asset_ids", [])),
                "section": item.get("section"),
            }
            for item in composition.get("claims", [])
            if isinstance(item, dict)
        ],
    }


def _compact_contract(contract: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": contract.get("ok"),
        "issue_count": len(contract.get("dynamic_validation", {}).get("issues", []))
        + len(contract.get("static_validation", {}).get("issues", [])),
        "success_assertions_passed": contract.get("success_predicates_passed", 0),
        "success_assertion_count": contract.get("success_predicate_count", 0),
        "policy_invariants_failed": contract.get("policy_invariants_failed", 0),
    }


def _compact_args(args: dict[str, Any]) -> str:
    parts = []
    for k, v in args.items():
        val = str(v)
        if len(val) > 60:
            val = val[:57] + "..."
        parts.append(f"{k}={val}")
    return ", ".join(parts)
