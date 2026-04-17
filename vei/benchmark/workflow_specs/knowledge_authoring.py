from __future__ import annotations

from vei.benchmark.workflow_models import KnowledgeAuthoringWorkflowParams
from vei.scenario_engine.models import WorkflowScenarioSpec


def _build_knowledge_authoring_spec(
    params: KnowledgeAuthoringWorkflowParams,
    *,
    variant_name: str,
) -> WorkflowScenarioSpec:
    artifact_field = f"components.knowledge.assets.{params.expected_artifact_id}"
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "knowledge_authoring",
            "objective": {
                "statement": "Draft a grounded client proposal that stays tied to live pricing, delivery, and client-conversation evidence.",
                "success": [
                    "proposal composed",
                    "citations present",
                    "fresh sources only",
                    "template sections intact",
                ],
            },
            "world": {"catalog": "campaign_launch_guardrail"},
            "actors": [
                {"actor_id": "account-lead", "role": "Account Lead"},
                {"actor_id": "strategy-lead", "role": "Strategy Lead"},
            ],
            "constraints": [
                {
                    "name": "grounded_authoring",
                    "description": "Every proposal claim must stay tied to current company knowledge.",
                }
            ],
            "steps": [
                {
                    "step_id": "compose_proposal",
                    "description": "Compose the grounded Apex proposal.",
                    "graph_domain": "knowledge_graph",
                    "graph_action": "compose_artifact",
                    "args": {
                        "target": params.target,
                        "template_id": params.template_id,
                        "subject_object_ref": params.subject_object_ref,
                        "mode": "heuristic_baseline",
                        "prompt": params.prompt,
                    },
                }
            ],
            "success_assertions": [
                {"kind": "state_exists", "field": artifact_field},
                {
                    "kind": "state_equals",
                    "field": f"{artifact_field}.kind",
                    "equals": params.target,
                },
                {"kind": "citations_present", "field": artifact_field},
                {"kind": "citations_resolve", "field": artifact_field},
                {"kind": "sources_within_shelf_life", "field": artifact_field},
                {
                    "kind": "numbers_reconcile",
                    "field": artifact_field,
                    "params": {"tolerance": 0.01},
                },
                {"kind": "format_matches_template", "field": artifact_field},
                {
                    "kind": "state_contains",
                    "field": f"{artifact_field}.body",
                    "contains": "Executive summary",
                },
                *[
                    {
                        "kind": "state_contains",
                        "field": f"{artifact_field}.body",
                        "contains": f"[{asset_id}]",
                    }
                    for asset_id in params.required_source_asset_ids
                ],
                {"kind": "time_max_ms", "max_value": params.deadline_max_ms},
            ],
            "tags": ["benchmark-family", "knowledge", "authoring", variant_name],
            "metadata": {
                "benchmark_family": "knowledge_authoring",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


__all__ = ["_build_knowledge_authoring_spec"]
