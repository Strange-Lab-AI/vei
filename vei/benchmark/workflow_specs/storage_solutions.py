from __future__ import annotations

from vei.benchmark.workflow_models import StorageSolutionsWorkflowParams
from vei.scenario_engine.api import WorkflowScenarioSpec


def _build_storage_solutions_spec(
    params: StorageSolutionsWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "storage_solutions",
            "objective": {
                "statement": "Turn a risky storage quote into a feasible commitment before the customer hears a bad number.",
                "success": [
                    "capacity feasible",
                    "quote revised",
                    "vendor action assigned",
                    "artifacts updated",
                ],
            },
            "world": {"catalog": "capacity_quote_commitment"},
            "actors": [
                {"actor_id": "solutions-engineer", "role": "Solutions Engineer"},
                {"actor_id": "ops-lead", "role": "Operations Lead"},
            ],
            "constraints": [
                {
                    "name": "capacity_feasibility",
                    "description": "Do not commit more capacity than the network can actually support.",
                }
            ],
            "approvals": [
                {"stage": "dispatch", "approver": "ops-lead", "required": True}
            ],
            "steps": [
                {
                    "step_id": "advance_dispatch_request",
                    "description": "Advance the dispatch approval request.",
                    "graph_domain": "work_graph",
                    "graph_action": "update_request_approval",
                    "args": {
                        "request_id": params.request_id,
                        "approval_stage": "dispatch",
                        "approval_status": "APPROVED",
                        "comment": "Dispatch approval captured for strategic storage commitment.",
                    },
                },
                {
                    "step_id": "allocate_capacity",
                    "description": "Reserve feasible capacity for the strategic quote.",
                    "graph_domain": "inventory_graph",
                    "graph_action": "allocate_capacity",
                    "args": {
                        "quote_id": params.quote_id,
                        "pool_id": params.pool_id,
                        "units": params.units,
                    },
                },
                {
                    "step_id": "revise_quote",
                    "description": "Revise the quote to the feasible site and commitment.",
                    "graph_domain": "inventory_graph",
                    "graph_action": "revise_quote",
                    "args": {
                        "quote_id": params.quote_id,
                        "site_id": params.site_id,
                        "committed_units": params.units,
                    },
                },
                {
                    "step_id": "assign_vendor_action",
                    "description": "Assign downstream vendor execution.",
                    "graph_domain": "inventory_graph",
                    "graph_action": "assign_vendor_action",
                    "args": {
                        "order_id": params.order_id,
                        "vendor_id": params.vendor_id,
                        "status": "scheduled",
                    },
                },
                {
                    "step_id": "update_rollout_plan",
                    "description": "Update the rollout plan artifact.",
                    "graph_domain": "doc_graph",
                    "graph_action": "update_document",
                    "args": {"doc_id": params.doc_id, "body": params.doc_update_note},
                },
                {
                    "step_id": "comment_tracker",
                    "description": "Update the tracker with feasible commitment details.",
                    "graph_domain": "work_graph",
                    "graph_action": "add_issue_comment",
                    "args": {"issue_id": params.ticket_id, "body": params.ticket_note},
                },
                {
                    "step_id": "log_account_note",
                    "description": "Record the commercial commitment note.",
                    "graph_domain": "revenue_graph",
                    "graph_action": "log_activity",
                    "args": {
                        "kind": "note",
                        "deal_id": params.deal_id,
                        "note": params.crm_note,
                    },
                },
                {
                    "step_id": "post_commitment_summary",
                    "description": "Post the safe commitment summary in Slack.",
                    "graph_domain": "comm_graph",
                    "graph_action": "post_message",
                    "args": {
                        "channel": params.slack_channel,
                        "text": params.slack_summary,
                    },
                },
            ],
            "success_assertions": [
                {
                    "kind": "state_equals",
                    "field": f"components.inventory_ops.quotes.{params.quote_id}.site_id",
                    "equals": params.site_id,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.inventory_ops.quotes.{params.quote_id}.committed_units",
                    "equals": params.units,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.inventory_ops.orders.{params.order_id}.vendor_id",
                    "equals": params.vendor_id,
                },
                {
                    "kind": "state_contains",
                    "field": "components.inventory_ops.allocations",
                    "contains": params.quote_id,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.docs.docs.{params.doc_id}.body",
                    "contains": params.doc_update_note,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                    "contains": params.ticket_note,
                },
                {
                    "kind": "state_contains",
                    "field": "components.crm.activities",
                    "contains": params.crm_note,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.slack.channels.{params.slack_channel}.messages",
                    "contains": params.slack_summary,
                },
                {"kind": "time_max_ms", "max_value": params.deadline_max_ms},
            ],
            "failure_paths": [
                {
                    "name": "overcommit_branch",
                    "trigger_step": "allocate_capacity",
                    "recovery_steps": [
                        "revise_quote",
                        "assign_vendor_action",
                        "post_commitment_summary",
                    ],
                    "notes": "Branch if capacity is partially reserved but quote or vendor execution is still inconsistent.",
                }
            ],
            "tags": ["benchmark-family", "vertical", "storage", variant_name],
            "metadata": {
                "benchmark_family": "storage_solutions",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


__all__ = ["_build_storage_solutions_spec"]
