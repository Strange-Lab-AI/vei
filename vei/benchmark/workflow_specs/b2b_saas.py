from __future__ import annotations

from vei.benchmark.workflow_models import B2bSaasWorkflowParams
from vei.scenario_engine.api import WorkflowScenarioSpec


def _build_b2b_saas_spec(
    params: B2bSaasWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "b2b_saas",
            "objective": {
                "statement": "Save a $480K enterprise renewal by fixing the product, rebuilding trust, and closing the deal.",
                "success": [
                    "integration fixed",
                    "stakeholder engaged",
                    "competitive threat neutralized",
                    "renewal advanced",
                ],
            },
            "world": {"catalog": "enterprise_renewal_risk"},
            "actors": [
                {"actor_id": "cs-lead", "role": "Customer Success Lead"},
                {"actor_id": "sales-rep", "role": "Account Executive"},
            ],
            "constraints": [
                {
                    "name": "customer_trust",
                    "description": "Do not advance the commercial conversation before the product issue is resolved.",
                }
            ],
            "approvals": [
                {
                    "stage": "engineering",
                    "approver": "product-lead",
                    "required": True,
                }
            ],
            "steps": [
                {
                    "step_id": "advance_fix_approval",
                    "description": "Approve the hotfix release.",
                    "graph_domain": "work_graph",
                    "graph_action": "update_request_approval",
                    "args": {
                        "request_id": params.request_id,
                        "approval_stage": "engineering",
                        "approval_status": "APPROVED",
                        "comment": "Hotfix approved for customer-facing release.",
                    },
                },
                {
                    "step_id": "resolve_p1",
                    "description": "Close the P1 ticket after the fix is deployed.",
                    "graph_domain": "work_graph",
                    "graph_action": "add_issue_comment",
                    "args": {
                        "issue_id": params.ticket_id,
                        "body": params.ticket_note,
                    },
                },
                {
                    "step_id": "update_renewal_doc",
                    "description": "Update the renewal proposal with the fix evidence.",
                    "graph_domain": "doc_graph",
                    "graph_action": "update_document",
                    "args": {
                        "doc_id": params.doc_id,
                        "body": "Renewal proposal updated with integration fix confirmation and success metrics.",
                    },
                },
                {
                    "step_id": "log_renewal_activity",
                    "description": "Record the renewal progress in CRM.",
                    "graph_domain": "revenue_graph",
                    "graph_action": "log_activity",
                    "args": {
                        "kind": "note",
                        "deal_id": params.deal_id,
                        "note": params.crm_note,
                    },
                },
                {
                    "step_id": "post_save_summary",
                    "description": "Post the renewal save plan summary to Slack.",
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
                    "name": "discount_without_fix",
                    "trigger_step": "log_renewal_activity",
                    "recovery_steps": [
                        "resolve_p1",
                        "update_renewal_doc",
                        "post_save_summary",
                    ],
                    "notes": "Branch if the commercial move happens before the product fix lands.",
                }
            ],
            "tags": ["benchmark-family", "vertical", "saas", variant_name],
            "metadata": {
                "benchmark_family": "b2b_saas",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


__all__ = ["_build_b2b_saas_spec"]
