from __future__ import annotations

from vei.benchmark.workflow_models import DigitalMarketingAgencyWorkflowParams
from vei.scenario_engine.api import WorkflowScenarioSpec


def _build_digital_marketing_agency_spec(
    params: DigitalMarketingAgencyWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "digital_marketing_agency",
            "objective": {
                "statement": "Make the client launch safe by clearing approvals, pacing, and reporting drift before spend burns.",
                "success": [
                    "creative approved",
                    "pacing normalized",
                    "report refreshed",
                    "client artifacts updated",
                ],
            },
            "world": {"catalog": "campaign_launch_guardrail"},
            "actors": [
                {"actor_id": "account-lead", "role": "Account Lead"},
                {"actor_id": "creative-director", "role": "Creative Director"},
            ],
            "constraints": [
                {
                    "name": "launch_safety",
                    "description": "Do not launch unapproved or overspending campaign state.",
                }
            ],
            "approvals": [
                {"stage": "creative", "approver": "creative-director", "required": True}
            ],
            "steps": [
                {
                    "step_id": "advance_launch_request",
                    "description": "Advance the launch request approval.",
                    "graph_domain": "work_graph",
                    "graph_action": "update_request_approval",
                    "args": {
                        "request_id": params.request_id,
                        "approval_stage": "creative",
                        "approval_status": "APPROVED",
                        "comment": "Creative sign-off captured for launch guardrail.",
                    },
                },
                {
                    "step_id": "approve_creative",
                    "description": "Approve the pending launch creative.",
                    "graph_domain": "campaign_graph",
                    "graph_action": "approve_creative",
                    "args": {
                        "creative_id": params.creative_id,
                        "approval_id": params.approval_id,
                    },
                },
                {
                    "step_id": "normalize_pacing",
                    "description": "Reduce pacing to a safe launch level.",
                    "graph_domain": "campaign_graph",
                    "graph_action": "adjust_budget_pacing",
                    "args": {
                        "campaign_id": params.campaign_id,
                        "pacing_pct": params.pacing_pct,
                    },
                },
                {
                    "step_id": "refresh_report",
                    "description": "Refresh the stale launch report artifact.",
                    "graph_domain": "campaign_graph",
                    "graph_action": "publish_report_note",
                    "args": {"report_id": params.report_id, "note": params.report_note},
                },
                {
                    "step_id": "update_launch_brief",
                    "description": "Update the launch brief for the client team.",
                    "graph_domain": "doc_graph",
                    "graph_action": "update_document",
                    "args": {"doc_id": params.doc_id, "body": params.doc_update_note},
                },
                {
                    "step_id": "annotate_tracker",
                    "description": "Update the tracker issue.",
                    "graph_domain": "work_graph",
                    "graph_action": "add_issue_comment",
                    "args": {"issue_id": params.ticket_id, "body": params.ticket_note},
                },
                {
                    "step_id": "log_client_note",
                    "description": "Record the client/commercial note.",
                    "graph_domain": "revenue_graph",
                    "graph_action": "log_activity",
                    "args": {
                        "kind": "note",
                        "deal_id": params.deal_id,
                        "note": params.crm_note,
                    },
                },
                {
                    "step_id": "post_launch_summary",
                    "description": "Post the launch-safe summary to Slack.",
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
                    "field": f"components.campaign_ops.creatives.{params.creative_id}.status",
                    "equals": "approved",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.campaign_ops.approvals.{params.approval_id}.status",
                    "equals": "approved",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.campaign_ops.campaigns.{params.campaign_id}.pacing_pct",
                    "equals": params.pacing_pct,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.campaign_ops.reports.{params.report_id}.stale",
                    "equals": False,
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
                    "name": "pause_launch_branch",
                    "trigger_step": "normalize_pacing",
                    "recovery_steps": [
                        "refresh_report",
                        "update_launch_brief",
                        "post_launch_summary",
                    ],
                    "notes": "Branch if pacing is corrected but approval/artifact follow-through still drifts.",
                }
            ],
            "tags": ["benchmark-family", "vertical", "marketing", variant_name],
            "metadata": {
                "benchmark_family": "digital_marketing_agency",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


__all__ = ["_build_digital_marketing_agency_spec"]
