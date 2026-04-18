from __future__ import annotations

from vei.benchmark.workflow_models import RevenueIncidentMitigationWorkflowParams
from vei.scenario_engine.api import WorkflowScenarioSpec


def _build_revenue_incident_spec(
    params: RevenueIncidentMitigationWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "revenue_incident_mitigation",
            "objective": {
                "statement": (
                    "Contain a checkout incident with targeted rollback controls, "
                    "quantified revenue impact, and coordinated cross-surface recovery."
                ),
                "success": [
                    "incident acknowledged",
                    "rollout reduced",
                    "kill switch enabled",
                    "revenue impact quantified",
                    "communications updated",
                    "ticket and CRM follow-through completed",
                    "service marked recovering",
                    "incident resolved",
                ],
            },
            "world": {"catalog": "checkout_spike_mitigation"},
            "actors": [
                {"actor_id": "commerce-oncall", "role": "incident_commander"},
                {"actor_id": "release-controller", "role": "feature_flag_operator"},
            ],
            "constraints": [
                {
                    "name": "targeted_rollback",
                    "description": "Use control-plane actions before risky data writes.",
                },
                {
                    "name": "safe_recovery",
                    "description": "Resolve the incident only after mitigation is active.",
                },
                {
                    "name": "revenue_impact_recorded",
                    "description": "Quantify checkout impact before closing the page.",
                },
            ],
            "steps": [
                {
                    "step_id": "ack_incident",
                    "description": "Acknowledge the paging incident.",
                    "graph_domain": "obs_graph",
                    "graph_action": "ack_incident",
                    "args": {
                        "incident_id": params.incident_id,
                        "assignee": params.assignee,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "acknowledged",
                        }
                    ],
                },
                {
                    "step_id": "review_service",
                    "description": "Inspect the degraded checkout service before mitigation.",
                    "tool": "datadog.get_service",
                    "args": {"service_id": params.service_id},
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "degraded",
                        }
                    ],
                },
                {
                    "step_id": "reduce_rollout",
                    "description": "Reduce rollout on checkout_v2 to shrink blast radius.",
                    "graph_domain": "ops_graph",
                    "graph_action": "update_rollout",
                    "args": {
                        "flag_key": params.rollout_flag_key,
                        "rollout_pct": params.rollout_pct,
                        "reason": "Contain checkout spike while assessing rollback.",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "rollout_pct",
                            "equals": params.rollout_pct,
                        }
                    ],
                    "on_failure": "jump:enable_kill_switch",
                },
                {
                    "step_id": "enable_kill_switch",
                    "description": "Enable the checkout kill switch as a safe fallback.",
                    "graph_domain": "ops_graph",
                    "graph_action": "set_flag",
                    "args": {
                        "flag_key": params.kill_switch_flag_key,
                        "enabled": True,
                        "reason": "Mitigate checkout failure spike.",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "enabled",
                            "equals": True,
                        }
                    ],
                },
                {
                    "step_id": "record_order_loss",
                    "description": "Write the estimated lost-order rate into the spreadsheet.",
                    "tool": "spreadsheet.update_cell",
                    "args": {
                        "workbook_id": params.workbook_id,
                        "sheet_id": params.sheet_id,
                        "cell": params.order_loss_cell,
                        "value": params.order_loss_per_hour,
                        "note": params.spreadsheet_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "cell",
                            "equals": params.order_loss_cell,
                        }
                    ],
                },
                {
                    "step_id": "record_revenue_loss",
                    "description": "Write the estimated revenue loss into the spreadsheet.",
                    "tool": "spreadsheet.update_cell",
                    "args": {
                        "workbook_id": params.workbook_id,
                        "sheet_id": params.sheet_id,
                        "cell": params.revenue_loss_cell,
                        "value": params.revenue_loss_usd,
                        "note": params.spreadsheet_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "cell",
                            "equals": params.revenue_loss_cell,
                        }
                    ],
                },
                {
                    "step_id": "record_impact_row",
                    "description": "Update the spreadsheet table with the quantified impact row.",
                    "tool": "spreadsheet.upsert_row",
                    "args": {
                        "workbook_id": params.workbook_id,
                        "sheet_id": params.sheet_id,
                        "match_field": "metric",
                        "match_value": "estimated_revenue_loss_usd",
                        "table_id": params.impact_table_id,
                        "row": {
                            "metric": "estimated_revenue_loss_usd",
                            "value": params.revenue_loss_usd,
                            "notes": params.spreadsheet_note,
                        },
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "sheet_id",
                            "equals": params.sheet_id,
                        }
                    ],
                },
                {
                    "step_id": "set_impact_formula",
                    "description": "Set a formula backstop for the impact sheet.",
                    "tool": "spreadsheet.set_formula",
                    "args": {
                        "workbook_id": params.workbook_id,
                        "sheet_id": params.sheet_id,
                        "cell": params.formula_cell,
                        "formula": f"={params.order_loss_cell}*297",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "cell",
                            "equals": params.formula_cell,
                        }
                    ],
                },
                {
                    "step_id": "update_comms_doc",
                    "description": "Refresh the incident guidance doc with customer-facing language.",
                    "tool": "docs.update",
                    "args": {
                        "doc_id": params.comms_doc_id,
                        "body": (
                            "Checkout mitigation in progress.\n\n"
                            f"{params.doc_update_note}\n\n"
                            "Revenue impact has been quantified in the incident workbook."
                        ),
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "doc_id",
                            "equals": params.comms_doc_id,
                        }
                    ],
                },
                {
                    "step_id": "comment_ticket",
                    "description": "Annotate the incident ticket with the mitigation state.",
                    "tool": "tickets.add_comment",
                    "args": {
                        "ticket_id": params.ticket_id,
                        "body": params.ticket_update_note,
                        "author": "commerce-ic",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "ticket_id",
                            "equals": params.ticket_id,
                        }
                    ],
                },
                {
                    "step_id": "log_revenue_followthrough",
                    "description": "Log the quantified impact against the active CRM deal.",
                    "tool": "crm.log_activity",
                    "args": {
                        "kind": "note",
                        "deal_id": params.deal_id,
                        "note": params.crm_activity_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "ok",
                            "equals": True,
                        }
                    ],
                },
                {
                    "step_id": "post_slack_summary",
                    "description": "Post a stakeholder summary once impact is quantified.",
                    "tool": "slack.send_message",
                    "args": {
                        "channel": params.slack_channel,
                        "text": params.slack_summary,
                    },
                },
                {
                    "step_id": "mark_service_recovering",
                    "description": "Mark the checkout service as recovering after mitigation.",
                    "tool": "datadog.update_service",
                    "args": {
                        "service_id": params.service_id,
                        "status": "recovering",
                        "note": params.recovering_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "recovering",
                        }
                    ],
                },
                {
                    "step_id": "resolve_ticket",
                    "description": "Resolve the ticket once rollback, docs, and CRM are updated.",
                    "tool": "tickets.transition",
                    "args": {"ticket_id": params.ticket_id, "status": "resolved"},
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "resolved",
                        }
                    ],
                },
                {
                    "step_id": "resolve_incident",
                    "description": "Resolve the paging incident with the mitigation note.",
                    "tool": "pagerduty.resolve_incident",
                    "args": {
                        "incident_id": params.incident_id,
                        "note": params.resolution_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "resolved",
                        }
                    ],
                },
            ],
            "success_assertions": [
                {
                    "kind": "state_equals",
                    "field": f"components.pagerduty.incidents.{params.incident_id}.status",
                    "equals": "resolved",
                },
                {
                    "kind": "state_equals",
                    "field": (
                        f"components.feature_flags.flags.{params.rollout_flag_key}."
                        "rollout_pct"
                    ),
                    "equals": params.rollout_pct,
                },
                {
                    "kind": "state_equals",
                    "field": (
                        f"components.feature_flags.flags.{params.kill_switch_flag_key}."
                        "enabled"
                    ),
                    "equals": True,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.datadog.services.{params.service_id}.status",
                    "equals": "recovering",
                },
                {
                    "kind": "state_contains",
                    "field": f"components.pagerduty.incidents.{params.incident_id}.notes",
                    "contains": "stabilized",
                },
                {
                    "kind": "state_equals",
                    "field": (
                        f"components.spreadsheet.workbooks.{params.workbook_id}.sheets."
                        f"{params.sheet_id}.cells.{params.order_loss_cell}"
                    ),
                    "equals": params.order_loss_per_hour,
                },
                {
                    "kind": "state_equals",
                    "field": (
                        f"components.spreadsheet.workbooks.{params.workbook_id}.sheets."
                        f"{params.sheet_id}.cells.{params.revenue_loss_cell}"
                    ),
                    "equals": params.revenue_loss_usd,
                },
                {
                    "kind": "state_equals",
                    "field": (
                        f"components.spreadsheet.workbooks.{params.workbook_id}.sheets."
                        f"{params.sheet_id}.formulas.{params.formula_cell}"
                    ),
                    "equals": f"={params.order_loss_cell}*297",
                },
                {
                    "kind": "state_contains",
                    "field": (
                        f"components.spreadsheet.workbooks.{params.workbook_id}.sheets."
                        f"{params.sheet_id}.rows"
                    ),
                    "contains": params.spreadsheet_note,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.docs.docs.{params.comms_doc_id}.body",
                    "contains": "Revenue impact has been quantified",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.tickets.tickets.{params.ticket_id}.status",
                    "equals": "resolved",
                },
                {
                    "kind": "state_contains",
                    "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                    "contains": "impact",
                },
                {
                    "kind": "state_contains",
                    "field": "components.crm.activities",
                    "contains": "quantified",
                },
                {
                    "kind": "state_contains",
                    "field": f"components.slack.channels.{params.slack_channel}.messages",
                    "contains": "impact workbook updated",
                },
                {
                    "kind": "time_max_ms",
                    "max_value": params.deadline_max_ms,
                },
            ],
            "failure_paths": [
                {
                    "name": "rollout_reduction_falls_back_to_kill_switch",
                    "trigger_step": "reduce_rollout",
                    "recovery_steps": ["enable_kill_switch"],
                    "notes": "Use the kill switch if the rollout change does not stick.",
                },
                {
                    "name": "impact_must_be_recorded_before_resolution",
                    "trigger_step": "resolve_incident",
                    "recovery_steps": [
                        "record_order_loss",
                        "record_revenue_loss",
                        "record_impact_row",
                        "update_comms_doc",
                        "log_revenue_followthrough",
                    ],
                    "notes": "Do not close the incident before the revenue flight deck is updated.",
                },
            ],
            "tags": ["benchmark-family", "incident", "revenue", variant_name],
            "metadata": {
                "benchmark_family": "revenue_incident_mitigation",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


__all__ = ["_build_revenue_incident_spec"]
