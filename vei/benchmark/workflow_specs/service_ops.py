from __future__ import annotations

from vei.benchmark.workflow_models import ServiceOpsWorkflowParams
from vei.scenario_engine.api import WorkflowScenarioSpec


def _build_service_ops_spec(
    params: ServiceOpsWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "service_ops",
            "objective": {
                "statement": "Stabilize a bad service morning by recovering dispatch, holding billing safely, and resolving the visible exception trail.",
                "success": [
                    "dispatch recovered",
                    "billing held",
                    "exception trail resolved",
                    "customer story aligned",
                ],
            },
            "world": {"catalog": variant_name},
            "actors": [
                {"actor_id": "ops-manager", "role": "Service Operations Manager"},
                {"actor_id": "dispatch-lead", "role": "Dispatch Lead"},
            ],
            "constraints": [
                {
                    "name": "single_service_loop",
                    "description": "Dispatch, billing, and follow-through must stay coherent across the same customer account.",
                }
            ],
            "approvals": [
                {"stage": "dispatch", "approver": "ops-manager", "required": True}
            ],
            "steps": [
                {
                    "step_id": "advance_dispatch_request",
                    "description": "Advance the VIP dispatch approval before rerouting the appointment.",
                    "graph_domain": "work_graph",
                    "graph_action": "update_request_approval",
                    "args": {
                        "request_id": params.request_id,
                        "approval_stage": "dispatch",
                        "approval_status": "APPROVED",
                        "comment": "Emergency dispatch reroute approved for Clearwater Medical.",
                    },
                },
                {
                    "step_id": "assign_backup_technician",
                    "description": "Assign the backup controls technician to the failing VIP work order.",
                    "graph_domain": "ops_graph",
                    "graph_action": "assign_dispatch",
                    "args": {
                        "work_order_id": params.work_order_id,
                        "appointment_id": params.appointment_id,
                        "technician_id": params.technician_id,
                        "note": params.dispatch_note,
                    },
                },
                {
                    "step_id": "hold_disputed_billing",
                    "description": "Hold the disputed billing case before any follow-up goes out.",
                    "graph_domain": "ops_graph",
                    "graph_action": "hold_billing",
                    "args": {
                        "billing_case_id": params.billing_case_id,
                        "reason": params.billing_note,
                    },
                },
                {
                    "step_id": "resolve_finance_exception",
                    "description": "Resolve the linked finance exception with an explicit note.",
                    "graph_domain": "ops_graph",
                    "graph_action": "clear_exception",
                    "args": {
                        "exception_id": params.exception_id,
                        "resolution_note": params.billing_note,
                    },
                },
                {
                    "step_id": "update_handoff_note",
                    "description": "Update the handoff document so dispatch and billing share one story.",
                    "graph_domain": "doc_graph",
                    "graph_action": "update_document",
                    "args": {"doc_id": params.doc_id, "body": params.doc_update_note},
                },
                {
                    "step_id": "annotate_tracker",
                    "description": "Write the same-day recovery note into the tracker.",
                    "graph_domain": "work_graph",
                    "graph_action": "add_issue_comment",
                    "args": {"issue_id": params.ticket_id, "body": params.ticket_note},
                },
                {
                    "step_id": "post_ops_summary",
                    "description": "Post the stabilized service summary back into dispatch Slack.",
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
                    "field": f"components.service_ops.work_orders.{params.work_order_id}.technician_id",
                    "equals": params.technician_id,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.service_ops.appointments.{params.appointment_id}.dispatch_status",
                    "equals": "assigned",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.service_ops.billing_cases.{params.billing_case_id}.hold",
                    "equals": True,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.service_ops.exceptions.{params.exception_id}.status",
                    "equals": "resolved",
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
                    "field": f"components.slack.channels.{params.slack_channel}.messages",
                    "contains": params.slack_summary,
                },
                {"kind": "time_max_ms", "max_value": params.deadline_max_ms},
            ],
            "failure_paths": [
                {
                    "name": "service_loop_fragmentation",
                    "trigger_step": "assign_backup_technician",
                    "recovery_steps": [
                        "hold_disputed_billing",
                        "resolve_finance_exception",
                        "update_handoff_note",
                    ],
                    "notes": "Branch if dispatch recovers but the billing or exception trail still drifts.",
                }
            ],
            "tags": ["benchmark-family", "vertical", "service", variant_name],
            "metadata": {
                "benchmark_family": "service_ops",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


__all__ = ["_build_service_ops_spec"]
