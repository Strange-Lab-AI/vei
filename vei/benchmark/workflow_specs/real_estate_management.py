from __future__ import annotations

from vei.benchmark.workflow_models import RealEstateManagementWorkflowParams
from vei.scenario_engine.api import WorkflowScenarioSpec


def _build_real_estate_management_spec(
    params: RealEstateManagementWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "real_estate_management",
            "objective": {
                "statement": "Restore tenant opening readiness without losing lease, vendor, and artifact consistency.",
                "success": [
                    "lease ready",
                    "vendor assigned",
                    "unit reserved",
                    "artifacts updated",
                ],
            },
            "world": {"catalog": "tenant_opening_conflict"},
            "actors": [
                {"actor_id": "property-ops", "role": "Property Ops Lead"},
                {"actor_id": "leasing", "role": "Leasing Manager"},
            ],
            "constraints": [
                {
                    "name": "tenant_readiness",
                    "description": "Do not allow an invalid or unprepared tenant opening.",
                }
            ],
            "approvals": [
                {"stage": "vendor", "approver": "property-ops", "required": True}
            ],
            "steps": [
                {
                    "step_id": "approve_vendor_request",
                    "description": "Advance the vendor approval request for tenant prep.",
                    "graph_domain": "work_graph",
                    "graph_action": "update_request_approval",
                    "args": {
                        "request_id": params.request_id,
                        "approval_stage": "vendor",
                        "approval_status": "APPROVED",
                        "comment": "Vendor prep approved for anchor tenant opening.",
                    },
                },
                {
                    "step_id": "execute_lease",
                    "description": "Execute the pending lease amendment.",
                    "graph_domain": "property_graph",
                    "graph_action": "update_lease_milestone",
                    "args": {
                        "lease_id": params.lease_id,
                        "milestone": "executed",
                        "status": "ready",
                    },
                },
                {
                    "step_id": "assign_hvac_vendor",
                    "description": "Assign the HVAC vendor to the blocking work order.",
                    "graph_domain": "property_graph",
                    "graph_action": "assign_vendor",
                    "args": {
                        "work_order_id": params.work_order_id,
                        "vendor_id": params.vendor_id,
                        "note": params.vendor_note,
                    },
                },
                {
                    "step_id": "reserve_opening_unit",
                    "description": "Reserve the opening unit for the anchor tenant.",
                    "graph_domain": "property_graph",
                    "graph_action": "reserve_unit",
                    "args": {
                        "unit_id": params.unit_id,
                        "tenant_id": params.tenant_id,
                        "status": "reserved",
                    },
                },
                {
                    "step_id": "update_opening_checklist",
                    "description": "Refresh the opening checklist artifact.",
                    "graph_domain": "doc_graph",
                    "graph_action": "update_document",
                    "args": {"doc_id": params.doc_id, "body": params.doc_update_note},
                },
                {
                    "step_id": "comment_tracker",
                    "description": "Write the property tracker follow-through note.",
                    "graph_domain": "work_graph",
                    "graph_action": "add_issue_comment",
                    "args": {"issue_id": params.ticket_id, "body": params.ticket_note},
                },
                {
                    "step_id": "post_summary",
                    "description": "Post the opening-ready summary to Slack.",
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
                    "field": f"components.property_ops.leases.{params.lease_id}.milestone",
                    "equals": "executed",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.property_ops.leases.{params.lease_id}.status",
                    "equals": "ready",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.property_ops.work_orders.{params.work_order_id}.vendor_id",
                    "equals": params.vendor_id,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.property_ops.units.{params.unit_id}.reserved_for",
                    "equals": params.tenant_id,
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
                    "name": "opening_ready_branch",
                    "trigger_step": "assign_hvac_vendor",
                    "recovery_steps": [
                        "execute_lease",
                        "reserve_opening_unit",
                        "update_opening_checklist",
                    ],
                    "notes": "Branch if vendor assignment lands but lease or unit state still blocks the opening.",
                }
            ],
            "tags": ["benchmark-family", "vertical", "real-estate", variant_name],
            "metadata": {
                "benchmark_family": "real_estate_management",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


__all__ = ["_build_real_estate_management_spec"]
