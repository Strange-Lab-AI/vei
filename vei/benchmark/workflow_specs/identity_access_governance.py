from __future__ import annotations

from typing import Any

from vei.benchmark.workflow_models import IdentityAccessGovernanceWorkflowParams
from vei.scenario_engine.api import WorkflowScenarioSpec


def _build_identity_access_governance_spec(
    params: IdentityAccessGovernanceWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    base_steps: list[dict[str, Any]]
    success_assertions: list[dict[str, Any]]
    objective_success: list[str]

    if variant_name == "oversharing_remediation":
        objective_success = [
            "external sharing removed",
            "artifact trail updated",
            "stakeholder summary sent",
        ]
        base_steps = [
            {
                "step_id": "restrict_share",
                "description": "Reduce imported Drive sharing to an internal posture.",
                "graph_domain": "doc_graph",
                "graph_action": "restrict_drive_share",
                "args": {
                    "doc_id": params.doc_id,
                    "visibility": "internal",
                    "note": "Imported policy prohibits external share domains.",
                },
                "expect": [
                    {
                        "kind": "result_equals",
                        "field": "visibility",
                        "equals": "internal",
                    },
                    {
                        "kind": "result_equals",
                        "field": "shared_with_count",
                        "equals": params.allowed_share_count,
                    },
                ],
            },
            {
                "step_id": "update_governance_doc",
                "description": "Record the imported ACL remediation in the governance doc.",
                "graph_domain": "doc_graph",
                "graph_action": "update_document",
                "args": {
                    "doc_id": params.cutover_doc_id,
                    "body": params.doc_update_note,
                },
            },
            {
                "step_id": "comment_tracker",
                "description": "Annotate the tracker issue with the remediation result.",
                "graph_domain": "work_graph",
                "graph_action": "add_issue_comment",
                "args": {
                    "issue_id": params.ticket_id,
                    "body": params.ticket_note,
                    "author": "identity-admin",
                },
            },
            {
                "step_id": "post_summary",
                "description": "Notify the shared channel that imported oversharing is fixed.",
                "graph_domain": "comm_graph",
                "graph_action": "post_message",
                "args": {"channel": params.slack_channel, "text": params.slack_summary},
            },
        ]
        success_assertions = [
            {
                "kind": "state_equals",
                "field": f"components.google_admin.drive_shares.{params.doc_id}.visibility",
                "equals": "internal",
            },
            {
                "kind": "state_count_equals",
                "field": f"components.google_admin.drive_shares.{params.doc_id}.shared_with",
                "equals": params.allowed_share_count,
            },
            {
                "kind": "state_not_contains",
                "field": f"components.google_admin.drive_shares.{params.doc_id}.shared_with",
                "contains": params.revoked_share_email,
            },
            {
                "kind": "state_contains",
                "field": f"components.docs.docs.{params.cutover_doc_id}.body",
                "contains": "policy",
            },
            {
                "kind": "state_contains",
                "field": f"components.slack.channels.{params.slack_channel}.messages",
                "contains": "oversharing",
            },
        ]
    elif variant_name == "approval_bottleneck":
        objective_success = [
            "approval chain advanced",
            "approved app assigned",
            "artifact trail updated",
        ]
        base_steps = [
            {
                "step_id": "advance_approval",
                "description": "Advance the imported approval stage.",
                "graph_domain": "work_graph",
                "graph_action": "update_request_approval",
                "args": {
                    "request_id": params.request_id,
                    "approval_stage": "identity",
                    "approval_status": "APPROVED",
                    "status": "APPROVED",
                    "comment": params.request_comment,
                },
                "expect": [
                    {"kind": "result_equals", "field": "status", "equals": "APPROVED"}
                ],
            },
            {
                "step_id": "assign_primary_app",
                "description": "Grant the policy-approved application.",
                "graph_domain": "identity_graph",
                "graph_action": "assign_application",
                "args": {"user_id": params.user_id, "app_id": params.primary_app_id},
                "expect": [
                    {
                        "kind": "result_equals",
                        "field": "app_id",
                        "equals": params.primary_app_id,
                    }
                ],
            },
            {
                "step_id": "comment_tracker",
                "description": "Update the tracker with the approval outcome.",
                "graph_domain": "work_graph",
                "graph_action": "add_issue_comment",
                "args": {
                    "issue_id": params.ticket_id,
                    "body": params.ticket_note,
                    "author": "identity-admin",
                },
            },
            {
                "step_id": "post_summary",
                "description": "Notify the governance channel once the bottleneck is cleared.",
                "graph_domain": "comm_graph",
                "graph_action": "post_message",
                "args": {"channel": params.slack_channel, "text": params.slack_summary},
            },
        ]
        success_assertions = [
            {
                "kind": "state_contains",
                "field": f"components.okta.users.{params.user_id}.applications",
                "contains": params.primary_app_id,
            },
            {
                "kind": "state_contains",
                "field": f"components.servicedesk.requests.{params.request_id}.approvals",
                "contains": "APPROVED",
            },
            {
                "kind": "state_contains",
                "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                "contains": "approval",
            },
            {
                "kind": "state_contains",
                "field": f"components.slack.channels.{params.slack_channel}.messages",
                "contains": "approval",
            },
        ]
    elif variant_name == "stale_entitlement_cleanup":
        objective_success = [
            "stale entitlement removed",
            "tracker updated",
            "stakeholder summary sent",
        ]
        base_steps = [
            {
                "step_id": "remove_stale_app",
                "description": "Remove imported stale application access.",
                "graph_domain": "identity_graph",
                "graph_action": "remove_application",
                "args": {"user_id": params.user_id, "app_id": params.stale_app_id},
                "expect": [
                    {
                        "kind": "result_equals",
                        "field": "app_id",
                        "equals": params.stale_app_id,
                    }
                ],
            },
            {
                "step_id": "comment_tracker",
                "description": "Record the entitlement cleanup in the tracker.",
                "graph_domain": "work_graph",
                "graph_action": "add_issue_comment",
                "args": {
                    "issue_id": params.ticket_id,
                    "body": params.ticket_note,
                    "author": "identity-admin",
                },
            },
            {
                "step_id": "post_summary",
                "description": "Notify the channel once stale access is removed.",
                "graph_domain": "comm_graph",
                "graph_action": "post_message",
                "args": {"channel": params.slack_channel, "text": params.slack_summary},
            },
        ]
        success_assertions = [
            {
                "kind": "state_not_contains",
                "field": f"components.okta.users.{params.user_id}.applications",
                "contains": params.stale_app_id,
            },
            {
                "kind": "state_contains",
                "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                "contains": "stale",
            },
            {
                "kind": "state_contains",
                "field": f"components.slack.channels.{params.slack_channel}.messages",
                "contains": "stale entitlement",
            },
        ]
    else:
        objective_success = [
            "break-glass follow-up recorded",
            "temporary access removed",
            "artifacts updated",
        ]
        base_steps = [
            {
                "step_id": "remove_break_glass_app",
                "description": "Remove the imported temporary application access.",
                "graph_domain": "identity_graph",
                "graph_action": "remove_application",
                "args": {"user_id": params.user_id, "app_id": params.stale_app_id},
                "expect": [
                    {
                        "kind": "result_equals",
                        "field": "app_id",
                        "equals": params.stale_app_id,
                    }
                ],
            },
            {
                "step_id": "update_followup_doc",
                "description": "Write the break-glass follow-up into the governance doc.",
                "graph_domain": "doc_graph",
                "graph_action": "update_document",
                "args": {
                    "doc_id": params.cutover_doc_id,
                    "body": f"Break-glass follow-up.\n\n{params.doc_update_note}",
                },
            },
            {
                "step_id": "comment_tracker",
                "description": "Record the break-glass follow-up in the tracker.",
                "graph_domain": "work_graph",
                "graph_action": "add_issue_comment",
                "args": {
                    "issue_id": params.ticket_id,
                    "body": params.ticket_note,
                    "author": "security-review",
                },
            },
            {
                "step_id": "post_summary",
                "description": "Notify the channel that imported break-glass follow-up completed.",
                "graph_domain": "comm_graph",
                "graph_action": "post_message",
                "args": {"channel": params.slack_channel, "text": params.slack_summary},
            },
        ]
        success_assertions = [
            {
                "kind": "state_not_contains",
                "field": f"components.okta.users.{params.user_id}.applications",
                "contains": params.stale_app_id,
            },
            {
                "kind": "state_contains",
                "field": f"components.docs.docs.{params.cutover_doc_id}.body",
                "contains": "Break-glass",
            },
            {
                "kind": "state_contains",
                "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                "contains": "break-glass",
            },
        ]

    success_assertions.append(
        {"kind": "time_max_ms", "max_value": params.deadline_max_ms}
    )
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "identity_access_governance",
            "objective": {
                "statement": "Resolve imported identity governance drift using graph-native enterprise actions.",
                "success": objective_success,
            },
            "world": {"catalog": "acquired_sales_onboarding"},
            "actors": [
                {
                    "actor_id": "identity-admin",
                    "role": "Identity Admin",
                    "email": "identity-admin@example.com",
                },
                {
                    "actor_id": "sales-manager",
                    "role": "Sales Manager",
                    "email": params.manager_email,
                },
            ],
            "constraints": [
                {
                    "name": "least_privilege",
                    "description": "Keep imported access limited to the intended application set.",
                }
            ],
            "approvals": [
                {
                    "stage": "identity",
                    "approver": "identity-admin",
                    "required": variant_name == "approval_bottleneck",
                }
            ],
            "steps": base_steps,
            "success_assertions": success_assertions,
            "failure_paths": [
                {
                    "name": "artifact_followthrough_required",
                    "trigger_step": base_steps[-1]["step_id"],
                    "recovery_steps": [step["step_id"] for step in base_steps[:-1]],
                    "notes": "Artifact follow-through must be visible before the workflow is considered complete.",
                }
            ],
            "tags": ["benchmark-family", "identity", "governance", variant_name],
            "metadata": {
                "benchmark_family": "identity_access_governance",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


__all__ = ["_build_identity_access_governance_spec"]
