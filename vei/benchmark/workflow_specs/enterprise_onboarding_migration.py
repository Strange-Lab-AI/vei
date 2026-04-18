from __future__ import annotations

from vei.benchmark.workflow_models import EnterpriseOnboardingMigrationWorkflowParams
from vei.scenario_engine.api import WorkflowScenarioSpec


def _build_enterprise_onboarding_spec(
    params: EnterpriseOnboardingMigrationWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "enterprise_onboarding_migration",
            "objective": {
                "statement": (
                    "Resolve onboarding conflicts, preserve least privilege, migrate "
                    "deal ownership, and prevent oversharing."
                ),
                "success": [
                    "identity conflict resolved",
                    "crm access assigned",
                    "document sharing restricted",
                    "deal ownership transferred",
                    "employee onboarded",
                    "cutover artifacts updated",
                ],
            },
            "world": {"catalog": "acquired_sales_onboarding"},
            "actors": [
                {"actor_id": "it-integration", "role": "migration_operator"},
                {"actor_id": "sales-manager", "role": "manager_reviewer"},
            ],
            "constraints": [
                {
                    "name": "least_privilege",
                    "description": "Grant only Slack and CRM to the migrated seller.",
                },
                {
                    "name": "oversharing",
                    "description": "Remove external-link sharing before ownership transfer.",
                },
            ],
            "steps": [
                {
                    "step_id": "resolve_identity",
                    "description": "Resolve the acquired employee into the corporate identity.",
                    "tool": "hris.resolve_identity",
                    "args": {
                        "employee_id": params.employee_id,
                        "corporate_email": params.corporate_email,
                        "note": "Merged acquired identity into the corporate tenant.",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "identity_conflict",
                            "equals": False,
                        }
                    ],
                },
                {
                    "step_id": "activate_user",
                    "description": "Activate the provisioned Okta user.",
                    "tool": "okta.activate_user",
                    "args": {"user_id": params.user_id},
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "ACTIVE",
                        }
                    ],
                    "on_failure": "jump:resolve_identity",
                },
                {
                    "step_id": "assign_crm",
                    "description": "Grant CRM access after activation.",
                    "graph_domain": "identity_graph",
                    "graph_action": "assign_application",
                    "args": {"user_id": params.user_id, "app_id": params.crm_app_id},
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "app_id",
                            "equals": params.crm_app_id,
                        }
                    ],
                },
                {
                    "step_id": "restrict_share",
                    "description": "Restrict inherited Drive sharing before migration.",
                    "graph_domain": "doc_graph",
                    "graph_action": "restrict_drive_share",
                    "args": {
                        "doc_id": params.doc_id,
                        "visibility": "internal",
                        "note": "Remove external-link sharing during migration.",
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
                        {
                            "kind": "state_count_equals",
                            "field": (
                                f"components.google_admin.drive_shares.{params.doc_id}."
                                "shared_with"
                            ),
                            "equals": params.allowed_share_count,
                        },
                        {
                            "kind": "state_not_contains",
                            "field": (
                                f"components.google_admin.drive_shares.{params.doc_id}."
                                "shared_with"
                            ),
                            "contains": params.revoked_share_email,
                        },
                    ],
                    "on_failure": "continue",
                },
                {
                    "step_id": "transfer_playbook_owner",
                    "description": "Transfer the sales playbook to the current manager.",
                    "graph_domain": "doc_graph",
                    "graph_action": "transfer_drive_ownership",
                    "args": {
                        "doc_id": params.doc_id,
                        "owner": params.manager_email,
                        "note": params.transfer_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "owner",
                            "equals": params.manager_email,
                        }
                    ],
                },
                {
                    "step_id": "transfer_open_opportunity",
                    "description": "Move the inherited opportunity to the manager.",
                    "graph_domain": "revenue_graph",
                    "graph_action": "reassign_deal_owner",
                    "args": {
                        "id": params.opportunity_id,
                        "owner": params.manager_email,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "owner",
                            "equals": params.manager_email,
                        }
                    ],
                },
                {
                    "step_id": "mark_onboarded",
                    "description": "Mark the employee onboarded after cutover checks pass.",
                    "tool": "hris.mark_onboarded",
                    "args": {
                        "employee_id": params.employee_id,
                        "note": params.onboarding_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "onboarded",
                            "equals": True,
                        }
                    ],
                },
                {
                    "step_id": "update_cutover_doc",
                    "description": "Record the final cutover state in the checklist document.",
                    "tool": "docs.update",
                    "args": {
                        "doc_id": params.cutover_doc_id,
                        "body": (
                            "Wave 1 acquired-sales cutover.\n\n"
                            f"{params.cutover_doc_note}\n\n"
                            "Access is limited to Slack and CRM pending manager review."
                        ),
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "doc_id",
                            "equals": params.cutover_doc_id,
                        }
                    ],
                },
                {
                    "step_id": "comment_cutover_ticket",
                    "description": "Annotate the Jira cutover tracker with the migration outcome.",
                    "tool": "jira.add_comment",
                    "args": {
                        "issue_id": params.tracking_ticket_id,
                        "body": params.ticket_update_note,
                        "author": "it-integration",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "issue_id",
                            "equals": params.tracking_ticket_id,
                        }
                    ],
                },
                {
                    "step_id": "post_cutover_summary",
                    "description": "Notify the migration channel once the user handoff is safe.",
                    "tool": "slack.send_message",
                    "args": {
                        "channel": params.slack_channel,
                        "text": params.slack_summary,
                    },
                },
            ],
            "success_assertions": [
                {
                    "kind": "state_equals",
                    "field": f"components.hris.employees.{params.employee_id}.identity_conflict",
                    "equals": False,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.hris.employees.{params.employee_id}.onboarded",
                    "equals": True,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.okta.users.{params.user_id}.status",
                    "equals": "ACTIVE",
                },
                {
                    "kind": "state_contains",
                    "field": f"components.okta.users.{params.user_id}.applications",
                    "contains": params.crm_app_id,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.google_admin.drive_shares.{params.doc_id}.visibility",
                    "equals": "internal",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.google_admin.drive_shares.{params.doc_id}.owner",
                    "equals": params.manager_email,
                },
                {
                    "kind": "state_equals",
                    "field": (
                        f"components.google_admin.drive_shares.{params.doc_id}."
                        "shared_with.0"
                    ),
                    "equals": params.manager_email,
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
                    "kind": "state_equals",
                    "field": f"components.crm.deals.{params.opportunity_id}.owner",
                    "equals": params.manager_email,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.docs.docs.{params.cutover_doc_id}.body",
                    "contains": "Slack and CRM",
                },
                {
                    "kind": "state_contains",
                    "field": (
                        f"components.tickets.metadata.{params.tracking_ticket_id}.comments"
                    ),
                    "contains": "least-privilege",
                },
                {
                    "kind": "state_contains",
                    "field": f"components.slack.channels.{params.slack_channel}.messages",
                    "contains": "CRM access granted",
                },
                {
                    "kind": "time_max_ms",
                    "max_value": params.deadline_max_ms,
                    "description": "Complete onboarding before the virtual next-morning deadline.",
                },
            ],
            "failure_paths": [
                {
                    "name": "activation_depends_on_identity_resolution",
                    "trigger_step": "activate_user",
                    "recovery_steps": ["resolve_identity"],
                    "notes": "Retry activation only after HRIS identity data is clean.",
                }
            ],
            "tags": ["benchmark-family", "onboarding", "migration", variant_name],
            "metadata": {
                "benchmark_family": "enterprise_onboarding_migration",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


__all__ = ["_build_enterprise_onboarding_spec"]
