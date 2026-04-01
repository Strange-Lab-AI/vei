from __future__ import annotations

from typing import Any, Dict, List, Optional

from vei.benchmark.models import (
    BenchmarkWorkflowParameter,
    BenchmarkWorkflowVariantManifest,
)
from vei.benchmark.workflow_catalog import (
    _PARAMETER_DESCRIPTIONS,
    _SCENARIO_TO_WORKFLOW,
    _VARIANT_CATALOG,
    _VariantDefinition,
)
from vei.benchmark.workflow_models import (
    B2bSaasWorkflowParams,
    DigitalMarketingAgencyWorkflowParams,
    EnterpriseOnboardingMigrationWorkflowParams,
    IdentityAccessGovernanceWorkflowParams,
    RealEstateManagementWorkflowParams,
    RevenueIncidentMitigationWorkflowParams,
    SecurityContainmentWorkflowParams,
    ServiceOpsWorkflowParams,
    StorageSolutionsWorkflowParams,
)
from vei.scenario_engine.models import WorkflowScenarioSpec


def _build_security_containment_spec(
    params: SecurityContainmentWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "security_containment",
            "objective": {
                "statement": (
                    "Contain a malicious OAuth app with targeted action, preserve "
                    "evidence, and record the customer-notification decision."
                ),
                "success": [
                    "oauth app suspended",
                    "evidence preserved",
                    "notification decision recorded",
                    "incident artifacts updated",
                ],
            },
            "world": {"catalog": "oauth_app_containment"},
            "actors": [
                {"actor_id": "sec-lead", "role": "incident_commander"},
                {"actor_id": "siem", "role": "forensics_system"},
            ],
            "constraints": [
                {
                    "name": "targeted_containment",
                    "description": "Contain the suspicious app without broad user disruption.",
                },
                {
                    "name": "evidence",
                    "description": "Preserve evidence before or during containment.",
                },
            ],
            "steps": [
                {
                    "step_id": "inspect_app",
                    "description": "Inspect the suspicious OAuth app in Google Admin.",
                    "tool": "google_admin.get_oauth_app",
                    "args": {"app_id": params.app_id},
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "risk_level",
                            "equals": "critical",
                        },
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "ACTIVE",
                        },
                    ],
                },
                {
                    "step_id": "preserve_google_evidence",
                    "description": "Preserve Google-side evidence before disabling the app.",
                    "tool": "google_admin.preserve_oauth_evidence",
                    "args": {
                        "app_id": params.app_id,
                        "note": params.evidence_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "evidence_hold",
                            "equals": True,
                        }
                    ],
                    "on_failure": "jump:inspect_app",
                },
                {
                    "step_id": "preserve_siem_evidence",
                    "description": "Link the originating alert into the incident case.",
                    "tool": "siem.preserve_evidence",
                    "args": {
                        "alert_id": params.alert_id,
                        "case_id": params.case_id,
                        "note": "Preserved during OAuth containment.",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "evidence_preserved",
                            "equals": True,
                        }
                    ],
                },
                {
                    "step_id": "suspend_oauth_app",
                    "description": "Suspend only the malicious OAuth app.",
                    "tool": "google_admin.suspend_oauth_app",
                    "args": {
                        "app_id": params.app_id,
                        "reason": params.suspension_reason,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "SUSPENDED",
                        }
                    ],
                    "on_failure": "jump:preserve_google_evidence",
                },
                {
                    "step_id": "record_notification_decision",
                    "description": "Update the case with containment state and customer notification.",
                    "tool": "siem.update_case",
                    "args": {
                        "case_id": params.case_id,
                        "status": "CONTAINED",
                        "customer_notification_required": params.notification_required,
                        "note": params.case_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "CONTAINED",
                        },
                        {
                            "kind": "result_equals",
                            "field": "customer_notification_required",
                            "equals": params.notification_required,
                        },
                    ],
                },
                {
                    "step_id": "update_incident_brief",
                    "description": "Refresh the containment brief with the notification posture.",
                    "tool": "docs.update",
                    "args": {
                        "doc_id": params.brief_doc_id,
                        "body": (
                            "OAuth app containment summary.\n\n"
                            f"{params.case_note}\n\n"
                            f"{params.brief_update_note}"
                        ),
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "doc_id",
                            "equals": params.brief_doc_id,
                        }
                    ],
                },
                {
                    "step_id": "comment_tracking_ticket",
                    "description": "Annotate the security tracking ticket with the containment outcome.",
                    "tool": "jira.add_comment",
                    "args": {
                        "issue_id": params.ticket_id,
                        "body": params.ticket_note,
                        "author": "sec-lead",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "issue_id",
                            "equals": params.ticket_id,
                        }
                    ],
                },
                {
                    "step_id": "post_security_summary",
                    "description": "Send a channel update after the containment decision is recorded.",
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
                    "field": f"components.google_admin.oauth_apps.{params.app_id}.status",
                    "equals": "SUSPENDED",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.google_admin.oauth_apps.{params.app_id}.evidence_hold",
                    "equals": True,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.siem.alerts.{params.alert_id}.evidence_preserved",
                    "equals": True,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.siem.cases.{params.case_id}.evidence_refs",
                    "contains": params.alert_id,
                },
                {
                    "kind": "state_equals",
                    "field": (
                        f"components.siem.cases.{params.case_id}."
                        "customer_notification_required"
                    ),
                    "equals": params.notification_required,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.docs.docs.{params.brief_doc_id}.body",
                    "contains": "notification",
                },
                {
                    "kind": "state_contains",
                    "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                    "contains": "Evidence preserved",
                },
                {
                    "kind": "state_contains",
                    "field": f"components.slack.channels.{params.slack_channel}.messages",
                    "contains": "notification decision",
                },
            ],
            "failure_paths": [
                {
                    "name": "containment_requires_preserved_evidence",
                    "trigger_step": "suspend_oauth_app",
                    "recovery_steps": [
                        "preserve_google_evidence",
                        "preserve_siem_evidence",
                    ],
                    "notes": "Re-establish evidence preservation before retrying containment.",
                }
            ],
            "tags": ["benchmark-family", "security", "containment", variant_name],
            "metadata": {
                "benchmark_family": "security_containment",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


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


_WORKFLOW_BUILDERS = {
    "security_containment": _build_security_containment_spec,
    "enterprise_onboarding_migration": _build_enterprise_onboarding_spec,
    "revenue_incident_mitigation": _build_revenue_incident_spec,
    "identity_access_governance": _build_identity_access_governance_spec,
    "real_estate_management": _build_real_estate_management_spec,
    "digital_marketing_agency": _build_digital_marketing_agency_spec,
    "storage_solutions": _build_storage_solutions_spec,
    "b2b_saas": _build_b2b_saas_spec,
    "service_ops": _build_service_ops_spec,
}


def _parameter_value_type(value: str | int | float | bool) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "str"


def _resolve_variant_metadata(
    family_name: str, definition: _VariantDefinition
) -> tuple[str, str]:
    """Return (title, description), falling back to the vertical variant."""
    if definition.title and definition.description:
        return definition.title, definition.description
    try:
        from vei.verticals import get_vertical_scenario_variant

        vsv = get_vertical_scenario_variant(family_name, definition.name)
        title = definition.title or vsv.title
        desc = definition.description or vsv.description
        return title, desc
    except (KeyError, ImportError):
        return definition.title or definition.name, definition.description or ""


def _variant_manifest(
    family_name: str, definition: _VariantDefinition
) -> BenchmarkWorkflowVariantManifest:
    descriptions = _PARAMETER_DESCRIPTIONS[family_name]
    parameters = [
        BenchmarkWorkflowParameter(
            name=name,
            value=value,
            value_type=_parameter_value_type(value),
            description=descriptions.get(name),
        )
        for name, value in definition.parameters.model_dump(mode="python").items()
    ]
    title, desc = _resolve_variant_metadata(family_name, definition)
    return BenchmarkWorkflowVariantManifest(
        family_name=family_name,
        workflow_name=family_name,
        variant_name=definition.name,
        title=title,
        description=desc,
        scenario_name=definition.scenario_name,
        parameters=parameters,
    )


def _resolve_variant_name(family_name: str, variant_name: Optional[str]) -> str:
    catalog = _VARIANT_CATALOG[family_name]
    if variant_name is None:
        return next(iter(catalog))
    key = variant_name.strip().lower()
    if key not in catalog:
        raise KeyError(f"unknown workflow variant for {family_name}: {variant_name}")
    return key


def get_benchmark_family_workflow_spec(
    name: str,
    variant_name: Optional[str] = None,
    parameter_overrides: Optional[Dict[str, Any]] = None,
) -> WorkflowScenarioSpec:
    key = name.strip().lower()
    if key not in _VARIANT_CATALOG:
        raise KeyError(f"unknown benchmark family workflow: {name}")
    resolved_variant = _resolve_variant_name(key, variant_name)
    definition = _VARIANT_CATALOG[key][resolved_variant]
    builder = _WORKFLOW_BUILDERS[key]
    params = definition.parameters.model_copy(deep=True)
    if parameter_overrides:
        params = params.model_copy(update=dict(parameter_overrides))
    return builder(params, variant_name=resolved_variant)


def list_benchmark_family_workflow_specs() -> List[WorkflowScenarioSpec]:
    return [
        get_benchmark_family_workflow_spec(name) for name in sorted(_VARIANT_CATALOG)
    ]


def get_benchmark_family_workflow_variant(
    family_name: str, variant_name: str
) -> BenchmarkWorkflowVariantManifest:
    key = family_name.strip().lower()
    if key not in _VARIANT_CATALOG:
        raise KeyError(f"unknown benchmark family workflow: {family_name}")
    resolved_variant = _resolve_variant_name(key, variant_name)
    return _variant_manifest(key, _VARIANT_CATALOG[key][resolved_variant])


def list_benchmark_family_workflow_variants(
    family_name: Optional[str] = None,
) -> List[BenchmarkWorkflowVariantManifest]:
    family_names = (
        [family_name.strip().lower()]
        if family_name is not None
        else sorted(_VARIANT_CATALOG)
    )
    variants: List[BenchmarkWorkflowVariantManifest] = []
    for key in family_names:
        if key not in _VARIANT_CATALOG:
            raise KeyError(f"unknown benchmark family workflow: {family_name}")
        for variant_name in _VARIANT_CATALOG[key]:
            variants.append(_variant_manifest(key, _VARIANT_CATALOG[key][variant_name]))
    return variants


def resolve_benchmark_workflow_name(
    *,
    family_name: Optional[str] = None,
    scenario_name: Optional[str] = None,
) -> Optional[str]:
    if family_name:
        key = family_name.strip().lower()
        return key if key in _VARIANT_CATALOG else None
    if scenario_name:
        return _SCENARIO_TO_WORKFLOW.get(scenario_name.strip())
    return None


__all__ = [
    "BenchmarkWorkflowVariantManifest",
    "get_benchmark_family_workflow_spec",
    "get_benchmark_family_workflow_variant",
    "list_benchmark_family_workflow_specs",
    "list_benchmark_family_workflow_variants",
    "resolve_benchmark_workflow_name",
]
