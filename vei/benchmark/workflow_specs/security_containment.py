from __future__ import annotations

from vei.benchmark.workflow_models import SecurityContainmentWorkflowParams
from vei.scenario_engine.api import WorkflowScenarioSpec


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


__all__ = ["_build_security_containment_spec"]
