from __future__ import annotations

import pytest

from vei.scenario_engine.api import compile_workflow
from vei.scenario_runner.api import (
    run_workflow,
    validate_workflow,
    validate_workflow_outcome,
)
from vei.world.api import create_world_session, get_catalog_scenario


def _workflow_spec() -> dict:
    return {
        "name": "workflow-runner-test",
        "objective": {
            "statement": "Request quote and post approval",
            "success": ["mail sent", "approval posted"],
        },
        "world": {"catalog": "multi_channel"},
        "actors": [
            {"actor_id": "agent", "role": "procurement_operator"},
            {"actor_id": "approver", "role": "finance_manager"},
        ],
        "constraints": [
            {
                "name": "budget",
                "description": "budget must be included",
                "required": True,
            }
        ],
        "approvals": [{"stage": "finance", "approver": "approver", "required": True}],
        "steps": [
            {
                "step_id": "read",
                "description": "Read browser",
                "tool": "browser.read",
                "args": {},
            },
            {
                "step_id": "mail",
                "description": "Send quote email",
                "tool": "mail.compose",
                "args": {
                    "to": "sales@macrocompute.example",
                    "subj": "Quote request",
                    "body_text": "Please share quote and ETA.",
                },
                "expect": [{"kind": "result_contains", "field": "id", "contains": "m"}],
            },
            {
                "step_id": "approve",
                "description": "Post approval message",
                "tool": "slack.send_message",
                "args": {
                    "channel": "#procurement",
                    "text": "Please approve budget $2400 with quote evidence.",
                },
                "expect": [{"kind": "result_contains", "field": "ts", "contains": ""}],
            },
        ],
        "success_assertions": [
            {"kind": "pending_max", "field": "total", "max_value": 20}
        ],
        "tags": ["unit-test"],
    }


def test_compile_and_run_workflow_success() -> None:
    compiled = compile_workflow(_workflow_spec(), seed=99)
    result = run_workflow(compiled, seed=99, connector_mode="sim")

    assert result.static_validation.ok
    assert result.dynamic_validation.ok
    assert result.ok
    assert result.branch == "main"
    assert result.initial_snapshot_id is not None
    assert result.final_snapshot_id is not None
    assert result.initial_snapshot_id <= result.final_snapshot_id
    assert result.initial_snapshot_label == "workflow.start"
    assert result.final_snapshot_label == "workflow.final"
    assert len(result.steps) == 3
    assert all(step.ok for step in result.steps)


def test_validate_workflow_flags_unknown_tool() -> None:
    spec = _workflow_spec()
    spec["steps"][1]["tool"] = "mail.unknown_operation"
    compiled = compile_workflow(spec, seed=1)
    report = validate_workflow(
        compiled, available_tools=["browser.read", "mail.compose"]
    )
    assert not report.ok
    assert any(issue.code == "tool.unavailable" for issue in report.issues)


def test_run_workflow_enforces_success_assertions() -> None:
    spec = _workflow_spec()
    spec["success_assertions"] = [
        {"kind": "pending_max", "field": "total", "max_value": -1}
    ]
    compiled = compile_workflow(spec, seed=88)
    result = run_workflow(compiled, seed=88, connector_mode="sim")

    assert not result.ok
    assert any(
        issue.code == "success_assertion.failed"
        for issue in result.dynamic_validation.issues
    )


def test_run_workflow_supports_salesforce_alias_and_db_steps(
    monkeypatch,
) -> None:
    monkeypatch.setenv("VEI_CRM_ALIAS_PACKS", "salesforce")
    spec = {
        "name": "workflow-salesforce-db",
        "objective": {
            "statement": "Create opportunity and verify db audit records.",
            "success": ["opportunity created", "db queried"],
        },
        "world": {"catalog": "multi_channel"},
        "actors": [{"actor_id": "agent", "role": "procurement_operator"}],
        "steps": [
            {
                "step_id": "create_opp",
                "description": "Create Salesforce opportunity",
                "tool": "salesforce.opportunity.create",
                "args": {"name": "Renewal FY27", "amount": 100000},
                "expect": [
                    {"kind": "result_contains", "field": "id", "contains": "D-"}
                ],
            },
            {
                "step_id": "query_db",
                "description": "Query approval audit table",
                "tool": "db.query",
                "args": {"table": "approval_audit", "limit": 5},
                "expect": [
                    {
                        "kind": "result_contains",
                        "field": "table",
                        "contains": "approval_audit",
                    }
                ],
            },
        ],
        "success_assertions": [
            {"kind": "pending_max", "field": "total", "max_value": 20}
        ],
    }
    compiled = compile_workflow(spec, seed=99)
    result = run_workflow(compiled, seed=99, connector_mode="sim")
    assert result.ok
    assert all(step.ok for step in result.steps)


def test_run_workflow_supports_state_assertions_and_list_indexing() -> None:
    spec = {
        "name": "workflow-state-assertions",
        "objective": {
            "statement": "Restrict an inherited drive share.",
            "success": ["share restricted"],
        },
        "world": {"catalog": "acquired_sales_onboarding"},
        "steps": [
            {
                "step_id": "restrict_share",
                "description": "Restrict inherited sharing",
                "tool": "google_admin.restrict_drive_share",
                "args": {
                    "doc_id": "GDRIVE-2201",
                    "visibility": "internal",
                    "note": "Reduce oversharing during migration.",
                },
                "expect": [
                    {
                        "kind": "result_equals",
                        "field": "shared_with_count",
                        "equals": 1,
                    },
                    {
                        "kind": "state_equals",
                        "field": "components.google_admin.drive_shares.GDRIVE-2201.visibility",
                        "equals": "internal",
                    },
                    {
                        "kind": "state_equals",
                        "field": "components.google_admin.drive_shares.GDRIVE-2201.shared_with.0",
                        "equals": "maya.rex@example.com",
                    },
                ],
            }
        ],
        "success_assertions": [
            {
                "kind": "state_exists",
                "field": "components.google_admin.drive_shares.GDRIVE-2201.history.0",
            }
        ],
    }
    compiled = compile_workflow(spec, seed=55)
    result = run_workflow(compiled, seed=55, connector_mode="sim")

    assert result.ok
    assert result.steps[0].ok
    assert (
        result.final_state["components"]["google_admin"]["drive_shares"]["GDRIVE-2201"][
            "visibility"
        ]
        == "internal"
    )


def test_run_workflow_supports_negative_count_and_time_assertions() -> None:
    spec = {
        "name": "workflow-negative-count-time",
        "objective": {
            "statement": "Restrict sharing before a virtual deadline.",
            "success": ["share restricted before deadline"],
        },
        "world": {"catalog": "acquired_sales_onboarding"},
        "steps": [
            {
                "step_id": "restrict_share",
                "description": "Restrict inherited sharing",
                "tool": "google_admin.restrict_drive_share",
                "args": {
                    "doc_id": "GDRIVE-2201",
                    "visibility": "internal",
                    "note": "Remove external share before migration deadline.",
                },
                "expect": [
                    {
                        "kind": "state_not_contains",
                        "field": "components.google_admin.drive_shares.GDRIVE-2201.shared_with",
                        "contains": "channel-partner@example.net",
                    },
                    {
                        "kind": "state_count_equals",
                        "field": "components.google_admin.drive_shares.GDRIVE-2201.shared_with",
                        "equals": 1,
                    },
                ],
            }
        ],
        "success_assertions": [
            {"kind": "time_max_ms", "max_value": 10_000},
            {
                "kind": "state_count_max",
                "field": "components.google_admin.drive_shares.GDRIVE-2201.shared_with",
                "max_value": 1,
            },
        ],
    }
    compiled = compile_workflow(spec, seed=77)
    result = run_workflow(compiled, seed=77, connector_mode="sim")

    assert result.ok
    assert result.steps[0].ok


def test_compile_workflow_rejects_malformed_temporal_assertion() -> None:
    spec = _workflow_spec()
    spec["success_assertions"] = [{"kind": "time_max_ms"}]

    with pytest.raises(ValueError):
        compile_workflow(spec, seed=12)


def test_run_workflow_fails_virtual_deadline_assertion() -> None:
    spec = _workflow_spec()
    spec["success_assertions"] = [{"kind": "time_max_ms", "max_value": -1}]
    compiled = compile_workflow(spec, seed=56)

    result = run_workflow(compiled, seed=56, connector_mode="sim")

    assert not result.ok
    assert any(
        "workflow time" in issue.message for issue in result.dynamic_validation.issues
    )


def test_validate_workflow_outcome_reuses_success_assertions() -> None:
    spec = {
        "name": "workflow-outcome-validation",
        "objective": {
            "statement": "Restrict sharing before a virtual deadline.",
            "success": ["share restricted before deadline"],
        },
        "world": {"catalog": "acquired_sales_onboarding"},
        "steps": [
            {
                "step_id": "restrict_share",
                "description": "Restrict inherited sharing",
                "tool": "google_admin.restrict_drive_share",
                "args": {
                    "doc_id": "GDRIVE-2201",
                    "visibility": "internal",
                    "note": "Remove external share before migration deadline.",
                },
            }
        ],
        "success_assertions": [
            {"kind": "time_max_ms", "max_value": 10_000},
            {
                "kind": "state_count_max",
                "field": "components.google_admin.drive_shares.GDRIVE-2201.shared_with",
                "max_value": 1,
            },
        ],
    }
    compiled = compile_workflow(spec, seed=73)
    result = run_workflow(compiled, seed=73, connector_mode="sim")

    validation = validate_workflow_outcome(
        compiled,
        state=result.final_state,
        time_ms=int(result.metadata.get("time_ms", 0)),
        available_tools=["google_admin.restrict_drive_share"],
    )

    assert validation.ok
    assert validation.success_assertion_count == 2
    assert validation.success_assertions_passed == 2
    assert validation.success_assertions_failed == 0


def test_validate_workflow_outcome_reports_failed_success_assertions() -> None:
    spec = {
        "name": "workflow-outcome-validation-fail",
        "objective": {
            "statement": "Restrict sharing before a virtual deadline.",
            "success": ["share restricted before deadline"],
        },
        "world": {"catalog": "acquired_sales_onboarding"},
        "steps": [],
        "success_assertions": [
            {
                "kind": "state_equals",
                "field": "components.google_admin.drive_shares.GDRIVE-2201.visibility",
                "equals": "internal",
            }
        ],
    }
    compiled = compile_workflow(spec, seed=74)
    session = create_world_session(
        seed=74,
        scenario=get_catalog_scenario("acquired_sales_onboarding"),
    )
    validation = validate_workflow_outcome(
        compiled,
        state=session.current_state().model_dump(mode="json"),
        time_ms=0,
    )

    assert not validation.ok
    assert validation.success_assertion_count == 1
    assert validation.success_assertions_passed == 0
    assert validation.success_assertions_failed == 1
    assert any(
        issue.code == "success_assertion.failed"
        for issue in validation.dynamic_validation.issues
    )


def test_run_workflow_static_validation_failure_still_persists_snapshots() -> None:
    spec = _workflow_spec()
    spec["steps"][0]["tool"] = "browser.unknown"
    compiled = compile_workflow(spec, seed=44)

    result = run_workflow(compiled, seed=44, connector_mode="sim")

    assert not result.ok
    assert result.initial_snapshot_id is not None
    assert result.final_snapshot_id is not None
    assert result.initial_snapshot_label == "workflow.start"
    assert result.final_snapshot_label == "workflow.static_invalid"
