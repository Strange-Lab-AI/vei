from __future__ import annotations

from vei.contract.api import build_contract_from_workflow, evaluate_contract
from vei.scenario_engine.api import compile_workflow
from vei.scenario_runner.api import run_workflow, validate_workflow_outcome
from vei.verticals.contract_variants import (
    apply_vertical_contract_variant,
    get_vertical_contract_variant,
)


def test_build_contract_from_workflow_classifies_predicates_and_boundary() -> None:
    workflow = compile_workflow(
        {
            "name": "contract-classification",
            "objective": {
                "statement": "Restrict sharing before a deadline.",
                "success": ["share restricted"],
            },
            "world": {"catalog": "acquired_sales_onboarding"},
            "constraints": [
                {
                    "name": "least_privilege",
                    "description": "Avoid oversharing during cutover.",
                }
            ],
            "approvals": [
                {
                    "stage": "manager",
                    "approver": "maya.rex@example.com",
                    "required": True,
                }
            ],
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
                {
                    "kind": "state_equals",
                    "field": "components.google_admin.drive_shares.GDRIVE-2201.visibility",
                    "equals": "internal",
                },
                {
                    "kind": "state_not_contains",
                    "field": "components.google_admin.drive_shares.GDRIVE-2201.shared_with",
                    "contains": "channel-partner@example.net",
                },
                {"kind": "time_max_ms", "max_value": 10000},
            ],
        },
        seed=41,
    )

    contract = build_contract_from_workflow(workflow)

    assert contract.name == "contract-classification.contract"
    assert contract.scenario_name == "acquired_sales_onboarding"
    assert len(contract.success_predicates) == 1
    assert len(contract.forbidden_predicates) == 2
    assert contract.observation_boundary.allowed_tools == [
        "google_admin.restrict_drive_share"
    ]
    assert contract.policy_invariants
    assert contract.intervention_rules
    assert contract.reward_terms


def test_evaluate_contract_uses_oracle_state_separately_from_visible_observation() -> (
    None
):
    workflow = compile_workflow(
        {
            "name": "contract-oracle-visible",
            "objective": {
                "statement": "Validate a hidden state fact.",
                "success": ["hidden state validated"],
            },
            "world": {"catalog": "multi_channel"},
            "steps": [],
            "success_assertions": [
                {
                    "kind": "state_equals",
                    "field": "components.crm.accounts.0.owner",
                    "equals": "ops@example.com",
                }
            ],
        },
        seed=42,
    )
    contract = build_contract_from_workflow(workflow)

    passing = evaluate_contract(
        contract,
        oracle_state={
            "components": {"crm": {"accounts": [{"owner": "ops@example.com"}]}}
        },
        visible_observation={"summary": "No owner visible here."},
        time_ms=0,
    )
    failing = evaluate_contract(
        contract,
        oracle_state={
            "components": {"crm": {"accounts": [{"owner": "sales@example.com"}]}}
        },
        visible_observation={"summary": "No owner visible here."},
        time_ms=0,
    )

    assert passing.ok
    assert not failing.ok
    assert failing.success_predicates_failed == 1


def test_validate_workflow_outcome_returns_contract_backed_metadata() -> None:
    workflow = compile_workflow(
        {
            "name": "contract-outcome",
            "objective": {
                "statement": "Restrict sharing before a deadline.",
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
                {
                    "kind": "state_not_contains",
                    "field": "components.google_admin.drive_shares.GDRIVE-2201.shared_with",
                    "contains": "channel-partner@example.net",
                },
                {
                    "kind": "state_count_max",
                    "field": "components.google_admin.drive_shares.GDRIVE-2201.shared_with",
                    "max_value": 1,
                },
            ],
        },
        seed=43,
    )
    result = run_workflow(workflow, seed=43, connector_mode="sim")

    assert result.contract_validation is not None
    validation = validate_workflow_outcome(
        workflow,
        oracle_state=result.final_state,
        visible_observation={"summary": "bounded"},
        time_ms=int(result.metadata.get("time_ms", 0)),
        available_tools=["google_admin.restrict_drive_share"],
    )

    assert validation.ok
    assert validation.contract_name == "contract-outcome.contract"
    assert validation.forbidden_predicate_count == 2
    assert validation.forbidden_predicates_failed == 0
    assert validation.metadata["observation_boundary"]["allowed_tools"] == [
        "google_admin.restrict_drive_share"
    ]


def test_service_ops_policy_invariant_failure_affects_contract_result() -> None:
    workflow = compile_workflow(
        {
            "name": "service-ops-policy-contract",
            "objective": {
                "statement": "Keep the service account safe.",
                "success": ["case stays safe"],
            },
            "world": {"catalog": "service_day_collision"},
            "steps": [],
            "success_assertions": [
                {
                    "kind": "state_equals",
                    "field": "components.service_ops.work_orders.WO-1.status",
                    "equals": "open",
                }
            ],
        },
        seed=44,
    )
    contract = apply_vertical_contract_variant(
        build_contract_from_workflow(workflow),
        get_vertical_contract_variant("service_ops", "protect_revenue"),
    )

    validation = evaluate_contract(
        contract,
        oracle_state={
            "components": {
                "service_ops": {
                    "work_orders": {"WO-1": {"status": "open"}},
                    "billing_cases": {
                        "BILL-1": {
                            "billing_case_id": "BILL-1",
                            "dispute_status": "open",
                            "hold": False,
                        }
                    },
                }
            }
        },
        visible_observation={},
        time_ms=0,
    )

    assert validation.ok is False
    assert validation.success_predicates_failed == 0
    assert validation.policy_invariants_failed == 1
    assert validation.metadata["failed_policy_invariants"] == ["billing_safety_first"]
