from __future__ import annotations

from typing import Any, Iterable, List

from vei.scenario_engine.api import CompiledWorkflow
from vei.scenario_engine.models import AssertionSpec, WorkflowScenarioSpec

from .assertions import evaluate_assertion_specs, infer_assertion_source
from .models import (
    ContractEvaluationResult,
    ContractPredicateSpec,
    ContractSpec,
    ContractValidationIssue,
    ContractValidationReport,
    InterventionRuleSpec,
    ObservationBoundarySpec,
    PolicyInvariantSpec,
    RewardTermSpec,
)

_FORBIDDEN_ASSERTION_KINDS = {
    "result_not_contains",
    "observation_not_contains",
    "state_not_contains",
    "state_count_max",
    "pending_max",
    "time_max_ms",
}

_CATEGORY_PATTERNS = [
    ("service_ops.work_orders", "dispatch"),
    ("service_ops.appointments", "dispatch"),
    ("service_ops.billing_cases", "billing"),
    ("service_ops.exceptions", "billing"),
    ("docs.", "communication"),
    ("tickets.", "communication"),
    ("slack.", "communication"),
    ("mail.", "communication"),
]

_TERMINAL_EXCEPTION_STATUSES = {"resolved", "mitigated", "closed", "completed"}
_ACTIVE_DISPUTE_STATUSES = {"open", "reopened", "disputed"}
_DISPATCH_EXCEPTION_TYPES = {"technician_unavailable", "sla_risk", "schedule_collision"}
_BILLING_EXCEPTION_TYPES = {
    "billing_dispute_open",
    "duplicate_bill_risk",
    "overdue_balance_conflict",
}


def _infer_assertion_category(assertion: "AssertionSpec") -> str:
    if assertion.kind == "time_max_ms":
        return "sla_timing"
    field = assertion.field or ""
    for pattern, category in _CATEGORY_PATTERNS:
        if pattern in field:
            return category
    return "general"


def build_contract_from_workflow(
    workflow: CompiledWorkflow | WorkflowScenarioSpec,
) -> ContractSpec:
    spec = workflow.spec if isinstance(workflow, CompiledWorkflow) else workflow
    scenario_name = None
    if isinstance(spec.world, dict) and spec.world.get("catalog") is not None:
        scenario_name = str(spec.world.get("catalog"))

    success_predicates: List[ContractPredicateSpec] = []
    forbidden_predicates: List[ContractPredicateSpec] = []
    for index, assertion in enumerate(spec.success_assertions, start=1):
        predicate = ContractPredicateSpec(
            name=f"predicate_{index}",
            source=infer_assertion_source(assertion),
            assertion=assertion,
            description=assertion.description,
            metadata={"category": _infer_assertion_category(assertion)},
        )
        if assertion.kind in _FORBIDDEN_ASSERTION_KINDS:
            forbidden_predicates.append(predicate)
        else:
            success_predicates.append(predicate)

    focus_hints = sorted(
        {
            assertion.focus or "summary"
            for assertion in spec.success_assertions
            if assertion.kind.startswith("observation_")
        }
    )
    has_graph_steps = any(
        getattr(step, "graph_domain", None) and getattr(step, "graph_action", None)
        for step in spec.steps
    )
    graph_focus_hints = sorted(
        {
            str(getattr(step, "graph_domain"))
            for step in spec.steps
            if getattr(step, "graph_domain", None)
            and getattr(step, "graph_action", None)
        }
    )
    if graph_focus_hints:
        focus_hints = sorted(set(focus_hints) | set(graph_focus_hints))
    allowed_tools = sorted(
        {step.tool for step in spec.steps if step.tool}
        | ({"vei.graph_action", "vei.graph_plan"} if has_graph_steps else set())
    )
    hidden_state_fields = sorted(
        {
            assertion.field or ""
            for assertion in spec.success_assertions
            if infer_assertion_source(assertion) == "oracle_state" and assertion.field
        }
    )
    observation_boundary = ObservationBoundarySpec(
        allowed_tools=allowed_tools,
        focus_hints=focus_hints or ["summary"],
        hidden_state_fields=hidden_state_fields,
        description="Agent-visible observations remain bounded to declared focuses.",
    )

    policy_invariants = [
        PolicyInvariantSpec(
            name=constraint.name,
            description=constraint.description,
            required=constraint.required,
        )
        for constraint in spec.constraints
    ]
    policy_invariants.extend(
        PolicyInvariantSpec(
            name=f"approval:{approval.stage}",
            description=f"Approval required from {approval.approver} at {approval.stage}",
            required=approval.required,
            evidence=approval.evidence,
        )
        for approval in spec.approvals
    )

    intervention_rules: List[InterventionRuleSpec] = [
        InterventionRuleSpec(
            name=f"approval:{approval.stage}",
            trigger=f"approval:{approval.stage}",
            action="request_approval",
            actor=approval.approver,
            required=approval.required,
            evidence=approval.evidence,
        )
        for approval in spec.approvals
    ]
    intervention_rules.extend(
        InterventionRuleSpec(
            name=path.name,
            trigger=path.trigger_step,
            action=" -> ".join(path.recovery_steps) if path.recovery_steps else "fail",
            actor=None,
            required=True,
            evidence=path.notes,
        )
        for path in spec.failure_paths
    )

    reward_terms: List[RewardTermSpec] = []
    if success_predicates:
        weight = round(1.0 / len(success_predicates), 3)
        reward_terms.extend(
            RewardTermSpec(
                name=predicate.name,
                weight=weight,
                term_type="success",
                description=predicate.description,
            )
            for predicate in success_predicates
        )
    if forbidden_predicates:
        penalty = round(1.0 / len(forbidden_predicates), 3)
        reward_terms.extend(
            RewardTermSpec(
                name=predicate.name,
                weight=penalty,
                term_type="penalty",
                description=predicate.description,
            )
            for predicate in forbidden_predicates
        )

    metadata = dict(spec.metadata or {})
    metadata.update(
        {
            "workflow_tags": list(spec.tags),
            "objective": spec.objective.statement,
            "success_outcomes": list(spec.objective.success),
        }
    )

    return ContractSpec(
        name=f"{spec.name}.contract",
        workflow_name=spec.name,
        scenario_name=scenario_name,
        success_predicates=success_predicates,
        forbidden_predicates=forbidden_predicates,
        observation_boundary=observation_boundary,
        policy_invariants=policy_invariants,
        reward_terms=reward_terms,
        intervention_rules=intervention_rules,
        metadata=metadata,
    )


def evaluate_contract(
    contract: ContractSpec,
    *,
    oracle_state: dict[str, Any],
    visible_observation: dict[str, Any] | None = None,
    result: object | None = None,
    pending: dict[str, int] | None = None,
    time_ms: int = 0,
    available_tools: Iterable[str] | None = None,
    validation_mode: str = "state",
) -> ContractEvaluationResult:
    visible_observation = visible_observation or {}
    pending = pending or {}
    tool_set = set(available_tools or [])
    static_issues: List[ContractValidationIssue] = []
    for tool in contract.observation_boundary.allowed_tools:
        if tool_set and tool not in tool_set:
            static_issues.append(
                ContractValidationIssue(
                    code="tool.unavailable",
                    message=f"Contract requires unavailable tool: {tool}",
                    source="visible_observation",
                    metadata={"tool": tool},
                )
            )

    dynamic_issues: List[ContractValidationIssue] = []
    success_failures = _evaluate_predicates(
        predicates=contract.success_predicates,
        oracle_state=oracle_state,
        visible_observation=visible_observation,
        result=result,
        pending=pending,
        time_ms=time_ms,
        code="success_predicate.failed",
    )
    forbidden_failures = _evaluate_predicates(
        predicates=contract.forbidden_predicates,
        oracle_state=oracle_state,
        visible_observation=visible_observation,
        result=result,
        pending=pending,
        time_ms=time_ms,
        code="forbidden_predicate.violated",
    )
    dynamic_issues.extend(success_failures)
    dynamic_issues.extend(forbidden_failures)
    policy_failures = _evaluate_policy_invariants(
        contract,
        oracle_state=oracle_state,
    )
    dynamic_issues.extend(policy_failures)

    static_report = ContractValidationReport(
        ok=not any(issue.severity == "error" for issue in static_issues),
        issues=static_issues,
    )
    dynamic_report = ContractValidationReport(
        ok=not any(issue.severity == "error" for issue in dynamic_issues),
        issues=dynamic_issues,
    )
    failed_predicate_names = {
        issue.predicate_name
        for issue in success_failures + forbidden_failures
        if issue.predicate_name
    }
    predicate_categories: dict[str, str] = {}
    for pred in contract.success_predicates + contract.forbidden_predicates:
        predicate_categories[pred.name] = pred.metadata.get("category", "general")

    category_weights = dict(contract.metadata.get("category_weights") or {})

    return ContractEvaluationResult(
        ok=static_report.ok and dynamic_report.ok,
        contract_name=contract.name,
        workflow_name=contract.workflow_name,
        static_validation=static_report,
        dynamic_validation=dynamic_report,
        success_predicate_count=len(contract.success_predicates),
        success_predicates_passed=max(
            0, len(contract.success_predicates) - len(success_failures)
        ),
        success_predicates_failed=len(success_failures),
        forbidden_predicate_count=len(contract.forbidden_predicates),
        forbidden_predicates_failed=len(forbidden_failures),
        policy_invariant_count=len(contract.policy_invariants),
        policy_invariants_failed=len(policy_failures),
        metadata={
            "validation_mode": validation_mode,
            "scenario_name": contract.scenario_name,
            "observation_boundary": contract.observation_boundary.model_dump(
                mode="json"
            ),
            "reward_terms": [
                item.model_dump(mode="json") for item in contract.reward_terms
            ],
            "predicate_categories": predicate_categories,
            "failed_predicate_names": sorted(failed_predicate_names),
            "failed_policy_invariants": sorted(
                issue.predicate_name
                for issue in policy_failures
                if issue.predicate_name
            ),
            "category_weights": category_weights,
        },
    )


def _evaluate_policy_invariants(
    contract: ContractSpec,
    *,
    oracle_state: dict[str, Any],
) -> list[ContractValidationIssue]:
    issues: list[ContractValidationIssue] = []
    for invariant in contract.policy_invariants:
        if not invariant.required:
            continue
        failure = _policy_invariant_failure(
            invariant=invariant,
            oracle_state=oracle_state,
        )
        if failure is None:
            continue
        issues.append(
            ContractValidationIssue(
                code="policy_invariant.failed",
                message=failure,
                predicate_name=invariant.name,
                source="oracle_state",
                metadata={"policy_invariant": invariant.name},
            )
        )
    return issues


def _policy_invariant_failure(
    *,
    invariant: PolicyInvariantSpec,
    oracle_state: dict[str, Any],
) -> str | None:
    name = invariant.name.strip().lower()
    if name.startswith("approval:"):
        return None

    components = oracle_state.get("components")
    if not isinstance(components, dict):
        return None

    if name == "dispatch_before_breach":
        return _dispatch_before_breach_failure(components)
    if name == "billing_safety_first":
        return _billing_safety_first_failure(components)
    if name == "single_customer_story":
        return _single_customer_story_failure(components)
    return None


def _dispatch_before_breach_failure(components: dict[str, Any]) -> str | None:
    service_ops = _service_ops_state(components)
    if not service_ops:
        return None

    work_orders = _records(service_ops, "work_orders")
    appointments = _records(service_ops, "appointments")
    exceptions = _records(service_ops, "exceptions")

    for exception_id, issue in exceptions.items():
        issue_type = str(issue.get("type") or "").lower()
        status = str(issue.get("status") or "").lower()
        if issue_type not in _DISPATCH_EXCEPTION_TYPES:
            continue
        if status in _TERMINAL_EXCEPTION_STATUSES:
            continue
        work_order_id = str(issue.get("work_order_id") or "")
        work_order = work_orders.get(work_order_id, {})
        appointment_id = str(work_order.get("appointment_id") or "")
        appointment = appointments.get(appointment_id, {})
        dispatch_status = str(appointment.get("dispatch_status") or "").lower()
        technician_id = str(work_order.get("technician_id") or "")
        if technician_id and dispatch_status == "assigned":
            continue
        return (
            f"dispatch risk remains open on work order {work_order_id or exception_id}; "
            "the backup route is not fully assigned"
        )
    return None


def _billing_safety_first_failure(components: dict[str, Any]) -> str | None:
    service_ops = _service_ops_state(components)
    if not service_ops:
        return None

    billing_cases = _records(service_ops, "billing_cases")
    for billing_case_id, billing_case in billing_cases.items():
        dispute_status = str(billing_case.get("dispute_status") or "").lower()
        if dispute_status not in _ACTIVE_DISPUTE_STATUSES:
            continue
        if bool(billing_case.get("hold")):
            continue
        return (
            f"billing case {billing_case_id} is still live while the dispute is "
            f"{dispute_status}"
        )
    return None


def _single_customer_story_failure(components: dict[str, Any]) -> str | None:
    service_ops = _service_ops_state(components)
    if not service_ops:
        return None

    billing_failure = _billing_safety_first_failure(components)
    if billing_failure is not None:
        return billing_failure

    exceptions = _records(service_ops, "exceptions")
    for exception_id, issue in exceptions.items():
        issue_type = str(issue.get("type") or "").lower()
        status = str(issue.get("status") or "").lower()
        if issue_type not in _BILLING_EXCEPTION_TYPES:
            continue
        if status in _TERMINAL_EXCEPTION_STATUSES:
            continue
        return (
            f"billing exception {exception_id} is still {status}; the customer story "
            "is not yet coherent"
        )
    return None


def _service_ops_state(components: dict[str, Any]) -> dict[str, Any]:
    service_ops = components.get("service_ops")
    if isinstance(service_ops, dict):
        return service_ops
    return {}


def _records(service_ops: dict[str, Any], key: str) -> dict[str, dict[str, Any]]:
    records = service_ops.get(key)
    if not isinstance(records, dict):
        return {}
    return {
        str(record_id): payload
        for record_id, payload in records.items()
        if isinstance(payload, dict)
    }


def _evaluate_predicates(
    *,
    predicates: List[ContractPredicateSpec],
    oracle_state: dict[str, Any],
    visible_observation: dict[str, Any],
    result: object | None,
    pending: dict[str, int],
    time_ms: int,
    code: str,
) -> List[ContractValidationIssue]:
    issues: List[ContractValidationIssue] = []
    for predicate in predicates:
        failures = evaluate_assertion_specs(
            assertions=[predicate.assertion],
            result=result or {},
            observation=visible_observation,
            pending=pending,
            oracle_state=oracle_state,
            time_ms=time_ms,
        )
        for message in failures:
            issues.append(
                ContractValidationIssue(
                    code=code,
                    message=message,
                    predicate_name=predicate.name,
                    source=predicate.source,
                )
            )
    return issues
