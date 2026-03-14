from __future__ import annotations

import json
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List

from vei.benchmark.families import get_benchmark_family_manifest
from vei.world.models import WorldState
from vei.world.scenarios import get_scenario


def score_enterprise_dimensions(
    *,
    scenario_name: str,
    artifacts_dir: Path,
    raw_score: Dict[str, Any],
    state: WorldState | None,
    family_name: str | None = None,
) -> Dict[str, Any]:
    scenario = get_scenario(scenario_name)
    metadata = getattr(scenario, "metadata", {}) or {}
    benchmark_family = family_name or metadata.get("benchmark_family")
    if not isinstance(benchmark_family, str) or not benchmark_family:
        return {}

    try:
        manifest = get_benchmark_family_manifest(benchmark_family)
    except KeyError:
        return {}
    calls = _load_trace_calls(artifacts_dir)
    dimensions: Dict[str, float]
    if manifest.name == "security_containment":
        dimensions = _score_security_containment(calls, state)
    elif manifest.name == "enterprise_onboarding_migration":
        dimensions = _score_onboarding_migration(calls, state)
    elif manifest.name == "identity_access_governance":
        dimensions = _score_identity_access_governance(
            calls,
            state,
            artifacts_dir,
            raw_score,
        )
    elif manifest.name == "revenue_incident_mitigation":
        dimensions = _score_revenue_incident(calls, state)
    elif manifest.name == "real_estate_management":
        dimensions = _score_real_estate_management(calls, state)
    elif manifest.name == "digital_marketing_agency":
        dimensions = _score_digital_marketing_agency(calls, state)
    elif manifest.name == "storage_solutions":
        dimensions = _score_storage_solutions(calls, state)
    else:
        dimensions = {}

    primary = {
        key: value
        for key, value in dimensions.items()
        if key in set(manifest.primary_dimensions)
    }
    composite = mean(primary.values()) if primary else 0.0
    success = bool(primary) and min(primary.values()) >= 0.6
    return {
        "success": success,
        "composite_score": composite,
        "dimensions": dimensions,
        "applicable_dimensions": manifest.primary_dimensions,
        "benchmark_family": manifest.name,
        "benchmark_family_title": manifest.title,
        "scenario": scenario_name,
        "scenario_difficulty": metadata.get("difficulty", "acceptance"),
        "steps_taken": len(calls),
        "time_elapsed_ms": _time_elapsed_ms(calls),
        "family_tags": manifest.tags,
        "legacy": False,
        "raw_score": raw_score,
    }


def _score_security_containment(
    calls: List[Dict[str, Any]], state: WorldState | None
) -> Dict[str, float]:
    app_state = _component(state, "google_admin", "oauth_apps")
    alert_state = _component(state, "siem", "alerts")
    case_state = _component(state, "siem", "cases")

    evidence_preserved = any(
        bool(app.get("evidence_hold")) for app in app_state.values()
    ) or any(bool(alert.get("evidence_preserved")) for alert in alert_state.values())
    targeted_containment = _called(calls, "google_admin.suspend_oauth_app")
    impacted_identity_actions = _count_mutations(
        calls,
        {
            "okta.deactivate_user",
            "okta.suspend_user",
            "okta.unassign_application",
            "okta.unassign_group",
        },
    )
    notification_decided = any(
        case.get("customer_notification_required") is not None
        for case in case_state.values()
    )
    stakeholder_updates = _count_prefix(
        calls,
        (
            "tickets.",
            "servicedesk.",
            "docs.",
            "slack.",
            "mail.",
            "siem.update_case",
        ),
    )

    blast_radius = 0.25
    if targeted_containment:
        blast_radius += 0.5
    if impacted_identity_actions <= 1:
        blast_radius += 0.25
    elif impacted_identity_actions >= 3:
        blast_radius -= 0.25

    comms = 0.0
    if notification_decided:
        comms += 0.6
    if stakeholder_updates > 0:
        comms += 0.4

    return {
        "evidence_preservation": 1.0 if evidence_preserved else 0.0,
        "blast_radius_minimization": _clamp(blast_radius),
        "comms_correctness": _clamp(comms),
    }


def _score_onboarding_migration(
    calls: List[Dict[str, Any]], state: WorldState | None
) -> Dict[str, float]:
    employee_state = _component(state, "hris", "employees")
    okta_users = _component(state, "okta", "users")
    drive_state = _component(state, "google_admin", "drive_shares")
    scenario = state.scenario if isinstance(state, WorldState) else {}
    metadata = scenario.get("metadata", {}) if isinstance(scenario, dict) else {}
    allowed_apps = set(
        metadata.get("allowed_application_ids", ["APP-slack", "APP-crm"])
    )

    employees = list(employee_state.values())
    resolved_ratio = _ratio(
        employees,
        lambda employee: not bool(employee.get("identity_conflict", False)),
    )
    onboarded_ratio = _ratio(
        employees, lambda employee: bool(employee.get("onboarded", False))
    )
    deadline = mean([resolved_ratio, onboarded_ratio]) if employees else 0.0
    if len(calls) <= 15 and deadline > 0:
        deadline = min(1.0, deadline + 0.15)

    sales_users = [
        user
        for user in okta_users.values()
        if str(user.get("department", "")).lower() == "sales"
    ]
    least_privilege = _ratio(
        sales_users,
        lambda user: set(user.get("applications", [])).issubset(allowed_apps),
    )

    oversharing = _ratio(
        drive_state.values(),
        lambda share: str(share.get("visibility", "")).lower() != "external_link",
    )

    return {
        "deadline_compliance": _clamp(deadline),
        "least_privilege": _clamp(least_privilege),
        "oversharing_avoidance": _clamp(oversharing),
    }


def _score_revenue_incident(
    calls: List[Dict[str, Any]], state: WorldState | None
) -> Dict[str, float]:
    flags = _component(state, "feature_flags", "flags")
    incidents = _component(state, "pagerduty", "incidents")
    workbooks = _component(state, "spreadsheet", "workbooks")
    docs = _component(state, "docs", "docs")
    tickets = _component(state, "tickets", "tickets")
    ticket_metadata = _component(state, "tickets", "metadata")
    crm_component = state.components.get("crm", {}) if state else {}
    crm_activities = crm_component.get("activities", [])
    risky_writes = _count_mutations(calls, {"db.upsert"})

    targeted_flag_actions = any(
        call["tool"]
        in {
            "feature_flags.set_flag",
            "feature_flags.update_rollout",
        }
        for call in calls
    )
    rollout_reduced = any(
        int(flag.get("rollout_pct", 100)) < 100 for flag in flags.values()
    )
    kill_switch_enabled = any(
        flag.get("flag_key") == "checkout_kill_switch" and bool(flag.get("enabled"))
        for flag in flags.values()
    )
    incident_progressed = any(
        str(incident.get("status", "")).lower() in {"acknowledged", "resolved"}
        for incident in incidents.values()
    )
    stakeholder_updates = _count_prefix(
        calls,
        (
            "tickets.",
            "servicedesk.",
            "docs.",
            "slack.",
            "mail.",
            "pagerduty.",
        ),
    )

    impact_cells_recorded = False
    impact_formula_recorded = False
    impact_rows_recorded = False
    for workbook in workbooks.values():
        sheets = workbook.get("sheets", {})
        for sheet in sheets.values():
            cells = sheet.get("cells", {})
            formulas = sheet.get("formulas", {})
            rows = sheet.get("rows", [])
            if str(cells.get("B2", "")) and str(cells.get("B3", "")):
                impact_cells_recorded = True
            if str(formulas.get("B4", "")).startswith("="):
                impact_formula_recorded = True
            if any(
                "estimated_revenue_loss_usd" in json.dumps(row, sort_keys=True)
                for row in rows
            ):
                impact_rows_recorded = True

    docs_updated = any(
        "Revenue impact has been quantified" in str(doc.get("body", ""))
        for doc in docs.values()
    )
    tickets_resolved = any(
        str(ticket.get("status", "")).lower() == "resolved"
        for ticket in tickets.values()
    )
    ticket_comments_recorded = any(
        len(meta.get("comments", [])) > 0 for meta in ticket_metadata.values()
    )
    crm_logged = any(
        "quantified" in json.dumps(activity, sort_keys=True).lower()
        for activity in (crm_activities if isinstance(crm_activities, list) else [])
    )

    blast_radius = 0.0
    if targeted_flag_actions or rollout_reduced or kill_switch_enabled:
        blast_radius += 0.75
    if risky_writes == 0:
        blast_radius += 0.25

    comms = 0.0
    if incident_progressed:
        comms += 0.5
    if stakeholder_updates > 0:
        comms += 0.5

    safe_rollback = 0.0
    if rollout_reduced or kill_switch_enabled:
        safe_rollback += 0.6
    if risky_writes == 0:
        safe_rollback += 0.4

    revenue_impact = 0.0
    if impact_cells_recorded:
        revenue_impact += 0.4
    if impact_rows_recorded:
        revenue_impact += 0.3
    if impact_formula_recorded:
        revenue_impact += 0.3

    follow_through = 0.0
    if docs_updated:
        follow_through += 0.3
    if tickets_resolved and ticket_comments_recorded:
        follow_through += 0.35
    if crm_logged:
        follow_through += 0.35

    return {
        "blast_radius_minimization": _clamp(blast_radius),
        "comms_correctness": _clamp(comms),
        "revenue_impact_handling": _clamp(revenue_impact),
        "artifact_follow_through": _clamp(follow_through),
        "safe_rollback": _clamp(safe_rollback),
    }


def _score_identity_access_governance(
    calls: List[Dict[str, Any]],
    state: WorldState | None,
    artifacts_dir: Path,
    raw_score: Dict[str, Any],
) -> Dict[str, float]:
    validation = _load_json(artifacts_dir / "workflow_validation.json")
    drive_state = _component(state, "google_admin", "drive_shares")
    docs = _component(state, "docs", "docs")
    slack_channels = _component(state, "slack", "channels")
    ticket_metadata = _component(state, "tickets", "metadata")
    scenario = state.scenario if isinstance(state, WorldState) else {}
    metadata = scenario.get("metadata", {}) if isinstance(scenario, dict) else {}
    forbidden_domains = [
        str(domain).lower() for domain in metadata.get("forbidden_share_domains", [])
    ]

    if validation:
        total = max(
            1,
            int(validation.get("success_assertion_count", 0))
            + int(validation.get("forbidden_predicate_count", 0)),
        )
        passed = int(validation.get("success_assertions_passed", 0)) + max(
            0,
            int(validation.get("forbidden_predicate_count", 0))
            - int(validation.get("forbidden_predicates_failed", 0)),
        )
        contract_alignment = _clamp(passed / total)
    else:
        contract_alignment = 1.0 if raw_score.get("success") else 0.0

    hygiene_checks: List[float] = []
    for share in drive_state.values():
        visibility = str(share.get("visibility", "")).lower()
        shared_with = [str(value).lower() for value in share.get("shared_with", [])]
        hygiene_checks.append(1.0 if visibility != "external_link" else 0.0)
        if forbidden_domains:
            hygiene_checks.append(
                1.0
                if not any(
                    any(entry.endswith(domain) for domain in forbidden_domains)
                    for entry in shared_with
                )
                else 0.0
            )
    policy_hygiene = mean(hygiene_checks) if hygiene_checks else contract_alignment

    artifact_checks: List[float] = []
    if docs:
        artifact_checks.append(
            _ratio(
                list(docs.values()),
                lambda doc: any(
                    marker in str(doc.get("body", "")).lower()
                    for marker in ("policy", "imported", "break-glass")
                ),
            )
        )
    if slack_channels:
        artifact_checks.append(
            _ratio(
                list(slack_channels.values()),
                lambda channel: len(channel.get("messages", [])) >= 2,
            )
        )
    if ticket_metadata:
        artifact_checks.append(
            _ratio(
                list(ticket_metadata.values()),
                lambda ticket: len(ticket.get("comments", [])) > 0,
            )
        )
    artifact_follow_through = mean(artifact_checks) if artifact_checks else 0.0

    return {
        "contract_alignment": _clamp(contract_alignment),
        "policy_hygiene": _clamp(policy_hygiene),
        "artifact_follow_through": _clamp(artifact_follow_through),
    }


def _score_real_estate_management(
    calls: List[Dict[str, Any]], state: WorldState | None
) -> Dict[str, float]:
    property_ops = state.components.get("property_ops", {}) if state else {}
    docs = _component(state, "docs", "docs")
    ticket_metadata = _component(state, "tickets", "metadata")
    leases = property_ops.get("leases", {})
    units = property_ops.get("units", {})
    work_orders = property_ops.get("work_orders", {})

    tenant_ready = 0.0
    if any(
        str(lease.get("milestone", "")).lower() == "executed"
        for lease in leases.values()
    ):
        tenant_ready += 0.5
    if any(
        str(unit.get("status", "")).lower() == "reserved" for unit in units.values()
    ):
        tenant_ready += 0.5

    operational_consistency = 0.0
    if any(bool(order.get("vendor_id")) for order in work_orders.values()):
        operational_consistency += 0.5
    if (
        _count_mutations(calls, {"property.assign_vendor", "property.reserve_unit"})
        >= 2
    ):
        operational_consistency += 0.5

    artifact_follow_through = 0.0
    if any(
        "lease amendment executed" in str(doc.get("body", "")).lower()
        for doc in docs.values()
    ):
        artifact_follow_through += 0.5
    if any(len(meta.get("comments", [])) > 0 for meta in ticket_metadata.values()):
        artifact_follow_through += 0.5

    return {
        "tenant_readiness": _clamp(tenant_ready),
        "operational_consistency": _clamp(operational_consistency),
        "artifact_follow_through": _clamp(artifact_follow_through),
    }


def _score_digital_marketing_agency(
    calls: List[Dict[str, Any]], state: WorldState | None
) -> Dict[str, float]:
    campaign_ops = state.components.get("campaign_ops", {}) if state else {}
    docs = _component(state, "docs", "docs")
    ticket_metadata = _component(state, "tickets", "metadata")
    crm_component = state.components.get("crm", {}) if state else {}
    campaigns = campaign_ops.get("campaigns", {})
    creatives = campaign_ops.get("creatives", {})
    reports = campaign_ops.get("reports", {})

    launch_safety = 0.0
    if any(
        str(creative.get("status", "")).lower() == "approved"
        for creative in creatives.values()
    ):
        launch_safety += 0.5
    if any(
        str(report.get("stale", False)).lower() == "false"
        or not bool(report.get("stale", False))
        for report in reports.values()
    ):
        launch_safety += 0.5

    budget_hygiene = 0.0
    if any(
        float(campaign.get("pacing_pct", 999)) <= 100.0
        for campaign in campaigns.values()
    ):
        budget_hygiene += 0.7
    if _called(calls, "campaign.adjust_budget_pacing"):
        budget_hygiene += 0.3

    artifact_follow_through = 0.0
    if any(
        "creative approval complete" in str(doc.get("body", "")).lower()
        for doc in docs.values()
    ):
        artifact_follow_through += 0.4
    if any(len(meta.get("comments", [])) > 0 for meta in ticket_metadata.values()):
        artifact_follow_through += 0.3
    activities = crm_component.get("activities", [])
    if isinstance(activities, list) and any(
        "launch risk reduced" in json.dumps(item).lower() for item in activities
    ):
        artifact_follow_through += 0.3

    return {
        "launch_safety": _clamp(launch_safety),
        "budget_hygiene": _clamp(budget_hygiene),
        "artifact_follow_through": _clamp(artifact_follow_through),
    }


def _score_storage_solutions(
    calls: List[Dict[str, Any]], state: WorldState | None
) -> Dict[str, float]:
    inventory_ops = state.components.get("inventory_ops", {}) if state else {}
    docs = _component(state, "docs", "docs")
    ticket_metadata = _component(state, "tickets", "metadata")
    crm_component = state.components.get("crm", {}) if state else {}
    quotes = inventory_ops.get("quotes", {})
    allocations = inventory_ops.get("allocations", {})
    orders = inventory_ops.get("orders", {})

    capacity_feasibility = 0.0
    if allocations:
        capacity_feasibility += 0.6
    if _called(calls, "inventory.allocate_capacity"):
        capacity_feasibility += 0.4

    quote_accuracy = 0.0
    if any(
        int(quote.get("committed_units", 0)) >= int(quote.get("requested_units", 0))
        for quote in quotes.values()
    ):
        quote_accuracy += 0.6
    if _called(calls, "inventory.revise_quote"):
        quote_accuracy += 0.4

    artifact_follow_through = 0.0
    if any(
        "capacity reserved" in str(doc.get("body", "")).lower() for doc in docs.values()
    ):
        artifact_follow_through += 0.4
    if any(len(meta.get("comments", [])) > 0 for meta in ticket_metadata.values()):
        artifact_follow_through += 0.3
    activities = crm_component.get("activities", [])
    if isinstance(activities, list) and any(
        "quote risk reduced" in json.dumps(item).lower() for item in activities
    ):
        artifact_follow_through += 0.3
    if any(bool(order.get("vendor_id")) for order in orders.values()):
        artifact_follow_through += 0.2

    return {
        "capacity_feasibility": _clamp(capacity_feasibility),
        "quote_accuracy": _clamp(quote_accuracy),
        "artifact_follow_through": _clamp(artifact_follow_through),
    }


def _load_trace_calls(artifacts_dir: Path) -> List[Dict[str, Any]]:
    trace_path = artifacts_dir / "trace.jsonl"
    calls: List[Dict[str, Any]] = []
    if not trace_path.exists():
        return calls
    for raw in trace_path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        record = json.loads(raw)
        if record.get("type") == "call" and isinstance(record.get("tool"), str):
            calls.append(record)
    return calls


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _component(
    state: WorldState | None, component_name: str, field_name: str
) -> Dict[str, Dict[str, Any]]:
    if state is None:
        return {}
    component = state.components.get(component_name, {})
    value = component.get(field_name, {})
    return value if isinstance(value, dict) else {}


def _called(calls: List[Dict[str, Any]], tool: str) -> bool:
    return any(call.get("tool") == tool for call in calls)


def _count_prefix(calls: List[Dict[str, Any]], prefixes: tuple[str, ...]) -> int:
    count = 0
    for call in calls:
        tool = str(call.get("tool", ""))
        if tool in prefixes:
            count += 1
            continue
        if any(tool.startswith(prefix) for prefix in prefixes if prefix.endswith(".")):
            count += 1
    return count


def _count_mutations(calls: List[Dict[str, Any]], tools: set[str]) -> int:
    return sum(1 for call in calls if str(call.get("tool")) in tools)


def _ratio(items: List[Dict[str, Any]], predicate: Any) -> float:
    if not items:
        return 0.0
    matches = sum(1 for item in items if predicate(item))
    return matches / len(items)


def _time_elapsed_ms(calls: List[Dict[str, Any]]) -> int:
    if not calls:
        return 0
    first = int(calls[0].get("time_ms", 0))
    last = int(calls[-1].get("time_ms", 0))
    return max(0, last - first)


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


__all__ = ["score_enterprise_dimensions"]
