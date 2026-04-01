from __future__ import annotations

from typing import Dict, List, Sequence

from vei.benchmark.models import BenchmarkFamilyManifest

_FAMILY_CATALOG: Dict[str, BenchmarkFamilyManifest] = {
    "security_containment": BenchmarkFamilyManifest(
        name="security_containment",
        title="Security Containment",
        description=(
            "Contain high-risk security incidents while preserving evidence, limiting "
            "blast radius, and making the right notification decisions."
        ),
        workflow_name="security_containment",
        primary_workflow_variant="customer_notify",
        workflow_variants=["customer_notify", "internal_only_review"],
        scenario_names=["oauth_app_containment"],
        primary_dimensions=[
            "evidence_preservation",
            "blast_radius_minimization",
            "comms_correctness",
        ],
        tags=["security", "incident-response", "containment"],
    ),
    "enterprise_onboarding_migration": BenchmarkFamilyManifest(
        name="enterprise_onboarding_migration",
        title="Enterprise Onboarding And Migration",
        description=(
            "Migrate acquired or reorganized employees into production systems while "
            "preserving least privilege, avoiding oversharing, and meeting deadlines."
        ),
        workflow_name="enterprise_onboarding_migration",
        primary_workflow_variant="manager_cutover",
        workflow_variants=["manager_cutover", "alias_cutover"],
        scenario_names=["acquired_sales_onboarding"],
        primary_dimensions=[
            "deadline_compliance",
            "least_privilege",
            "oversharing_avoidance",
        ],
        tags=["onboarding", "migration", "identity"],
    ),
    "identity_access_governance": BenchmarkFamilyManifest(
        name="identity_access_governance",
        title="Identity Access Governance",
        description=(
            "Resolve identity-policy drift across imported org state while preserving "
            "policy hygiene, completing the required artifact trail, and satisfying "
            "scenario contracts."
        ),
        workflow_name="identity_access_governance",
        primary_workflow_variant="oversharing_remediation",
        workflow_variants=[
            "oversharing_remediation",
            "approval_bottleneck",
            "stale_entitlement_cleanup",
            "break_glass_follow_up",
        ],
        scenario_names=["acquired_sales_onboarding"],
        primary_dimensions=[
            "contract_alignment",
            "policy_hygiene",
            "artifact_follow_through",
        ],
        tags=["identity", "governance", "imports", "least-privilege"],
    ),
    "revenue_incident_mitigation": BenchmarkFamilyManifest(
        name="revenue_incident_mitigation",
        title="Revenue Incident Mitigation",
        description=(
            "Mitigate revenue-critical operational failures using targeted control-plane "
            "actions, quantified impact analysis, and coordinated recovery artifacts."
        ),
        workflow_name="revenue_incident_mitigation",
        primary_workflow_variant="revenue_ops_flightdeck",
        workflow_variants=[
            "revenue_ops_flightdeck",
            "kill_switch_backstop",
            "canary_floor",
        ],
        scenario_names=["checkout_spike_mitigation"],
        primary_dimensions=[
            "blast_radius_minimization",
            "revenue_impact_handling",
            "artifact_follow_through",
            "safe_rollback",
        ],
        tags=["reliability", "incident-response", "revenue", "mixed-stack"],
    ),
    "real_estate_management": BenchmarkFamilyManifest(
        name="real_estate_management",
        title="Real Estate Management",
        description=(
            "Keep a high-value tenant opening on track by resolving lease, vendor, "
            "and property-readiness blockers without losing the artifact trail."
        ),
        workflow_name="real_estate_management",
        primary_workflow_variant="tenant_opening_conflict",
        workflow_variants=[
            "tenant_opening_conflict",
            "vendor_no_show",
            "lease_revision_late",
            "double_booked_unit",
        ],
        scenario_names=[
            "tenant_opening_conflict",
            "vendor_no_show",
            "lease_revision_late",
            "double_booked_unit",
        ],
        primary_dimensions=[
            "tenant_readiness",
            "operational_consistency",
            "artifact_follow_through",
        ],
        tags=["vertical", "real-estate", "property", "opening"],
    ),
    "digital_marketing_agency": BenchmarkFamilyManifest(
        name="digital_marketing_agency",
        title="Digital Marketing Agency",
        description=(
            "Launch a major client campaign only when approvals, pacing, and "
            "reporting artifacts are safe and current."
        ),
        workflow_name="digital_marketing_agency",
        primary_workflow_variant="campaign_launch_guardrail",
        workflow_variants=[
            "campaign_launch_guardrail",
            "creative_not_approved",
            "budget_runaway",
            "client_reporting_mismatch",
        ],
        scenario_names=[
            "campaign_launch_guardrail",
            "creative_not_approved",
            "budget_runaway",
            "client_reporting_mismatch",
        ],
        primary_dimensions=[
            "launch_safety",
            "budget_hygiene",
            "artifact_follow_through",
        ],
        tags=["vertical", "marketing", "campaigns", "launch"],
    ),
    "storage_solutions": BenchmarkFamilyManifest(
        name="storage_solutions",
        title="Storage Solutions",
        description=(
            "Commit strategic storage capacity only after feasible allocation, "
            "quote alignment, and downstream ops follow-through are all in place."
        ),
        workflow_name="storage_solutions",
        primary_workflow_variant="capacity_quote_commitment",
        workflow_variants=[
            "capacity_quote_commitment",
            "vendor_dispatch_gap",
            "fragmented_capacity",
            "overcommit_quote_risk",
        ],
        scenario_names=[
            "capacity_quote_commitment",
            "vendor_dispatch_gap",
            "fragmented_capacity",
            "overcommit_quote_risk",
        ],
        primary_dimensions=[
            "capacity_feasibility",
            "quote_accuracy",
            "artifact_follow_through",
        ],
        tags=["vertical", "storage", "capacity", "quotes"],
    ),
    "service_ops": BenchmarkFamilyManifest(
        name="service_ops",
        title="Service Operations",
        description=(
            "Recover a bad service morning by coordinating dispatch, billing safety, "
            "and exception follow-through from one shared operating loop."
        ),
        workflow_name="service_ops",
        primary_workflow_variant="service_day_collision",
        workflow_variants=[
            "service_day_collision",
            "technician_no_show",
            "billing_dispute_reopened",
        ],
        scenario_names=[
            "service_day_collision",
            "technician_no_show",
            "billing_dispute_reopened",
        ],
        primary_dimensions=[
            "dispatch_recovery",
            "billing_safety",
            "artifact_follow_through",
        ],
        tags=["vertical", "service", "dispatch", "billing"],
    ),
}


def get_benchmark_family_manifest(name: str) -> BenchmarkFamilyManifest:
    key = name.strip().lower()
    if key not in _FAMILY_CATALOG:
        raise KeyError(f"unknown benchmark family: {name}")
    return _FAMILY_CATALOG[key]


def list_benchmark_family_manifest() -> List[BenchmarkFamilyManifest]:
    return sorted(_FAMILY_CATALOG.values(), key=lambda item: item.name)


def resolve_family_scenarios(family_names: Sequence[str]) -> List[str]:
    resolved: List[str] = []
    seen: set[str] = set()
    for name in family_names:
        manifest = get_benchmark_family_manifest(name)
        for scenario_name in manifest.scenario_names:
            if scenario_name in seen:
                continue
            seen.add(scenario_name)
            resolved.append(scenario_name)
    return resolved


__all__ = [
    "BenchmarkFamilyManifest",
    "get_benchmark_family_manifest",
    "list_benchmark_family_manifest",
    "resolve_family_scenarios",
]
