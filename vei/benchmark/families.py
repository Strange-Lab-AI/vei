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
