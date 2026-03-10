from __future__ import annotations

from typing import Dict, List, Optional

from vei.benchmark.families import (
    get_benchmark_family_manifest,
    list_benchmark_family_manifest,
)
from vei.benchmark.workflows import (
    get_benchmark_family_workflow_spec,
    resolve_benchmark_workflow_name,
)
from vei.contract.api import build_contract_from_workflow
from vei.scenario_engine.api import compile_workflow
from vei.world.manifest import get_scenario_manifest

from .models import (
    BlueprintContractSummary,
    BlueprintScenarioSummary,
    BlueprintSpec,
    FacadeManifest,
)


_FACADE_CATALOG: Dict[str, FacadeManifest] = {
    "slack": FacadeManifest(
        name="slack",
        title="Slack",
        domain="comm_graph",
        router_module="vei.router.core",
        description="Chat and thread surface for agent collaboration and approvals.",
        surfaces=["mcp", "chat"],
        primary_tools=["slack.send_message", "slack.read_channel"],
        state_roots=["components.slack"],
        tags=["communication", "chat"],
    ),
    "mail": FacadeManifest(
        name="mail",
        title="Mail",
        domain="comm_graph",
        router_module="vei.router.core",
        description="Email inbox and compose surface for threaded external communication.",
        surfaces=["mcp", "email"],
        primary_tools=["mail.compose", "mail.read_inbox"],
        state_roots=["components.mail"],
        tags=["communication", "email"],
    ),
    "browser": FacadeManifest(
        name="browser",
        title="Browser",
        domain="doc_graph",
        router_module="vei.router.core",
        description="Read-only admin and knowledge UI facade for browsing synthetic pages.",
        surfaces=["mcp", "ui"],
        primary_tools=["browser.read", "browser.click"],
        state_roots=["components.browser"],
        tags=["knowledge", "ui"],
    ),
    "docs": FacadeManifest(
        name="docs",
        title="Docs",
        domain="doc_graph",
        router_module="vei.router.docs",
        description="Document and ACL surface for shared knowledge artifacts.",
        surfaces=["mcp", "ui"],
        primary_tools=["docs.read", "docs.write"],
        state_roots=["components.docs"],
        tags=["knowledge", "documents"],
    ),
    "calendar": FacadeManifest(
        name="calendar",
        title="Calendar",
        domain="comm_graph",
        router_module="vei.router.calendar",
        description="Calendar and meeting scheduling surface.",
        surfaces=["mcp", "ui"],
        primary_tools=["calendar.list_events", "calendar.create_event"],
        state_roots=["components.calendar"],
        tags=["communication", "calendar"],
    ),
    "tickets": FacadeManifest(
        name="tickets",
        title="Tickets",
        domain="work_graph",
        router_module="vei.router.tickets",
        description="Generic work-queue and ticket handling surface.",
        surfaces=["mcp", "ui"],
        primary_tools=["tickets.list", "tickets.update"],
        state_roots=["components.tickets"],
        tags=["work", "support"],
    ),
    "servicedesk": FacadeManifest(
        name="servicedesk",
        title="ServiceDesk",
        domain="work_graph",
        router_module="vei.router.servicedesk",
        description="Incident and request workflows with approvals and comments.",
        surfaces=["mcp", "ui"],
        primary_tools=["servicedesk.list_incidents", "servicedesk.update_request"],
        state_roots=["components.servicedesk"],
        tags=["work", "service-management"],
    ),
    "jira": FacadeManifest(
        name="jira",
        title="Jira",
        domain="work_graph",
        router_module="vei.router.jira",
        description="Jira-style issue tracking facade for project and support work.",
        surfaces=["mcp", "ui"],
        primary_tools=["jira.list_issues", "jira.update_issue"],
        state_roots=["components.jira"],
        tags=["work", "issues"],
    ),
    "identity": FacadeManifest(
        name="identity",
        title="Identity",
        domain="identity_graph",
        router_module="vei.router.identity",
        description="Okta-style identity graph for users, groups, apps, and assignments.",
        surfaces=["mcp", "ui"],
        primary_tools=["okta.get_user", "okta.assign_app"],
        state_roots=["components.identity"],
        tags=["identity", "access"],
    ),
    "google_admin": FacadeManifest(
        name="google_admin",
        title="Google Admin",
        domain="identity_graph",
        router_module="vei.router.google_admin",
        description="Google Workspace admin facade for OAuth apps, Drive sharing, and users.",
        surfaces=["mcp", "ui"],
        primary_tools=[
            "google_admin.get_oauth_app",
            "google_admin.preserve_oauth_evidence",
            "google_admin.restrict_drive_share",
        ],
        state_roots=["components.google_admin"],
        tags=["identity", "admin"],
    ),
    "hris": FacadeManifest(
        name="hris",
        title="HRIS",
        domain="identity_graph",
        router_module="vei.router.hris",
        description="HR and employee lifecycle facade for onboarding and status changes.",
        surfaces=["mcp", "ui"],
        primary_tools=["hris.get_employee", "hris.complete_onboarding"],
        state_roots=["components.hris"],
        tags=["identity", "people-ops"],
    ),
    "crm": FacadeManifest(
        name="crm",
        title="CRM",
        domain="revenue_graph",
        router_module="vei.router.crm",
        description="Revenue ownership and opportunity management facade.",
        surfaces=["mcp", "ui"],
        primary_tools=["crm.get_opportunity", "crm.transfer_opportunity"],
        state_roots=["components.crm"],
        tags=["revenue", "sales"],
    ),
    "erp": FacadeManifest(
        name="erp",
        title="ERP",
        domain="ops_graph",
        router_module="vei.router.erp",
        description="Back-office operations and procurement facade.",
        surfaces=["mcp", "ui"],
        primary_tools=["erp.get_order", "erp.update_order"],
        state_roots=["components.erp"],
        tags=["operations", "finance"],
    ),
    "database": FacadeManifest(
        name="database",
        title="Database",
        domain="data_graph",
        router_module="vei.router.database",
        description="Deterministic tabular data facade for audit and query workflows.",
        surfaces=["mcp", "api"],
        primary_tools=["db.query", "db.insert"],
        state_roots=["components.db"],
        tags=["data", "audit"],
    ),
    "siem": FacadeManifest(
        name="siem",
        title="SIEM",
        domain="obs_graph",
        router_module="vei.router.siem",
        description="Security operations facade for alerts, evidence, and case state.",
        surfaces=["mcp", "ui"],
        primary_tools=["siem.preserve_evidence", "siem.update_case"],
        state_roots=["components.siem"],
        tags=["observability", "security"],
    ),
    "datadog": FacadeManifest(
        name="datadog",
        title="Datadog",
        domain="obs_graph",
        router_module="vei.router.datadog",
        description="Monitoring and service-health facade for production signals.",
        surfaces=["mcp", "ui"],
        primary_tools=["datadog.get_service", "datadog.update_service"],
        state_roots=["components.datadog"],
        tags=["observability", "reliability"],
    ),
    "pagerduty": FacadeManifest(
        name="pagerduty",
        title="PagerDuty",
        domain="obs_graph",
        router_module="vei.router.pagerduty",
        description="Incident paging and acknowledgement facade.",
        surfaces=["mcp", "ui"],
        primary_tools=["pagerduty.get_incident", "pagerduty.resolve_incident"],
        state_roots=["components.pagerduty"],
        tags=["observability", "incident-response"],
    ),
    "feature_flags": FacadeManifest(
        name="feature_flags",
        title="Feature Flags",
        domain="ops_graph",
        router_module="vei.router.feature_flags",
        description="Rollout and kill-switch control plane facade.",
        surfaces=["mcp", "ui"],
        primary_tools=["feature_flags.get_flag", "feature_flags.set_rollout"],
        state_roots=["components.feature_flags"],
        tags=["operations", "rollout"],
    ),
}

_TOOL_FAMILY_TO_FACADE = {
    "slack": "slack",
    "mail": "mail",
    "browser": "browser",
    "docs": "docs",
    "calendar": "calendar",
    "tickets": "tickets",
    "servicedesk": "servicedesk",
    "jira": "jira",
    "okta": "identity",
    "identity": "identity",
    "google_admin": "google_admin",
    "hris": "hris",
    "crm": "crm",
    "erp": "erp",
    "db": "database",
    "database": "database",
    "siem": "siem",
    "datadog": "datadog",
    "pagerduty": "pagerduty",
    "feature_flags": "feature_flags",
}


def get_facade_manifest(name: str) -> FacadeManifest:
    key = name.strip().lower()
    if key not in _FACADE_CATALOG:
        raise KeyError(f"unknown facade: {name}")
    return _FACADE_CATALOG[key]


def list_facade_manifest() -> List[FacadeManifest]:
    return sorted(_FACADE_CATALOG.values(), key=lambda item: item.name)


def build_blueprint_for_family(
    family_name: str,
    *,
    variant_name: Optional[str] = None,
) -> BlueprintSpec:
    family = get_benchmark_family_manifest(family_name)
    workflow_name = family.workflow_name
    if workflow_name is None:
        raise ValueError(f"benchmark family {family_name} has no workflow")
    workflow_variant = variant_name or family.primary_workflow_variant
    scenario_name = family.scenario_names[0]
    return build_blueprint_for_scenario(
        scenario_name,
        family_name=family.name,
        workflow_name=workflow_name,
        workflow_variant=workflow_variant,
        title=family.title,
        description=family.description,
        metadata={
            "primary_dimensions": list(family.primary_dimensions),
            "family_tags": list(family.tags),
        },
    )


def build_blueprint_for_scenario(
    scenario_name: str,
    *,
    family_name: Optional[str] = None,
    workflow_name: Optional[str] = None,
    workflow_variant: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> BlueprintSpec:
    scenario = get_scenario_manifest(scenario_name)
    resolved_family_name = family_name or scenario.benchmark_family
    resolved_workflow_name = workflow_name or resolve_benchmark_workflow_name(
        scenario_name=scenario.name
    )
    scenario_summary = BlueprintScenarioSummary(
        name=scenario.name,
        difficulty=scenario.difficulty,
        benchmark_family=resolved_family_name,
        tool_families=list(scenario.tool_families),
        expected_steps_min=scenario.expected_steps_min,
        expected_steps_max=scenario.expected_steps_max,
        tags=list(scenario.tags),
    )

    contract_summary = None
    workflow_tool_families: List[str] = []
    if resolved_workflow_name:
        workflow_spec = get_benchmark_family_workflow_spec(
            resolved_workflow_name, variant_name=workflow_variant
        )
        compiled = compile_workflow(workflow_spec)
        contract = build_contract_from_workflow(compiled)
        contract_summary = BlueprintContractSummary(
            name=contract.name,
            workflow_name=contract.workflow_name,
            success_predicate_count=len(contract.success_predicates),
            forbidden_predicate_count=len(contract.forbidden_predicates),
            policy_invariant_count=len(contract.policy_invariants),
            intervention_rule_count=len(contract.intervention_rules),
            observation_focus_hints=list(contract.observation_boundary.focus_hints),
            hidden_state_fields=list(contract.observation_boundary.hidden_state_fields),
        )
        workflow_tool_families = sorted(
            {step.tool.split(".", 1)[0].strip().lower() for step in compiled.steps}
        )

    facade_names = _resolve_facade_names(
        scenario_summary.tool_families + workflow_tool_families
    )
    facades = [get_facade_manifest(name) for name in facade_names]
    capability_domains = sorted({facade.domain for facade in facades})
    state_roots = sorted(
        {state_root for facade in facades for state_root in facade.state_roots}
    )
    surfaces = sorted({surface for facade in facades for surface in facade.surfaces})
    blueprint_name = resolved_family_name or scenario.name
    blueprint_title = title or _titleize(blueprint_name)
    blueprint_description = description or (
        f"Blueprint for the {resolved_family_name} benchmark family."
        if resolved_family_name
        else f"Blueprint for the {scenario.name} scenario."
    )
    merged_metadata = dict(metadata or {})
    merged_metadata.update(
        {
            "scenario_type": (
                "benchmark_family" if resolved_family_name else "catalog_scenario"
            ),
            "scenario_tags": list(scenario.tags),
        }
    )
    return BlueprintSpec(
        name=f"{blueprint_name}.blueprint",
        title=blueprint_title,
        description=blueprint_description,
        family_name=resolved_family_name,
        workflow_name=resolved_workflow_name,
        workflow_variant=workflow_variant,
        scenario=scenario_summary,
        contract=contract_summary,
        capability_domains=capability_domains,
        facades=facades,
        state_roots=state_roots,
        surfaces=surfaces,
        metadata=merged_metadata,
    )


def list_blueprint_specs() -> List[BlueprintSpec]:
    return [
        build_blueprint_for_family(item.name)
        for item in list_benchmark_family_manifest()
    ]


def _resolve_facade_names(tool_families: List[str]) -> List[str]:
    resolved: List[str] = []
    seen: set[str] = set()
    for item in tool_families:
        facade_name = _TOOL_FAMILY_TO_FACADE.get(item.strip().lower())
        if facade_name is None or facade_name in seen:
            continue
        seen.add(facade_name)
        resolved.append(facade_name)
    return resolved


def _titleize(value: str) -> str:
    return value.replace("_", " ").title()
