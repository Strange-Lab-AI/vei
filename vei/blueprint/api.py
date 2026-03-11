from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

from vei.benchmark.families import (
    get_benchmark_family_manifest,
    list_benchmark_family_manifest,
)
from vei.benchmark.workflows import (
    get_benchmark_family_workflow_spec,
    get_benchmark_family_workflow_variant,
    resolve_benchmark_workflow_name,
)
from vei.contract.api import build_contract_from_workflow
from vei.scenario_engine.api import compile_workflow
from vei.world.manifest import build_scenario_manifest, get_scenario_manifest
from vei.world.scenario import (
    Document,
    IdentityApplicationSeed,
    IdentityGroupSeed,
    IdentityUserSeed,
    Scenario,
    ServiceDeskRequest,
    Ticket,
)
from vei.world.scenarios import get_scenario

from .models import (
    BlueprintAsset,
    BlueprintCapabilityGraphsAsset,
    BlueprintContractDefaults,
    BlueprintContractSummary,
    BlueprintDocumentAsset,
    BlueprintEnvironmentAsset,
    BlueprintEnvironmentSummary,
    BlueprintIdentityApplicationAsset,
    BlueprintIdentityGroupAsset,
    BlueprintIdentityUserAsset,
    BlueprintRunDefaults,
    BlueprintScenarioSummary,
    BlueprintServiceRequestAsset,
    BlueprintSpec,
    BlueprintTicketAsset,
    BlueprintWorkflowDefaults,
    CapabilityGraphSummary,
    CompiledBlueprint,
    FacadeManifest,
)
from .plugins import (
    get_facade_plugin,
    list_facade_plugins,
    resolve_facade_plugins_for_tool_families,
)


def list_blueprint_builder_examples() -> List[str]:
    from vei.grounding.api import list_grounding_bundle_examples

    return sorted(item.name for item in list_grounding_bundle_examples())


def build_blueprint_asset_for_example(name: str) -> BlueprintAsset:
    from vei.grounding.api import (
        build_grounding_bundle_example,
        compile_identity_governance_bundle,
    )

    bundle = build_grounding_bundle_example(name)
    return compile_identity_governance_bundle(bundle)


def get_facade_manifest(name: str) -> FacadeManifest:
    return get_facade_plugin(name).manifest


def list_facade_manifest() -> List[FacadeManifest]:
    return [plugin.manifest for plugin in list_facade_plugins()]


def build_blueprint_asset_for_family(
    family_name: str,
    *,
    variant_name: Optional[str] = None,
) -> BlueprintAsset:
    family = get_benchmark_family_manifest(family_name)
    workflow_name = family.workflow_name
    if workflow_name is None:
        raise ValueError(f"benchmark family {family_name} has no workflow")
    workflow_variant = variant_name or family.primary_workflow_variant
    scenario_name = family.scenario_names[0]
    if workflow_variant is not None:
        scenario_name = get_benchmark_family_workflow_variant(
            workflow_name, workflow_variant
        ).scenario_name
    return BlueprintAsset(
        name=f"{family.name}.blueprint",
        title=family.title,
        description=family.description,
        scenario_name=scenario_name,
        family_name=family.name,
        workflow_name=workflow_name,
        workflow_variant=workflow_variant,
        workflow_parameters={},
        metadata={
            "primary_dimensions": list(family.primary_dimensions),
            "family_tags": list(family.tags),
        },
    )


def build_blueprint_asset_for_scenario(
    scenario_name: str,
    *,
    family_name: Optional[str] = None,
    workflow_name: Optional[str] = None,
    workflow_variant: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    requested_facades: Optional[List[str]] = None,
    metadata: Optional[dict] = None,
) -> BlueprintAsset:
    scenario = get_scenario_manifest(scenario_name)
    resolved_family_name = family_name or scenario.benchmark_family
    return BlueprintAsset(
        name=f"{scenario.name}.blueprint",
        title=title or scenario.name.replace("_", " ").title(),
        description=description or f"Compiled blueprint for scenario {scenario.name}.",
        scenario_name=scenario.name,
        family_name=resolved_family_name,
        workflow_name=workflow_name,
        workflow_variant=workflow_variant,
        workflow_parameters={},
        requested_facades=list(requested_facades or []),
        metadata=dict(metadata or {}),
    )


def compile_blueprint(asset: BlueprintAsset) -> CompiledBlueprint:
    scenario_seed = materialize_scenario_from_blueprint(asset)
    runtime_scenario_name = _runtime_scenario_name(asset)
    scenario_manifest = build_scenario_manifest(runtime_scenario_name, scenario_seed)
    workflow_lookup_scenario = asset.scenario_name
    resolved_family_name = asset.family_name or scenario_manifest.benchmark_family
    resolved_workflow_name = asset.workflow_name or resolve_benchmark_workflow_name(
        family_name=resolved_family_name,
        scenario_name=workflow_lookup_scenario,
    )
    workflow_variant = asset.workflow_variant

    scenario_summary = BlueprintScenarioSummary(
        name=scenario_manifest.name,
        difficulty=scenario_manifest.difficulty,
        benchmark_family=resolved_family_name,
        tool_families=list(scenario_manifest.tool_families),
        expected_steps_min=scenario_manifest.expected_steps_min,
        expected_steps_max=scenario_manifest.expected_steps_max,
        tags=list(scenario_manifest.tags),
    )

    contract_summary: Optional[BlueprintContractSummary] = None
    workflow_defaults = BlueprintWorkflowDefaults(
        workflow_name=resolved_workflow_name,
        workflow_variant=workflow_variant,
        expected_steps_min=scenario_manifest.expected_steps_min,
        expected_steps_max=scenario_manifest.expected_steps_max,
    )
    contract_defaults = BlueprintContractDefaults()
    workflow_tool_families: List[str] = []
    if resolved_workflow_name:
        workflow_spec = get_benchmark_family_workflow_spec(
            resolved_workflow_name,
            variant_name=workflow_variant,
            parameter_overrides=asset.workflow_parameters,
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
        workflow_defaults.allowed_tools = list(
            contract.observation_boundary.allowed_tools
        )
        workflow_defaults.focus_hints = [
            item
            for item in contract.observation_boundary.focus_hints
            if item and item != "summary"
        ]
        workflow_tool_families = sorted(
            {step.tool.split(".", 1)[0].strip().lower() for step in compiled.steps}
        )
        contract_defaults = BlueprintContractDefaults(
            contract_name=contract.name,
            success_predicate_count=len(contract.success_predicates),
            forbidden_predicate_count=len(contract.forbidden_predicates),
            policy_invariant_count=len(contract.policy_invariants),
            intervention_rule_count=len(contract.intervention_rules),
            hidden_state_fields=list(contract.observation_boundary.hidden_state_fields),
            observation_focus_hints=list(contract.observation_boundary.focus_hints),
        )

    facade_names = _resolve_facade_names(
        requested_facades=asset.requested_facades,
        tool_families=scenario_summary.tool_families + workflow_tool_families,
    )
    facade_plugins = [get_facade_plugin(name) for name in facade_names]
    facades = [plugin.manifest for plugin in facade_plugins]

    metadata: Dict[str, Any] = dict(asset.metadata)
    metadata.update(
        {
            "resolved_tool_families": sorted(
                set(scenario_summary.tool_families + workflow_tool_families)
            ),
            "compiled_from_asset": asset.name,
            "scenario_template_name": asset.scenario_name,
            "scenario_materialization": _scenario_materialization_mode(asset),
        }
    )
    state_roots = sorted(
        {
            root
            for plugin in facade_plugins
            for root in plugin.manifest.state_roots
            if root
        }
    )
    surfaces = sorted(
        {
            surface
            for plugin in facade_plugins
            for surface in plugin.manifest.surfaces
            if surface
        }
    )
    capability_domains = sorted(
        {plugin.manifest.domain for plugin in facade_plugins if plugin.manifest.domain}
    )
    scenario_seed_fields = sorted(
        {
            field_name
            for plugin in facade_plugins
            for field_name in plugin.scenario_seed_fields
            if field_name
        }
    )
    focus_hints = workflow_defaults.focus_hints or [
        plugin.manifest.name for plugin in facade_plugins
    ]
    inspection_focuses = sorted(
        {focus for plugin in facade_plugins for focus in plugin.focuses if focus}
    )
    if not inspection_focuses:
        inspection_focuses = ["browser"]
    run_defaults = BlueprintRunDefaults(
        scenario_name=scenario_manifest.name,
        benchmark_family=resolved_family_name,
        inspection_focus=(
            focus_hints[0]
            if focus_hints
            else (inspection_focuses[0] if inspection_focuses else "browser")
        ),
        inspection_focuses=sorted(set(focus_hints + inspection_focuses)),
        suggested_branch_prefix=resolved_family_name or scenario_manifest.name,
    )

    return CompiledBlueprint(
        name=asset.name,
        title=asset.title,
        description=asset.description,
        family_name=resolved_family_name,
        workflow_name=resolved_workflow_name,
        workflow_variant=workflow_variant,
        scenario=scenario_summary,
        contract=contract_summary,
        capability_domains=capability_domains,
        facades=facades,
        state_roots=state_roots,
        surfaces=surfaces,
        metadata=metadata,
        asset=asset,
        environment_summary=_build_environment_summary(asset),
        graph_summaries=_build_graph_summaries(asset),
        scenario_seed_fields=scenario_seed_fields,
        workflow_defaults=workflow_defaults,
        contract_defaults=contract_defaults,
        run_defaults=run_defaults,
    )


def build_blueprint_for_family(
    family_name: str,
    *,
    variant_name: Optional[str] = None,
) -> CompiledBlueprint:
    asset = build_blueprint_asset_for_family(family_name, variant_name=variant_name)
    return compile_blueprint(asset)


def build_blueprint_for_scenario(
    scenario_name: str,
    *,
    family_name: Optional[str] = None,
    workflow_name: Optional[str] = None,
    workflow_variant: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    requested_facades: Optional[List[str]] = None,
    metadata: Optional[dict] = None,
) -> CompiledBlueprint:
    asset = build_blueprint_asset_for_scenario(
        scenario_name,
        family_name=family_name,
        workflow_name=workflow_name,
        workflow_variant=workflow_variant,
        title=title,
        description=description,
        requested_facades=requested_facades,
        metadata=metadata,
    )
    return compile_blueprint(asset)


def list_blueprint_specs() -> List[BlueprintSpec]:
    return [
        build_blueprint_for_family(item.name)
        for item in list_benchmark_family_manifest()
        if item.workflow_name is not None
    ]


def materialize_scenario_from_blueprint(asset: BlueprintAsset) -> Scenario:
    scenario = deepcopy(get_scenario(asset.scenario_name))
    environment = _resolve_environment_asset(asset)
    if environment is None:
        return scenario

    if environment.slack_initial_message:
        scenario.slack_initial_message = environment.slack_initial_message
    if environment.slack_channels:
        scenario.slack_channels = {
            channel.channel: {
                "messages": [
                    message.model_dump(mode="json") for message in channel.messages
                ],
                "unread": channel.unread,
            }
            for channel in environment.slack_channels
        }
    if environment.documents:
        scenario.documents = {
            document.doc_id: _build_document_seed(document)
            for document in environment.documents
        }
    if environment.tickets:
        scenario.tickets = {
            ticket.ticket_id: _build_ticket_seed(ticket)
            for ticket in environment.tickets
        }
    if environment.identity_users:
        scenario.identity_users = {
            user.user_id: _build_identity_user_seed(user)
            for user in environment.identity_users
        }
    if environment.identity_groups:
        scenario.identity_groups = {
            group.group_id: _build_identity_group_seed(group)
            for group in environment.identity_groups
        }
    if environment.identity_applications:
        scenario.identity_applications = {
            application.app_id: _build_identity_application_seed(application)
            for application in environment.identity_applications
        }
    if environment.service_requests:
        scenario.service_requests = {
            request.request_id: _build_service_request_seed(request)
            for request in environment.service_requests
        }
    if environment.google_drive_shares:
        google_admin = dict(scenario.google_admin or {})
        google_admin["drive_shares"] = {
            share.doc_id: share.model_dump(mode="json")
            for share in environment.google_drive_shares
        }
        scenario.google_admin = google_admin
    if environment.hris_employees:
        scenario.hris = dict(scenario.hris or {})
        scenario.hris["employees"] = {
            employee.employee_id: employee.model_dump(mode="json")
            for employee in environment.hris_employees
        }
    if environment.crm_companies or environment.crm_contacts or environment.crm_deals:
        scenario.crm = dict(scenario.crm or {})
        if environment.crm_companies:
            scenario.crm["companies"] = [
                company.model_dump(mode="json") for company in environment.crm_companies
            ]
        if environment.crm_contacts:
            scenario.crm["contacts"] = [
                contact.model_dump(mode="json") for contact in environment.crm_contacts
            ]
        if environment.crm_deals:
            scenario.crm["deals"] = [
                deal.model_dump(mode="json") for deal in environment.crm_deals
            ]

    metadata: Dict[str, Any] = dict(scenario.metadata or {})
    metadata.update(
        {
            "builder_mode": _scenario_materialization_mode(asset),
            "builder_organization_name": environment.organization_name,
            "builder_organization_domain": environment.organization_domain,
            "builder_timezone": environment.timezone,
            "builder_runtime_scenario_name": _runtime_scenario_name(asset),
            "scenario_template_name": asset.scenario_name,
        }
    )
    if environment.scenario_brief:
        metadata["builder_scenario_brief"] = environment.scenario_brief
    if environment.metadata:
        metadata["builder_environment"] = dict(environment.metadata)
    if asset.capability_graphs is not None:
        metadata["builder_capability_graphs"] = asset.capability_graphs.model_dump(
            mode="json"
        )
    existing_tags = metadata.get("tags", [])
    if isinstance(existing_tags, list):
        metadata["tags"] = sorted(
            {str(tag) for tag in existing_tags if isinstance(tag, str)}
            | {"builder", "identity-governance"}
        )
    scenario.metadata = metadata
    return scenario


def create_world_session_from_blueprint(
    asset: BlueprintAsset,
    *,
    seed: int = 42042,
    artifacts_dir: Optional[str] = None,
    connector_mode: Optional[str] = None,
    branch: str = "main",
):
    from vei.world.api import create_world_session

    scenario = materialize_scenario_from_blueprint(asset)
    compiled = compile_blueprint(asset)
    metadata = dict(scenario.metadata or {})
    metadata["builder_blueprint_orientation"] = {
        "runtime_scenario_name": compiled.run_defaults.scenario_name,
        "family_name": compiled.family_name,
        "workflow_name": compiled.workflow_name,
        "workflow_variant": compiled.workflow_variant,
        "focus_hints": list(compiled.workflow_defaults.focus_hints),
        "inspection_focus": compiled.run_defaults.inspection_focus,
        "inspection_focuses": list(compiled.run_defaults.inspection_focuses),
        "allowed_tools": list(compiled.workflow_defaults.allowed_tools),
        "facades": [item.name for item in compiled.facades],
        "capability_domains": list(compiled.capability_domains),
    }
    scenario.metadata = metadata
    return create_world_session(
        seed=seed,
        artifacts_dir=artifacts_dir,
        scenario=scenario,
        connector_mode=connector_mode,
        branch=branch,
    )


def _resolve_facade_names(
    *,
    requested_facades: List[str],
    tool_families: List[str],
) -> List[str]:
    resolved: List[str] = []
    seen: set[str] = set()
    for plugin in resolve_facade_plugins_for_tool_families(tool_families):
        if plugin.manifest.name not in seen:
            resolved.append(plugin.manifest.name)
            seen.add(plugin.manifest.name)
    for requested in requested_facades:
        key = requested.strip().lower()
        try:
            plugin = get_facade_plugin(key)
        except KeyError as exc:
            raise ValueError(f"unknown requested facade: {requested}") from exc
        if plugin.manifest.name not in seen:
            resolved.append(plugin.manifest.name)
            seen.add(plugin.manifest.name)
    if not resolved:
        raise ValueError("compiled blueprint resolved no facades")
    return sorted(resolved)


def _runtime_scenario_name(asset: BlueprintAsset) -> str:
    if asset.environment is None and asset.capability_graphs is None:
        return asset.scenario_name
    if asset.name.endswith(".blueprint"):
        return asset.name[: -len(".blueprint")]
    return asset.name


def _build_environment_summary(
    asset: BlueprintAsset,
) -> Optional[BlueprintEnvironmentSummary]:
    environment = _resolve_environment_asset(asset)
    if environment is None:
        return None
    return BlueprintEnvironmentSummary(
        organization_name=environment.organization_name,
        organization_domain=environment.organization_domain,
        timezone=environment.timezone,
        identity_user_count=len(environment.identity_users),
        identity_group_count=len(environment.identity_groups),
        identity_application_count=len(environment.identity_applications),
        document_count=len(environment.documents),
        drive_share_count=len(environment.google_drive_shares),
        ticket_count=len(environment.tickets),
        service_request_count=len(environment.service_requests),
        hris_employee_count=len(environment.hris_employees),
        crm_deal_count=len(environment.crm_deals),
        slack_channel_count=len(environment.slack_channels),
        scenario_template_name=asset.scenario_name,
    )


def _build_graph_summaries(asset: BlueprintAsset) -> List[CapabilityGraphSummary]:
    graphs = asset.capability_graphs
    if graphs is None:
        return []
    summaries: List[CapabilityGraphSummary] = []
    if graphs.comm_graph is not None:
        summaries.append(
            CapabilityGraphSummary(
                domain="comm_graph",
                entity_count=len(graphs.comm_graph.slack_channels),
                facet_counts={
                    "channels": len(graphs.comm_graph.slack_channels),
                    "messages": sum(
                        len(channel.messages)
                        for channel in graphs.comm_graph.slack_channels
                    ),
                },
            )
        )
    if graphs.doc_graph is not None:
        summaries.append(
            CapabilityGraphSummary(
                domain="doc_graph",
                entity_count=len(graphs.doc_graph.documents)
                + len(graphs.doc_graph.drive_shares),
                facet_counts={
                    "documents": len(graphs.doc_graph.documents),
                    "drive_shares": len(graphs.doc_graph.drive_shares),
                },
            )
        )
    if graphs.work_graph is not None:
        summaries.append(
            CapabilityGraphSummary(
                domain="work_graph",
                entity_count=len(graphs.work_graph.tickets)
                + len(graphs.work_graph.service_requests),
                facet_counts={
                    "tickets": len(graphs.work_graph.tickets),
                    "service_requests": len(graphs.work_graph.service_requests),
                },
            )
        )
    if graphs.identity_graph is not None:
        summaries.append(
            CapabilityGraphSummary(
                domain="identity_graph",
                entity_count=len(graphs.identity_graph.users)
                + len(graphs.identity_graph.groups)
                + len(graphs.identity_graph.applications)
                + len(graphs.identity_graph.hris_employees)
                + len(graphs.identity_graph.policies),
                facet_counts={
                    "users": len(graphs.identity_graph.users),
                    "groups": len(graphs.identity_graph.groups),
                    "applications": len(graphs.identity_graph.applications),
                    "hris_employees": len(graphs.identity_graph.hris_employees),
                    "policies": len(graphs.identity_graph.policies),
                },
            )
        )
    if graphs.revenue_graph is not None:
        summaries.append(
            CapabilityGraphSummary(
                domain="revenue_graph",
                entity_count=len(graphs.revenue_graph.companies)
                + len(graphs.revenue_graph.contacts)
                + len(graphs.revenue_graph.deals),
                facet_counts={
                    "companies": len(graphs.revenue_graph.companies),
                    "contacts": len(graphs.revenue_graph.contacts),
                    "deals": len(graphs.revenue_graph.deals),
                },
            )
        )
    return summaries


def _resolve_environment_asset(
    asset: BlueprintAsset,
) -> Optional[BlueprintEnvironmentAsset]:
    if asset.capability_graphs is not None:
        return _environment_from_capability_graphs(asset.capability_graphs)
    return asset.environment


def _environment_from_capability_graphs(
    graphs: BlueprintCapabilityGraphsAsset,
) -> BlueprintEnvironmentAsset:
    comm_graph = graphs.comm_graph
    doc_graph = graphs.doc_graph
    work_graph = graphs.work_graph
    identity_graph = graphs.identity_graph
    revenue_graph = graphs.revenue_graph
    return BlueprintEnvironmentAsset(
        organization_name=graphs.organization_name,
        organization_domain=graphs.organization_domain,
        timezone=graphs.timezone,
        scenario_brief=graphs.scenario_brief,
        slack_initial_message=(
            comm_graph.slack_initial_message if comm_graph is not None else None
        ),
        slack_channels=(
            list(comm_graph.slack_channels) if comm_graph is not None else []
        ),
        documents=list(doc_graph.documents) if doc_graph is not None else [],
        tickets=list(work_graph.tickets) if work_graph is not None else [],
        identity_users=list(identity_graph.users) if identity_graph is not None else [],
        identity_groups=(
            list(identity_graph.groups) if identity_graph is not None else []
        ),
        identity_applications=(
            list(identity_graph.applications) if identity_graph is not None else []
        ),
        service_requests=(
            list(work_graph.service_requests) if work_graph is not None else []
        ),
        google_drive_shares=(
            list(doc_graph.drive_shares) if doc_graph is not None else []
        ),
        hris_employees=(
            list(identity_graph.hris_employees) if identity_graph is not None else []
        ),
        crm_companies=(
            list(revenue_graph.companies) if revenue_graph is not None else []
        ),
        crm_contacts=(
            list(revenue_graph.contacts) if revenue_graph is not None else []
        ),
        crm_deals=list(revenue_graph.deals) if revenue_graph is not None else [],
        metadata=dict(graphs.metadata),
    )


def _scenario_materialization_mode(asset: BlueprintAsset) -> str:
    if asset.capability_graphs is not None:
        return "capability_graphs"
    if asset.environment is not None:
        return "environment_asset"
    return "catalog"


def _build_document_seed(document: BlueprintDocumentAsset) -> Document:
    return Document(
        doc_id=document.doc_id,
        title=document.title,
        body=document.body,
        tags=list(document.tags),
    )


def _build_ticket_seed(ticket: BlueprintTicketAsset) -> Ticket:
    return Ticket(
        ticket_id=ticket.ticket_id,
        title=ticket.title,
        status=ticket.status,
        assignee=ticket.assignee,
        description=ticket.description,
        history=[{"status": ticket.status}],
    )


def _build_identity_user_seed(user: BlueprintIdentityUserAsset) -> IdentityUserSeed:
    return IdentityUserSeed(
        user_id=user.user_id,
        email=user.email,
        first_name=user.first_name,
        last_name=user.last_name,
        login=user.login,
        display_name=user.display_name,
        status=user.status,
        department=user.department,
        title=user.title,
        manager=user.manager,
        groups=list(user.groups),
        applications=list(user.applications),
        factors=list(user.factors),
        last_login_ms=user.last_login_ms,
    )


def _build_identity_group_seed(
    group: BlueprintIdentityGroupAsset,
) -> IdentityGroupSeed:
    return IdentityGroupSeed(
        group_id=group.group_id,
        name=group.name,
        description=group.description,
        members=list(group.members),
    )


def _build_identity_application_seed(
    application: BlueprintIdentityApplicationAsset,
) -> IdentityApplicationSeed:
    return IdentityApplicationSeed(
        app_id=application.app_id,
        label=application.label,
        status=application.status,
        description=application.description,
        sign_on_mode=application.sign_on_mode,
        assignments=list(application.assignments),
    )


def _build_service_request_seed(
    request: BlueprintServiceRequestAsset,
) -> ServiceDeskRequest:
    return ServiceDeskRequest(
        request_id=request.request_id,
        title=request.title,
        status=request.status,
        requester=request.requester,
        description=request.description,
        approvals=[item.model_dump(mode="json") for item in request.approvals],
    )


__all__ = [
    "BlueprintAsset",
    "CapabilityGraphSummary",
    "CompiledBlueprint",
    "build_blueprint_asset_for_example",
    "build_blueprint_asset_for_family",
    "build_blueprint_asset_for_scenario",
    "build_blueprint_for_family",
    "build_blueprint_for_scenario",
    "compile_blueprint",
    "create_world_session_from_blueprint",
    "get_facade_manifest",
    "list_blueprint_builder_examples",
    "list_blueprint_specs",
    "list_facade_manifest",
    "materialize_scenario_from_blueprint",
]
