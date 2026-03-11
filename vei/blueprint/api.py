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
    CompiledBlueprint,
    FacadeManifest,
)
from .plugins import (
    get_facade_plugin,
    list_facade_plugins,
    resolve_facade_plugins_for_tool_families,
)


def list_blueprint_builder_examples() -> List[str]:
    return sorted(_BUILDER_EXAMPLE_FACTORIES)


def build_blueprint_asset_for_example(name: str) -> BlueprintAsset:
    key = name.strip().lower()
    if key not in _BUILDER_EXAMPLE_FACTORIES:
        raise KeyError(f"unknown blueprint builder example: {name}")
    return _BUILDER_EXAMPLE_FACTORIES[key]()


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
            "scenario_materialization": (
                "environment_asset" if asset.environment is not None else "catalog"
            ),
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
    environment = asset.environment
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
            "builder_mode": "environment_asset",
            "builder_organization_name": environment.organization_name,
            "builder_organization_domain": environment.organization_domain,
            "builder_timezone": environment.timezone,
            "scenario_template_name": asset.scenario_name,
        }
    )
    if environment.scenario_brief:
        metadata["builder_scenario_brief"] = environment.scenario_brief
    if environment.metadata:
        metadata["builder_environment"] = dict(environment.metadata)
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
    if asset.environment is None:
        return asset.scenario_name
    if asset.name.endswith(".blueprint"):
        return asset.name[: -len(".blueprint")]
    return asset.name


def _build_environment_summary(
    asset: BlueprintAsset,
) -> Optional[BlueprintEnvironmentSummary]:
    environment = asset.environment
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


def _build_acquired_user_cutover_asset() -> BlueprintAsset:
    return BlueprintAsset(
        name="acquired_user_cutover.blueprint",
        title="Acquired User Cutover",
        description=(
            "Compile a wave cutover environment with HRIS conflicts, Okta access, "
            "Drive oversharing cleanup, Jira tracking, and Slack handoff."
        ),
        scenario_name="acquired_sales_onboarding",
        family_name="enterprise_onboarding_migration",
        workflow_name="enterprise_onboarding_migration",
        workflow_variant="manager_cutover",
        requested_facades=["hris", "identity", "google_admin", "jira", "docs", "slack"],
        environment=BlueprintEnvironmentAsset(
            organization_name="MacroCompute",
            organization_domain="macrocompute.example",
            timezone="America/Los_Angeles",
            scenario_brief=(
                "Wave 2 acquired-sales cutover with one identity conflict, one "
                "overshared Drive artifact, and one inherited opportunity."
            ),
            slack_initial_message=(
                "Wave 2 seller cutover starts now. Resolve the HRIS conflict, "
                "preserve least privilege, remove oversharing, and hand off safely "
                "before tomorrow morning."
            ),
            slack_channels=[
                {
                    "channel": "#sales-cutover",
                    "messages": [
                        {
                            "ts": "1",
                            "user": "it-integration",
                            "text": (
                                "Wave 2 acquired-sales cutover is live. Resolve the "
                                "identity conflict, restrict Drive visibility, and "
                                "post a clean handoff summary."
                            ),
                        }
                    ],
                }
            ],
            documents=[
                {
                    "doc_id": "POL-ACCESS-9",
                    "title": "Acquisition Access Policy",
                    "body": (
                        "Grant least privilege first. Sales users receive Slack and CRM. "
                        "No external Drive sharing before manager review is complete."
                    ),
                    "tags": ["policy", "identity", "migration"],
                },
                {
                    "doc_id": "CUTOVER-2201",
                    "title": "Wave 2 Seller Cutover Checklist",
                    "body": (
                        "Wave 2 handoff checklist.\n\n"
                        "- resolve HRIS identity conflict\n"
                        "- activate corporate Okta identity\n"
                        "- grant CRM access only\n"
                        "- remove external sharing before transfer\n"
                        "- update Jira and Slack once safe"
                    ),
                    "tags": ["cutover", "sales", "wave-2"],
                },
            ],
            tickets=[
                {
                    "ticket_id": "JRA-204",
                    "title": "Wave 2 onboarding tracker",
                    "status": "open",
                    "assignee": "it-integration",
                    "description": "Track the acquired-user cutover and least-privilege review.",
                }
            ],
            identity_users=[
                {
                    "user_id": "USR-ACQ-1",
                    "email": "jordan.sellers@oldco.example.com",
                    "login": "jordan.sellers",
                    "first_name": "Jordan",
                    "last_name": "Sellers",
                    "title": "Account Executive",
                    "department": "Sales",
                    "status": "PROVISIONED",
                    "groups": ["GRP-acquired-sales"],
                    "applications": ["APP-slack"],
                },
                {
                    "user_id": "USR-ACQ-2",
                    "email": "maya.rex@example.com",
                    "login": "maya.rex",
                    "first_name": "Maya",
                    "last_name": "Rex",
                    "title": "Sales Manager",
                    "department": "Sales",
                    "status": "ACTIVE",
                    "groups": ["GRP-sales-managers"],
                    "applications": ["APP-slack", "APP-crm"],
                },
            ],
            identity_groups=[
                {
                    "group_id": "GRP-acquired-sales",
                    "name": "Acquired Sales",
                    "members": ["USR-ACQ-1"],
                },
                {
                    "group_id": "GRP-sales-managers",
                    "name": "Sales Managers",
                    "members": ["USR-ACQ-2"],
                },
            ],
            identity_applications=[
                {
                    "app_id": "APP-crm",
                    "label": "Salesforce",
                    "assignments": ["USR-ACQ-2"],
                },
                {
                    "app_id": "APP-slack",
                    "label": "Slack",
                    "assignments": ["USR-ACQ-1", "USR-ACQ-2"],
                },
            ],
            service_requests=[
                {
                    "request_id": "REQ-2201",
                    "title": "Wave 2 seller activation",
                    "status": "PENDING_APPROVAL",
                    "requester": "maya.rex@example.com",
                    "description": "Approve seller activation after least-privilege review.",
                    "approvals": [
                        {"stage": "manager", "status": "APPROVED"},
                        {"stage": "identity", "status": "PENDING"},
                    ],
                }
            ],
            google_drive_shares=[
                {
                    "doc_id": "GDRIVE-2201",
                    "title": "Enterprise Accounts Playbook",
                    "owner": "departed.manager@oldco.example.com",
                    "visibility": "external_link",
                    "classification": "internal",
                    "shared_with": [
                        "channel-partner@example.net",
                        "maya.rex@example.com",
                    ],
                }
            ],
            hris_employees=[
                {
                    "employee_id": "EMP-2201",
                    "email": "jordan.sellers@oldco.example.com",
                    "display_name": "Jordan Sellers",
                    "department": "Sales",
                    "manager": "maya.rex@example.com",
                    "status": "pre_start",
                    "cohort": "acquired-sales-wave-2",
                    "identity_conflict": True,
                    "onboarded": False,
                    "notes": ["Needs alias merge before activation."],
                },
                {
                    "employee_id": "EMP-2202",
                    "email": "erin.falcon@oldco.example.com",
                    "display_name": "Erin Falcon",
                    "department": "Sales",
                    "manager": "maya.rex@example.com",
                    "status": "pre_start",
                    "cohort": "acquired-sales-wave-2",
                    "identity_conflict": False,
                    "onboarded": False,
                },
            ],
            crm_companies=[
                {
                    "id": "CO-100",
                    "name": "Northwind Retail",
                    "domain": "northwind.example.com",
                }
            ],
            crm_contacts=[
                {
                    "id": "C-100",
                    "email": "buyer@northwind.example.com",
                    "first_name": "Nina",
                    "last_name": "Buyer",
                    "company_id": "CO-100",
                }
            ],
            crm_deals=[
                {
                    "id": "D-100",
                    "name": "Northwind Expansion",
                    "amount": 240000,
                    "stage": "Negotiation",
                    "owner": "departed.manager@oldco.example.com",
                    "contact_id": "C-100",
                    "company_id": "CO-100",
                }
            ],
            metadata={
                "builder_example": "acquired_user_cutover",
                "wedge": "identity_access_governance",
                "deadline": "9 AM virtual time tomorrow",
            },
        ),
        metadata={
            "builder_example": "acquired_user_cutover",
            "wedge": "identity_access_governance",
        },
    )


_BUILDER_EXAMPLE_FACTORIES = {
    "acquired_user_cutover": _build_acquired_user_cutover_asset,
}


__all__ = [
    "BlueprintAsset",
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
