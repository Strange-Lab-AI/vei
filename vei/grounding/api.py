from __future__ import annotations

from typing import List

from vei.blueprint.models import (
    BlueprintAsset,
    BlueprintCapabilityGraphsAsset,
    BlueprintCommGraphAsset,
    BlueprintDocGraphAsset,
    BlueprintIdentityApplicationAsset,
    BlueprintIdentityGraphAsset,
    BlueprintIdentityGroupAsset,
    BlueprintIdentityPolicyAsset,
    BlueprintIdentityUserAsset,
    BlueprintDocumentAsset,
    BlueprintGoogleDriveShareAsset,
    BlueprintHrisEmployeeAsset,
    BlueprintRevenueGraphAsset,
    BlueprintServiceRequestAsset,
    BlueprintSlackChannelAsset,
    BlueprintSlackMessageAsset,
    BlueprintTicketAsset,
    BlueprintWorkGraphAsset,
    BlueprintApprovalAsset,
    BlueprintCrmCompanyAsset,
    BlueprintCrmContactAsset,
    BlueprintCrmDealAsset,
)

from .models import (
    GroundingBundleManifest,
    IdentityGovernanceBundle,
    IdentityGovernanceWorkflowSeed,
)


def compile_identity_governance_bundle(
    bundle: IdentityGovernanceBundle,
) -> BlueprintAsset:
    metadata = dict(bundle.metadata)
    metadata.update(
        {
            "grounding_bundle": bundle.name,
            "grounding_wedge": "identity_access_governance",
            "policy_notes": list(bundle.policy_notes),
            "incident_history": list(bundle.incident_history),
            "acceptance_focus": list(bundle.acceptance_focus),
            "source_manifests": list(bundle.source_manifests),
            "org_units": list(bundle.org_units),
            "approval_policies": list(bundle.approval_policies),
            "entitlement_policies": list(bundle.entitlement_policies),
            "audit_events": list(bundle.audit_events),
            "change_references": list(bundle.change_references),
        }
    )
    return BlueprintAsset(
        name=f"{bundle.name}.blueprint",
        title=bundle.title,
        description=bundle.description,
        scenario_name=bundle.scenario_template_name,
        family_name=bundle.family_name,
        workflow_name=bundle.workflow_name,
        workflow_variant=bundle.workflow_variant,
        workflow_parameters=bundle.workflow_seed.model_dump(mode="json"),
        requested_facades=list(bundle.requested_facades),
        capability_graphs=bundle.capability_graphs,
        metadata=metadata,
    )


def list_grounding_bundle_examples() -> List[GroundingBundleManifest]:
    return sorted(
        [
            GroundingBundleManifest(
                name=bundle.name,
                title=bundle.title,
                description=bundle.description,
                wedge="identity_access_governance",
                scenario_template_name=bundle.scenario_template_name,
                workflow_name=bundle.workflow_name,
                workflow_variant=bundle.workflow_variant,
                organization_domain=bundle.capability_graphs.organization_domain,
                capability_domains=[
                    domain
                    for domain, graph in (
                        ("comm_graph", bundle.capability_graphs.comm_graph),
                        ("doc_graph", bundle.capability_graphs.doc_graph),
                        ("work_graph", bundle.capability_graphs.work_graph),
                        ("identity_graph", bundle.capability_graphs.identity_graph),
                        ("revenue_graph", bundle.capability_graphs.revenue_graph),
                    )
                    if graph is not None
                ],
            )
            for bundle in _GROUNDING_BUNDLE_EXAMPLES.values()
        ],
        key=lambda item: item.name,
    )


def build_grounding_bundle_example(name: str) -> IdentityGovernanceBundle:
    key = name.strip().lower()
    if key not in _GROUNDING_BUNDLE_EXAMPLES:
        raise KeyError(f"unknown grounding bundle example: {name}")
    return _GROUNDING_BUNDLE_EXAMPLES[key].model_copy(deep=True)


def _acquired_user_cutover_bundle() -> IdentityGovernanceBundle:
    graphs = BlueprintCapabilityGraphsAsset(
        organization_name="MacroCompute",
        organization_domain="macrocompute.example",
        timezone="America/Los_Angeles",
        scenario_brief=(
            "Wave 2 acquired-sales cutover with one identity conflict, one "
            "overshared Drive artifact, and one inherited opportunity."
        ),
        comm_graph=BlueprintCommGraphAsset(
            slack_initial_message=(
                "Wave 2 seller cutover starts now. Resolve the HRIS conflict, "
                "preserve least privilege, remove oversharing, and hand off safely "
                "before tomorrow morning."
            ),
            slack_channels=[
                BlueprintSlackChannelAsset(
                    channel="#sales-cutover",
                    messages=[
                        BlueprintSlackMessageAsset(
                            ts="1",
                            user="it-integration",
                            text=(
                                "Wave 2 acquired-sales cutover is live. Resolve the "
                                "identity conflict, restrict Drive visibility, and "
                                "post a clean handoff summary."
                            ),
                        )
                    ],
                )
            ],
        ),
        doc_graph=BlueprintDocGraphAsset(
            documents=[
                BlueprintDocumentAsset(
                    doc_id="POL-ACCESS-9",
                    title="Acquisition Access Policy",
                    body=(
                        "Grant least privilege first. Sales users receive Slack and CRM. "
                        "No external Drive sharing before manager review is complete."
                    ),
                    tags=["policy", "identity", "migration"],
                ),
                BlueprintDocumentAsset(
                    doc_id="CUTOVER-2201",
                    title="Wave 2 Seller Cutover Checklist",
                    body=(
                        "Wave 2 handoff checklist.\n\n"
                        "- resolve HRIS identity conflict\n"
                        "- activate corporate Okta identity\n"
                        "- grant CRM access only\n"
                        "- remove external sharing before transfer\n"
                        "- update Jira and Slack once safe"
                    ),
                    tags=["cutover", "sales", "wave-2"],
                ),
            ],
            drive_shares=[
                BlueprintGoogleDriveShareAsset(
                    doc_id="GDRIVE-2201",
                    title="Enterprise Accounts Playbook",
                    owner="departed.manager@oldco.example.com",
                    visibility="external_link",
                    classification="internal",
                    shared_with=[
                        "channel-partner@example.net",
                        "maya.rex@example.com",
                    ],
                )
            ],
        ),
        work_graph=BlueprintWorkGraphAsset(
            tickets=[
                BlueprintTicketAsset(
                    ticket_id="JRA-204",
                    title="Wave 2 onboarding tracker",
                    status="open",
                    assignee="it-integration",
                    description=(
                        "Track the acquired-user cutover and least-privilege review."
                    ),
                )
            ],
            service_requests=[
                BlueprintServiceRequestAsset(
                    request_id="REQ-2201",
                    title="Wave 2 seller activation",
                    status="PENDING_APPROVAL",
                    requester="maya.rex@example.com",
                    description=(
                        "Approve seller activation after least-privilege review."
                    ),
                    approvals=[
                        BlueprintApprovalAsset(stage="manager", status="APPROVED"),
                        BlueprintApprovalAsset(stage="identity", status="PENDING"),
                    ],
                )
            ],
        ),
        identity_graph=BlueprintIdentityGraphAsset(
            users=[
                BlueprintIdentityUserAsset(
                    user_id="USR-ACQ-1",
                    email="jordan.sellers@oldco.example.com",
                    login="jordan.sellers",
                    first_name="Jordan",
                    last_name="Sellers",
                    title="Account Executive",
                    department="Sales",
                    status="PROVISIONED",
                    groups=["GRP-acquired-sales"],
                    applications=["APP-slack"],
                ),
                BlueprintIdentityUserAsset(
                    user_id="USR-ACQ-2",
                    email="maya.rex@example.com",
                    login="maya.rex",
                    first_name="Maya",
                    last_name="Rex",
                    title="Sales Manager",
                    department="Sales",
                    status="ACTIVE",
                    groups=["GRP-sales-managers"],
                    applications=["APP-slack", "APP-crm"],
                ),
            ],
            groups=[
                BlueprintIdentityGroupAsset(
                    group_id="GRP-acquired-sales",
                    name="Acquired Sales",
                    members=["USR-ACQ-1"],
                ),
                BlueprintIdentityGroupAsset(
                    group_id="GRP-sales-managers",
                    name="Sales Managers",
                    members=["USR-ACQ-2"],
                ),
            ],
            applications=[
                BlueprintIdentityApplicationAsset(
                    app_id="APP-crm",
                    label="Salesforce",
                    assignments=["USR-ACQ-2"],
                ),
                BlueprintIdentityApplicationAsset(
                    app_id="APP-slack",
                    label="Slack",
                    assignments=["USR-ACQ-1", "USR-ACQ-2"],
                ),
            ],
            hris_employees=[
                BlueprintHrisEmployeeAsset(
                    employee_id="EMP-2201",
                    email="jordan.sellers@oldco.example.com",
                    display_name="Jordan Sellers",
                    department="Sales",
                    manager="maya.rex@example.com",
                    status="pre_start",
                    cohort="acquired-sales-wave-2",
                    identity_conflict=True,
                    onboarded=False,
                    notes=["Needs alias merge before activation."],
                ),
                BlueprintHrisEmployeeAsset(
                    employee_id="EMP-2202",
                    email="erin.falcon@oldco.example.com",
                    display_name="Erin Falcon",
                    department="Sales",
                    manager="maya.rex@example.com",
                    status="pre_start",
                    cohort="acquired-sales-wave-2",
                    identity_conflict=False,
                    onboarded=False,
                ),
            ],
            policies=[
                BlueprintIdentityPolicyAsset(
                    policy_id="POL-WAVE2",
                    title="Wave 2 least-privilege cutover policy",
                    allowed_application_ids=["APP-slack", "APP-crm"],
                    forbidden_share_domains=["example.net"],
                    required_approval_stages=["manager", "identity"],
                    deadline_max_ms=86_400_000,
                    metadata={"deadline": "9 AM virtual time tomorrow"},
                )
            ],
        ),
        revenue_graph=BlueprintRevenueGraphAsset(
            companies=[
                BlueprintCrmCompanyAsset(
                    id="CO-100",
                    name="Northwind Retail",
                    domain="northwind.example.com",
                )
            ],
            contacts=[
                BlueprintCrmContactAsset(
                    id="C-100",
                    email="buyer@northwind.example.com",
                    first_name="Nina",
                    last_name="Buyer",
                    company_id="CO-100",
                )
            ],
            deals=[
                BlueprintCrmDealAsset(
                    id="D-100",
                    name="Northwind Expansion",
                    amount=240000,
                    stage="Negotiation",
                    owner="departed.manager@oldco.example.com",
                    contact_id="C-100",
                    company_id="CO-100",
                )
            ],
        ),
        metadata={"builder_example": "acquired_user_cutover"},
    )
    return IdentityGovernanceBundle(
        name="acquired_user_cutover",
        title="Acquired User Cutover",
        description=(
            "Compile a grounded identity/access-governance cutover environment from "
            "capability graphs and policy constraints."
        ),
        requested_facades=[
            "hris",
            "identity",
            "google_admin",
            "jira",
            "docs",
            "slack",
            "crm",
        ],
        capability_graphs=graphs,
        workflow_seed=IdentityGovernanceWorkflowSeed(
            employee_id="EMP-2201",
            user_id="USR-ACQ-1",
            corporate_email="jordan.sellers@example.com",
            manager_email="maya.rex@example.com",
            crm_app_id="APP-crm",
            doc_id="GDRIVE-2201",
            tracking_ticket_id="JRA-204",
            cutover_doc_id="CUTOVER-2201",
            opportunity_id="D-100",
            allowed_share_count=1,
            revoked_share_email="channel-partner@example.net",
            deadline_max_ms=86_400_000,
            transfer_note="Manager assumes ownership after access review completes.",
            onboarding_note="Wave 2 cutover completed with least privilege intact.",
            ticket_update_note=(
                "Wave 2 seller cutover complete; identity resolved, least-privilege "
                "access confirmed, and external sharing removed."
            ),
            cutover_doc_note=(
                "Identity conflict resolved, Drive ownership transferred, and CRM "
                "access granted after least-privilege review."
            ),
            slack_channel="#sales-cutover",
            slack_summary=(
                "Wave 2 seller cutover complete: CRM access granted, external "
                "sharing removed, and manager handoff is safe."
            ),
        ),
        metadata={
            "builder_example": "acquired_user_cutover",
            "wedge": "identity_access_governance",
        },
        policy_notes=[
            "Grant only Slack and CRM during first-wave cutover.",
            "Remove external sharing before ownership transfer.",
        ],
        incident_history=[
            {
                "kind": "cutover_alert",
                "note": "Acquired seller playbook remained externally shared after import.",
            }
        ],
        acceptance_focus=[
            "identity resolution",
            "least privilege",
            "oversharing removal",
            "manager handoff",
        ],
    )


_GROUNDING_BUNDLE_EXAMPLES = {
    "acquired_user_cutover": _acquired_user_cutover_bundle(),
}


__all__ = [
    "GroundingBundleManifest",
    "IdentityGovernanceBundle",
    "IdentityGovernanceWorkflowSeed",
    "build_grounding_bundle_example",
    "compile_identity_governance_bundle",
    "list_grounding_bundle_examples",
]
