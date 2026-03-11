from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


CapabilityDomain = Literal[
    "comm_graph",
    "doc_graph",
    "work_graph",
    "identity_graph",
    "revenue_graph",
    "obs_graph",
    "data_graph",
    "ops_graph",
]

FacadeSurface = Literal["mcp", "api", "ui", "chat", "email", "cli"]


class FacadeManifest(BaseModel):
    name: str
    title: str
    domain: CapabilityDomain
    router_module: str
    description: str
    surfaces: List[FacadeSurface] = Field(default_factory=list)
    primary_tools: List[str] = Field(default_factory=list)
    state_roots: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)


class BlueprintScenarioSummary(BaseModel):
    name: str
    difficulty: str = "standard"
    benchmark_family: Optional[str] = None
    tool_families: List[str] = Field(default_factory=list)
    expected_steps_min: Optional[int] = None
    expected_steps_max: Optional[int] = None
    tags: List[str] = Field(default_factory=list)


class BlueprintContractSummary(BaseModel):
    name: str
    workflow_name: str
    success_predicate_count: int = 0
    forbidden_predicate_count: int = 0
    policy_invariant_count: int = 0
    intervention_rule_count: int = 0
    observation_focus_hints: List[str] = Field(default_factory=list)
    hidden_state_fields: List[str] = Field(default_factory=list)


class BlueprintSlackMessageAsset(BaseModel):
    ts: str
    user: str
    text: str
    thread_ts: Optional[str] = None


class BlueprintSlackChannelAsset(BaseModel):
    channel: str
    messages: List[BlueprintSlackMessageAsset] = Field(default_factory=list)
    unread: int = 0


class BlueprintDocumentAsset(BaseModel):
    doc_id: str
    title: str
    body: str
    tags: List[str] = Field(default_factory=list)


class BlueprintTicketAsset(BaseModel):
    ticket_id: str
    title: str
    status: str
    assignee: Optional[str] = None
    description: Optional[str] = None


class BlueprintIdentityUserAsset(BaseModel):
    user_id: str
    email: str
    first_name: str
    last_name: str
    login: Optional[str] = None
    display_name: Optional[str] = None
    status: str = "ACTIVE"
    department: Optional[str] = None
    title: Optional[str] = None
    manager: Optional[str] = None
    groups: List[str] = Field(default_factory=list)
    applications: List[str] = Field(default_factory=list)
    factors: List[str] = Field(default_factory=list)
    last_login_ms: Optional[int] = None


class BlueprintIdentityGroupAsset(BaseModel):
    group_id: str
    name: str
    description: Optional[str] = None
    members: List[str] = Field(default_factory=list)


class BlueprintIdentityApplicationAsset(BaseModel):
    app_id: str
    label: str
    status: str = "ACTIVE"
    description: Optional[str] = None
    sign_on_mode: str = "SAML_2_0"
    assignments: List[str] = Field(default_factory=list)


class BlueprintApprovalAsset(BaseModel):
    stage: str
    status: str


class BlueprintServiceRequestAsset(BaseModel):
    request_id: str
    title: str
    status: str
    requester: Optional[str] = None
    description: Optional[str] = None
    approvals: List[BlueprintApprovalAsset] = Field(default_factory=list)


class BlueprintGoogleDriveShareAsset(BaseModel):
    doc_id: str
    title: str
    owner: str
    visibility: str = "internal"
    classification: str = "internal"
    shared_with: List[str] = Field(default_factory=list)


class BlueprintHrisEmployeeAsset(BaseModel):
    employee_id: str
    email: str
    display_name: str
    department: str
    manager: str
    status: str = "pre_start"
    cohort: Optional[str] = None
    identity_conflict: bool = False
    onboarded: bool = False
    notes: List[str] = Field(default_factory=list)


class BlueprintCrmCompanyAsset(BaseModel):
    id: str
    name: str
    domain: str
    created_ms: int = 1700000000000


class BlueprintCrmContactAsset(BaseModel):
    id: str
    email: str
    first_name: str
    last_name: str
    do_not_contact: bool = False
    company_id: Optional[str] = None
    created_ms: int = 1700000000000


class BlueprintCrmDealAsset(BaseModel):
    id: str
    name: str
    amount: float
    stage: str
    owner: str
    contact_id: Optional[str] = None
    company_id: Optional[str] = None
    created_ms: int = 1700000000000


class BlueprintEnvironmentAsset(BaseModel):
    organization_name: str
    organization_domain: str
    timezone: str = "UTC"
    scenario_brief: Optional[str] = None
    slack_initial_message: Optional[str] = None
    slack_channels: List[BlueprintSlackChannelAsset] = Field(default_factory=list)
    documents: List[BlueprintDocumentAsset] = Field(default_factory=list)
    tickets: List[BlueprintTicketAsset] = Field(default_factory=list)
    identity_users: List[BlueprintIdentityUserAsset] = Field(default_factory=list)
    identity_groups: List[BlueprintIdentityGroupAsset] = Field(default_factory=list)
    identity_applications: List[BlueprintIdentityApplicationAsset] = Field(
        default_factory=list
    )
    service_requests: List[BlueprintServiceRequestAsset] = Field(default_factory=list)
    google_drive_shares: List[BlueprintGoogleDriveShareAsset] = Field(
        default_factory=list
    )
    hris_employees: List[BlueprintHrisEmployeeAsset] = Field(default_factory=list)
    crm_companies: List[BlueprintCrmCompanyAsset] = Field(default_factory=list)
    crm_contacts: List[BlueprintCrmContactAsset] = Field(default_factory=list)
    crm_deals: List[BlueprintCrmDealAsset] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintEnvironmentSummary(BaseModel):
    organization_name: str
    organization_domain: str
    timezone: str
    identity_user_count: int = 0
    identity_group_count: int = 0
    identity_application_count: int = 0
    document_count: int = 0
    drive_share_count: int = 0
    ticket_count: int = 0
    service_request_count: int = 0
    hris_employee_count: int = 0
    crm_deal_count: int = 0
    slack_channel_count: int = 0
    scenario_template_name: Optional[str] = None


class BlueprintIdentityPolicyAsset(BaseModel):
    policy_id: str
    title: str
    allowed_application_ids: List[str] = Field(default_factory=list)
    forbidden_share_domains: List[str] = Field(default_factory=list)
    required_approval_stages: List[str] = Field(default_factory=list)
    deadline_max_ms: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintCommGraphAsset(BaseModel):
    slack_initial_message: Optional[str] = None
    slack_channels: List[BlueprintSlackChannelAsset] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintDocGraphAsset(BaseModel):
    documents: List[BlueprintDocumentAsset] = Field(default_factory=list)
    drive_shares: List[BlueprintGoogleDriveShareAsset] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintWorkGraphAsset(BaseModel):
    tickets: List[BlueprintTicketAsset] = Field(default_factory=list)
    service_requests: List[BlueprintServiceRequestAsset] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintIdentityGraphAsset(BaseModel):
    users: List[BlueprintIdentityUserAsset] = Field(default_factory=list)
    groups: List[BlueprintIdentityGroupAsset] = Field(default_factory=list)
    applications: List[BlueprintIdentityApplicationAsset] = Field(default_factory=list)
    hris_employees: List[BlueprintHrisEmployeeAsset] = Field(default_factory=list)
    policies: List[BlueprintIdentityPolicyAsset] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintRevenueGraphAsset(BaseModel):
    companies: List[BlueprintCrmCompanyAsset] = Field(default_factory=list)
    contacts: List[BlueprintCrmContactAsset] = Field(default_factory=list)
    deals: List[BlueprintCrmDealAsset] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintCapabilityGraphsAsset(BaseModel):
    organization_name: str
    organization_domain: str
    timezone: str = "UTC"
    scenario_brief: Optional[str] = None
    comm_graph: Optional[BlueprintCommGraphAsset] = None
    doc_graph: Optional[BlueprintDocGraphAsset] = None
    work_graph: Optional[BlueprintWorkGraphAsset] = None
    identity_graph: Optional[BlueprintIdentityGraphAsset] = None
    revenue_graph: Optional[BlueprintRevenueGraphAsset] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CapabilityGraphSummary(BaseModel):
    domain: CapabilityDomain
    entity_count: int = 0
    facet_counts: Dict[str, int] = Field(default_factory=dict)


class BlueprintSpec(BaseModel):
    name: str
    title: str
    description: str
    family_name: Optional[str] = None
    workflow_name: Optional[str] = None
    workflow_variant: Optional[str] = None
    scenario: BlueprintScenarioSummary
    contract: Optional[BlueprintContractSummary] = None
    capability_domains: List[CapabilityDomain] = Field(default_factory=list)
    facades: List[FacadeManifest] = Field(default_factory=list)
    state_roots: List[str] = Field(default_factory=list)
    surfaces: List[FacadeSurface] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintAsset(BaseModel):
    name: str
    title: str
    description: str
    scenario_name: str
    family_name: Optional[str] = None
    workflow_name: Optional[str] = None
    workflow_variant: Optional[str] = None
    workflow_parameters: Dict[str, Any] = Field(default_factory=dict)
    requested_facades: List[str] = Field(default_factory=list)
    capability_graphs: Optional[BlueprintCapabilityGraphsAsset] = None
    environment: Optional[BlueprintEnvironmentAsset] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintWorkflowDefaults(BaseModel):
    workflow_name: Optional[str] = None
    workflow_variant: Optional[str] = None
    allowed_tools: List[str] = Field(default_factory=list)
    focus_hints: List[str] = Field(default_factory=list)
    expected_steps_min: Optional[int] = None
    expected_steps_max: Optional[int] = None


class BlueprintContractDefaults(BaseModel):
    contract_name: Optional[str] = None
    success_predicate_count: int = 0
    forbidden_predicate_count: int = 0
    policy_invariant_count: int = 0
    intervention_rule_count: int = 0
    hidden_state_fields: List[str] = Field(default_factory=list)
    observation_focus_hints: List[str] = Field(default_factory=list)


class BlueprintRunDefaults(BaseModel):
    scenario_name: str
    benchmark_family: Optional[str] = None
    recommended_runner: str = "workflow"
    comparison_runner: str = "scripted"
    inspection_focus: str = "browser"
    inspection_focuses: List[str] = Field(default_factory=list)
    suggested_branch_prefix: Optional[str] = None


class CompiledBlueprint(BlueprintSpec):
    asset: BlueprintAsset
    environment_summary: Optional[BlueprintEnvironmentSummary] = None
    graph_summaries: List[CapabilityGraphSummary] = Field(default_factory=list)
    scenario_seed_fields: List[str] = Field(default_factory=list)
    workflow_defaults: BlueprintWorkflowDefaults = Field(
        default_factory=BlueprintWorkflowDefaults
    )
    contract_defaults: BlueprintContractDefaults = Field(
        default_factory=BlueprintContractDefaults
    )
    run_defaults: BlueprintRunDefaults
