from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

CapabilityDomain = Literal[
    "comm_graph",
    "doc_graph",
    "work_graph",
    "identity_graph",
    "revenue_graph",
    "knowledge_graph",
    "obs_graph",
    "data_graph",
    "ops_graph",
    "property_graph",
    "campaign_graph",
    "inventory_graph",
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


class BlueprintMailMessageAsset(BaseModel):
    from_address: str
    to_address: str
    subject: str
    body_text: str
    unread: bool = True
    time_ms: Optional[int] = None


class BlueprintMailThreadAsset(BaseModel):
    thread_id: str
    title: Optional[str] = None
    category: str = "external"
    messages: List[BlueprintMailMessageAsset] = Field(default_factory=list)


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


class BlueprintPropertyAsset(BaseModel):
    property_id: str
    name: str
    city: str
    state: str
    portfolio: Optional[str] = None
    status: str = "active"


class BlueprintBuildingAsset(BaseModel):
    building_id: str
    property_id: str
    name: str
    status: str = "operational"


class BlueprintUnitAsset(BaseModel):
    unit_id: str
    building_id: str
    label: str
    status: str = "vacant"
    reserved_for: Optional[str] = None


class BlueprintTenantAsset(BaseModel):
    tenant_id: str
    name: str
    segment: Optional[str] = None
    opening_deadline_ms: Optional[int] = None


class BlueprintLeaseAsset(BaseModel):
    lease_id: str
    tenant_id: str
    unit_id: str
    status: str
    milestone: str = "draft"
    amendment_pending: bool = False


class BlueprintVendorAsset(BaseModel):
    vendor_id: str
    name: str
    specialty: str
    status: str = "active"


class BlueprintWorkOrderAsset(BaseModel):
    work_order_id: str
    property_id: str
    title: str
    status: str
    vendor_id: Optional[str] = None
    scheduled_for_ms: Optional[int] = None


class BlueprintClientAsset(BaseModel):
    client_id: str
    name: str
    tier: str = "standard"


class BlueprintCampaignAsset(BaseModel):
    campaign_id: str
    client_id: str
    name: str
    channel: str
    status: str
    budget_usd: float
    spend_usd: float = 0.0
    pacing_pct: float = 100.0


class BlueprintCreativeAsset(BaseModel):
    creative_id: str
    campaign_id: str
    title: str
    status: str
    approval_required: bool = True


class BlueprintCampaignApprovalAsset(BaseModel):
    approval_id: str
    campaign_id: str
    stage: str
    status: str


class BlueprintCampaignReportAsset(BaseModel):
    report_id: str
    campaign_id: str
    title: str
    status: str
    stale: bool = False


class BlueprintSiteAsset(BaseModel):
    site_id: str
    name: str
    city: str
    region: str
    status: str = "active"


class BlueprintCapacityPoolAsset(BaseModel):
    pool_id: str
    site_id: str
    name: str
    total_units: int
    reserved_units: int = 0


class BlueprintStorageUnitAsset(BaseModel):
    unit_id: str
    pool_id: str
    label: str
    status: str = "available"


class BlueprintQuoteAsset(BaseModel):
    quote_id: str
    customer_name: str
    requested_units: int
    status: str
    site_id: Optional[str] = None
    committed_units: int = 0


class BlueprintOrderAsset(BaseModel):
    order_id: str
    quote_id: str
    status: str
    site_id: Optional[str] = None


class BlueprintAllocationAsset(BaseModel):
    allocation_id: str
    quote_id: str
    pool_id: str
    units: int
    status: str


class BlueprintOpsFlagAsset(BaseModel):
    flag_key: str
    service: Optional[str] = None
    env: Optional[str] = None
    enabled: bool = False
    rollout_pct: int = 0


class BlueprintServiceCustomerAsset(BaseModel):
    customer_id: str
    name: str
    tier: str = "standard"
    account_status: str = "active"
    vip: bool = False
    dispute_open: bool = False


class BlueprintServiceWorkOrderAsset(BaseModel):
    work_order_id: str
    service_request_id: str
    customer_id: str
    title: str
    status: str
    required_skill: Optional[str] = None
    technician_id: Optional[str] = None
    appointment_id: Optional[str] = None
    estimated_amount_usd: float = 0.0


class BlueprintTechnicianAsset(BaseModel):
    technician_id: str
    name: str
    status: str = "available"
    skills: List[str] = Field(default_factory=list)
    current_appointment_id: Optional[str] = None
    home_zone: Optional[str] = None


class BlueprintDispatchAppointmentAsset(BaseModel):
    appointment_id: str
    service_request_id: str
    customer_id: str
    work_order_id: str
    status: str
    technician_id: Optional[str] = None
    scheduled_for_ms: Optional[int] = None
    dispatch_status: str = "pending"
    reschedule_count: int = 0


class BlueprintBillingCaseAsset(BaseModel):
    billing_case_id: str
    customer_id: str
    invoice_id: Optional[str] = None
    dispute_status: str = "clear"
    hold: bool = False
    amount_usd: float = 0.0


class BlueprintServiceExceptionAsset(BaseModel):
    exception_id: str
    type: str
    severity: str = "medium"
    status: str = "open"
    customer_id: Optional[str] = None
    service_request_id: Optional[str] = None
    work_order_id: Optional[str] = None


class BlueprintServicePolicyAsset(BaseModel):
    approval_threshold_usd: float = 1000.0
    vip_priority_override: bool = True
    billing_hold_on_dispute: bool = True
    max_auto_reschedules: int = 2


class BlueprintOpsGraphAsset(BaseModel):
    flags: List[BlueprintOpsFlagAsset] = Field(default_factory=list)
    customers: List[BlueprintServiceCustomerAsset] = Field(default_factory=list)
    work_orders: List[BlueprintServiceWorkOrderAsset] = Field(default_factory=list)
    technicians: List[BlueprintTechnicianAsset] = Field(default_factory=list)
    appointments: List[BlueprintDispatchAppointmentAsset] = Field(default_factory=list)
    billing_cases: List[BlueprintBillingCaseAsset] = Field(default_factory=list)
    exceptions: List[BlueprintServiceExceptionAsset] = Field(default_factory=list)
    policy: Optional[BlueprintServicePolicyAsset] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintKnowledgeProvenanceAsset(BaseModel):
    source: str
    source_id: str = ""
    import_id: str = ""
    captured_at: str = ""
    shelf_life_ms: Optional[int] = None
    authority: float = 1.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintKnowledgeClaimAsset(BaseModel):
    claim_id: str
    text: str
    citation_asset_ids: List[str] = Field(default_factory=list)
    section: Optional[str] = None
    metric_key: Optional[str] = None
    metric_value: Optional[float | int | str] = None


class BlueprintKnowledgeCitationSpanAsset(BaseModel):
    asset_id: str
    marker: str
    section: Optional[str] = None
    quote: str = ""


class BlueprintKnowledgeMetricBindingAsset(BaseModel):
    metric_key: str
    expected_value: float | int | str
    cited_asset_id: str
    source_field: str = ""


class BlueprintKnowledgeValidationAsset(BaseModel):
    citations_present: bool = True
    citations_resolve: bool = True
    sources_within_shelf_life: bool = True
    numbers_reconcile: bool = True
    format_matches_template: bool = True
    issues: List[str] = Field(default_factory=list)


class BlueprintKnowledgeCompositionAsset(BaseModel):
    target: str = "proposal"
    template_id: str = ""
    subject_object_ref: str = ""
    mode: str = "heuristic_baseline"
    provider: Optional[str] = None
    model: Optional[str] = None
    prompt: str = ""
    required_sections: List[str] = Field(default_factory=list)
    sections: List[str] = Field(default_factory=list)
    claims: List[BlueprintKnowledgeClaimAsset] = Field(default_factory=list)
    citation_spans: List[BlueprintKnowledgeCitationSpanAsset] = Field(
        default_factory=list
    )
    metric_bindings: List[BlueprintKnowledgeMetricBindingAsset] = Field(
        default_factory=list
    )
    validation: BlueprintKnowledgeValidationAsset = Field(
        default_factory=BlueprintKnowledgeValidationAsset
    )
    reviewer_feedback: List[str] = Field(default_factory=list)


class BlueprintKnowledgeAsset(BaseModel):
    asset_id: str
    kind: str
    title: str
    body: str
    summary: str = ""
    tags: List[str] = Field(default_factory=list)
    provenance: BlueprintKnowledgeProvenanceAsset
    linked_object_refs: List[str] = Field(default_factory=list)
    supersedes: List[str] = Field(default_factory=list)
    derived_from: List[str] = Field(default_factory=list)
    status: str = "active"
    metrics: Dict[str, float | int | str] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    composition: Optional[BlueprintKnowledgeCompositionAsset] = None


class BlueprintKnowledgeEdgeAsset(BaseModel):
    edge_id: str
    kind: str
    from_asset_id: str
    to_ref: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintKnowledgeGraphAsset(BaseModel):
    assets: List[BlueprintKnowledgeAsset] = Field(default_factory=list)
    edges: List[BlueprintKnowledgeEdgeAsset] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintEnvironmentAsset(BaseModel):
    organization_name: str
    organization_domain: str
    timezone: str = "UTC"
    scenario_brief: Optional[str] = None
    slack_initial_message: Optional[str] = None
    slack_channels: List[BlueprintSlackChannelAsset] = Field(default_factory=list)
    mail_threads: List[BlueprintMailThreadAsset] = Field(default_factory=list)
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
    knowledge_asset_count: int = 0
    slack_channel_count: int = 0
    mail_thread_count: int = 0
    property_count: int = 0
    unit_count: int = 0
    lease_count: int = 0
    work_order_count: int = 0
    campaign_count: int = 0
    creative_count: int = 0
    report_count: int = 0
    site_count: int = 0
    quote_count: int = 0
    order_count: int = 0
    service_customer_count: int = 0
    service_work_order_count: int = 0
    technician_count: int = 0
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
    mail_threads: List[BlueprintMailThreadAsset] = Field(default_factory=list)
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


class BlueprintPropertyGraphAsset(BaseModel):
    properties: List[BlueprintPropertyAsset] = Field(default_factory=list)
    buildings: List[BlueprintBuildingAsset] = Field(default_factory=list)
    units: List[BlueprintUnitAsset] = Field(default_factory=list)
    tenants: List[BlueprintTenantAsset] = Field(default_factory=list)
    leases: List[BlueprintLeaseAsset] = Field(default_factory=list)
    vendors: List[BlueprintVendorAsset] = Field(default_factory=list)
    work_orders: List[BlueprintWorkOrderAsset] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintCampaignGraphAsset(BaseModel):
    clients: List[BlueprintClientAsset] = Field(default_factory=list)
    campaigns: List[BlueprintCampaignAsset] = Field(default_factory=list)
    creatives: List[BlueprintCreativeAsset] = Field(default_factory=list)
    approvals: List[BlueprintCampaignApprovalAsset] = Field(default_factory=list)
    reports: List[BlueprintCampaignReportAsset] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlueprintInventoryGraphAsset(BaseModel):
    sites: List[BlueprintSiteAsset] = Field(default_factory=list)
    capacity_pools: List[BlueprintCapacityPoolAsset] = Field(default_factory=list)
    storage_units: List[BlueprintStorageUnitAsset] = Field(default_factory=list)
    quotes: List[BlueprintQuoteAsset] = Field(default_factory=list)
    orders: List[BlueprintOrderAsset] = Field(default_factory=list)
    allocations: List[BlueprintAllocationAsset] = Field(default_factory=list)
    vendors: List[BlueprintVendorAsset] = Field(default_factory=list)
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
    knowledge_graph: Optional[BlueprintKnowledgeGraphAsset] = None
    ops_graph: Optional[BlueprintOpsGraphAsset] = None
    property_graph: Optional[BlueprintPropertyGraphAsset] = None
    campaign_graph: Optional[BlueprintCampaignGraphAsset] = None
    inventory_graph: Optional[BlueprintInventoryGraphAsset] = None
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


SurfaceFidelityLevel = Literal["L1", "L2", "L3"]


class SurfaceFidelitySpec(BaseModel):
    """Per-surface fidelity configuration for progressive disclosure.

    L1 (Static)  — canned responses, no state
    L2 (Stateful) — key-value store, CRUD works, no cross-system causality
    L3 (Coherent) — full capability graphs with causal links (default VEI)
    """

    level: SurfaceFidelityLevel = "L3"
    static_responses: Dict[str, Any] = Field(default_factory=dict)


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
    surface_fidelity: Dict[str, SurfaceFidelitySpec] = Field(default_factory=dict)
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
