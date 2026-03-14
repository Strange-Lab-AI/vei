from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field
from vei.blueprint.models import CapabilityDomain


class CommGraphChannelView(BaseModel):
    channel: str
    unread: int = 0
    message_count: int = 0
    latest_text: Optional[str] = None


class CommGraphView(BaseModel):
    channels: List[CommGraphChannelView] = Field(default_factory=list)
    inbox_count: int = 0


class DocumentView(BaseModel):
    doc_id: str
    title: str
    tags: List[str] = Field(default_factory=list)


class DriveShareView(BaseModel):
    doc_id: str
    title: str
    owner: str
    visibility: str = "internal"
    classification: str = "internal"
    shared_with: List[str] = Field(default_factory=list)


class DocGraphView(BaseModel):
    documents: List[DocumentView] = Field(default_factory=list)
    drive_shares: List[DriveShareView] = Field(default_factory=list)


class WorkItemView(BaseModel):
    item_id: str
    title: str
    status: str
    assignee: Optional[str] = None
    kind: str


class ServiceRequestView(BaseModel):
    request_id: str
    title: str
    status: str
    requester: Optional[str] = None
    approval_stages: List[str] = Field(default_factory=list)


class WorkGraphView(BaseModel):
    tickets: List[WorkItemView] = Field(default_factory=list)
    service_requests: List[ServiceRequestView] = Field(default_factory=list)
    incidents: List[WorkItemView] = Field(default_factory=list)


class IdentityUserView(BaseModel):
    user_id: str
    email: str
    display_name: Optional[str] = None
    status: str = "UNKNOWN"
    groups: List[str] = Field(default_factory=list)
    applications: List[str] = Field(default_factory=list)


class IdentityGroupView(BaseModel):
    group_id: str
    name: str
    members: List[str] = Field(default_factory=list)


class IdentityApplicationView(BaseModel):
    app_id: str
    label: str
    status: str = "UNKNOWN"
    assignments: List[str] = Field(default_factory=list)


class HrisEmployeeView(BaseModel):
    employee_id: str
    email: str
    display_name: str
    department: str
    manager: str
    status: str
    identity_conflict: bool = False
    onboarded: bool = False


class IdentityPolicyView(BaseModel):
    policy_id: str
    title: str
    allowed_application_ids: List[str] = Field(default_factory=list)
    forbidden_share_domains: List[str] = Field(default_factory=list)
    required_approval_stages: List[str] = Field(default_factory=list)
    deadline_max_ms: Optional[int] = None


class IdentityGraphView(BaseModel):
    users: List[IdentityUserView] = Field(default_factory=list)
    groups: List[IdentityGroupView] = Field(default_factory=list)
    applications: List[IdentityApplicationView] = Field(default_factory=list)
    hris_employees: List[HrisEmployeeView] = Field(default_factory=list)
    policies: List[IdentityPolicyView] = Field(default_factory=list)


class RevenueCompanyView(BaseModel):
    company_id: str
    name: str
    domain: str


class RevenueContactView(BaseModel):
    contact_id: str
    email: str
    full_name: str
    company_id: Optional[str] = None


class RevenueDealView(BaseModel):
    deal_id: str
    name: str
    amount: float
    stage: str
    owner: str
    company_id: Optional[str] = None
    contact_id: Optional[str] = None


class RevenueGraphView(BaseModel):
    companies: List[RevenueCompanyView] = Field(default_factory=list)
    contacts: List[RevenueContactView] = Field(default_factory=list)
    deals: List[RevenueDealView] = Field(default_factory=list)


class PropertyView(BaseModel):
    property_id: str
    name: str
    city: str
    state: str
    status: str = "active"


class UnitView(BaseModel):
    unit_id: str
    building_id: str
    label: str
    status: str = "vacant"
    reserved_for: Optional[str] = None


class LeaseView(BaseModel):
    lease_id: str
    tenant_id: str
    unit_id: str
    status: str
    milestone: str = "draft"
    amendment_pending: bool = False


class VendorView(BaseModel):
    vendor_id: str
    name: str
    specialty: str
    status: str = "active"


class WorkOrderGraphView(BaseModel):
    work_order_id: str
    property_id: str
    title: str
    status: str
    vendor_id: Optional[str] = None
    scheduled_for_ms: Optional[int] = None


class PropertyGraphView(BaseModel):
    properties: List[PropertyView] = Field(default_factory=list)
    units: List[UnitView] = Field(default_factory=list)
    leases: List[LeaseView] = Field(default_factory=list)
    vendors: List[VendorView] = Field(default_factory=list)
    work_orders: List[WorkOrderGraphView] = Field(default_factory=list)


class ClientView(BaseModel):
    client_id: str
    name: str
    tier: str = "standard"


class CampaignView(BaseModel):
    campaign_id: str
    client_id: str
    name: str
    channel: str
    status: str
    budget_usd: float
    spend_usd: float = 0.0
    pacing_pct: float = 100.0


class CreativeView(BaseModel):
    creative_id: str
    campaign_id: str
    title: str
    status: str
    approval_required: bool = True


class CampaignApprovalView(BaseModel):
    approval_id: str
    campaign_id: str
    stage: str
    status: str


class CampaignReportView(BaseModel):
    report_id: str
    campaign_id: str
    title: str
    status: str
    stale: bool = False


class CampaignGraphView(BaseModel):
    clients: List[ClientView] = Field(default_factory=list)
    campaigns: List[CampaignView] = Field(default_factory=list)
    creatives: List[CreativeView] = Field(default_factory=list)
    approvals: List[CampaignApprovalView] = Field(default_factory=list)
    reports: List[CampaignReportView] = Field(default_factory=list)


class SiteView(BaseModel):
    site_id: str
    name: str
    city: str
    region: str
    status: str = "active"


class CapacityPoolView(BaseModel):
    pool_id: str
    site_id: str
    name: str
    total_units: int
    reserved_units: int = 0


class QuoteView(BaseModel):
    quote_id: str
    customer_name: str
    requested_units: int
    status: str
    site_id: Optional[str] = None
    committed_units: int = 0


class OrderView(BaseModel):
    order_id: str
    quote_id: str
    status: str
    site_id: Optional[str] = None


class AllocationView(BaseModel):
    allocation_id: str
    quote_id: str
    pool_id: str
    units: int
    status: str


class InventoryGraphView(BaseModel):
    sites: List[SiteView] = Field(default_factory=list)
    capacity_pools: List[CapacityPoolView] = Field(default_factory=list)
    quotes: List[QuoteView] = Field(default_factory=list)
    orders: List[OrderView] = Field(default_factory=list)
    allocations: List[AllocationView] = Field(default_factory=list)
    vendors: List[VendorView] = Field(default_factory=list)


class DataWorkbookView(BaseModel):
    workbook_id: str
    title: str
    owner: Optional[str] = None
    sheet_count: int = 0
    shared_with: List[str] = Field(default_factory=list)


class DataGraphView(BaseModel):
    workbooks: List[DataWorkbookView] = Field(default_factory=list)


class ObsServiceView(BaseModel):
    service_id: str
    name: str
    status: str
    error_rate_pct: Optional[float] = None
    latency_p95_ms: Optional[int] = None
    revenue_tier: Optional[str] = None


class ObsMonitorView(BaseModel):
    monitor_id: str
    title: str
    service_id: Optional[str] = None
    status: str
    severity: Optional[str] = None
    muted: bool = False


class ObsIncidentView(BaseModel):
    incident_id: str
    title: str
    status: str
    urgency: Optional[str] = None
    service_id: Optional[str] = None
    assignee: Optional[str] = None


class ObsGraphView(BaseModel):
    services: List[ObsServiceView] = Field(default_factory=list)
    monitors: List[ObsMonitorView] = Field(default_factory=list)
    incidents: List[ObsIncidentView] = Field(default_factory=list)


class OpsFlagView(BaseModel):
    flag_key: str
    service: Optional[str] = None
    env: Optional[str] = None
    enabled: bool = False
    rollout_pct: int = 0


class OpsGraphView(BaseModel):
    flags: List[OpsFlagView] = Field(default_factory=list)


GraphActionPriority = Literal["high", "medium", "low"]


class CapabilityGraphActionSchema(BaseModel):
    domain: CapabilityDomain
    action: str
    title: str
    description: str
    tool: str
    required_args: List[str] = Field(default_factory=list)
    optional_args: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)


class CapabilityGraphActionInput(BaseModel):
    domain: Optional[CapabilityDomain] = None
    action: Optional[str] = None
    args: Dict[str, Any] = Field(default_factory=dict)
    step_id: Optional[str] = None


class CapabilityGraphPlanStep(BaseModel):
    step_id: str
    domain: CapabilityDomain
    action: str
    title: str
    rationale: str
    priority: GraphActionPriority = "medium"
    tool: str
    args: Dict[str, Any] = Field(default_factory=dict)
    target_id: Optional[str] = None
    target_kind: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class CapabilityGraphPlan(BaseModel):
    branch: str
    clock_ms: int
    scenario_name: Optional[str] = None
    available_domains: List[str] = Field(default_factory=list)
    available_actions: List[CapabilityGraphActionSchema] = Field(default_factory=list)
    suggested_steps: List[CapabilityGraphPlanStep] = Field(default_factory=list)
    next_focuses: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CapabilityGraphActionResult(BaseModel):
    ok: bool
    branch: str
    clock_ms: int
    domain: CapabilityDomain
    action: str
    tool: str
    tool_args: Dict[str, Any] = Field(default_factory=dict)
    step_id: Optional[str] = None
    result: Dict[str, Any] = Field(default_factory=dict)
    graph: Dict[str, Any] = Field(default_factory=dict)
    next_focuses: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RuntimeCapabilityGraphs(BaseModel):
    branch: str
    clock_ms: int
    available_domains: List[str] = Field(default_factory=list)
    comm_graph: Optional[CommGraphView] = None
    doc_graph: Optional[DocGraphView] = None
    work_graph: Optional[WorkGraphView] = None
    identity_graph: Optional[IdentityGraphView] = None
    revenue_graph: Optional[RevenueGraphView] = None
    data_graph: Optional[DataGraphView] = None
    obs_graph: Optional[ObsGraphView] = None
    ops_graph: Optional[OpsGraphView] = None
    property_graph: Optional[PropertyGraphView] = None
    campaign_graph: Optional[CampaignGraphView] = None
    inventory_graph: Optional[InventoryGraphView] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


__all__ = [
    "CommGraphChannelView",
    "CommGraphView",
    "CapabilityGraphActionInput",
    "CapabilityGraphActionResult",
    "CapabilityGraphActionSchema",
    "CapabilityGraphPlan",
    "CapabilityGraphPlanStep",
    "DataGraphView",
    "DataWorkbookView",
    "DocGraphView",
    "DocumentView",
    "DriveShareView",
    "GraphActionPriority",
    "HrisEmployeeView",
    "IdentityApplicationView",
    "IdentityGraphView",
    "IdentityGroupView",
    "IdentityPolicyView",
    "IdentityUserView",
    "LeaseView",
    "ObsGraphView",
    "ObsIncidentView",
    "ObsMonitorView",
    "ObsServiceView",
    "OpsFlagView",
    "OpsGraphView",
    "OrderView",
    "PropertyGraphView",
    "PropertyView",
    "QuoteView",
    "RevenueCompanyView",
    "RevenueContactView",
    "RevenueDealView",
    "RevenueGraphView",
    "RuntimeCapabilityGraphs",
    "SiteView",
    "UnitView",
    "VendorView",
    "WorkOrderGraphView",
    "ClientView",
    "CampaignGraphView",
    "CampaignView",
    "CampaignApprovalView",
    "CampaignReportView",
    "CreativeView",
    "CapacityPoolView",
    "AllocationView",
    "InventoryGraphView",
    "ServiceRequestView",
    "WorkGraphView",
    "WorkItemView",
]
