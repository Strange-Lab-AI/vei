from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


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


class RuntimeCapabilityGraphs(BaseModel):
    branch: str
    clock_ms: int
    available_domains: List[str] = Field(default_factory=list)
    comm_graph: Optional[CommGraphView] = None
    doc_graph: Optional[DocGraphView] = None
    work_graph: Optional[WorkGraphView] = None
    identity_graph: Optional[IdentityGraphView] = None
    revenue_graph: Optional[RevenueGraphView] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


__all__ = [
    "CommGraphChannelView",
    "CommGraphView",
    "DocGraphView",
    "DocumentView",
    "DriveShareView",
    "HrisEmployeeView",
    "IdentityApplicationView",
    "IdentityGraphView",
    "IdentityGroupView",
    "IdentityPolicyView",
    "IdentityUserView",
    "RevenueCompanyView",
    "RevenueContactView",
    "RevenueDealView",
    "RevenueGraphView",
    "RuntimeCapabilityGraphs",
    "ServiceRequestView",
    "WorkGraphView",
    "WorkItemView",
]
