from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field

from vei.blueprint.models import BlueprintCapabilityGraphsAsset


class IdentityGovernanceWorkflowSeed(BaseModel):
    employee_id: str
    user_id: str
    corporate_email: str
    manager_email: str
    crm_app_id: str = "APP-crm"
    doc_id: str
    tracking_ticket_id: str
    cutover_doc_id: str
    opportunity_id: str
    allowed_share_count: int = 1
    revoked_share_email: str
    deadline_max_ms: int = 86_400_000
    transfer_note: str
    onboarding_note: str
    ticket_update_note: str
    cutover_doc_note: str
    slack_channel: str = "#sales-cutover"
    slack_summary: str


class IdentityGovernanceBundle(BaseModel):
    name: str
    title: str
    description: str
    scenario_template_name: str = "acquired_sales_onboarding"
    family_name: str = "enterprise_onboarding_migration"
    workflow_name: str = "enterprise_onboarding_migration"
    workflow_variant: str = "manager_cutover"
    requested_facades: List[str] = Field(default_factory=list)
    capability_graphs: BlueprintCapabilityGraphsAsset
    workflow_seed: IdentityGovernanceWorkflowSeed
    metadata: Dict[str, Any] = Field(default_factory=dict)
    policy_notes: List[str] = Field(default_factory=list)
    incident_history: List[Dict[str, Any]] = Field(default_factory=list)
    acceptance_focus: List[str] = Field(default_factory=list)


class GroundingBundleManifest(BaseModel):
    name: str
    title: str
    description: str
    wedge: str
    scenario_template_name: str
    workflow_name: str
    workflow_variant: str
    organization_domain: str
    capability_domains: List[str] = Field(default_factory=list)


__all__ = [
    "GroundingBundleManifest",
    "IdentityGovernanceBundle",
    "IdentityGovernanceWorkflowSeed",
]
