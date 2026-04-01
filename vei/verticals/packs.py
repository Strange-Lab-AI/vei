from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List

from pydantic import BaseModel, Field

from vei.blueprint.models import BlueprintAsset

from .packs_marketing import build as _build_marketing_blueprint
from .packs_real_estate import build as _build_real_estate_blueprint
from .packs_saas import build as _build_saas_blueprint
from .packs_service_ops import build as _build_service_ops_blueprint
from .packs_storage import build as _build_storage_blueprint


class VerticalPackManifest(BaseModel):
    name: str
    title: str
    description: str
    company_name: str
    company_briefing: str
    failure_impact: str
    objective_focus: str
    scenario_name: str
    workflow_name: str
    workflow_variant: str
    runtime_component: str | None = None
    runtime_family: str | None = None
    key_surfaces: List[str] = Field(default_factory=list)
    proves: List[str] = Field(default_factory=list)
    what_if_branches: List[str] = Field(default_factory=list)


@dataclass(frozen=True)
class VerticalPackDefinition:
    manifest: VerticalPackManifest
    blueprint_builder: Callable[[], BlueprintAsset]


def list_vertical_pack_manifests() -> List[VerticalPackManifest]:
    return sorted(
        (item.manifest for item in _VERTICAL_PACKS.values()),
        key=lambda item: item.name,
    )


def list_vertical_pack_names() -> List[str]:
    return [item.name for item in list_vertical_pack_manifests()]


def get_vertical_pack_manifest(name: str) -> VerticalPackManifest:
    key = name.strip().lower()
    if key not in _VERTICAL_PACKS:
        raise KeyError(f"unknown vertical pack: {name}")
    return _VERTICAL_PACKS[key].manifest


def build_vertical_blueprint_asset(name: str) -> BlueprintAsset:
    key = name.strip().lower()
    definition = _VERTICAL_PACKS.get(key)
    if definition is None:
        raise KeyError(f"unknown vertical pack: {name}")
    return definition.blueprint_builder()


def _definition(
    manifest: VerticalPackManifest,
    blueprint_builder: Callable[[], BlueprintAsset],
) -> VerticalPackDefinition:
    return VerticalPackDefinition(
        manifest=manifest,
        blueprint_builder=blueprint_builder,
    )


_VERTICAL_PACKS: Dict[str, VerticalPackDefinition] = {
    "real_estate_management": _definition(
        VerticalPackManifest(
            name="real_estate_management",
            title="Real Estate Management",
            description="Lease, vendor, and property-readiness conflict for a high-stakes tenant opening.",
            company_name="Harbor Point Management",
            company_briefing=(
                "Harbor Point Management operates retail and mixed-use properties, coordinating "
                "leasing, property operations, vendors, tenant readiness, and customer-facing artifacts."
            ),
            failure_impact=(
                "If this scenario goes badly, Harbor Point misses a flagship tenant opening, loses tenant trust, "
                "and creates an expensive operational scramble across leasing, facilities, and vendors."
            ),
            objective_focus=(
                "Keep the opening valid and business-real: lease state, unit readiness, vendor work, and tenant-facing "
                "artifacts all need to line up before Monday morning."
            ),
            scenario_name="tenant_opening_conflict",
            workflow_name="real_estate_management",
            workflow_variant="tenant_opening_conflict",
            runtime_component="property_ops",
            runtime_family="property",
            key_surfaces=[
                "property_graph",
                "docs",
                "slack",
                "mail",
                "jira",
                "servicedesk",
            ],
            proves=[
                "branchable opening readiness",
                "vendor/lease coordination",
                "artifact follow-through",
            ],
            what_if_branches=[
                "Delay vendor assignment and miss opening",
                "Execute amendment but leave the unit unreserved",
            ],
        ),
        _build_real_estate_blueprint,
    ),
    "digital_marketing_agency": _definition(
        VerticalPackManifest(
            name="digital_marketing_agency",
            title="Digital Marketing Agency",
            description="Launch guardrail workflow for a campaign with approval, pacing, and reporting risk.",
            company_name="Northstar Growth",
            company_briefing=(
                "Northstar Growth runs client campaigns across channels, creative approvals, reporting, budgets, "
                "and account communication, with launch integrity depending on multiple teams staying aligned."
            ),
            failure_impact=(
                "If this scenario breaks, the agency can launch unapproved creative, overspend budget, and erode client trust "
                "with stale reporting and confused communication."
            ),
            objective_focus=(
                "Protect launch integrity: approvals, pacing, reporting, and client-facing artifacts should all be trustworthy "
                "before spend is allowed to keep flowing."
            ),
            scenario_name="campaign_launch_guardrail",
            workflow_name="digital_marketing_agency",
            workflow_variant="campaign_launch_guardrail",
            runtime_component="campaign_ops",
            runtime_family="campaign",
            key_surfaces=["campaign_graph", "docs", "slack", "mail", "jira", "crm"],
            proves=["launch safety", "budget control", "client artifact hygiene"],
            what_if_branches=[
                "Pause the launch and protect spend",
                "Push through with stale reporting and approval drift",
            ],
        ),
        _build_marketing_blueprint,
    ),
    "storage_solutions": _definition(
        VerticalPackManifest(
            name="storage_solutions",
            title="Storage Solutions",
            description="Strategic customer quote with fragmented capacity and fulfillment coordination pressure.",
            company_name="Atlas Storage Systems",
            company_briefing=(
                "Atlas Storage Systems designs and fulfills large-scale storage rollouts, coordinating quotes, capacity, "
                "site allocation, vendors, fulfillment planning, and customer commitments."
            ),
            failure_impact=(
                "If this scenario fails, Atlas can overcommit capacity, send an impossible quote, and create downstream "
                "fulfillment failures for a strategic customer rollout."
            ),
            objective_focus=(
                "Keep the commercial promise feasible: capacity allocation, ops planning, vendor follow-through, and "
                "customer-facing artifacts must remain internally consistent."
            ),
            scenario_name="capacity_quote_commitment",
            workflow_name="storage_solutions",
            workflow_variant="capacity_quote_commitment",
            runtime_component="inventory_ops",
            runtime_family="inventory",
            key_surfaces=["inventory_graph", "docs", "slack", "mail", "jira", "crm"],
            proves=["capacity feasibility", "quote accuracy", "ops follow-through"],
            what_if_branches=[
                "Reserve fragmented capacity and keep the customer timeline",
                "Overcommit the quote and create a downstream fulfillment failure",
            ],
        ),
        _build_storage_blueprint,
    ),
    "b2b_saas": _definition(
        VerticalPackManifest(
            name="b2b_saas",
            title="B2B SaaS",
            description="Enterprise renewal at risk with integration failure, champion departure, and competitive pressure.",
            company_name="Pinnacle Analytics",
            company_briefing=(
                "Pinnacle Analytics is a B2B analytics SaaS company. Their largest customer, "
                "Apex Financial ($480K ARR), is 6 weeks from renewal with a broken integration, "
                "a departed champion, and a competitor circling."
            ),
            failure_impact=(
                "If this scenario fails, Pinnacle loses a $480K renewal, the Q3 expansion target collapses, "
                "and the competitor gains a reference customer in financial services."
            ),
            objective_focus=(
                "Save the renewal: fix the integration, rebuild stakeholder trust, neutralize the competitive threat, "
                "and get the proposal in front of the new decision-maker before she signs with DataVault."
            ),
            scenario_name="enterprise_renewal_risk",
            workflow_name="b2b_saas",
            workflow_variant="enterprise_renewal_risk",
            runtime_component="crm",
            runtime_family="revenue",
            key_surfaces=[
                "revenue_graph",
                "docs",
                "slack",
                "mail",
                "jira",
                "servicedesk",
            ],
            proves=[
                "cross-functional renewal coordination",
                "competitive displacement defense",
                "product-to-CS handoff under pressure",
            ],
            what_if_branches=[
                "Fix the integration first and rebuild trust before the renewal conversation",
                "Lead with the discount and hope the product issues don't kill the deal",
            ],
        ),
        _build_saas_blueprint,
    ),
    "service_ops": _definition(
        VerticalPackManifest(
            name="service_ops",
            title="Service Operations",
            description="Dispatch, billing, and exception collision for a VIP service account under morning pressure.",
            company_name="Clearwater Field Services",
            company_briefing=(
                "Clearwater Field Services coordinates field dispatch, account handling, billing follow-through, "
                "and exception management across a high-volume service business."
            ),
            failure_impact=(
                "If this scenario fails, Clearwater misses a VIP SLA, aggravates an open billing dispute, "
                "and turns one bad morning into churn and manual cleanup."
            ),
            objective_focus=(
                "Protect the service loop end to end: route the right technician, keep billing safe, "
                "and resolve the morning with a credible cross-team trail."
            ),
            scenario_name="service_day_collision",
            workflow_name="service_ops",
            workflow_variant="service_day_collision",
            runtime_component="service_ops",
            runtime_family="service_ops",
            key_surfaces=["ops_graph", "docs", "slack", "mail", "jira", "servicedesk"],
            proves=[
                "dispatch recovery",
                "billing safety",
                "policy-shaped service operations",
            ],
            what_if_branches=[
                "Leave billing live while the dispute is still open",
                "Change one policy threshold and rerun the same morning",
            ],
        ),
        _build_service_ops_blueprint,
    ),
}
