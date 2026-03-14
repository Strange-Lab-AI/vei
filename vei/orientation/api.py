from __future__ import annotations

from typing import Any, Dict, List

from vei.capability_graph.api import build_runtime_capability_graphs
from vei.world.models import WorldState

from .models import OrientationObject, OrientationPolicyHint, WorldOrientation


def build_world_orientation(state: WorldState) -> WorldOrientation:
    graphs = build_runtime_capability_graphs(state)
    metadata = _scenario_metadata(state)
    hint_block = metadata.get("builder_blueprint_orientation")
    hint_map = hint_block if isinstance(hint_block, dict) else {}

    available_surfaces = _resolve_available_surfaces(
        discovered=_available_surfaces(state.components),
        hint_map=hint_map,
    )
    active_policies = _active_policies(graphs)
    key_objects = _key_objects(graphs)
    suggested_focuses = _suggested_focuses(
        graphs.available_domains,
        available_surfaces,
        active_policies,
        key_objects,
        hint_map,
    )
    next_questions = _next_questions(
        graphs.available_domains,
        available_surfaces,
        active_policies,
        key_objects,
    )

    scenario_template_name = _optional_str(metadata.get("scenario_template_name"))
    runtime_scenario_name = _optional_str(
        metadata.get("builder_runtime_scenario_name")
    ) or _optional_str(hint_map.get("runtime_scenario_name"))
    scenario_name = (
        _optional_str(state.scenario.get("name"))
        or runtime_scenario_name
        or _optional_str(metadata.get("scenario_name"))
        or scenario_template_name
        or "unknown"
    )
    organization_name = _optional_str(metadata.get("builder_organization_name"))
    organization_domain = _optional_str(metadata.get("builder_organization_domain"))
    timezone = _optional_str(metadata.get("builder_timezone"))
    builder_mode = _optional_str(metadata.get("builder_mode"))

    summary_parts = [f"Scenario {scenario_name}"]
    if organization_name:
        summary_parts.append(f"for {organization_name}")
    if available_surfaces:
        summary_parts.append(
            f"with surfaces {', '.join(available_surfaces[:6])}"
            + ("..." if len(available_surfaces) > 6 else "")
        )
    if active_policies:
        summary_parts.append(
            f"and {len(active_policies)} active policy constraint"
            + ("s" if len(active_policies) != 1 else "")
        )

    return WorldOrientation(
        scenario_name=scenario_name,
        scenario_template_name=scenario_template_name,
        organization_name=organization_name,
        organization_domain=organization_domain,
        timezone=timezone,
        builder_mode=builder_mode,
        available_domains=graphs.available_domains,
        available_surfaces=available_surfaces,
        active_policies=active_policies,
        key_objects=key_objects,
        suggested_focuses=suggested_focuses,
        next_questions=next_questions,
        summary=" ".join(summary_parts) + ".",
    )


def _available_surfaces(components: Dict[str, Dict[str, Any]]) -> List[str]:
    surfaces: List[str] = []
    for name, payload in sorted(components.items()):
        if not isinstance(payload, dict):
            continue
        available = payload.get("available")
        if available is False:
            continue
        if available is True or payload:
            surfaces.append(name)
    return surfaces


def _resolve_available_surfaces(
    *,
    discovered: List[str],
    hint_map: Dict[str, Any],
) -> List[str]:
    hinted = [str(item) for item in hint_map.get("facades", []) or [] if str(item)]
    if hinted:
        ordered: List[str] = []
        seen: set[str] = set()
        for surface in hinted:
            if surface not in seen:
                ordered.append(surface)
                seen.add(surface)
        return ordered
    return sorted(discovered)


def _active_policies(graphs: Any) -> List[OrientationPolicyHint]:
    identity_graph = graphs.identity_graph
    if identity_graph is None:
        return []
    policies = []
    for policy in identity_graph.policies:
        parts = []
        if policy.allowed_application_ids:
            parts.append(
                "allowed apps: " + ", ".join(policy.allowed_application_ids[:4])
            )
        if policy.forbidden_share_domains:
            parts.append(
                "forbidden share domains: "
                + ", ".join(policy.forbidden_share_domains[:4])
            )
        if policy.required_approval_stages:
            parts.append(
                "required approvals: " + ", ".join(policy.required_approval_stages[:4])
            )
        if policy.deadline_max_ms is not None:
            parts.append(f"deadline <= {policy.deadline_max_ms} ms")
        policies.append(
            OrientationPolicyHint(
                policy_id=policy.policy_id,
                title=policy.title,
                summary="; ".join(parts) if parts else policy.title,
            )
        )
    return policies


def _key_objects(graphs: Any) -> List[OrientationObject]:
    objects: List[OrientationObject] = []
    if graphs.identity_graph is not None:
        for employee in graphs.identity_graph.hris_employees:
            if employee.identity_conflict or not employee.onboarded:
                objects.append(
                    OrientationObject(
                        domain="identity_graph",
                        kind="employee",
                        object_id=employee.employee_id,
                        title=employee.display_name,
                        status=employee.status,
                        reason=(
                            "identity conflict"
                            if employee.identity_conflict
                            else "not onboarded"
                        ),
                    )
                )
        for user in graphs.identity_graph.users:
            if user.status.upper() == "ACTIVE":
                continue
            objects.append(
                OrientationObject(
                    domain="identity_graph",
                    kind="user",
                    object_id=user.user_id,
                    title=user.display_name or user.email,
                    status=user.status,
                    reason="account requires review",
                )
            )
    if graphs.doc_graph is not None:
        for share in graphs.doc_graph.drive_shares:
            if share.visibility != "internal" or share.shared_with:
                objects.append(
                    OrientationObject(
                        domain="doc_graph",
                        kind="drive_share",
                        object_id=share.doc_id,
                        title=share.title,
                        status=share.visibility,
                        reason="sharing posture requires review",
                    )
                )
                break
    if graphs.work_graph is not None:
        for ticket in graphs.work_graph.tickets[:2]:
            objects.append(
                OrientationObject(
                    domain="work_graph",
                    kind="ticket",
                    object_id=ticket.item_id,
                    title=ticket.title,
                    status=ticket.status,
                )
            )
    if graphs.revenue_graph is not None:
        for deal in graphs.revenue_graph.deals[:2]:
            objects.append(
                OrientationObject(
                    domain="revenue_graph",
                    kind="deal",
                    object_id=deal.deal_id,
                    title=deal.name,
                    status=deal.stage,
                )
            )
    if graphs.data_graph is not None:
        for workbook in graphs.data_graph.workbooks[:1]:
            objects.append(
                OrientationObject(
                    domain="data_graph",
                    kind="workbook",
                    object_id=workbook.workbook_id,
                    title=workbook.title,
                    status=f"{workbook.sheet_count} sheets",
                    reason="analysis surface available",
                )
            )
    if graphs.obs_graph is not None:
        for incident in graphs.obs_graph.incidents[:1]:
            objects.append(
                OrientationObject(
                    domain="obs_graph",
                    kind="incident",
                    object_id=incident.incident_id,
                    title=incident.title,
                    status=incident.status,
                    reason="live operational signal",
                )
            )
        for service in graphs.obs_graph.services[:1]:
            objects.append(
                OrientationObject(
                    domain="obs_graph",
                    kind="service",
                    object_id=service.service_id,
                    title=service.name,
                    status=service.status,
                    reason="service health requires review",
                )
            )
    if graphs.ops_graph is not None:
        for flag in graphs.ops_graph.flags[:1]:
            objects.append(
                OrientationObject(
                    domain="ops_graph",
                    kind="feature_flag",
                    object_id=flag.flag_key,
                    title=flag.flag_key,
                    status=f"{flag.rollout_pct}% rollout",
                    reason="control plane is available",
                )
            )
    if graphs.property_graph is not None:
        for lease in graphs.property_graph.leases[:1]:
            objects.append(
                OrientationObject(
                    domain="property_graph",
                    kind="lease",
                    object_id=lease.lease_id,
                    title=lease.lease_id,
                    status=lease.status,
                    reason="tenant opening depends on lease readiness",
                )
            )
        for work_order in graphs.property_graph.work_orders[:1]:
            objects.append(
                OrientationObject(
                    domain="property_graph",
                    kind="work_order",
                    object_id=work_order.work_order_id,
                    title=work_order.title,
                    status=work_order.status,
                    reason="site readiness may still be blocked",
                )
            )
    if graphs.campaign_graph is not None:
        for campaign in graphs.campaign_graph.campaigns[:1]:
            objects.append(
                OrientationObject(
                    domain="campaign_graph",
                    kind="campaign",
                    object_id=campaign.campaign_id,
                    title=campaign.name,
                    status=campaign.status,
                    reason="launch pacing and approvals need review",
                )
            )
        for creative in graphs.campaign_graph.creatives[:1]:
            objects.append(
                OrientationObject(
                    domain="campaign_graph",
                    kind="creative",
                    object_id=creative.creative_id,
                    title=creative.title,
                    status=creative.status,
                    reason="creative approval may block launch",
                )
            )
    if graphs.inventory_graph is not None:
        for quote in graphs.inventory_graph.quotes[:1]:
            objects.append(
                OrientationObject(
                    domain="inventory_graph",
                    kind="quote",
                    object_id=quote.quote_id,
                    title=quote.customer_name,
                    status=quote.status,
                    reason="quote feasibility depends on capacity and fulfillment",
                )
            )
        for allocation in graphs.inventory_graph.allocations[:1]:
            objects.append(
                OrientationObject(
                    domain="inventory_graph",
                    kind="allocation",
                    object_id=allocation.allocation_id,
                    title=allocation.quote_id,
                    status=allocation.status,
                    reason="capacity reservation affects customer commitment",
                )
            )
    if graphs.comm_graph is not None:
        for channel in graphs.comm_graph.channels[:2]:
            objects.append(
                OrientationObject(
                    domain="comm_graph",
                    kind="channel",
                    object_id=channel.channel,
                    title=channel.latest_text,
                    status=f"{channel.message_count} messages",
                )
            )
    return objects[:10]


def _suggested_focuses(
    domains: List[str],
    surfaces: List[str],
    policies: List[OrientationPolicyHint],
    key_objects: List[OrientationObject],
    hint_map: Dict[str, Any],
) -> List[str]:
    focuses: List[str] = []
    for focus in hint_map.get("focus_hints", []) or []:
        if focus not in focuses:
            focuses.append(str(focus))
    inspection_focus = hint_map.get("inspection_focus")
    if inspection_focus and inspection_focus not in focuses:
        focuses.append(str(inspection_focus))
    if policies and "identity_graph" not in focuses:
        focuses.append("identity_graph")
    for domain in domains:
        if domain not in focuses:
            focuses.append(domain)
    for surface in surfaces:
        if (
            surface in {"google_admin", "datadog", "pagerduty", "feature_flags"}
            and surface not in focuses
        ):
            focuses.append(surface)
    for item in key_objects:
        if item.domain not in focuses:
            focuses.append(item.domain)
    return focuses[:8]


def _next_questions(
    domains: List[str],
    surfaces: List[str],
    policies: List[OrientationPolicyHint],
    key_objects: List[OrientationObject],
) -> List[str]:
    questions: List[str] = []
    if policies:
        questions.append("Which active policy constraints can block the task?")
    if any(
        surface in {"datadog", "pagerduty", "feature_flags"} for surface in surfaces
    ):
        questions.append(
            "Which alert, incident, or rollout control should be checked first?"
        )
    if "revenue_graph" in domains:
        questions.append("Which revenue object or owner changes depend on the task?")
    if "data_graph" in domains:
        questions.append("Which workbook or analysis sheet needs to be updated?")
    if any(item.domain == "identity_graph" for item in key_objects):
        questions.append("Which identity records or approvals are currently unsafe?")
    if any(item.domain == "doc_graph" for item in key_objects):
        questions.append("Is any document or drive share still overshared?")
    if any(item.domain == "work_graph" for item in key_objects):
        questions.append("Which tracking ticket or request should be updated next?")
    if any(item.domain == "ops_graph" for item in key_objects):
        questions.append("Which rollout control can safely reduce blast radius?")
    if any(item.domain == "property_graph" for item in key_objects):
        questions.append("Which lease, unit, or work order is blocking the opening?")
    if any(item.domain == "campaign_graph" for item in key_objects):
        questions.append(
            "Which campaign approval, pacing, or reporting artifact is unsafe?"
        )
    if any(item.domain == "inventory_graph" for item in key_objects):
        questions.append("Which quote or capacity pool risks overcommitment?")
    if not questions:
        questions.append("Which domain should the agent inspect first?")
    return questions[:5]


def _scenario_metadata(state: WorldState) -> Dict[str, Any]:
    scenario = state.scenario or {}
    metadata = scenario.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


__all__ = ["build_world_orientation"]
