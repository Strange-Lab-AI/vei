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
    if any(item.domain == "identity_graph" for item in key_objects):
        questions.append("Which identity records or approvals are currently unsafe?")
    if any(item.domain == "doc_graph" for item in key_objects):
        questions.append("Is any document or drive share still overshared?")
    if any(item.domain == "work_graph" for item in key_objects):
        questions.append("Which tracking ticket or request should be updated next?")
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
