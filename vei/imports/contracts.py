from __future__ import annotations

from copy import deepcopy
from typing import Any

from vei.grounding.models import IdentityGovernanceBundle


def bootstrap_contract_from_import_bundle(
    *,
    bundle: IdentityGovernanceBundle,
    contract_payload: dict[str, Any],
    scenario_name: str,
    workflow_parameters: dict[str, Any],
) -> dict[str, Any]:
    payload = deepcopy(contract_payload)
    identity_graph = bundle.capability_graphs.identity_graph
    if identity_graph is None:
        return payload
    policy = identity_graph.policies[0] if identity_graph.policies else None
    if policy is None:
        raise ValueError("Cannot bootstrap contract without imported identity policy")

    metadata = payload.setdefault("metadata", {})
    metadata.update(
        {
            "import_policy_id": policy.policy_id,
            "grounding_bundle": bundle.name,
            "contract_bootstrap": "import_policy_acl",
            "imported_vs_derived": {
                "imported": True,
                "derived": True,
                "simulated": True,
            },
        }
    )
    metadata.setdefault("rule_provenance", [])
    metadata.setdefault(
        "contract_bootstrap_summary",
        {
            "policy_id": policy.policy_id,
            "scenario_name": scenario_name,
            "applied_rule_count": 0,
        },
    )

    required_approval_stages = list(policy.required_approval_stages)
    for stage in required_approval_stages:
        _append_policy_invariant(
            payload,
            {
                "name": f"import_policy:{stage}",
                "description": f"Imported policy {policy.policy_id} requires approval stage {stage}.",
                "required": True,
                "evidence": policy.policy_id,
                "metadata": {
                    "origin": "imported",
                    "source_policy_id": policy.policy_id,
                    "stage": stage,
                    "applies_to": [
                        workflow_parameters.get("request_id"),
                        workflow_parameters.get("ticket_id"),
                    ],
                },
            },
            reason="Approval stage imported from tenant policy posture.",
            source_policy_id=policy.policy_id,
            object_refs=_compact_refs(
                f"service_request:{workflow_parameters.get('request_id')}",
                f"ticket:{workflow_parameters.get('ticket_id')}",
            ),
            origin="imported",
        )

    seed = bundle.workflow_seed
    doc_id = workflow_parameters.get("doc_id") or seed.doc_id
    cutover_doc_id = workflow_parameters.get("cutover_doc_id") or seed.cutover_doc_id
    ticket_id = workflow_parameters.get("ticket_id") or seed.tracking_ticket_id
    request_id = workflow_parameters.get("request_id") or "REQ-2201"
    user_id = workflow_parameters.get("user_id") or seed.user_id
    primary_app_id = workflow_parameters.get("primary_app_id") or seed.crm_app_id
    stale_app_id = workflow_parameters.get("stale_app_id")
    slack_channel = workflow_parameters.get("slack_channel") or seed.slack_channel
    doc_update_note = (
        workflow_parameters.get("doc_update_note") or seed.cutover_doc_note
    )
    ticket_note = workflow_parameters.get("ticket_note") or seed.ticket_update_note
    slack_summary = workflow_parameters.get("slack_summary") or seed.slack_summary
    request_comment = workflow_parameters.get("request_comment")
    policy_metadata = dict(getattr(policy, "metadata", {}) or {})

    for domain in policy.forbidden_share_domains:
        if not doc_id:
            continue
        _append_predicate(
            payload,
            bucket="forbidden_predicates",
            spec={
                "name": f"forbidden_share_domain:{domain}",
                "source": "oracle_state",
                "assertion": {
                    "kind": "state_not_contains",
                    "field": (
                        f"components.google_admin.drive_shares.{doc_id}.shared_with"
                    ),
                    "contains": domain,
                    "description": f"Imported policy forbids share domain {domain}.",
                },
                "description": f"Imported policy forbids share domain {domain}.",
                "metadata": {
                    "origin": "imported",
                    "source_policy_id": policy.policy_id,
                    "object_ref": f"drive_share:{doc_id}",
                    "forbidden_domain": domain,
                },
            },
            reason="Forbidden sharing domain derived directly from imported ACL policy.",
            source_policy_id=policy.policy_id,
            object_refs=[f"drive_share:{doc_id}"],
            origin="imported",
        )

    if (
        primary_app_id
        and user_id
        and scenario_name
        in {
            "acquired_user_cutover",
            "joiner_mover_leaver",
            "stale_entitlement_cleanup",
            "approval_bottleneck",
            "break_glass_follow_up",
        }
    ):
        _append_predicate(
            payload,
            bucket="success_predicates",
            spec={
                "name": f"primary_app_present:{primary_app_id}",
                "source": "oracle_state",
                "assertion": {
                    "kind": "state_contains",
                    "field": f"components.okta.users.{user_id}.applications",
                    "contains": primary_app_id,
                    "description": "Imported entitlement posture requires the primary application to remain assigned.",
                },
                "description": "Imported entitlement posture requires the primary application to remain assigned.",
                "metadata": {
                    "origin": "imported",
                    "source_policy_id": policy.policy_id,
                    "object_ref": f"identity_user:{user_id}",
                    "primary_app_id": primary_app_id,
                },
            },
            reason="Allowed application set imported from tenant entitlement posture.",
            source_policy_id=policy.policy_id,
            object_refs=[
                f"identity_user:{user_id}",
                f"identity_application:{primary_app_id}",
            ],
            origin="imported",
        )

    if (
        stale_app_id
        and user_id
        and scenario_name
        in {
            "stale_entitlement_cleanup",
            "break_glass_follow_up",
        }
    ):
        _append_predicate(
            payload,
            bucket="forbidden_predicates",
            spec={
                "name": f"stale_app_removed:{stale_app_id}",
                "source": "oracle_state",
                "assertion": {
                    "kind": "state_not_contains",
                    "field": f"components.okta.users.{user_id}.applications",
                    "contains": stale_app_id,
                    "description": "Imported least-privilege policy requires removing stale application access.",
                },
                "description": "Imported least-privilege policy requires removing stale application access.",
                "metadata": {
                    "origin": "imported",
                    "source_policy_id": policy.policy_id,
                    "object_ref": f"identity_user:{user_id}",
                    "stale_app_id": stale_app_id,
                },
            },
            reason="Imported least-privilege policy marks this assignment as drift.",
            source_policy_id=policy.policy_id,
            object_refs=[
                f"identity_user:{user_id}",
                f"identity_application:{stale_app_id}",
            ],
            origin="imported",
        )

    if request_id and scenario_name in {
        "approval_bottleneck",
        "acquired_user_cutover",
        "joiner_mover_leaver",
    }:
        _append_predicate(
            payload,
            bucket="success_predicates",
            spec={
                "name": f"approval_completed:{request_id}",
                "source": "oracle_state",
                "assertion": {
                    "kind": "state_contains",
                    "field": f"components.servicedesk.requests.{request_id}.approvals",
                    "contains": "APPROVED",
                    "description": "Imported approval chain must reach an approved state.",
                },
                "description": "Imported approval chain must reach an approved state.",
                "metadata": {
                    "origin": "imported",
                    "source_policy_id": policy.policy_id,
                    "object_ref": f"service_request:{request_id}",
                },
            },
            reason="Approval completion is required by imported governance policy.",
            source_policy_id=policy.policy_id,
            object_refs=[f"service_request:{request_id}"],
            origin="imported",
        )
        if request_comment:
            _append_predicate(
                payload,
                bucket="success_predicates",
                spec={
                    "name": f"approval_comment_recorded:{request_id}",
                    "source": "oracle_state",
                    "assertion": {
                        "kind": "state_contains",
                        "field": f"components.servicedesk.requests.{request_id}.comments",
                        "contains": request_comment,
                        "description": "Imported approval workflow should leave an auditable comment trail.",
                    },
                    "description": "Imported approval workflow should leave an auditable comment trail.",
                    "metadata": {
                        "origin": "inferred",
                        "source_policy_id": policy.policy_id,
                        "object_ref": f"service_request:{request_id}",
                    },
                },
                reason="Imported approval chains should preserve comment evidence for operators.",
                source_policy_id=policy.policy_id,
                object_refs=[f"service_request:{request_id}"],
                origin="derived",
            )

    if ticket_id and ticket_note:
        _append_predicate(
            payload,
            bucket="success_predicates",
            spec={
                "name": f"tracker_followthrough:{ticket_id}",
                "source": "oracle_state",
                "assertion": {
                    "kind": "state_contains",
                    "field": f"components.tickets.metadata.{ticket_id}.comments",
                    "contains": ticket_note,
                    "description": "Imported governance workflows should leave tracker evidence.",
                },
                "description": "Imported governance workflows should leave tracker evidence.",
                "metadata": {
                    "origin": "derived",
                    "source_policy_id": policy.policy_id,
                    "object_ref": f"ticket:{ticket_id}",
                },
            },
            reason="Operational follow-through should be visible in the tracker for imported environments.",
            source_policy_id=policy.policy_id,
            object_refs=[f"ticket:{ticket_id}"],
            origin="derived",
        )

    if (
        cutover_doc_id
        and doc_update_note
        and scenario_name
        in {
            "acquired_user_cutover",
            "joiner_mover_leaver",
            "oversharing_remediation",
            "break_glass_follow_up",
        }
    ):
        _append_predicate(
            payload,
            bucket="success_predicates",
            spec={
                "name": f"governance_doc_updated:{cutover_doc_id}",
                "source": "oracle_state",
                "assertion": {
                    "kind": "state_contains",
                    "field": f"components.docs.docs.{cutover_doc_id}.body",
                    "contains": doc_update_note,
                    "description": "Imported identity governance should update its primary artifact.",
                },
                "description": "Imported identity governance should update its primary artifact.",
                "metadata": {
                    "origin": "derived",
                    "source_policy_id": policy.policy_id,
                    "object_ref": f"document:{cutover_doc_id}",
                },
            },
            reason="Documented evidence should be preserved even when the source environment was partially imported.",
            source_policy_id=policy.policy_id,
            object_refs=[f"document:{cutover_doc_id}"],
            origin="derived",
        )

    if slack_channel and slack_summary:
        _append_predicate(
            payload,
            bucket="success_predicates",
            spec={
                "name": f"stakeholder_summary_sent:{slack_channel}",
                "source": "oracle_state",
                "assertion": {
                    "kind": "state_contains",
                    "field": f"components.slack.channels.{slack_channel}.messages",
                    "contains": slack_summary,
                    "description": "Imported governance workflows should leave a stakeholder summary.",
                },
                "description": "Imported governance workflows should leave a stakeholder summary.",
                "metadata": {
                    "origin": "derived",
                    "source_policy_id": policy.policy_id,
                    "object_ref": f"comm_channel:{slack_channel}",
                },
            },
            reason="Stakeholder communication is part of the expected enterprise follow-through.",
            source_policy_id=policy.policy_id,
            object_refs=[f"comm_channel:{slack_channel}"],
            origin="derived",
        )

    if (
        policy_metadata.get("break_glass_requires_followup")
        and scenario_name == "break_glass_follow_up"
    ):
        _append_intervention_rule(
            payload,
            {
                "name": "break_glass_followup_required",
                "trigger": "imported_break_glass_detected",
                "action": "remove_temporary_access_and_record_followup",
                "actor": "security-review",
                "required": True,
                "evidence": policy.policy_id,
            },
            reason="Imported policy states that emergency access must be followed by explicit cleanup.",
            source_policy_id=policy.policy_id,
            object_refs=_compact_refs(
                f"identity_user:{user_id}" if user_id else None,
                f"document:{cutover_doc_id}" if cutover_doc_id else None,
                f"ticket:{ticket_id}" if ticket_id else None,
            ),
            origin="imported",
        )

    if policy.deadline_max_ms:
        _append_reward_term(
            payload,
            {
                "name": f"{scenario_name}:deadline",
                "weight": 0.2,
                "term_type": "success",
                "description": f"Imported policy deadline {policy.deadline_max_ms}ms respected.",
                "metadata": {
                    "origin": "imported",
                    "source_policy_id": policy.policy_id,
                    "deadline_max_ms": policy.deadline_max_ms,
                },
            },
            reason="Tenant policy included an explicit deadline or review window.",
            source_policy_id=policy.policy_id,
            object_refs=[],
            origin="imported",
        )

    summary = metadata.get("contract_bootstrap_summary", {})
    summary["applied_rule_count"] = len(metadata.get("rule_provenance", []))
    metadata["contract_bootstrap_summary"] = summary
    return payload


def _append_predicate(
    payload: dict[str, Any],
    *,
    bucket: str,
    spec: dict[str, Any],
    reason: str,
    source_policy_id: str,
    object_refs: list[str],
    origin: str,
) -> None:
    predicates = payload.setdefault(bucket, [])
    if any(item.get("name") == spec["name"] for item in predicates):
        return
    predicates.append(spec)
    _append_rule_provenance(
        payload,
        rule_name=spec["name"],
        rule_kind=bucket,
        reason=reason,
        source_policy_id=source_policy_id,
        object_refs=object_refs,
        origin=origin,
    )


def _append_policy_invariant(
    payload: dict[str, Any],
    spec: dict[str, Any],
    *,
    reason: str,
    source_policy_id: str,
    object_refs: list[str],
    origin: str,
) -> None:
    invariants = payload.setdefault("policy_invariants", [])
    if any(item.get("name") == spec["name"] for item in invariants):
        return
    invariants.append(spec)
    _append_rule_provenance(
        payload,
        rule_name=spec["name"],
        rule_kind="policy_invariants",
        reason=reason,
        source_policy_id=source_policy_id,
        object_refs=object_refs,
        origin=origin,
    )


def _append_reward_term(
    payload: dict[str, Any],
    spec: dict[str, Any],
    *,
    reason: str,
    source_policy_id: str,
    object_refs: list[str],
    origin: str,
) -> None:
    reward_terms = payload.setdefault("reward_terms", [])
    if any(item.get("name") == spec["name"] for item in reward_terms):
        return
    reward_terms.append(spec)
    _append_rule_provenance(
        payload,
        rule_name=spec["name"],
        rule_kind="reward_terms",
        reason=reason,
        source_policy_id=source_policy_id,
        object_refs=object_refs,
        origin=origin,
    )


def _append_intervention_rule(
    payload: dict[str, Any],
    spec: dict[str, Any],
    *,
    reason: str,
    source_policy_id: str,
    object_refs: list[str],
    origin: str,
) -> None:
    rules = payload.setdefault("intervention_rules", [])
    if any(item.get("name") == spec["name"] for item in rules):
        return
    rules.append(spec)
    _append_rule_provenance(
        payload,
        rule_name=spec["name"],
        rule_kind="intervention_rules",
        reason=reason,
        source_policy_id=source_policy_id,
        object_refs=object_refs,
        origin=origin,
    )


def _append_rule_provenance(
    payload: dict[str, Any],
    *,
    rule_name: str,
    rule_kind: str,
    reason: str,
    source_policy_id: str,
    object_refs: list[str],
    origin: str,
) -> None:
    metadata = payload.setdefault("metadata", {})
    rule_provenance = metadata.setdefault("rule_provenance", [])
    if any(item.get("name") == rule_name for item in rule_provenance):
        return
    rule_provenance.append(
        {
            "name": rule_name,
            "kind": rule_kind,
            "origin": origin,
            "source_policy_id": source_policy_id,
            "object_refs": object_refs,
            "reason": reason,
        }
    )


def _compact_refs(*values: str | None) -> list[str]:
    return [value for value in values if value not in (None, "")]
