from __future__ import annotations

from typing import Iterable

from vei.grounding.models import IdentityGovernanceBundle

from .models import GeneratedScenarioCandidate, ProvenanceRecord


def generate_identity_scenario_candidates(
    bundle: IdentityGovernanceBundle,
    provenance: Iterable[ProvenanceRecord] | None = None,
) -> list[GeneratedScenarioCandidate]:
    seed_params = dict(bundle.workflow_seed.model_dump(mode="json"))
    onboarding_params = dict(seed_params)
    identity_params = {
        "user_id": seed_params["user_id"],
        "employee_id": seed_params["employee_id"],
        "primary_app_id": seed_params["crm_app_id"],
        "stale_app_id": seed_params["crm_app_id"],
        "doc_id": seed_params["doc_id"],
        "request_id": "REQ-2201",
        "ticket_id": seed_params["tracking_ticket_id"],
        "cutover_doc_id": seed_params["cutover_doc_id"],
        "manager_email": seed_params["manager_email"],
        "revoked_share_email": seed_params["revoked_share_email"],
        "allowed_share_count": seed_params["allowed_share_count"],
        "deadline_max_ms": seed_params["deadline_max_ms"],
        "ticket_note": seed_params["ticket_update_note"],
        "doc_update_note": seed_params["cutover_doc_note"],
        "slack_channel": seed_params["slack_channel"],
        "slack_summary": seed_params["slack_summary"],
        "onboarding_note": seed_params["onboarding_note"],
    }
    primary_policy = (
        bundle.capability_graphs.identity_graph.policies[0]
        if bundle.capability_graphs.identity_graph
        and bundle.capability_graphs.identity_graph.policies
        else None
    )
    stale_app_id = _select_stale_application(
        (
            bundle.capability_graphs.identity_graph.users[0].applications
            if bundle.capability_graphs.identity_graph
            and bundle.capability_graphs.identity_graph.users
            else []
        ),
        primary_policy.allowed_application_ids if primary_policy is not None else [],
    )
    request_id = (
        bundle.capability_graphs.work_graph.service_requests[0].request_id
        if bundle.capability_graphs.work_graph
        and bundle.capability_graphs.work_graph.service_requests
        else "REQ-0001"
    )
    doc_id = str(identity_params["doc_id"])
    ticket_id = str(identity_params["ticket_id"])
    request_ref = f"service_request:{request_id}"
    supporting_refs = {
        "identity_user": f"identity_user:{identity_params['user_id']}",
        "hris_employee": f"hris_employee:{identity_params['employee_id']}",
        "drive_share": f"drive_share:{doc_id}",
        "ticket": f"ticket:{ticket_id}",
        "service_request": request_ref,
        "document": f"document:{identity_params['cutover_doc_id']}",
        "comm_channel": f"comm_channel:{identity_params['slack_channel']}",
    }
    origin_index = _index_provenance(provenance)
    base_metadata = {
        "generated_from": bundle.name,
        "grounding_wedge": "identity_access_governance",
        "acceptance_focus": list(bundle.acceptance_focus),
        "origin_counts": _origin_counts(provenance),
        "policy_ids": [
            policy.policy_id
            for policy in (
                bundle.capability_graphs.identity_graph.policies
                if bundle.capability_graphs.identity_graph
                else []
            )
        ],
        "source_systems": list(bundle.metadata.get("source_systems", [])),
    }
    candidates = [
        GeneratedScenarioCandidate(
            name="acquired_user_cutover",
            title="Acquired User Cutover",
            description="Resolve the imported acquired-user cutover with least-privilege access and document hygiene.",
            scenario_name=bundle.scenario_template_name,
            workflow_name="enterprise_onboarding_migration",
            workflow_variant="manager_cutover",
            workflow_parameters=dict(onboarding_params),
            inspection_focus="identity_graph",
            tags=["generated", "identity", "cutover", "recommended"],
            hidden_faults={"identity_conflict": True, "overshared_document": True},
            actor_hints=["it-integration", "sales-manager"],
            metadata={
                **base_metadata,
                "candidate_family": "acquired_user_cutover",
                "priority": "high",
                "required_domains": ["identity_graph", "doc_graph", "work_graph"],
                "supporting_refs": [
                    supporting_refs["identity_user"],
                    supporting_refs["hris_employee"],
                    supporting_refs["drive_share"],
                    supporting_refs["ticket"],
                    supporting_refs["document"],
                ],
                "state_labels": _ref_state_labels(
                    origin_index,
                    [
                        supporting_refs["identity_user"],
                        supporting_refs["hris_employee"],
                        supporting_refs["drive_share"],
                        supporting_refs["ticket"],
                        supporting_refs["document"],
                    ],
                ),
                "generation_reasons": [
                    "identity conflict or onboarding drift detected",
                    "document sharing posture requires cleanup before handoff",
                ],
            },
        ),
        GeneratedScenarioCandidate(
            name="joiner_mover_leaver",
            title="Joiner / Mover / Leaver",
            description="Exercise the imported identity environment as a user lifecycle handoff with alias-style cutover.",
            scenario_name=bundle.scenario_template_name,
            workflow_name="enterprise_onboarding_migration",
            workflow_variant="alias_cutover",
            workflow_parameters={
                **dict(onboarding_params),
                "corporate_email": onboarding_params["corporate_email"].replace(
                    "@", "+cutover@"
                ),
            },
            inspection_focus="identity_graph",
            tags=["generated", "identity", "lifecycle"],
            hidden_faults={"alias_conflict": True},
            actor_hints=["identity-admin", "manager"],
            metadata={
                **base_metadata,
                "candidate_family": "joiner_mover_leaver",
                "priority": "medium",
                "required_domains": ["identity_graph", "work_graph"],
                "supporting_refs": [
                    supporting_refs["identity_user"],
                    supporting_refs["hris_employee"],
                    supporting_refs["ticket"],
                ],
                "state_labels": _ref_state_labels(
                    origin_index,
                    [
                        supporting_refs["identity_user"],
                        supporting_refs["hris_employee"],
                        supporting_refs["ticket"],
                    ],
                ),
                "generation_reasons": [
                    "lifecycle transition can be exercised from imported org context",
                    "alias-style cutover pressure-tests partial exports and handoff logic",
                ],
            },
        ),
        GeneratedScenarioCandidate(
            name="oversharing_remediation",
            title="Oversharing Remediation",
            description="Remove imported external sharing and document the remediation path cleanly.",
            scenario_name=bundle.scenario_template_name,
            workflow_name="identity_access_governance",
            workflow_variant="oversharing_remediation",
            workflow_parameters={
                **dict(identity_params),
                "doc_update_note": (
                    "Imported policy-driven oversharing remediation completed; "
                    "external sharing removed and policy posture restored."
                ),
                "slack_summary": (
                    "Imported oversharing remediated; external share removed and "
                    "policy posture restored."
                ),
            },
            inspection_focus="doc_graph",
            tags=["generated", "docs", "oversharing", "recommended"],
            hidden_faults={"external_share": True},
            actor_hints=["identity-admin", "docs-owner"],
            metadata={
                **base_metadata,
                "candidate_family": "oversharing_remediation",
                "priority": "high",
                "required_domains": ["doc_graph", "work_graph", "comm_graph"],
                "supporting_refs": [
                    supporting_refs["drive_share"],
                    supporting_refs["ticket"],
                    supporting_refs["document"],
                    supporting_refs["comm_channel"],
                ],
                "state_labels": _ref_state_labels(
                    origin_index,
                    [
                        supporting_refs["drive_share"],
                        supporting_refs["ticket"],
                        supporting_refs["document"],
                        supporting_refs["comm_channel"],
                    ],
                ),
                "generation_reasons": [
                    "forbidden external sharing discovered in imported ACL posture",
                    "artifact follow-through required for governance evidence",
                ],
            },
        ),
        GeneratedScenarioCandidate(
            name="approval_bottleneck",
            title="Approval Bottleneck",
            description="Clear a pending approval chain before least-privilege activation can complete.",
            scenario_name=bundle.scenario_template_name,
            workflow_name="identity_access_governance",
            workflow_variant="approval_bottleneck",
            workflow_parameters={
                **dict(identity_params),
                "request_id": request_id,
                "ticket_note": (
                    "Imported approval bottleneck cleared and tracker updated after "
                    "identity approval completion."
                ),
                "slack_summary": (
                    "Imported approval bottleneck cleared; identity approval "
                    "completed and access granted."
                ),
            },
            inspection_focus="work_graph",
            tags=["generated", "approval", "identity", "recommended"],
            hidden_faults={"approval_blocked": True},
            actor_hints=["manager", "identity-approver"],
            metadata={
                **base_metadata,
                "candidate_family": "approval_bottleneck",
                "priority": "high",
                "required_domains": ["work_graph", "identity_graph", "comm_graph"],
                "supporting_refs": [
                    request_ref,
                    supporting_refs["identity_user"],
                    supporting_refs["ticket"],
                    supporting_refs["comm_channel"],
                ],
                "state_labels": _ref_state_labels(
                    origin_index,
                    [
                        request_ref,
                        supporting_refs["identity_user"],
                        supporting_refs["ticket"],
                        supporting_refs["comm_channel"],
                    ],
                ),
                "generation_reasons": [
                    "approval workflow exists or was derived from imported policy posture",
                    "activation of the approved app should remain blocked until the chain clears",
                ],
            },
        ),
        GeneratedScenarioCandidate(
            name="stale_entitlement_cleanup",
            title="Stale Entitlement Cleanup",
            description="Remove disallowed imported access while preserving the policy-approved application set.",
            scenario_name=bundle.scenario_template_name,
            workflow_name="identity_access_governance",
            workflow_variant="stale_entitlement_cleanup",
            workflow_parameters={
                **dict(identity_params),
                "stale_app_id": stale_app_id or identity_params["primary_app_id"],
                "ticket_note": (
                    "Imported stale entitlement removed after policy review and "
                    "tracker updated."
                ),
                "slack_summary": (
                    "Imported stale entitlement removed and least-privilege posture "
                    "restored."
                ),
            },
            inspection_focus="identity_graph",
            tags=["generated", "least_privilege", "identity"],
            hidden_faults={"stale_entitlement": stale_app_id is not None},
            actor_hints=["identity-admin"],
            metadata={
                **base_metadata,
                "candidate_family": "stale_entitlement_cleanup",
                "priority": "medium",
                "required_domains": ["identity_graph", "work_graph", "comm_graph"],
                "supporting_refs": [
                    supporting_refs["identity_user"],
                    supporting_refs["ticket"],
                    supporting_refs["comm_channel"],
                ],
                "state_labels": _ref_state_labels(
                    origin_index,
                    [
                        supporting_refs["identity_user"],
                        supporting_refs["ticket"],
                        supporting_refs["comm_channel"],
                    ],
                ),
                "generation_reasons": [
                    "user/application assignments indicate drift from the imported allowed set",
                    "tracker and channel evidence should survive entitlement cleanup",
                ],
            },
        ),
        GeneratedScenarioCandidate(
            name="break_glass_follow_up",
            title="Break-Glass Follow-Up",
            description="Record and clean up imported break-glass access before the next review window.",
            scenario_name=bundle.scenario_template_name,
            workflow_name="identity_access_governance",
            workflow_variant="break_glass_follow_up",
            workflow_parameters={
                **dict(identity_params),
                "stale_app_id": stale_app_id or identity_params["primary_app_id"],
                "ticket_note": (
                    "Imported break-glass follow-up completed and temporary access "
                    "removed."
                ),
                "doc_update_note": (
                    "Break-glass follow-up recorded after imported temporary access "
                    "review."
                ),
                "slack_summary": (
                    "Imported break-glass follow-up completed and temporary access "
                    "removed."
                ),
            },
            inspection_focus="identity_graph",
            tags=["generated", "break_glass", "follow_up"],
            hidden_faults={"break_glass_event": True},
            actor_hints=["security-review", "identity-admin"],
            metadata={
                **base_metadata,
                "candidate_family": "break_glass_follow_up",
                "priority": "medium",
                "required_domains": ["identity_graph", "doc_graph", "work_graph"],
                "supporting_refs": [
                    supporting_refs["identity_user"],
                    supporting_refs["ticket"],
                    supporting_refs["document"],
                ],
                "state_labels": _ref_state_labels(
                    origin_index,
                    [
                        supporting_refs["identity_user"],
                        supporting_refs["ticket"],
                        supporting_refs["document"],
                    ],
                ),
                "generation_reasons": [
                    "audit history suggests emergency or temporary access review pressure",
                    "follow-up artifacts must be updated after temporary access is removed",
                ],
            },
        ),
    ]
    return candidates


def build_generated_scenario_provenance(
    bundle: IdentityGovernanceBundle, generated: list[GeneratedScenarioCandidate]
) -> list[ProvenanceRecord]:
    records: list[ProvenanceRecord] = []
    policy_id = (
        bundle.capability_graphs.identity_graph.policies[0].policy_id
        if bundle.capability_graphs.identity_graph
        and bundle.capability_graphs.identity_graph.policies
        else "imported-policy"
    )
    for doc in (
        bundle.capability_graphs.doc_graph.documents
        if bundle.capability_graphs.doc_graph
        else []
    ):
        records.append(
            ProvenanceRecord(
                object_ref=f"document:{doc.doc_id}",
                label=doc.title,
                origin="derived",
                lineage=[f"identity_policy:{policy_id}"],
                metadata={"generated": True},
            )
        )
    if bundle.capability_graphs.comm_graph is not None:
        for channel in bundle.capability_graphs.comm_graph.slack_channels:
            records.append(
                ProvenanceRecord(
                    object_ref=f"comm_channel:{channel.channel}",
                    label=channel.channel,
                    origin="derived",
                    lineage=[f"identity_policy:{policy_id}"],
                    metadata={"generated": True},
                )
            )
    for candidate in generated:
        records.append(
            ProvenanceRecord(
                object_ref=f"scenario:{candidate.name}",
                label=candidate.title,
                origin="simulated",
                lineage=[bundle.name],
                metadata={
                    "workflow_name": candidate.workflow_name,
                    "workflow_variant": candidate.workflow_variant,
                    "priority": candidate.metadata.get("priority"),
                },
            )
        )
    return records


def _index_provenance(
    provenance: Iterable[ProvenanceRecord] | None,
) -> dict[str, ProvenanceRecord]:
    if provenance is None:
        return {}
    return {item.object_ref: item for item in provenance}


def _origin_counts(provenance: Iterable[ProvenanceRecord] | None) -> dict[str, int]:
    counts = {"imported": 0, "derived": 0, "simulated": 0}
    if provenance is None:
        return counts
    for item in provenance:
        counts[str(item.origin)] = counts.get(str(item.origin), 0) + 1
    return counts


def _ref_state_labels(
    provenance_index: dict[str, ProvenanceRecord], refs: list[str]
) -> dict[str, str]:
    labels: dict[str, str] = {}
    for ref in refs:
        record = provenance_index.get(ref)
        if record is not None:
            labels[ref] = str(record.origin)
        elif ref.startswith(("document:", "comm_channel:")):
            labels[ref] = "derived"
        else:
            labels[ref] = "simulated"
    return labels


def _select_stale_application(
    application_ids: list[str], allowed_application_ids: list[str]
) -> str | None:
    for app_id in application_ids:
        if allowed_application_ids and app_id not in allowed_application_ids:
            return app_id
    return None
