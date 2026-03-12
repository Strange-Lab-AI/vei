from __future__ import annotations

import csv
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable

from vei.blueprint.models import (
    BlueprintApprovalAsset,
    BlueprintCapabilityGraphsAsset,
    BlueprintCommGraphAsset,
    BlueprintCrmCompanyAsset,
    BlueprintCrmContactAsset,
    BlueprintCrmDealAsset,
    BlueprintDocGraphAsset,
    BlueprintDocumentAsset,
    BlueprintGoogleDriveShareAsset,
    BlueprintHrisEmployeeAsset,
    BlueprintIdentityApplicationAsset,
    BlueprintIdentityGraphAsset,
    BlueprintIdentityGroupAsset,
    BlueprintIdentityPolicyAsset,
    BlueprintIdentityUserAsset,
    BlueprintRevenueGraphAsset,
    BlueprintServiceRequestAsset,
    BlueprintSlackChannelAsset,
    BlueprintSlackMessageAsset,
    BlueprintTicketAsset,
    BlueprintWorkGraphAsset,
)
from vei.connectors.redaction import redact_payload
from vei.grounding.models import (
    IdentityGovernanceBundle,
    IdentityGovernanceWorkflowSeed,
)

from .models import (
    GeneratedScenarioCandidate,
    ImportPackage,
    ImportPackageArtifacts,
    ImportSourceManifest,
    ImportSourceSummary,
    MappingIssue,
    NormalizationReport,
    ProvenanceRecord,
    RedactionReport,
)
from .profiles import get_mapping_profile


_FIXTURE_ROOT = Path(__file__).with_name("fixtures")


def list_import_package_examples() -> list[str]:
    if not _FIXTURE_ROOT.exists():
        return []
    return sorted(
        item.name
        for item in _FIXTURE_ROOT.iterdir()
        if item.is_dir() and (item / "package.json").exists()
    )


def get_import_package_example_path(name: str) -> Path:
    key = name.strip().lower()
    candidate = _FIXTURE_ROOT / key
    if not (candidate / "package.json").exists():
        raise KeyError(f"unknown import package example: {name}")
    return candidate


def load_import_package(path: str | Path) -> ImportPackage:
    root = _package_root(path)
    payload = json.loads((root / "package.json").read_text(encoding="utf-8"))
    return ImportPackage.model_validate(payload)


def validate_import_package(path: str | Path) -> NormalizationReport:
    root = _package_root(path)
    package = load_import_package(root)
    issues: list[MappingIssue] = []
    source_summaries: list[ImportSourceSummary] = []
    for source in package.sources:
        profile_issue_count = 0
        source_path = root / source.relative_path
        unknown_fields: list[str] = []
        loaded_count = 0
        if not source_path.exists():
            issues.append(
                MappingIssue(
                    code="source.missing",
                    severity="error",
                    source_id=source.source_id,
                    message=f"Missing source file: {source.relative_path}",
                )
            )
            profile_issue_count += 1
        else:
            profile = get_mapping_profile(source.mapping_profile)
            try:
                rows = _load_source_rows(root, source)
                loaded_count = len(rows)
            except Exception as exc:  # noqa: BLE001
                issues.append(
                    MappingIssue(
                        code="source.invalid",
                        severity="error",
                        source_id=source.source_id,
                        message=f"Could not parse source: {exc}",
                    )
                )
                profile_issue_count += 1
                rows = []
            for row in rows[:5]:
                unknown_fields.extend(
                    sorted(set(row) - set(profile.expected_fields or row.keys()))
                )
            if profile.file_type != source.file_type:
                issues.append(
                    MappingIssue(
                        code="source.file_type_mismatch",
                        severity="error",
                        source_id=source.source_id,
                        message=(
                            f"Profile {profile.name} expects {profile.file_type}, "
                            f"but source declares {source.file_type}"
                        ),
                    )
                )
                profile_issue_count += 1
        source_summaries.append(
            ImportSourceSummary(
                source_id=source.source_id,
                source_system=source.source_system,
                mapping_profile=source.mapping_profile,
                loaded_record_count=loaded_count,
                issue_count=profile_issue_count,
                unknown_fields=sorted(set(unknown_fields)),
                redaction_status=source.redaction_status,
            )
        )
    return _build_report(package.name, issues, source_summaries, normalized_counts={})


def normalize_identity_import_package(path: str | Path) -> ImportPackageArtifacts:
    root = _package_root(path)
    package = load_import_package(root)
    source_rows: dict[str, list[dict[str, Any]]] = {}
    source_summaries: list[ImportSourceSummary] = []
    issues: list[MappingIssue] = []
    redaction_reports: list[RedactionReport] = []
    provenance: list[ProvenanceRecord] = []

    users: list[BlueprintIdentityUserAsset] = []
    groups: list[BlueprintIdentityGroupAsset] = []
    apps: list[BlueprintIdentityApplicationAsset] = []
    shares: list[BlueprintGoogleDriveShareAsset] = []
    employees: list[BlueprintHrisEmployeeAsset] = []
    tickets: list[BlueprintTicketAsset] = []
    requests: list[BlueprintServiceRequestAsset] = []
    policies: list[BlueprintIdentityPolicyAsset] = []
    audit_events: list[dict[str, Any]] = []
    deals: list[BlueprintCrmDealAsset] = []
    contacts: list[BlueprintCrmContactAsset] = []
    companies: list[BlueprintCrmCompanyAsset] = []
    org_units: set[str] = set()
    policy_notes: list[str] = []

    for source in package.sources:
        rows = _load_source_rows(root, source)
        source_rows[source.source_id] = rows
        profile = get_mapping_profile(source.mapping_profile)
        source_issues: list[MappingIssue] = []
        normalized_count = 0
        dropped_count = 0
        unknown_fields: set[str] = set()
        sample = rows[0] if rows else {}
        redacted_fields = _detect_redacted_fields(sample)
        redaction_reports.append(
            RedactionReport(
                source_id=source.source_id,
                status=source.redaction_status,
                redacted_field_count=len(redacted_fields),
                redacted_fields=redacted_fields,
                sample_preview=redact_payload(sample) if sample else {},
            )
        )

        for index, row in enumerate(rows, start=1):
            missing = [
                field
                for field in profile.required_fields
                if row.get(field) in (None, "", [])
            ]
            if missing:
                for field in missing:
                    source_issues.append(
                        MappingIssue(
                            code="field.required",
                            severity="error",
                            source_id=source.source_id,
                            row_number=index,
                            field=field,
                            message=f"Missing required field: {field}",
                            record_key=_record_key(row),
                        )
                    )
                dropped_count += 1
                continue
            extra = sorted(set(row) - set(profile.expected_fields))
            unknown_fields.update(extra)
            for field in extra:
                source_issues.append(
                    MappingIssue(
                        code="field.unknown",
                        severity="warning",
                        source_id=source.source_id,
                        row_number=index,
                        field=field,
                        message=f"Unknown field ignored: {field}",
                        record_key=_record_key(row),
                    )
                )

            normalized, coercion_issues = _normalize_row(row, source, profile, index)
            source_issues.extend(coercion_issues)
            if source.mapping_profile == "okta_users_v1":
                model = BlueprintIdentityUserAsset.model_validate(
                    {
                        "user_id": normalized["user_id"],
                        "email": normalized["email"],
                        "login": normalized.get("login"),
                        "first_name": normalized["first_name"],
                        "last_name": normalized["last_name"],
                        "status": normalized["status"],
                        "department": normalized.get("department"),
                        "title": normalized.get("title"),
                        "manager": normalized.get("manager"),
                        "groups": normalized.get("group_ids", []),
                        "applications": normalized.get("application_ids", []),
                        "last_login_ms": normalized.get("last_login_ms"),
                    }
                )
                users.append(model)
                if normalized.get("org_unit"):
                    org_units.add(str(normalized["org_unit"]))
                provenance.append(
                    _provenance(
                        object_ref=f"identity_user:{model.user_id}",
                        label=model.email,
                        origin="imported",
                        source=source,
                        raw_record_ref=f"{source.relative_path}#{index}",
                        redacted_fields=redacted_fields,
                        metadata={"email": model.email},
                    )
                )
            elif source.mapping_profile == "okta_groups_v1":
                model = BlueprintIdentityGroupAsset.model_validate(
                    {
                        "group_id": normalized["group_id"],
                        "name": normalized["name"],
                        "description": normalized.get("description"),
                        "members": normalized.get("members", []),
                    }
                )
                groups.append(model)
                provenance.append(
                    _provenance(
                        object_ref=f"identity_group:{model.group_id}",
                        label=model.name,
                        origin="imported",
                        source=source,
                        raw_record_ref=f"{source.relative_path}#{index}",
                        redacted_fields=redacted_fields,
                    )
                )
            elif source.mapping_profile == "okta_apps_v1":
                model = BlueprintIdentityApplicationAsset.model_validate(
                    {
                        "app_id": normalized["app_id"],
                        "label": normalized["label"],
                        "status": normalized["status"],
                        "description": normalized.get("description"),
                        "sign_on_mode": normalized.get("sign_on_mode") or "SAML_2_0",
                        "assignments": normalized.get("assignments", []),
                    }
                )
                apps.append(model)
                provenance.append(
                    _provenance(
                        object_ref=f"identity_application:{model.app_id}",
                        label=model.label,
                        origin="imported",
                        source=source,
                        raw_record_ref=f"{source.relative_path}#{index}",
                        redacted_fields=redacted_fields,
                    )
                )
            elif source.mapping_profile == "google_drive_shares_v1":
                model = BlueprintGoogleDriveShareAsset.model_validate(
                    {
                        "doc_id": normalized["doc_id"],
                        "title": normalized["title"],
                        "owner": normalized["owner"],
                        "visibility": normalized["visibility"],
                        "classification": normalized.get("classification")
                        or "internal",
                        "shared_with": normalized.get("shared_with", []),
                    }
                )
                shares.append(model)
                provenance.append(
                    _provenance(
                        object_ref=f"drive_share:{model.doc_id}",
                        label=model.title,
                        origin="imported",
                        source=source,
                        raw_record_ref=f"{source.relative_path}#{index}",
                        redacted_fields=redacted_fields,
                        metadata={"shared_with": model.shared_with},
                    )
                )
            elif source.mapping_profile == "hris_employees_v1":
                model = BlueprintHrisEmployeeAsset.model_validate(
                    {
                        "employee_id": normalized["employee_id"],
                        "email": normalized["email"],
                        "display_name": normalized["display_name"],
                        "department": normalized["department"],
                        "manager": normalized["manager"],
                        "status": normalized["status"],
                        "cohort": normalized.get("cohort"),
                        "identity_conflict": normalized.get("identity_conflict", False),
                        "onboarded": normalized.get("onboarded", False),
                        "notes": normalized.get("notes", []),
                    }
                )
                employees.append(model)
                if normalized.get("org_unit"):
                    org_units.add(str(normalized["org_unit"]))
                provenance.append(
                    _provenance(
                        object_ref=f"hris_employee:{model.employee_id}",
                        label=model.display_name,
                        origin="imported",
                        source=source,
                        raw_record_ref=f"{source.relative_path}#{index}",
                        redacted_fields=redacted_fields,
                        metadata={"email": model.email},
                    )
                )
            elif source.mapping_profile == "jira_issues_v1":
                model = BlueprintTicketAsset.model_validate(
                    {
                        "ticket_id": normalized["id"],
                        "title": normalized["title"],
                        "status": normalized["status"],
                        "assignee": normalized.get("assignee"),
                        "description": normalized.get("description"),
                    }
                )
                tickets.append(model)
                provenance.append(
                    _provenance(
                        object_ref=f"ticket:{model.ticket_id}",
                        label=model.title,
                        origin="imported",
                        source=source,
                        raw_record_ref=f"{source.relative_path}#{index}",
                        redacted_fields=redacted_fields,
                    )
                )
            elif source.mapping_profile == "approval_requests_v1":
                model = BlueprintServiceRequestAsset.model_validate(
                    {
                        "request_id": normalized["request_id"],
                        "title": normalized["title"],
                        "status": normalized["status"],
                        "requester": normalized.get("requester"),
                        "description": normalized.get("description"),
                        "approvals": [
                            BlueprintApprovalAsset.model_validate(item)
                            for item in normalized.get("approvals", [])
                        ],
                    }
                )
                requests.append(model)
                provenance.append(
                    _provenance(
                        object_ref=f"service_request:{model.request_id}",
                        label=model.title,
                        origin="imported",
                        source=source,
                        raw_record_ref=f"{source.relative_path}#{index}",
                        redacted_fields=redacted_fields,
                    )
                )
            elif source.mapping_profile == "identity_policies_v1":
                notes = list(normalized.get("notes", []))
                if notes:
                    policy_notes.extend(str(item) for item in notes)
                model = BlueprintIdentityPolicyAsset.model_validate(
                    {
                        "policy_id": normalized["policy_id"],
                        "title": normalized["title"],
                        "allowed_application_ids": normalized.get(
                            "allowed_application_ids", []
                        ),
                        "forbidden_share_domains": normalized.get(
                            "forbidden_share_domains", []
                        ),
                        "required_approval_stages": normalized.get(
                            "required_approval_stages", []
                        ),
                        "deadline_max_ms": normalized.get("deadline_max_ms"),
                        "metadata": {
                            "notes": notes,
                            "break_glass_requires_followup": normalized.get(
                                "break_glass_requires_followup", False
                            ),
                        },
                    }
                )
                policies.append(model)
                provenance.append(
                    _provenance(
                        object_ref=f"identity_policy:{model.policy_id}",
                        label=model.title,
                        origin="imported",
                        source=source,
                        raw_record_ref=f"{source.relative_path}#{index}",
                        redacted_fields=redacted_fields,
                    )
                )
            elif source.mapping_profile == "audit_events_v1":
                audit_events.append(normalized)
                provenance.append(
                    _provenance(
                        object_ref=f"audit_event:{normalized['event_id']}",
                        label=normalized["event_type"],
                        origin="imported",
                        source=source,
                        raw_record_ref=f"{source.relative_path}#{index}",
                        redacted_fields=redacted_fields,
                    )
                )
            elif source.mapping_profile == "crm_deals_v1":
                deal = BlueprintCrmDealAsset.model_validate(normalized)
                deals.append(deal)
                provenance.append(
                    _provenance(
                        object_ref=f"crm_deal:{deal.id}",
                        label=deal.name,
                        origin="imported",
                        source=source,
                        raw_record_ref=f"{source.relative_path}#{index}",
                        redacted_fields=redacted_fields,
                    )
                )
            normalized_count += 1

        issues.extend(source_issues)
        source_summaries.append(
            ImportSourceSummary(
                source_id=source.source_id,
                source_system=source.source_system,
                mapping_profile=source.mapping_profile,
                loaded_record_count=len(rows),
                normalized_record_count=normalized_count,
                dropped_record_count=dropped_count,
                issue_count=len(source_issues),
                unknown_fields=sorted(unknown_fields),
                redaction_status=source.redaction_status,
            )
        )

    issues.extend(_cross_validate_identity(users, apps, employees, requests, policies))
    primary = _select_primary_context(
        users, employees, shares, tickets, requests, deals, policies, audit_events
    )
    bundle = _build_identity_bundle(
        package=package,
        users=users,
        groups=groups,
        apps=apps,
        shares=shares,
        employees=employees,
        tickets=tickets,
        requests=requests,
        policies=policies,
        deals=deals,
        contacts=contacts,
        companies=companies,
        primary=primary,
        policy_notes=policy_notes,
        audit_events=audit_events,
        org_units=sorted(org_units),
        source_summaries=source_summaries,
        issues=issues,
    )
    generated = generate_identity_scenario_candidates(bundle, provenance)
    provenance.extend(_derived_provenance(bundle, generated))
    report = _build_report(
        package.name,
        issues,
        source_summaries,
        normalized_counts={
            "identity_users": len(users),
            "identity_groups": len(groups),
            "identity_applications": len(apps),
            "hris_employees": len(employees),
            "drive_shares": len(shares),
            "tickets": len(tickets),
            "service_requests": len(requests),
            "identity_policies": len(policies),
            "audit_events": len(audit_events),
            "crm_deals": len(deals),
            "generated_scenarios": len(generated),
        },
    )
    return ImportPackageArtifacts(
        package=package,
        normalized_bundle=bundle,
        normalization_report=report,
        provenance=provenance,
        redaction_reports=redaction_reports,
        generated_scenarios=generated,
    )


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
    base_metadata = {
        "generated_from": bundle.name,
        "grounding_wedge": "identity_access_governance",
        "acceptance_focus": list(bundle.acceptance_focus),
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
            tags=["generated", "identity", "cutover"],
            hidden_faults={"identity_conflict": True, "overshared_document": True},
            actor_hints=["it-integration", "sales-manager"],
            metadata={**base_metadata, "candidate_family": "acquired_user_cutover"},
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
            metadata={**base_metadata, "candidate_family": "joiner_mover_leaver"},
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
            tags=["generated", "docs", "oversharing"],
            hidden_faults={"external_share": True},
            actor_hints=["identity-admin", "docs-owner"],
            metadata={**base_metadata, "candidate_family": "oversharing_remediation"},
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
            tags=["generated", "approval", "identity"],
            hidden_faults={"approval_blocked": True},
            actor_hints=["manager", "identity-approver"],
            metadata={**base_metadata, "candidate_family": "approval_bottleneck"},
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
            metadata={**base_metadata, "candidate_family": "stale_entitlement_cleanup"},
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
            metadata={**base_metadata, "candidate_family": "break_glass_follow_up"},
        ),
    ]
    return candidates


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

    policy_invariants = payload.setdefault("policy_invariants", [])
    reward_terms = payload.setdefault("reward_terms", [])
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
    required_approval_stages = list(policy.required_approval_stages)
    for stage in required_approval_stages:
        if not any(
            item.get("name") == f"import_policy:{stage}" for item in policy_invariants
        ):
            policy_invariants.append(
                {
                    "name": f"import_policy:{stage}",
                    "description": f"Imported policy {policy.policy_id} requires approval stage {stage}.",
                    "required": True,
                    "evidence": policy.policy_id,
                }
            )

    doc_id = workflow_parameters.get("doc_id")
    if doc_id:
        for domain in policy.forbidden_share_domains:
            predicate_name = f"forbidden_share_domain:{domain}"
            if not any(
                item.get("name") == predicate_name
                for item in payload.get("forbidden_predicates", [])
            ):
                payload.setdefault("forbidden_predicates", []).append(
                    {
                        "name": predicate_name,
                        "source": "oracle_state",
                        "assertion": {
                            "kind": "state_not_contains",
                            "field": f"components.google_admin.drive_shares.{doc_id}.shared_with",
                            "contains": domain,
                            "description": f"Imported policy forbids share domain {domain}.",
                        },
                        "description": f"Imported policy forbids share domain {domain}.",
                    }
                )

    stale_app_id = workflow_parameters.get("stale_app_id")
    user_id = workflow_parameters.get("user_id")
    if (
        stale_app_id
        and user_id
        and scenario_name
        in {
            "stale_entitlement_cleanup",
            "break_glass_follow_up",
        }
    ):
        predicate_name = f"stale_app_removed:{stale_app_id}"
        if not any(
            item.get("name") == predicate_name
            for item in payload.get("forbidden_predicates", [])
        ):
            payload.setdefault("forbidden_predicates", []).append(
                {
                    "name": predicate_name,
                    "source": "oracle_state",
                    "assertion": {
                        "kind": "state_not_contains",
                        "field": f"components.okta.users.{user_id}.applications",
                        "contains": stale_app_id,
                        "description": "Imported least-privilege policy requires removing stale application access.",
                    },
                    "description": "Imported least-privilege policy requires removing stale application access.",
                }
            )

    if policy.deadline_max_ms:
        reward_terms.append(
            {
                "name": f"{scenario_name}:deadline",
                "weight": 0.2,
                "term_type": "success",
                "description": f"Imported policy deadline {policy.deadline_max_ms}ms respected.",
            }
        )
    return payload


def _package_root(path: str | Path) -> Path:
    candidate = Path(path).expanduser().resolve()
    if candidate.is_dir():
        manifest = candidate / "package.json"
    else:
        manifest = candidate
        candidate = manifest.parent
    if not manifest.exists():
        raise FileNotFoundError(f"import package manifest not found: {manifest}")
    return candidate


def _load_source_rows(root: Path, source: ImportSourceManifest) -> list[dict[str, Any]]:
    path = root / source.relative_path
    if source.file_type == "csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            return [dict(row) for row in reader]
    payload = json.loads(path.read_text(encoding="utf-8"))
    profile = get_mapping_profile(source.mapping_profile)
    if profile.root_list_key:
        raw = payload.get(profile.root_list_key, [])
        return [dict(item) for item in raw]
    if isinstance(payload, list):
        return [dict(item) for item in payload]
    raise ValueError(f"JSON source did not resolve to a list: {source.relative_path}")


def _normalize_row(
    row: dict[str, Any],
    source: ImportSourceManifest,
    profile,
    row_number: int,
) -> tuple[dict[str, Any], list[MappingIssue]]:
    normalized = dict(row)
    issues: list[MappingIssue] = []
    for field in profile.list_fields:
        value = normalized.get(field)
        if value in (None, ""):
            normalized[field] = []
        elif isinstance(value, list):
            normalized[field] = value
        elif isinstance(value, str):
            normalized[field] = [
                item.strip() for item in value.split(";") if item.strip()
            ]
            if ";" in value or "," in value:
                issues.append(
                    MappingIssue(
                        code="field.coerced_list",
                        severity="info",
                        source_id=source.source_id,
                        row_number=row_number,
                        field=field,
                        message=f"Coerced delimited string to list for {field}",
                        record_key=_record_key(row),
                    )
                )
        else:
            normalized[field] = [value]
    for field in profile.bool_fields:
        value = normalized.get(field)
        if isinstance(value, bool):
            continue
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "false", "yes", "no", "1", "0"}:
                normalized[field] = lowered in {"true", "yes", "1"}
                issues.append(
                    MappingIssue(
                        code="field.coerced_bool",
                        severity="info",
                        source_id=source.source_id,
                        row_number=row_number,
                        field=field,
                        message=f"Coerced string to boolean for {field}",
                        record_key=_record_key(row),
                    )
                )
    for field in profile.int_fields:
        value = normalized.get(field)
        if value in (None, ""):
            normalized[field] = None
            continue
        if isinstance(value, int):
            continue
        try:
            normalized[field] = int(value)
        except (TypeError, ValueError):
            issues.append(
                MappingIssue(
                    code="field.invalid_int",
                    severity="warning",
                    source_id=source.source_id,
                    row_number=row_number,
                    field=field,
                    message=f"Could not coerce {field} to int",
                    record_key=_record_key(row),
                    raw_value=value,
                )
            )
            normalized[field] = None
    status = normalized.get("status")
    if isinstance(status, str):
        upper = status.strip().upper()
        if upper != status:
            normalized["status"] = upper
            issues.append(
                MappingIssue(
                    code="field.coerced_enum",
                    severity="info",
                    source_id=source.source_id,
                    row_number=row_number,
                    field="status",
                    message="Coerced status to uppercase enum",
                    record_key=_record_key(row),
                )
            )
    return normalized, issues


def _cross_validate_identity(
    users: list[BlueprintIdentityUserAsset],
    apps: list[BlueprintIdentityApplicationAsset],
    employees: list[BlueprintHrisEmployeeAsset],
    requests: list[BlueprintServiceRequestAsset],
    policies: list[BlueprintIdentityPolicyAsset],
) -> list[MappingIssue]:
    issues: list[MappingIssue] = []
    user_ids = {item.user_id for item in users}
    emails = [item.email for item in users]
    app_ids = {item.app_id for item in apps}
    for employee in employees:
        matches = [email for email in emails if email == employee.email]
        if not matches:
            issues.append(
                MappingIssue(
                    code="identity.employee_without_user",
                    severity="warning",
                    message=f"No Okta user matched HRIS employee {employee.email}",
                    record_key=employee.employee_id,
                )
            )
    for app in apps:
        unknown = [user_id for user_id in app.assignments if user_id not in user_ids]
        if unknown:
            issues.append(
                MappingIssue(
                    code="identity.assignment_unknown_user",
                    severity="warning",
                    message=f"Application {app.app_id} has assignments to unknown users",
                    record_key=app.app_id,
                    raw_value=unknown,
                )
            )
    if requests and not policies:
        issues.append(
            MappingIssue(
                code="policy.missing",
                severity="warning",
                message="Service requests were imported without any identity policies.",
            )
        )
    for policy in policies:
        unknown_apps = [
            app_id for app_id in policy.allowed_application_ids if app_id not in app_ids
        ]
        if unknown_apps:
            issues.append(
                MappingIssue(
                    code="policy.unknown_application",
                    severity="warning",
                    message=f"Policy {policy.policy_id} references unknown applications",
                    record_key=policy.policy_id,
                    raw_value=unknown_apps,
                )
            )
    return issues


def _select_primary_context(
    users: list[BlueprintIdentityUserAsset],
    employees: list[BlueprintHrisEmployeeAsset],
    shares: list[BlueprintGoogleDriveShareAsset],
    tickets: list[BlueprintTicketAsset],
    requests: list[BlueprintServiceRequestAsset],
    deals: list[BlueprintCrmDealAsset],
    policies: list[BlueprintIdentityPolicyAsset],
    audit_events: list[dict[str, Any]],
) -> dict[str, Any]:
    primary_employee = next(
        (
            item
            for item in employees
            if item.identity_conflict or not item.onboarded or item.status != "ACTIVE"
        ),
        employees[0] if employees else None,
    )
    user = next(
        (
            item
            for item in users
            if primary_employee is not None and item.email == primary_employee.email
        ),
        users[0] if users else None,
    )
    share = shares[0] if shares else None
    ticket = tickets[0] if tickets else None
    request = requests[0] if requests else None
    deal = deals[0] if deals else None
    policy = policies[0] if policies else None
    audit = audit_events[0] if audit_events else None
    return {
        "employee": primary_employee,
        "user": user,
        "share": share,
        "ticket": ticket,
        "request": request,
        "deal": deal,
        "policy": policy,
        "audit_event": audit,
    }


def _build_identity_bundle(
    *,
    package: ImportPackage,
    users: list[BlueprintIdentityUserAsset],
    groups: list[BlueprintIdentityGroupAsset],
    apps: list[BlueprintIdentityApplicationAsset],
    shares: list[BlueprintGoogleDriveShareAsset],
    employees: list[BlueprintHrisEmployeeAsset],
    tickets: list[BlueprintTicketAsset],
    requests: list[BlueprintServiceRequestAsset],
    policies: list[BlueprintIdentityPolicyAsset],
    deals: list[BlueprintCrmDealAsset],
    contacts: list[BlueprintCrmContactAsset],
    companies: list[BlueprintCrmCompanyAsset],
    primary: dict[str, Any],
    policy_notes: list[str],
    audit_events: list[dict[str, Any]],
    org_units: list[str],
    source_summaries: list[ImportSourceSummary],
    issues: list[MappingIssue],
) -> IdentityGovernanceBundle:
    employee = primary["employee"]
    user = primary["user"]
    share = primary["share"]
    ticket = primary["ticket"]
    deal = primary["deal"]
    policy = primary["policy"]
    audit_event = primary["audit_event"]
    if employee is None or user is None or share is None or ticket is None:
        raise ValueError(
            "Imported package does not contain enough identity objects to build a workflow seed"
        )

    manager_email = employee.manager
    crm_app_id = _select_sales_app(apps, policy)
    revoked_share_email = _select_forbidden_share(share.shared_with, policy)
    cutover_doc_id = f"CUTOVER-{employee.employee_id}"
    policy_doc_id = (
        f"POLICY-{policy.policy_id}" if policy is not None else "POLICY-IMPORT"
    )
    notes = list(policy_notes)
    if audit_event is not None:
        notes.append(f"Audit anchor: {audit_event.get('event_type')}")
    workflow_seed = IdentityGovernanceWorkflowSeed(
        employee_id=employee.employee_id,
        user_id=user.user_id,
        corporate_email=user.email.replace("@oldco.", "@"),
        manager_email=manager_email,
        crm_app_id=crm_app_id,
        doc_id=share.doc_id,
        tracking_ticket_id=ticket.ticket_id,
        cutover_doc_id=cutover_doc_id,
        opportunity_id=deal.id if deal is not None else "D-0000",
        allowed_share_count=max(
            1, len([item for item in share.shared_with if item == manager_email])
        ),
        revoked_share_email=revoked_share_email,
        deadline_max_ms=(
            policy.deadline_max_ms if policy and policy.deadline_max_ms else 86_400_000
        ),
        transfer_note="Imported environment transfer prepared from Drive ownership metadata.",
        onboarding_note="Imported identity cutover completed successfully.",
        ticket_update_note="Imported ticket updated after access review and ACL remediation.",
        cutover_doc_note="Generated cutover document updated from imported topology and policy state.",
        slack_channel="#identity-cutover",
        slack_summary="Imported identity environment updated after least-privilege cutover.",
    )
    documents = [
        BlueprintDocumentAsset(
            doc_id=policy_doc_id,
            title=(policy.title if policy is not None else "Imported Identity Policy"),
            body="\n".join(notes) or "Imported identity governance policy bundle.",
            tags=["policy", "imported", "identity"],
        ),
        BlueprintDocumentAsset(
            doc_id=cutover_doc_id,
            title=f"{employee.display_name} Cutover Checklist",
            body=(
                "Generated from imported enterprise exports.\n\n"
                f"Resolve identity conflict for {employee.display_name}, remove "
                "forbidden sharing, complete approvals, and update the tracker."
            ),
            tags=["cutover", "generated", "identity"],
        ),
    ]
    capability_graphs = BlueprintCapabilityGraphsAsset(
        organization_name=package.organization_name,
        organization_domain=package.organization_domain,
        timezone=package.timezone,
        scenario_brief=(
            "Generated from imported identity exports with policy, ACL, and approval drift."
        ),
        comm_graph=BlueprintCommGraphAsset(
            slack_initial_message=(
                "Imported identity workspace ready. Resolve access drift, approval bottlenecks, and oversharing before handoff."
            ),
            slack_channels=[
                BlueprintSlackChannelAsset(
                    channel="#identity-cutover",
                    messages=[
                        BlueprintSlackMessageAsset(
                            ts="1",
                            user="vei-importer",
                            text="Workspace synthesized from imported enterprise exports. Review policy drift and approval bottlenecks first.",
                        )
                    ],
                )
            ],
            metadata={
                "imported_vs_derived": {"imported": 0, "derived": 2, "simulated": 0}
            },
        ),
        doc_graph=BlueprintDocGraphAsset(
            documents=documents,
            drive_shares=shares,
            metadata={
                "imported_doc_count": len(shares),
                "generated_doc_count": len(documents),
            },
        ),
        work_graph=BlueprintWorkGraphAsset(
            tickets=tickets,
            service_requests=requests,
            metadata={"change_references": [item.ticket_id for item in tickets]},
        ),
        identity_graph=BlueprintIdentityGraphAsset(
            users=users,
            groups=groups,
            applications=apps,
            hris_employees=employees,
            policies=policies,
            metadata={
                "org_units": org_units,
                "audit_events": audit_events,
            },
        ),
        revenue_graph=BlueprintRevenueGraphAsset(
            companies=companies,
            contacts=contacts,
            deals=deals,
        ),
        metadata={
            "import_package": package.name,
            "source_summaries": [
                item.model_dump(mode="json") for item in source_summaries
            ],
            "issue_count": len(issues),
        },
    )
    return IdentityGovernanceBundle(
        name=package.name,
        title=package.title,
        description=package.description,
        scenario_template_name="acquired_sales_onboarding",
        family_name="identity_access_governance",
        workflow_name="enterprise_onboarding_migration",
        workflow_variant="manager_cutover",
        requested_facades=[
            "slack",
            "docs",
            "jira",
            "identity",
            "google_admin",
            "hris",
            "crm",
            "servicedesk",
        ],
        capability_graphs=capability_graphs,
        workflow_seed=workflow_seed,
        metadata={
            "import_package": package.name,
            "organization_name": package.organization_name,
            "organization_domain": package.organization_domain,
            "source_systems": [item.source_system for item in package.sources],
            "scenario_materialization": "import_package",
            "generated": True,
        },
        policy_notes=policy_notes,
        incident_history=[
            {"ticket_id": item.ticket_id, "title": item.title, "status": item.status}
            for item in tickets
        ],
        acceptance_focus=["least_privilege", "approval_completion", "acl_hygiene"],
        source_manifests=[item.model_dump(mode="json") for item in package.sources],
        org_units=org_units,
        approval_policies=[
            {
                "policy_id": policy.policy_id,
                "required_approval_stages": list(policy.required_approval_stages),
            }
            for policy in policies
        ],
        entitlement_policies=[
            {
                "policy_id": policy.policy_id,
                "allowed_application_ids": list(policy.allowed_application_ids),
            }
            for policy in policies
        ],
        audit_events=audit_events,
        change_references=[
            {"ticket_id": item.ticket_id, "title": item.title} for item in tickets
        ],
    )


def _select_sales_app(
    apps: list[BlueprintIdentityApplicationAsset],
    policy: BlueprintIdentityPolicyAsset | None,
) -> str:
    if policy is not None:
        for app_id in policy.allowed_application_ids:
            if "crm" in app_id.lower() or app_id.lower().endswith("crm"):
                return app_id
    for app in apps:
        label = app.label.lower()
        if "salesforce" in label or "crm" in label:
            return app.app_id
    return apps[0].app_id if apps else "APP-crm"


def _select_forbidden_share(
    shared_with: list[str], policy: BlueprintIdentityPolicyAsset | None
) -> str:
    if policy is not None:
        for entry in shared_with:
            if any(
                entry.endswith(f"@{domain}") or domain in entry
                for domain in policy.forbidden_share_domains
            ):
                return entry
    return shared_with[0] if shared_with else "external@example.net"


def _select_stale_application(
    application_ids: list[str], allowed_application_ids: list[str]
) -> str | None:
    for app_id in application_ids:
        if allowed_application_ids and app_id not in allowed_application_ids:
            return app_id
    return None


def _detect_redacted_fields(record: dict[str, Any]) -> list[str]:
    redacted: list[str] = []
    for key, value in record.items():
        if redact_payload(value) != value:
            redacted.append(str(key))
    return sorted(redacted)


def _provenance(
    *,
    object_ref: str,
    label: str,
    origin: str,
    source: ImportSourceManifest,
    raw_record_ref: str,
    redacted_fields: list[str],
    metadata: dict[str, Any] | None = None,
) -> ProvenanceRecord:
    return ProvenanceRecord(
        object_ref=object_ref,
        label=label,
        origin=origin,
        source_id=source.source_id,
        source_system=source.source_system,
        raw_record_ref=raw_record_ref,
        mapping_profile=source.mapping_profile,
        redaction_status=source.redaction_status,
        redacted_fields=list(redacted_fields),
        metadata=dict(metadata or {}),
    )


def _derived_provenance(
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
                },
            )
        )
    return records


def _record_key(record: dict[str, Any]) -> str | None:
    for key in (
        "user_id",
        "employee_id",
        "group_id",
        "app_id",
        "doc_id",
        "id",
        "request_id",
        "policy_id",
        "event_id",
    ):
        if record.get(key):
            return str(record[key])
    return None


def _build_report(
    package_name: str,
    issues: list[MappingIssue],
    source_summaries: list[ImportSourceSummary],
    *,
    normalized_counts: dict[str, int],
) -> NormalizationReport:
    error_count = sum(1 for item in issues if item.severity == "error")
    warning_count = sum(1 for item in issues if item.severity == "warning")
    dropped = sum(item.dropped_record_count for item in source_summaries)
    return NormalizationReport(
        ok=error_count == 0,
        package_name=package_name,
        issue_count=len(issues),
        warning_count=warning_count,
        error_count=error_count,
        dropped_record_count=dropped,
        normalized_counts=normalized_counts,
        source_summaries=source_summaries,
        issues=issues,
    )


__all__ = [
    "bootstrap_contract_from_import_bundle",
    "get_import_package_example_path",
    "list_import_package_examples",
    "load_import_package",
    "normalize_identity_import_package",
    "validate_import_package",
    "generate_identity_scenario_candidates",
]
