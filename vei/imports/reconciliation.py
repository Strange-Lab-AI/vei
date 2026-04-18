from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from vei.blueprint.api import (
    BlueprintGoogleDriveShareAsset,
    BlueprintHrisEmployeeAsset,
    BlueprintIdentityUserAsset,
    BlueprintServiceRequestAsset,
    BlueprintTicketAsset,
)

from .models import (
    IdentityReconciliationSummary,
    IdentityResolutionLink,
    MappingIssue,
    ProvenanceRecord,
)


def reconcile_identity_sources(
    *,
    users: list[BlueprintIdentityUserAsset],
    employees: list[BlueprintHrisEmployeeAsset],
    shares: list[BlueprintGoogleDriveShareAsset],
    tickets: list[BlueprintTicketAsset],
    requests: list[BlueprintServiceRequestAsset],
    organization_domain: str,
) -> tuple[IdentityReconciliationSummary, list[MappingIssue], list[ProvenanceRecord]]:
    user_by_email, user_by_local = _index_users(users)
    employee_by_email, employee_by_local = _index_employees(employees)
    internal_domains = _internal_domains(users, employees, organization_domain)
    links: list[IdentityResolutionLink] = []
    issues: list[MappingIssue] = []
    provenance: list[ProvenanceRecord] = []

    def register(
        *,
        principal_ref: str,
        principal_label: str,
        principal_type: str,
        email: str | None,
        reason_prefix: str,
        lineage: Iterable[str] = (),
    ) -> None:
        link = _resolve_email_link(
            principal_ref=principal_ref,
            principal_label=principal_label,
            principal_type=principal_type,
            email=email,
            reason_prefix=reason_prefix,
            user_by_email=user_by_email,
            user_by_local=user_by_local,
            employee_by_email=employee_by_email,
            employee_by_local=employee_by_local,
            internal_domains=internal_domains,
        )
        links.append(link)
        issues.extend(_issues_for_link(link))
        if link.status in {"resolved", "ambiguous"} and link.matched_refs:
            provenance.append(
                ProvenanceRecord(
                    object_ref=f"identity_subject:{_slugify(principal_ref)}",
                    label=principal_label,
                    origin="derived",
                    lineage=[principal_ref, *link.matched_refs, *lineage],
                    metadata={
                        "principal_ref": principal_ref,
                        "principal_type": principal_type,
                        "status": link.status,
                        "reason": link.reason,
                    },
                )
            )

    for employee in employees:
        register(
            principal_ref=f"hris_employee:{employee.employee_id}",
            principal_label=employee.email,
            principal_type="hris_employee",
            email=employee.email,
            reason_prefix="HRIS employee",
        )
        if employee.manager:
            register(
                principal_ref=f"manager:{employee.employee_id}",
                principal_label=employee.manager,
                principal_type="manager_email",
                email=employee.manager,
                reason_prefix="Manager reference",
                lineage=[f"hris_employee:{employee.employee_id}"],
            )

    for share in shares:
        register(
            principal_ref=f"drive_owner:{share.doc_id}",
            principal_label=share.owner,
            principal_type="drive_owner",
            email=share.owner,
            reason_prefix="Drive owner",
            lineage=[f"drive_share:{share.doc_id}"],
        )
        for email in share.shared_with:
            register(
                principal_ref=f"drive_share_principal:{share.doc_id}:{email}",
                principal_label=email,
                principal_type="drive_share_principal",
                email=email,
                reason_prefix="Drive share principal",
                lineage=[f"drive_share:{share.doc_id}"],
            )

    for request in requests:
        if request.requester:
            register(
                principal_ref=f"service_request_requester:{request.request_id}",
                principal_label=request.requester,
                principal_type="service_request_requester",
                email=request.requester,
                reason_prefix="Service request requester",
                lineage=[f"service_request:{request.request_id}"],
            )

    for ticket in tickets:
        assignee = ticket.assignee
        if assignee:
            register(
                principal_ref=f"ticket_assignee:{ticket.ticket_id}",
                principal_label=assignee,
                principal_type="ticket_assignee",
                email=assignee if "@" in assignee else None,
                reason_prefix="Ticket assignee",
                lineage=[f"ticket:{ticket.ticket_id}"],
            )

    summary = IdentityReconciliationSummary(
        subject_count=len(links),
        resolved_count=sum(1 for item in links if item.status == "resolved"),
        ambiguous_count=sum(1 for item in links if item.status == "ambiguous"),
        unmatched_count=sum(1 for item in links if item.status == "unmatched"),
        external_count=sum(1 for item in links if item.status == "external"),
        links=links,
    )
    return summary, issues, provenance


def _resolve_email_link(
    *,
    principal_ref: str,
    principal_label: str,
    principal_type: str,
    email: str | None,
    reason_prefix: str,
    user_by_email: dict[str, list[str]],
    user_by_local: dict[str, list[str]],
    employee_by_email: dict[str, list[str]],
    employee_by_local: dict[str, list[str]],
    internal_domains: set[str],
) -> IdentityResolutionLink:
    if not email or "@" not in email:
        return IdentityResolutionLink(
            principal_ref=principal_ref,
            principal_label=principal_label,
            principal_type=principal_type,
            status="unmatched",
            reason=f"{reason_prefix} does not provide a resolvable email identity.",
            metadata={"email": email},
        )

    normalized = email.strip().lower()
    domain = normalized.split("@", 1)[1]
    local = normalized.split("@", 1)[0]
    exact_user_matches = user_by_email.get(normalized, [])
    exact_employee_matches = employee_by_email.get(normalized, [])
    matched_refs = sorted(set(exact_user_matches + exact_employee_matches))
    candidate_refs = matched_refs.copy()
    reason = f"{reason_prefix} matched exactly on email."
    status = "resolved"

    if len(exact_user_matches) > 1 or len(exact_employee_matches) > 1:
        status = "ambiguous"
        reason = f"{reason_prefix} matched multiple imported identity subjects on exact email."
    elif not matched_refs:
        alias_candidates = sorted(
            set(user_by_local.get(local, []) + employee_by_local.get(local, []))
        )
        if len(alias_candidates) == 1:
            matched_refs = alias_candidates
            candidate_refs = alias_candidates.copy()
            reason = f"{reason_prefix} matched by local-part alias across sources."
        elif len(alias_candidates) > 1:
            candidate_refs = alias_candidates
            status = "ambiguous"
            reason = f"{reason_prefix} matched multiple possible identities by local-part alias."
        elif domain not in internal_domains:
            status = "external"
            reason = f"{reason_prefix} points to an external principal outside the imported tenant."
        else:
            status = "unmatched"
            reason = f"{reason_prefix} did not match any imported identity subject."
    elif len(matched_refs) > 2:
        status = "ambiguous"
        reason = f"{reason_prefix} matched multiple imported identity subjects."

    return IdentityResolutionLink(
        principal_ref=principal_ref,
        principal_label=principal_label,
        principal_type=principal_type,
        status=status,
        matched_refs=matched_refs if status == "resolved" else [],
        candidate_refs=candidate_refs,
        reason=reason,
        metadata={"email": normalized},
    )


def _issues_for_link(link: IdentityResolutionLink) -> list[MappingIssue]:
    if link.status == "ambiguous":
        return [
            MappingIssue(
                code="identity.reconciliation.ambiguous",
                severity="warning",
                message=link.reason,
                record_key=link.principal_ref,
                raw_value=link.candidate_refs,
            )
        ]
    if link.status == "unmatched":
        return [
            MappingIssue(
                code="identity.reconciliation.unmatched",
                severity="warning",
                message=link.reason,
                record_key=link.principal_ref,
            )
        ]
    return []


def _index_users(
    users: list[BlueprintIdentityUserAsset],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    by_email: dict[str, list[str]] = defaultdict(list)
    by_local: dict[str, list[str]] = defaultdict(list)
    for user in users:
        email = user.email.strip().lower()
        by_email[email].append(f"identity_user:{user.user_id}")
        by_local[email.split("@", 1)[0]].append(f"identity_user:{user.user_id}")
    return by_email, by_local


def _index_employees(
    employees: list[BlueprintHrisEmployeeAsset],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    by_email: dict[str, list[str]] = defaultdict(list)
    by_local: dict[str, list[str]] = defaultdict(list)
    for employee in employees:
        email = employee.email.strip().lower()
        by_email[email].append(f"hris_employee:{employee.employee_id}")
        by_local[email.split("@", 1)[0]].append(f"hris_employee:{employee.employee_id}")
    return by_email, by_local


def _internal_domains(
    users: list[BlueprintIdentityUserAsset],
    employees: list[BlueprintHrisEmployeeAsset],
    organization_domain: str,
) -> set[str]:
    domains = {organization_domain.strip().lower()}
    for email in [item.email for item in users] + [item.email for item in employees]:
        if "@" in email:
            domains.add(email.strip().lower().split("@", 1)[1])
    return domains


def _slugify(value: str) -> str:
    return (
        value.strip()
        .lower()
        .replace(":", "-")
        .replace("@", "-")
        .replace(".", "-")
        .replace("/", "-")
    )


__all__ = ["reconcile_identity_sources"]
