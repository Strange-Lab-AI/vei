from __future__ import annotations

import json
from datetime import UTC, datetime
from email.utils import parseaddr, parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable

from vei.context.models import (
    BundleVerificationCheck,
    BundleVerificationResult,
    ContextSnapshot,
    ContextSourceResult,
)
from vei.context.providers.base import iso_now
from vei.context.providers.crm import capture_from_export as capture_crm_export
from vei.context.providers.gmail import capture_from_mbox
from vei.context.providers.google import capture_from_export as capture_google_export
from vei.context.providers.jira import capture_from_export as capture_jira_export
from vei.context.providers.salesforce import (
    capture_from_export as capture_salesforce_export,
)
from vei.context.providers.slack import capture_from_export as capture_slack_export
from vei.imports.api import normalize_identity_import_package
from vei.whatif.api import build_public_context as build_whatif_public_context
from vei.whatif.api import empty_public_context


def normalize_raw_exports(
    source_dir: str | Path,
    *,
    organization_name: str,
    organization_domain: str = "",
) -> ContextSnapshot:
    root = Path(source_dir).expanduser().resolve()
    sources: list[ContextSourceResult] = []
    existing_snapshot = _load_existing_snapshot(root)
    if existing_snapshot is not None:
        sources.extend(existing_snapshot.sources)

    if _looks_like_import_package(root):
        import_snapshot = _snapshot_from_import_package(root)
        sources = _merge_source_results(sources + import_snapshot.sources)
        organization_name = organization_name or import_snapshot.organization_name
        organization_domain = organization_domain or import_snapshot.organization_domain
    else:
        detected = _detect_export_sources(root)
        sources = _merge_source_results(sources + detected)

    if not sources:
        raise ValueError(f"no supported exports detected under: {root}")

    resolved_org_name = (
        organization_name
        or (existing_snapshot.organization_name if existing_snapshot else "")
        or "Imported Context"
    )
    resolved_org_domain = organization_domain or (
        existing_snapshot.organization_domain if existing_snapshot else ""
    )
    return ContextSnapshot(
        organization_name=resolved_org_name,
        organization_domain=resolved_org_domain,
        captured_at=iso_now(),
        sources=sources,
        metadata={"normalized_from": str(root)},
    )


def verify_context_snapshot(
    snapshot: ContextSnapshot,
    *,
    snapshot_path: str | Path | None = None,
) -> BundleVerificationResult:
    checks: list[BundleVerificationCheck] = []
    checks.append(
        BundleVerificationCheck(
            code="org.name_present",
            passed=bool(str(snapshot.organization_name).strip()),
            detail="organization_name must be present",
        )
    )
    checks.append(
        BundleVerificationCheck(
            code="org.domain_present",
            passed=bool(str(snapshot.organization_domain).strip()),
            severity="warning",
            detail="organization_domain should be present",
        )
    )
    checks.append(
        BundleVerificationCheck(
            code="sources.present",
            passed=bool(snapshot.sources),
            detail="snapshot must contain at least one source",
        )
    )

    for source in snapshot.sources:
        checks.extend(_source_checks(source))
    checks.extend(_bundle_timestamp_checks(snapshot))
    checks.extend(_cross_source_identity_checks(snapshot))

    return BundleVerificationResult(
        ok=not any(not check.passed and check.severity == "error" for check in checks),
        snapshot_path=(
            str(Path(snapshot_path).expanduser().resolve()) if snapshot_path else ""
        ),
        organization_name=snapshot.organization_name,
        organization_domain=snapshot.organization_domain,
        source_status={source.provider: source.status for source in snapshot.sources},
        checks=checks,
    )


def build_public_context_template(
    *,
    organization_name: str,
    organization_domain: str,
):
    return build_public_context_sidecar(
        organization_name=organization_name,
        organization_domain=organization_domain,
        live=False,
    )


def build_public_context_sidecar(
    *,
    organization_name: str,
    organization_domain: str,
    live: bool = True,
):
    if live:
        return build_whatif_public_context(
            organization_name=organization_name,
            organization_domain=organization_domain,
            live=True,
        )
    template = empty_public_context(
        organization_name=organization_name,
        organization_domain=organization_domain,
    )
    return template.model_copy(
        update={
            "prepared_at": iso_now(),
            "integration_hint": (
                "Fill in financial snapshots and public news events for this company."
            ),
        }
    )


def _load_existing_snapshot(root: Path) -> ContextSnapshot | None:
    if root.is_file():
        if root.name == "context_snapshot.json":
            return _snapshot_from_path(root)
        if root.suffix.lower() == ".json":
            try:
                return _snapshot_from_path(root)
            except ValueError:
                return None
        return None
    candidate = root / "context_snapshot.json"
    if candidate.exists():
        return _snapshot_from_path(candidate)
    return None


def _snapshot_from_path(path: Path) -> ContextSnapshot:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("sources"), list):
        return ContextSnapshot.model_validate(payload)
    if isinstance(payload, dict) and isinstance(payload.get("threads"), list):
        return ContextSnapshot(
            organization_name=str(payload.get("organization_name") or "").strip(),
            organization_domain=str(payload.get("organization_domain") or "").strip(),
            captured_at=str(payload.get("captured_at") or iso_now()).strip(),
            sources=[
                ContextSourceResult(
                    provider="mail_archive",
                    captured_at=str(payload.get("captured_at") or iso_now()).strip(),
                    status="ok" if payload.get("threads") else "empty",
                    record_counts={
                        "threads": len(payload.get("threads", [])),
                        "actors": len(payload.get("actors", [])),
                    },
                    data={
                        "threads": payload.get("threads", []),
                        "actors": payload.get("actors", []),
                    },
                )
            ],
        )
    raise ValueError(f"unsupported snapshot payload: {path}")


def _looks_like_import_package(root: Path) -> bool:
    if root.is_file():
        return False
    package_path = root / "package.json"
    if not package_path.exists():
        return False
    try:
        payload = json.loads(package_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return isinstance(payload, dict) and isinstance(payload.get("sources"), list)


def _snapshot_from_import_package(root: Path) -> ContextSnapshot:
    artifacts = normalize_identity_import_package(root)
    package = artifacts.package
    bundle = artifacts.normalized_bundle
    if bundle is None:
        raise ValueError(f"import package could not be normalized: {package.name}")

    graphs = bundle.capability_graphs
    identity_graph = graphs.identity_graph
    doc_graph = graphs.doc_graph
    work_graph = graphs.work_graph
    revenue_graph = graphs.revenue_graph

    google_source = ContextSourceResult(
        provider="google",
        captured_at=iso_now(),
        status=(
            "ok"
            if doc_graph.documents or doc_graph.drive_shares or identity_graph.users
            else "empty"
        ),
        record_counts={
            "users": len(identity_graph.users),
            "documents": len(doc_graph.documents),
            "drive_shares": len(doc_graph.drive_shares),
        },
        data={
            "users": [
                {
                    "id": user.user_id,
                    "email": user.email,
                    "name": user.display_name
                    or f"{user.first_name} {user.last_name}".strip(),
                    "org_unit": user.department or "",
                    "suspended": user.status != "ACTIVE",
                    "is_admin": False,
                }
                for user in identity_graph.users
            ],
            "documents": [
                {
                    "doc_id": doc.doc_id,
                    "title": doc.title,
                    "body": doc.body,
                    "tags": list(doc.tags),
                }
                for doc in doc_graph.documents
            ],
            "drive_shares": [
                share.model_dump(mode="json") for share in doc_graph.drive_shares
            ],
        },
    )

    jira_source = ContextSourceResult(
        provider="jira",
        captured_at=iso_now(),
        status="ok" if work_graph.tickets else "empty",
        record_counts={"issues": len(work_graph.tickets)},
        data={
            "issues": [
                {
                    "ticket_id": ticket.ticket_id,
                    "title": ticket.title,
                    "status": ticket.status,
                    "assignee": ticket.assignee or "",
                    "description": ticket.description or "",
                    "updated": "",
                    "comments": [],
                }
                for ticket in work_graph.tickets
            ]
        },
    )

    revenue_provider = _revenue_provider_name(package.metadata, package.sources)
    revenue_source = ContextSourceResult(
        provider=revenue_provider,
        captured_at=iso_now(),
        status=(
            "ok"
            if revenue_graph.deals or revenue_graph.contacts or revenue_graph.companies
            else "empty"
        ),
        record_counts={
            "companies": len(revenue_graph.companies),
            "contacts": len(revenue_graph.contacts),
            "deals": len(revenue_graph.deals),
        },
        data={
            "companies": [
                company.model_dump(mode="json") for company in revenue_graph.companies
            ],
            "contacts": [
                contact.model_dump(mode="json") for contact in revenue_graph.contacts
            ],
            "deals": [deal.model_dump(mode="json") for deal in revenue_graph.deals],
        },
    )

    return ContextSnapshot(
        organization_name=package.organization_name,
        organization_domain=package.organization_domain,
        captured_at=iso_now(),
        sources=_merge_source_results([google_source, jira_source, revenue_source]),
        metadata={"import_package": package.name},
    )


def _revenue_provider_name(
    metadata: dict[str, Any],
    sources: Iterable[Any],
) -> str:
    if str(metadata.get("crm_provider") or "").strip().lower() == "salesforce":
        return "salesforce"
    for source in sources:
        source_system = str(getattr(source, "source_system", "") or "").strip().lower()
        if "salesforce" in source_system:
            return "salesforce"
    return "crm"


def _detect_export_sources(root: Path) -> list[ContextSourceResult]:
    results: list[ContextSourceResult] = []
    if (
        root.is_dir()
        and (root / "users.json").exists()
        and (root / "channels.json").exists()
    ):
        results.append(_coerce_empty(capture_slack_export(root)))

    for mbox_path in _gmail_export_paths(root):
        results.append(_coerce_empty(capture_from_mbox(mbox_path)))

    for provider, loader in (
        ("jira", capture_jira_export),
        ("google", capture_google_export),
        ("crm", capture_crm_export),
        ("salesforce", capture_salesforce_export),
    ):
        export_path = _provider_export_path(root, provider)
        if export_path is None:
            continue
        results.append(_coerce_empty(loader(export_path)))

    return results


def _provider_export_path(root: Path, provider: str) -> Path | None:
    if root.is_file():
        return root if provider in root.name.lower() else None
    names = {
        "jira": ("jira.json", "jira.csv", "issues.json", "issues.csv"),
        "google": (
            "google.json",
            "google_docs.json",
            "google_docs.csv",
            "drive_export.json",
            "drive_export.csv",
            "google_drive_shares.csv",
        ),
        "crm": ("crm.json", "crm.csv", "crm_deals.json", "crm_deals.csv"),
        "salesforce": (
            "salesforce.json",
            "salesforce.csv",
            "salesforce_deals.json",
            "salesforce_deals.csv",
        ),
    }
    for name in names.get(provider, ()):
        candidate = root / name
        if candidate.exists():
            return candidate
    raw_dir = root / "raw"
    if raw_dir.is_dir():
        return _provider_export_path(raw_dir, provider)
    return None


def _gmail_export_paths(root: Path) -> list[Path]:
    if root.is_file():
        return [root] if root.suffix.lower() in {".mbox", ".mbx"} else []
    return sorted(root.rglob("*.mbox")) + sorted(root.rglob("*.mbx"))


def _coerce_empty(source: ContextSourceResult) -> ContextSourceResult:
    if source.status == "ok" and not any(source.record_counts.values()):
        return source.model_copy(update={"status": "empty"})
    return source


def _merge_source_results(
    sources: list[ContextSourceResult],
) -> list[ContextSourceResult]:
    merged: dict[str, ContextSourceResult] = {}
    for source in sources:
        current = merged.get(source.provider)
        if current is None:
            merged[source.provider] = source
            continue
        merged[source.provider] = current.model_copy(
            update={
                "captured_at": max(current.captured_at, source.captured_at),
                "status": _merge_status(current.status, source.status),
                "record_counts": _merge_counts(
                    current.record_counts, source.record_counts
                ),
                "data": _merge_data(current.data, source.data),
                "error": source.error or current.error,
            }
        )
    return list(merged.values())


def _merge_status(left: str, right: str) -> str:
    priorities = {"error": 3, "partial": 2, "ok": 1, "empty": 0}
    return left if priorities.get(left, -1) >= priorities.get(right, -1) else right


def _merge_counts(left: dict[str, int], right: dict[str, int]) -> dict[str, int]:
    merged = dict(left)
    for key, value in right.items():
        merged[key] = int(merged.get(key, 0)) + int(value)
    return merged


def _merge_data(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    merged = dict(left)
    for key, value in right.items():
        if key not in merged:
            merged[key] = value
            continue
        current = merged[key]
        if isinstance(current, list) and isinstance(value, list):
            merged[key] = current + value
            continue
        if isinstance(current, dict) and isinstance(value, dict):
            merged[key] = {**current, **value}
            continue
        merged[key] = value
    return merged


def _source_checks(source: ContextSourceResult) -> list[BundleVerificationCheck]:
    checks: list[BundleVerificationCheck] = [
        BundleVerificationCheck(
            code="source.status",
            passed=source.status != "error",
            provider=source.provider,
            detail=f"status={source.status}",
        )
    ]
    if source.status == "empty":
        checks.append(
            BundleVerificationCheck(
                code="source.not_empty",
                passed=False,
                severity="warning",
                provider=source.provider,
                detail="source contains no usable records",
            )
        )
    warnings = source.data.get("parse_warnings", [])
    if isinstance(warnings, list) and warnings:
        checks.append(
            BundleVerificationCheck(
                code="source.parse_warnings",
                passed=False,
                severity="warning",
                provider=source.provider,
                detail=f"{len(warnings)} parse warnings recorded",
            )
        )
    checks.extend(_timestamp_checks(source))
    checks.extend(_duplicate_checks(source))
    checks.extend(_actor_checks(source))
    checks.extend(_required_field_checks(source))
    return checks


def _bundle_timestamp_checks(
    snapshot: ContextSnapshot,
) -> list[BundleVerificationCheck]:
    timestamps: list[int] = []
    for source in snapshot.sources:
        timestamps.extend(value for value in _source_timestamps(source) if value > 0)
    if not timestamps:
        return [
            BundleVerificationCheck(
                code="bundle.timestamp_span",
                passed=False,
                severity="warning",
                detail="no parseable timestamps found across the bundle",
            )
        ]
    span_ms = max(timestamps) - min(timestamps)
    return [
        BundleVerificationCheck(
            code="bundle.timestamp_span",
            passed=span_ms >= 86_400_000,
            severity="warning",
            detail=f"span_ms={span_ms}",
        )
    ]


def _cross_source_identity_checks(
    snapshot: ContextSnapshot,
) -> list[BundleVerificationCheck]:
    email_to_names: dict[str, set[str]] = {}
    name_to_emails: dict[str, set[str]] = {}
    for source in snapshot.sources:
        for email, name in _source_identity_pairs(source):
            if email:
                email_to_names.setdefault(email, set())
                if name:
                    email_to_names[email].add(name)
            if name and email:
                name_to_emails.setdefault(name, set()).add(email)

    email_mismatches = sorted(
        email for email, names in email_to_names.items() if len(names) > 1
    )
    name_mismatches = sorted(
        name for name, emails in name_to_emails.items() if len(emails) > 1
    )
    return [
        BundleVerificationCheck(
            code="bundle.identity_email_names",
            passed=not email_mismatches,
            severity="warning",
            detail=(
                f"emails with multiple names: {', '.join(email_mismatches[:3])}"
                if email_mismatches
                else "emails resolve to one display name"
            ),
        ),
        BundleVerificationCheck(
            code="bundle.identity_name_emails",
            passed=not name_mismatches,
            severity="warning",
            detail=(
                f"names with multiple emails: {', '.join(name_mismatches[:3])}"
                if name_mismatches
                else "display names resolve to one email"
            ),
        ),
    ]


def _timestamp_checks(source: ContextSourceResult) -> list[BundleVerificationCheck]:
    timestamps = [value for value in _source_timestamps(source) if value > 0]
    if not timestamps:
        return [
            BundleVerificationCheck(
                code="source.timestamp_span",
                passed=False,
                severity="warning",
                provider=source.provider,
                detail="no parseable timestamps found",
            )
        ]
    span_ok = max(timestamps) > min(timestamps)
    return [
        BundleVerificationCheck(
            code="source.timestamp_span",
            passed=span_ok,
            severity="warning",
            provider=source.provider,
            detail=f"span_ms={max(timestamps) - min(timestamps)}",
        )
    ]


def _duplicate_checks(source: ContextSourceResult) -> list[BundleVerificationCheck]:
    checks: list[BundleVerificationCheck] = []
    ids = _source_ids(source)
    for label, values in ids.items():
        duplicates = _duplicate_values(values)
        checks.append(
            BundleVerificationCheck(
                code=f"source.unique_{label}",
                passed=not duplicates,
                provider=source.provider,
                detail=(
                    f"duplicates: {', '.join(duplicates[:3])}"
                    if duplicates
                    else f"{label} values are unique"
                ),
            )
        )
    return checks


def _actor_checks(source: ContextSourceResult) -> list[BundleVerificationCheck]:
    suspicious = _suspicious_identity_values(source)
    return [
        BundleVerificationCheck(
            code="source.actor_normalization",
            passed=not suspicious,
            severity="warning",
            provider=source.provider,
            detail=(
                f"suspicious values: {', '.join(suspicious[:3])}"
                if suspicious
                else "actor fields look normalized"
            ),
        )
    ]


def _required_field_checks(
    source: ContextSourceResult,
) -> list[BundleVerificationCheck]:
    missing = _missing_required_fields(source)
    return [
        BundleVerificationCheck(
            code="source.required_fields",
            passed=not missing,
            severity="warning",
            provider=source.provider,
            detail=(
                f"missing fields: {', '.join(missing[:3])}"
                if missing
                else "required fields present"
            ),
        )
    ]


def _source_timestamps(source: ContextSourceResult) -> list[int]:
    data = source.data if isinstance(source.data, dict) else {}
    provider = source.provider
    timestamps: list[int] = []
    if provider in {"mail_archive", "gmail"}:
        for thread in data.get("threads", []):
            if not isinstance(thread, dict):
                continue
            for message in thread.get("messages", []):
                if not isinstance(message, dict):
                    continue
                timestamps.append(
                    _timestamp_ms(message.get("date") or message.get("time_ms"))
                )
    elif provider in {"slack", "teams"}:
        for channel in data.get("channels", []):
            if not isinstance(channel, dict):
                continue
            for message in channel.get("messages", []):
                if not isinstance(message, dict):
                    continue
                timestamps.append(_timestamp_ms(message.get("ts")))
    elif provider == "jira":
        for issue in data.get("issues", []):
            if not isinstance(issue, dict):
                continue
            timestamps.append(_timestamp_ms(issue.get("updated")))
            for comment in issue.get("comments", []):
                if isinstance(comment, dict):
                    timestamps.append(_timestamp_ms(comment.get("created")))
    elif provider == "google":
        for document in data.get("documents", []):
            if not isinstance(document, dict):
                continue
            timestamps.append(_timestamp_ms(document.get("modified_time")))
            for comment in document.get("comments", []):
                if isinstance(comment, dict):
                    timestamps.append(_timestamp_ms(comment.get("created")))
            for version in document.get("versions", []):
                if isinstance(version, dict):
                    timestamps.append(_timestamp_ms(version.get("modified_time")))
    elif provider in {"crm", "salesforce"}:
        for deal in data.get("deals", []):
            if not isinstance(deal, dict):
                continue
            timestamps.extend(
                [
                    _timestamp_ms(deal.get("created_ms")),
                    _timestamp_ms(deal.get("updated_ms")),
                    _timestamp_ms(deal.get("closed_ms")),
                    _timestamp_ms(deal.get("close_date")),
                ]
            )
            history = deal.get("history", [])
            if isinstance(history, list):
                for item in history:
                    if isinstance(item, dict):
                        timestamps.append(
                            _timestamp_ms(
                                item.get("timestamp")
                                or item.get("timestamp_ms")
                                or item.get("changed_at")
                            )
                        )
    return timestamps


def _source_ids(source: ContextSourceResult) -> dict[str, list[str]]:
    data = source.data if isinstance(source.data, dict) else {}
    provider = source.provider
    if provider in {"mail_archive", "gmail"}:
        return {
            "thread_id": [
                str(thread.get("thread_id") or "").strip()
                for thread in data.get("threads", [])
                if isinstance(thread, dict)
            ]
        }
    if provider in {"slack", "teams"}:
        return {
            "channel_id": [
                str(channel.get("channel_id") or channel.get("channel") or "").strip()
                for channel in data.get("channels", [])
                if isinstance(channel, dict)
            ]
        }
    if provider == "jira":
        return {
            "ticket_id": [
                str(issue.get("ticket_id") or "").strip()
                for issue in data.get("issues", [])
                if isinstance(issue, dict)
            ]
        }
    if provider == "google":
        return {
            "doc_id": [
                str(document.get("doc_id") or "").strip()
                for document in data.get("documents", [])
                if isinstance(document, dict)
            ]
        }
    if provider in {"crm", "salesforce"}:
        return {
            "company_id": [
                str(company.get("id") or "").strip()
                for company in data.get("companies", [])
                if isinstance(company, dict)
            ],
            "contact_id": [
                str(contact.get("id") or "").strip()
                for contact in data.get("contacts", [])
                if isinstance(contact, dict)
            ],
            "deal_id": [
                str(deal.get("id") or "").strip()
                for deal in data.get("deals", [])
                if isinstance(deal, dict)
            ],
        }
    return {}


def _suspicious_identity_values(source: ContextSourceResult) -> list[str]:
    data = source.data if isinstance(source.data, dict) else {}
    suspicious: list[str] = []
    if source.provider in {"mail_archive", "gmail"}:
        for thread in data.get("threads", []):
            if not isinstance(thread, dict):
                continue
            for message in thread.get("messages", []):
                if not isinstance(message, dict):
                    continue
                for key in ("from", "to"):
                    for token in _address_tokens(message.get(key)):
                        if token and "@" not in token:
                            suspicious.append(token)
    elif source.provider == "google":
        for user in data.get("users", []):
            if not isinstance(user, dict):
                continue
            email = str(user.get("email") or "").strip()
            if email and "@" not in email:
                suspicious.append(email)
    elif source.provider in {"crm", "salesforce"}:
        for contact in data.get("contacts", []):
            if not isinstance(contact, dict):
                continue
            email = str(contact.get("email") or "").strip()
            if email and "@" not in email:
                suspicious.append(email)
    return suspicious


def _missing_required_fields(source: ContextSourceResult) -> list[str]:
    data = source.data if isinstance(source.data, dict) else {}
    missing: list[str] = []
    if source.provider == "google":
        for document in data.get("documents", []):
            if not isinstance(document, dict):
                continue
            if not str(document.get("doc_id") or "").strip():
                missing.append("google.documents.doc_id")
            if not str(document.get("title") or "").strip():
                missing.append("google.documents.title")
    elif source.provider == "jira":
        for issue in data.get("issues", []):
            if not isinstance(issue, dict):
                continue
            if not str(issue.get("ticket_id") or "").strip():
                missing.append("jira.issues.ticket_id")
            if not str(issue.get("title") or "").strip():
                missing.append("jira.issues.title")
    elif source.provider in {"crm", "salesforce"}:
        for deal in data.get("deals", []):
            if not isinstance(deal, dict):
                continue
            if not str(deal.get("id") or "").strip():
                missing.append(f"{source.provider}.deals.id")
            if not str(deal.get("name") or "").strip():
                missing.append(f"{source.provider}.deals.name")
            if not str(deal.get("stage") or "").strip():
                missing.append(f"{source.provider}.deals.stage")
            if not str(deal.get("owner") or "").strip():
                missing.append(f"{source.provider}.deals.owner")
    return missing


def _source_identity_pairs(source: ContextSourceResult) -> list[tuple[str, str]]:
    data = source.data if isinstance(source.data, dict) else {}
    pairs: list[tuple[str, str]] = []
    if source.provider == "slack":
        for user in data.get("users", []):
            if not isinstance(user, dict):
                continue
            pairs.append(
                (
                    _normalized_email(user.get("email")),
                    _normalized_name(user.get("real_name") or user.get("name")),
                )
            )
    elif source.provider in {"google", "teams"}:
        for user in data.get("users", []):
            if not isinstance(user, dict):
                continue
            pairs.append(
                (
                    _normalized_email(user.get("email")),
                    _normalized_name(user.get("name") or user.get("display_name")),
                )
            )
    elif source.provider in {"mail_archive", "gmail"}:
        for actor in data.get("actors", []):
            if not isinstance(actor, dict):
                continue
            pairs.append(
                (
                    _normalized_email(actor.get("email") or actor.get("actor_id")),
                    _normalized_name(
                        actor.get("display_name")
                        or actor.get("name")
                        or actor.get("actor_id")
                    ),
                )
            )
    elif source.provider in {"crm", "salesforce"}:
        for contact in data.get("contacts", []):
            if not isinstance(contact, dict):
                continue
            name = " ".join(
                part
                for part in (
                    str(contact.get("first_name") or "").strip(),
                    str(contact.get("last_name") or "").strip(),
                )
                if part
            ).strip()
            pairs.append(
                (_normalized_email(contact.get("email")), _normalized_name(name))
            )
    return [(email, name) for email, name in pairs if email or name]


def _duplicate_values(values: list[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: list[str] = []
    for value in values:
        if not value:
            continue
        if value in seen and value not in duplicates:
            duplicates.append(value)
        seen.add(value)
    return duplicates


def _timestamp_ms(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        number = int(value)
        return number if number > 1_000_000_000_000 else number * 1000
    text = str(value).strip()
    if not text:
        return 0
    if text.isdigit():
        number = int(text)
        return number if number > 1_000_000_000_000 else number * 1000
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError):
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1000)


def _address_tokens(value: object) -> list[str]:
    text = str(value or "").replace(";", ",")
    return [item.strip() for item in text.split(",") if item.strip()]


def _normalized_email(value: object) -> str:
    _display_name, address = parseaddr(str(value or "").strip())
    candidate = address or str(value or "").strip()
    if "@" not in candidate:
        return ""
    return candidate.strip().lower()


def _normalized_name(value: object) -> str:
    text = str(value or "").strip()
    if not text or "@" in text:
        return ""
    return " ".join(text.lower().split())
