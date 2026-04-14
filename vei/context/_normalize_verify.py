from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

from vei.context._normalize_cleanup import (
    address_tokens,
    normalized_email,
    normalized_name,
)
from vei.context.models import (
    BundleVerificationCheck,
    BundleVerificationResult,
    ContextProviderStatusSummary,
    ContextSnapshot,
    ContextSnapshotStatusSummary,
    ContextSourceResult,
    ContextStatusFinding,
    snapshot_role,
)


def verify_context_snapshot(
    snapshot: ContextSnapshot,
    *,
    snapshot_path: str | Path | None = None,
) -> BundleVerificationResult:
    checks: list[BundleVerificationCheck] = [
        BundleVerificationCheck(
            code="org.name_present",
            passed=bool(str(snapshot.organization_name).strip()),
            detail="organization_name must be present",
        ),
        BundleVerificationCheck(
            code="org.domain_present",
            passed=bool(str(snapshot.organization_domain).strip()),
            severity="warning",
            detail="organization_domain should be present",
        ),
        BundleVerificationCheck(
            code="sources.present",
            passed=bool(snapshot.sources),
            detail="snapshot must contain at least one source",
        ),
    ]
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


def summarize_context_snapshot(
    snapshot: ContextSnapshot,
) -> ContextSnapshotStatusSummary:
    verification = verify_context_snapshot(snapshot)
    provider_summaries: list[ContextProviderStatusSummary] = []
    duplicate_findings: list[ContextStatusFinding] = []
    identity_findings: list[ContextStatusFinding] = []
    timestamp_findings: list[ContextStatusFinding] = []

    cleanup_summary = snapshot.metadata.get("normalization_cleanup")
    if isinstance(cleanup_summary, dict):
        for provider, counts in cleanup_summary.items():
            if not isinstance(counts, dict):
                continue
            details = ", ".join(
                f"{key}={value}"
                for key, value in sorted(counts.items())
                if int(value) > 0
            )
            if details:
                identity_findings.append(
                    ContextStatusFinding(
                        code="source.identity_cleanup",
                        severity="info",
                        provider=str(provider),
                        detail=details,
                    )
                )

    dedup_map = snapshot.metadata.get("actor_dedup_map")
    if isinstance(dedup_map, dict) and dedup_map:
        identity_findings.append(
            ContextStatusFinding(
                code="bundle.actor_dedup_map",
                severity="info",
                detail=f"actors_deduplicated={len(dedup_map)}",
            )
        )

    for source in snapshot.sources:
        source_checks = [
            check
            for check in verification.checks
            if check.provider == source.provider and not check.passed
        ]
        first_timestamp, last_timestamp = _source_time_range(source)
        duplicate_id_findings = [
            _status_finding(check)
            for check in source_checks
            if check.code.startswith("source.unique_")
        ]
        identity_cleanup_findings = [
            _status_finding(check)
            for check in source_checks
            if check.code in {"source.actor_normalization", "source.required_fields"}
        ]
        timestamp_quality = _source_timestamp_quality(source)
        timestamp_quality_findings = [
            ContextStatusFinding(
                code="source.timestamp_quality",
                severity=(
                    "info" if timestamp_quality.startswith("state_only") else "warning"
                ),
                provider=source.provider,
                detail=timestamp_quality,
            )
        ]
        provider_summaries.append(
            ContextProviderStatusSummary(
                provider=source.provider,
                status=source.status,
                record_counts=dict(source.record_counts),
                first_timestamp=first_timestamp,
                last_timestamp=last_timestamp,
                timestamp_quality=timestamp_quality,
                duplicate_id_findings=duplicate_id_findings,
                identity_cleanup_findings=identity_cleanup_findings,
            )
        )
        duplicate_findings.extend(duplicate_id_findings)
        identity_findings.extend(identity_cleanup_findings)
        timestamp_findings.extend(timestamp_quality_findings)

    first_timestamp, last_timestamp = _bundle_time_range(snapshot)
    return ContextSnapshotStatusSummary(
        snapshot_role=snapshot_role(snapshot),
        organization_name=snapshot.organization_name,
        organization_domain=snapshot.organization_domain,
        captured_at=snapshot.captured_at,
        first_timestamp=first_timestamp,
        last_timestamp=last_timestamp,
        providers=provider_summaries,
        duplicate_id_findings=duplicate_findings,
        identity_cleanup_findings=identity_findings,
        timestamp_quality=timestamp_findings,
    )


def timestamp_ms(value: object) -> int:
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


def timestamp_text(value: int) -> str:
    if value <= 0:
        return ""
    return (
        datetime.fromtimestamp(value / 1000, tz=UTC).isoformat().replace("+00:00", "Z")
    )


def _status_finding(check: BundleVerificationCheck) -> ContextStatusFinding:
    return ContextStatusFinding(
        code=check.code,
        severity=check.severity,
        provider=check.provider,
        detail=check.detail,
    )


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
    if _is_state_only_document_source(source):
        timestamps = [value for value in _source_timestamps(source) if value > 0]
        detail = _source_timestamp_quality(source)
        return [
            BundleVerificationCheck(
                code="source.timestamp_span",
                passed=bool(timestamps),
                severity="info",
                provider=source.provider,
                detail=detail,
            )
        ]

    timestamps = [value for value in _source_timestamps(source) if value > 0]
    if timestamps:
        return [
            BundleVerificationCheck(
                code="source.timestamp_span",
                passed=max(timestamps) > min(timestamps),
                severity="warning",
                provider=source.provider,
                detail=f"span_ms={max(timestamps) - min(timestamps)}",
            )
        ]

    return [
        BundleVerificationCheck(
            code="source.timestamp_span",
            passed=False,
            severity="warning",
            provider=source.provider,
            detail="no parseable timestamps found",
        )
    ]


def _duplicate_checks(source: ContextSourceResult) -> list[BundleVerificationCheck]:
    checks: list[BundleVerificationCheck] = []
    for label, values in _source_ids(source).items():
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
    data = source.data.model_dump(mode="python")
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
                    timestamp_ms(message.get("date") or message.get("time_ms"))
                )
        return timestamps

    if provider in {"slack", "teams"}:
        for channel in data.get("channels", []):
            if not isinstance(channel, dict):
                continue
            for message in channel.get("messages", []):
                if not isinstance(message, dict):
                    continue
                timestamps.append(timestamp_ms(message.get("ts")))
        return timestamps

    if provider == "jira":
        for issue in data.get("issues", []):
            if not isinstance(issue, dict):
                continue
            timestamps.append(timestamp_ms(issue.get("updated")))
            for comment in issue.get("comments", []):
                if isinstance(comment, dict):
                    timestamps.append(timestamp_ms(comment.get("created")))
        return timestamps

    if provider == "google":
        for document in data.get("documents", []):
            if not isinstance(document, dict):
                continue
            timestamps.append(timestamp_ms(document.get("modified_time")))
            for comment in document.get("comments", []):
                if isinstance(comment, dict):
                    timestamps.append(timestamp_ms(comment.get("created")))
            for version in document.get("versions", []):
                if isinstance(version, dict):
                    timestamps.append(timestamp_ms(version.get("modified_time")))
        return timestamps

    if provider in {"crm", "salesforce"}:
        for deal in data.get("deals", []):
            if not isinstance(deal, dict):
                continue
            timestamps.extend(
                [
                    timestamp_ms(deal.get("created_ms")),
                    timestamp_ms(deal.get("updated_ms")),
                    timestamp_ms(deal.get("closed_ms")),
                    timestamp_ms(deal.get("close_date")),
                ]
            )
            history = deal.get("history", [])
            if isinstance(history, list):
                for item in history:
                    if isinstance(item, dict):
                        timestamps.append(
                            timestamp_ms(
                                item.get("timestamp")
                                or item.get("timestamp_ms")
                                or item.get("changed_at")
                            )
                        )
    return timestamps


def _source_ids(source: ContextSourceResult) -> dict[str, list[str]]:
    data = source.data.model_dump(mode="python")
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
    data = source.data.model_dump(mode="python")
    suspicious: list[str] = []
    if source.provider in {"mail_archive", "gmail"}:
        for thread in data.get("threads", []):
            if not isinstance(thread, dict):
                continue
            for message in thread.get("messages", []):
                if not isinstance(message, dict):
                    continue
                for key in ("from", "to", "cc", "bcc"):
                    for token in address_tokens(message.get(key)):
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
    data = source.data.model_dump(mode="python")
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
    data = source.data.model_dump(mode="python")
    pairs: list[tuple[str, str]] = []
    if source.provider == "slack":
        for user in data.get("users", []):
            if not isinstance(user, dict):
                continue
            pairs.append(
                (
                    normalized_email(user.get("email")),
                    normalized_name(user.get("real_name") or user.get("name")),
                )
            )
    elif source.provider in {"google", "teams"}:
        for user in data.get("users", []):
            if not isinstance(user, dict):
                continue
            pairs.append(
                (
                    normalized_email(user.get("email")),
                    normalized_name(user.get("name") or user.get("display_name")),
                )
            )
    elif source.provider in {"mail_archive", "gmail"}:
        for actor in data.get("actors", []):
            if not isinstance(actor, dict):
                continue
            pairs.append(
                (
                    normalized_email(actor.get("email") or actor.get("actor_id")),
                    normalized_name(
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
                (normalized_email(contact.get("email")), normalized_name(name))
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


def _source_time_range(source: ContextSourceResult) -> tuple[str, str]:
    timestamps = sorted(value for value in _source_timestamps(source) if value > 0)
    if not timestamps:
        return "", ""
    return timestamp_text(timestamps[0]), timestamp_text(timestamps[-1])


def _bundle_time_range(snapshot: ContextSnapshot) -> tuple[str, str]:
    timestamps: list[int] = []
    for source in snapshot.sources:
        timestamps.extend(value for value in _source_timestamps(source) if value > 0)
    if not timestamps:
        return "", ""
    timestamps.sort()
    return timestamp_text(timestamps[0]), timestamp_text(timestamps[-1])


def _is_state_only_document_source(source: ContextSourceResult) -> bool:
    if source.provider != "google":
        return False
    data = source.data.model_dump(mode="python")
    documents = [item for item in data.get("documents", []) if isinstance(item, dict)]
    if not documents:
        return False
    has_comment_or_version = any(
        isinstance(document.get("comments"), list)
        and document.get("comments")
        or isinstance(document.get("versions"), list)
        and document.get("versions")
        for document in documents
    )
    return not has_comment_or_version


def _source_timestamp_quality(source: ContextSourceResult) -> str:
    if _is_state_only_document_source(source):
        data = source.data.model_dump(mode="python")
        documents = [
            item for item in data.get("documents", []) if isinstance(item, dict)
        ]
        backfilled_count = sum(
            1
            for document in documents
            if str(document.get("timestamp_quality") or "").startswith("backfilled")
        )
        provided_count = sum(
            1
            for document in documents
            if str(document.get("timestamp_quality") or "").strip() == "provided"
        )
        if backfilled_count:
            return f"state_only_backfilled documents={backfilled_count}"
        if provided_count:
            return f"state_only_provided documents={provided_count}"
        return f"state_only_missing documents={len(documents)}"

    timestamps = [value for value in _source_timestamps(source) if value > 0]
    if timestamps:
        if max(timestamps) > min(timestamps):
            return "full_span"
        return "single_point"
    return "missing"
