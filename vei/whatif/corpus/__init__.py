from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

from ._aggregation import (  # noqa: E402
    branch_has_external_sharing,
    build_actor_profiles,
    build_thread_summaries,
    choose_branch_event,
    event_by_id,
    event_is_flagged,
    event_reason_labels,
    event_reference,
    external_recipient_count,
    has_external_recipients,
    internal_recipient_count,
    is_internal_recipient,
    matches_custodian_filter,
    recipient_scope,
    search_events,
    thread_events,
    thread_subject,
    touch_actor,
    _primary_case_id,
    _query_terms,
)
from ._company_history import (  # noqa: E402
    CHAT_SOURCE_PROVIDERS,
    COMPANY_HISTORY_CONTENT_NOTICE,
    CRM_SOURCE_PROVIDERS,
    DOC_SOURCE_PROVIDERS,
    EVENT_HISTORY_PROVIDERS,
    STATE_CONTEXT_PROVIDERS,
    SUPPORTED_HISTORY_PROVIDERS,
    WORK_SOURCE_PROVIDERS,
    load_company_history_world,
    _build_company_history_events,
    _company_history_chat_events,
    _company_history_jira_events,
    _event_history_provider_names,
    _history_actor_payload,
    _history_chat_user_lookup,
    _history_snapshot_identity,
    _resolved_chat_actor_value,
    _supported_history_provider_names,
)
from ._enron import (  # noqa: E402
    CONTENT_NOTICE,
    ENRON_DOMAIN,
    EXECUTIVE_MARKERS,
    artifact_flags,
    build_event,
    hydrate_event_snippets,
    load_content_by_event_ids,
    load_enron_world,
    touches_executive,
)
from ._mail_archive import (  # noqa: E402
    MAIL_ARCHIVE_CONTENT_NOTICE,
    MAIL_ARCHIVE_FILE_NAMES,
    MAIL_SOURCE_PROVIDERS,
    build_archive_event,
    load_history_snapshot,
    load_mail_archive_world,
    _archive_body_text,
    _archive_event_id,
    _archive_subject,
    _archive_thread_id,
    _archive_thread_subject,
    _archive_threads_from_gmail_payload,
    _archive_threads_from_snapshot,
    _archive_timestamp,
    _company_history_mail_events,
    _load_history_snapshot,
    _load_mail_archive_snapshot,
    _mail_archive_source_payload,
    _mail_archive_source_payload_or_empty,
    _merge_archive_thread_payloads,
    _snapshot_from_archive_payload,
    _snapshot_from_json_payload,
)
from ._time import (  # noqa: E402
    display_name,
    parse_time_value,
    resolve_time_window,
    safe_int,
    string_list,
    timestamp_to_ms,
    timestamp_to_text,
    _parse_timestamp_text,
)
from ._util import (  # noqa: E402
    _archive_event_type,
    _channel_event_type,
    _channel_message_timestamp_ms,
    _channel_subject,
    _company_history_event_id,
    _company_history_thread_id,
    _contains_keyword,
    _email_domain,
    _fallback_internal_address,
    _has_attachment_reference,
    _history_timestamp_ms,
    _jira_issue_event_type,
    _jira_issue_snippet,
    _message_flag,
    _normalized_actor_id,
    _organization_domain_from_snapshot,
    _organization_domain_from_threads,
    _organization_name_from_domain,
    _override_actor_profiles,
    _recipient_list,
    _timestamp_text_from_ms,
    _truncate_snippet,
)


def detect_whatif_source(source_dir: str | Path) -> str:
    resolved = Path(source_dir).expanduser().resolve()
    if _looks_like_enron_rosetta(resolved):
        return "enron"
    detected_snapshot_source = _detect_snapshot_source_kind(resolved)
    if detected_snapshot_source is not None:
        return detected_snapshot_source
    raise ValueError(f"could not detect historical source from: {resolved}")


def _looks_like_enron_rosetta(path: Path) -> bool:
    if path.is_file():
        return False
    return (path / "enron_rosetta_events_metadata.parquet").exists()


def _looks_like_mail_archive(path: Path) -> bool:
    if path.is_file():
        return path.suffix.lower() == ".json"
    return any((path / filename).exists() for filename in MAIL_ARCHIVE_FILE_NAMES)


def _detect_snapshot_source_kind(path: Path) -> str | None:
    try:
        snapshot = load_history_snapshot(path)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "whatif snapshot detection failed for %s (%s)",
            path,
            type(exc).__name__,
            extra={
                "source": "company_history",
                "provider": "snapshot",
                "file_path": str(path),
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return None
    provider_names = _event_history_provider_names(snapshot)
    if not provider_names:
        return None
    if (
        provider_names <= MAIL_SOURCE_PROVIDERS
        and provider_names & MAIL_SOURCE_PROVIDERS
    ):
        return "mail_archive"
    return "company_history"


__all__ = [
    "CHAT_SOURCE_PROVIDERS",
    "COMPANY_HISTORY_CONTENT_NOTICE",
    "CONTENT_NOTICE",
    "CRM_SOURCE_PROVIDERS",
    "DOC_SOURCE_PROVIDERS",
    "ENRON_DOMAIN",
    "EVENT_HISTORY_PROVIDERS",
    "EXECUTIVE_MARKERS",
    "MAIL_ARCHIVE_CONTENT_NOTICE",
    "MAIL_ARCHIVE_FILE_NAMES",
    "MAIL_SOURCE_PROVIDERS",
    "STATE_CONTEXT_PROVIDERS",
    "SUPPORTED_HISTORY_PROVIDERS",
    "WORK_SOURCE_PROVIDERS",
    "artifact_flags",
    "branch_has_external_sharing",
    "build_actor_profiles",
    "build_archive_event",
    "build_event",
    "build_thread_summaries",
    "choose_branch_event",
    "detect_whatif_source",
    "display_name",
    "event_by_id",
    "event_is_flagged",
    "event_reason_labels",
    "event_reference",
    "external_recipient_count",
    "has_external_recipients",
    "hydrate_event_snippets",
    "internal_recipient_count",
    "is_internal_recipient",
    "load_company_history_world",
    "load_content_by_event_ids",
    "load_enron_world",
    "load_history_snapshot",
    "load_mail_archive_world",
    "matches_custodian_filter",
    "parse_time_value",
    "recipient_scope",
    "resolve_time_window",
    "safe_int",
    "search_events",
    "string_list",
    "thread_events",
    "thread_subject",
    "timestamp_to_ms",
    "timestamp_to_text",
    "touch_actor",
    "touches_executive",
    "_archive_body_text",
    "_archive_event_id",
    "_archive_event_type",
    "_archive_subject",
    "_archive_thread_id",
    "_archive_thread_subject",
    "_archive_threads_from_gmail_payload",
    "_archive_threads_from_snapshot",
    "_archive_timestamp",
    "_build_company_history_events",
    "_channel_event_type",
    "_channel_message_timestamp_ms",
    "_channel_subject",
    "_company_history_chat_events",
    "_company_history_event_id",
    "_company_history_jira_events",
    "_company_history_mail_events",
    "_company_history_thread_id",
    "_contains_keyword",
    "_detect_snapshot_source_kind",
    "_email_domain",
    "_event_history_provider_names",
    "_fallback_internal_address",
    "_has_attachment_reference",
    "_history_actor_payload",
    "_history_chat_user_lookup",
    "_history_snapshot_identity",
    "_history_timestamp_ms",
    "_jira_issue_event_type",
    "_jira_issue_snippet",
    "_load_history_snapshot",
    "_load_mail_archive_snapshot",
    "_looks_like_enron_rosetta",
    "_looks_like_mail_archive",
    "_mail_archive_source_payload",
    "_mail_archive_source_payload_or_empty",
    "_merge_archive_thread_payloads",
    "_message_flag",
    "_normalized_actor_id",
    "_organization_domain_from_snapshot",
    "_organization_domain_from_threads",
    "_organization_name_from_domain",
    "_override_actor_profiles",
    "_parse_timestamp_text",
    "_primary_case_id",
    "_query_terms",
    "_recipient_list",
    "_resolved_chat_actor_value",
    "_snapshot_from_archive_payload",
    "_snapshot_from_json_payload",
    "_supported_history_provider_names",
    "_timestamp_text_from_ms",
    "_truncate_snippet",
]
