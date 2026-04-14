from __future__ import annotations

from typing import Any, Dict, Set

from .models import (
    ContextDiff,
    ContextDiffEntry,
    ContextSnapshot,
    CrmSourceData,
    GmailSourceData,
    GoogleSourceData,
    MailArchiveSourceData,
    OktaSourceData,
    SlackSourceData,
    TeamsSourceData,
    JiraSourceData,
    source_payload,
)


def compute_diff(
    before: ContextSnapshot,
    after: ContextSnapshot,
) -> ContextDiff:
    entries: list[ContextDiffEntry] = []

    _diff_slack(before, after, entries)
    _diff_jira(before, after, entries)
    _diff_google(before, after, entries)
    _diff_gmail(before, after, entries)
    _diff_teams(before, after, entries)
    _diff_okta(before, after, entries)
    _diff_crm(before, after, entries)
    _diff_salesforce(before, after, entries)
    _diff_mail_archive(before, after, entries)

    added = sum(1 for e in entries if e.kind == "added")
    removed = sum(1 for e in entries if e.kind == "removed")
    changed = sum(1 for e in entries if e.kind == "changed")
    parts = []
    if added:
        parts.append(f"{added} added")
    if removed:
        parts.append(f"{removed} removed")
    if changed:
        parts.append(f"{changed} changed")

    return ContextDiff(
        before_captured_at=before.captured_at,
        after_captured_at=after.captured_at,
        entries=entries,
        summary=", ".join(parts) if parts else "no changes",
    )


def _diff_slack(
    before: ContextSnapshot,
    after: ContextSnapshot,
    entries: list[ContextDiffEntry],
) -> None:
    b_source = source_payload(before.source_for("slack"), SlackSourceData)
    a_source = source_payload(after.source_for("slack"), SlackSourceData)
    b_channels = _keyed_list(
        b_source.channels if b_source else [],
        "channel",
    )
    a_channels = _keyed_list(
        a_source.channels if a_source else [],
        "channel",
    )
    _diff_keyed("channels", b_channels, a_channels, entries)


def _diff_jira(
    before: ContextSnapshot,
    after: ContextSnapshot,
    entries: list[ContextDiffEntry],
) -> None:
    b_source = source_payload(before.source_for("jira"), JiraSourceData)
    a_source = source_payload(after.source_for("jira"), JiraSourceData)
    b_issues = _keyed_list(
        b_source.issues if b_source else [],
        "ticket_id",
    )
    a_issues = _keyed_list(
        a_source.issues if a_source else [],
        "ticket_id",
    )
    _diff_keyed("issues", b_issues, a_issues, entries)


def _diff_google(
    before: ContextSnapshot,
    after: ContextSnapshot,
    entries: list[ContextDiffEntry],
) -> None:
    b_source = source_payload(before.source_for("google"), GoogleSourceData)
    a_source = source_payload(after.source_for("google"), GoogleSourceData)
    b_docs = _keyed_list(
        b_source.documents if b_source else [],
        "doc_id",
    )
    a_docs = _keyed_list(
        a_source.documents if a_source else [],
        "doc_id",
    )
    _diff_keyed("documents", b_docs, a_docs, entries)


def _diff_gmail(
    before: ContextSnapshot,
    after: ContextSnapshot,
    entries: list[ContextDiffEntry],
) -> None:
    b_source = source_payload(before.source_for("gmail"), GmailSourceData)
    a_source = source_payload(after.source_for("gmail"), GmailSourceData)
    b_threads = _keyed_list(
        b_source.threads if b_source else [],
        "thread_id",
    )
    a_threads = _keyed_list(
        a_source.threads if a_source else [],
        "thread_id",
    )
    _diff_keyed("mail_threads", b_threads, a_threads, entries)


def _diff_teams(
    before: ContextSnapshot,
    after: ContextSnapshot,
    entries: list[ContextDiffEntry],
) -> None:
    b_source = source_payload(before.source_for("teams"), TeamsSourceData)
    a_source = source_payload(after.source_for("teams"), TeamsSourceData)
    b_channels = _keyed_list(
        b_source.channels if b_source else [],
        "channel",
    )
    a_channels = _keyed_list(
        a_source.channels if a_source else [],
        "channel",
    )
    _diff_keyed("teams_channels", b_channels, a_channels, entries)


def _diff_okta(
    before: ContextSnapshot,
    after: ContextSnapshot,
    entries: list[ContextDiffEntry],
) -> None:
    b_source = source_payload(before.source_for("okta"), OktaSourceData)
    a_source = source_payload(after.source_for("okta"), OktaSourceData)
    b_users = _keyed_list(
        b_source.users if b_source else [],
        "id",
    )
    a_users = _keyed_list(
        a_source.users if a_source else [],
        "id",
    )
    _diff_keyed("users", b_users, a_users, entries)


def _diff_crm(
    before: ContextSnapshot,
    after: ContextSnapshot,
    entries: list[ContextDiffEntry],
) -> None:
    b_data = source_payload(before.source_for("crm"), CrmSourceData)
    a_data = source_payload(after.source_for("crm"), CrmSourceData)
    for collection, key in (
        ("companies", "id"),
        ("contacts", "id"),
        ("deals", "id"),
    ):
        _diff_keyed(
            collection,
            _keyed_list(getattr(b_data, collection, []), key),
            _keyed_list(getattr(a_data, collection, []), key),
            entries,
        )


def _diff_salesforce(
    before: ContextSnapshot,
    after: ContextSnapshot,
    entries: list[ContextDiffEntry],
) -> None:
    b_data = source_payload(before.source_for("salesforce"), CrmSourceData)
    a_data = source_payload(after.source_for("salesforce"), CrmSourceData)
    for collection, key in (
        ("companies", "id"),
        ("contacts", "id"),
        ("deals", "id"),
    ):
        _diff_keyed(
            collection,
            _keyed_list(getattr(b_data, collection, []), key),
            _keyed_list(getattr(a_data, collection, []), key),
            entries,
        )


def _diff_mail_archive(
    before: ContextSnapshot,
    after: ContextSnapshot,
    entries: list[ContextDiffEntry],
) -> None:
    b_data = source_payload(before.source_for("mail_archive"), MailArchiveSourceData)
    a_data = source_payload(after.source_for("mail_archive"), MailArchiveSourceData)
    _diff_keyed(
        "archive_threads",
        _keyed_list(b_data.threads if b_data else [], "thread_id"),
        _keyed_list(a_data.threads if a_data else [], "thread_id"),
        entries,
    )
    _diff_keyed(
        "actors",
        _keyed_list(b_data.actors if b_data else [], "actor_id"),
        _keyed_list(a_data.actors if a_data else [], "actor_id"),
        entries,
    )


def _keyed_list(
    items: Any,
    key: str,
) -> Dict[str, Dict[str, Any]]:
    if not isinstance(items, list):
        return {}
    return {
        str(item.get(key, i)): item
        for i, item in enumerate(items)
        if isinstance(item, dict)
    }


def _diff_keyed(
    domain: str,
    before: Dict[str, Dict[str, Any]],
    after: Dict[str, Dict[str, Any]],
    entries: list[ContextDiffEntry],
) -> None:
    before_keys: Set[str] = set(before.keys())
    after_keys: Set[str] = set(after.keys())

    for key in sorted(after_keys - before_keys):
        entries.append(ContextDiffEntry(kind="added", domain=domain, item_id=key))
    for key in sorted(before_keys - after_keys):
        entries.append(ContextDiffEntry(kind="removed", domain=domain, item_id=key))
    for key in sorted(before_keys & after_keys):
        if before[key] != after[key]:
            entries.append(ContextDiffEntry(kind="changed", domain=domain, item_id=key))
