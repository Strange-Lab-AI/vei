from __future__ import annotations

from typing import Any, Dict, Set

from .models import ContextDiff, ContextDiffEntry, ContextSnapshot


def compute_diff(
    before: ContextSnapshot,
    after: ContextSnapshot,
) -> ContextDiff:
    entries: list[ContextDiffEntry] = []

    _diff_slack(before, after, entries)
    _diff_jira(before, after, entries)
    _diff_google(before, after, entries)
    _diff_okta(before, after, entries)

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
    b_source = before.source_for("slack")
    a_source = after.source_for("slack")
    b_channels = _keyed_list(
        (b_source.data if b_source else {}).get("channels", []),
        "channel",
    )
    a_channels = _keyed_list(
        (a_source.data if a_source else {}).get("channels", []),
        "channel",
    )
    _diff_keyed("channels", b_channels, a_channels, entries)


def _diff_jira(
    before: ContextSnapshot,
    after: ContextSnapshot,
    entries: list[ContextDiffEntry],
) -> None:
    b_source = before.source_for("jira")
    a_source = after.source_for("jira")
    b_issues = _keyed_list(
        (b_source.data if b_source else {}).get("issues", []),
        "ticket_id",
    )
    a_issues = _keyed_list(
        (a_source.data if a_source else {}).get("issues", []),
        "ticket_id",
    )
    _diff_keyed("issues", b_issues, a_issues, entries)


def _diff_google(
    before: ContextSnapshot,
    after: ContextSnapshot,
    entries: list[ContextDiffEntry],
) -> None:
    b_source = before.source_for("google")
    a_source = after.source_for("google")
    b_docs = _keyed_list(
        (b_source.data if b_source else {}).get("documents", []),
        "doc_id",
    )
    a_docs = _keyed_list(
        (a_source.data if a_source else {}).get("documents", []),
        "doc_id",
    )
    _diff_keyed("documents", b_docs, a_docs, entries)


def _diff_okta(
    before: ContextSnapshot,
    after: ContextSnapshot,
    entries: list[ContextDiffEntry],
) -> None:
    b_source = before.source_for("okta")
    a_source = after.source_for("okta")
    b_users = _keyed_list(
        (b_source.data if b_source else {}).get("users", []),
        "id",
    )
    a_users = _keyed_list(
        (a_source.data if a_source else {}).get("users", []),
        "id",
    )
    _diff_keyed("users", b_users, a_users, entries)


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
