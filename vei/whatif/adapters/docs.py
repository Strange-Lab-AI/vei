from __future__ import annotations

from typing import Any

from vei.context.models import ContextSnapshot

from ..corpus import (
    _company_history_event_id,
    _company_history_thread_id,
    _contains_keyword,
    _history_timestamp_ms,
    _normalized_actor_id,
    _truncate_snippet,
    timestamp_to_text,
)
from ..models import WhatIfArtifactFlags, WhatIfEvent


def build_docs_events(
    *,
    snapshot: ContextSnapshot,
    organization_domain: str,
    include_content: bool,
) -> list[WhatIfEvent]:
    source = snapshot.source_for("google")
    if source is None or not isinstance(source.data, dict):
        return []

    documents = source.data.get("documents", [])
    drive_shares = source.data.get("drive_shares", [])
    if not isinstance(documents, list):
        return []

    share_lookup = _share_lookup(drive_shares)
    events: list[WhatIfEvent] = []
    for document_index, document in enumerate(documents):
        if not isinstance(document, dict):
            continue
        doc_id = str(document.get("doc_id") or "").strip()
        if not doc_id:
            continue
        thread_id = _company_history_thread_id("docs", doc_id)
        title = str(document.get("title") or doc_id).strip()
        owner = _normalized_actor_id(
            document.get("owner"),
            organization_domain=organization_domain,
            fallback=f"docs-owner-{document_index + 1}",
        )
        body_text = str(document.get("body") or "").strip()
        base_timestamp = document.get("created_time") or document.get("modified_time")

        events.append(
            WhatIfEvent(
                event_id=_company_history_event_id(
                    provider="docs",
                    raw_event_id=f"{doc_id}:document",
                    fallback_parts=(doc_id, "document"),
                ),
                timestamp=timestamp_to_text(base_timestamp),
                timestamp_ms=_history_timestamp_ms(
                    base_timestamp,
                    fallback_index=(document_index + 1) * 1000,
                ),
                actor_id=owner,
                event_type="message",
                thread_id=thread_id,
                surface="docs",
                conversation_anchor=doc_id,
                subject=title,
                snippet=_docs_snippet(
                    title=title,
                    body_text=body_text,
                    include_content=include_content,
                ),
                flags=_docs_flags(
                    title=title,
                    text=body_text,
                    owner=owner,
                    shared_with=share_lookup.get(doc_id, []),
                    source="google",
                ),
            )
        )

        for comment_index, comment in enumerate(_comment_rows(document)):
            body = str(comment.get("body") or "").strip()
            created = comment.get("created") or base_timestamp
            author = _normalized_actor_id(
                comment.get("author"),
                organization_domain=organization_domain,
                fallback=owner,
            )
            events.append(
                WhatIfEvent(
                    event_id=_company_history_event_id(
                        provider="docs",
                        raw_event_id=str(comment.get("id") or ""),
                        fallback_parts=(doc_id, "comment", str(comment_index + 1)),
                    ),
                    timestamp=timestamp_to_text(created),
                    timestamp_ms=_history_timestamp_ms(
                        created,
                        fallback_index=(document_index + 1) * 1000 + comment_index + 1,
                    ),
                    actor_id=author,
                    event_type="reply",
                    thread_id=thread_id,
                    surface="docs",
                    conversation_anchor=doc_id,
                    subject=title,
                    snippet=body if include_content else _truncate_snippet(body),
                    flags=_docs_flags(
                        title=title,
                        text=body,
                        owner=author,
                        shared_with=share_lookup.get(doc_id, []),
                        source="google",
                        is_reply=True,
                    ),
                )
            )

        version_rows = document.get("versions", [])
        if isinstance(version_rows, list):
            for version_index, version in enumerate(version_rows):
                if not isinstance(version, dict):
                    continue
                modified_time = version.get("modified_time") or base_timestamp
                editor = _normalized_actor_id(
                    version.get("modified_by") or version.get("editor"),
                    organization_domain=organization_domain,
                    fallback=owner,
                )
                summary = str(
                    version.get("summary")
                    or version.get("label")
                    or version.get("version_id")
                    or "Document revision saved"
                ).strip()
                events.append(
                    WhatIfEvent(
                        event_id=_company_history_event_id(
                            provider="docs",
                            raw_event_id=str(version.get("version_id") or ""),
                            fallback_parts=(
                                doc_id,
                                "version",
                                str(version_index + 1),
                            ),
                        ),
                        timestamp=timestamp_to_text(modified_time),
                        timestamp_ms=_history_timestamp_ms(
                            modified_time,
                            fallback_index=(
                                (document_index + 1) * 1000 + 100 + version_index
                            ),
                        ),
                        actor_id=editor,
                        event_type="message",
                        thread_id=thread_id,
                        surface="docs",
                        conversation_anchor=doc_id,
                        subject=title,
                        snippet=(
                            summary if include_content else _truncate_snippet(summary)
                        ),
                        flags=_docs_flags(
                            title=title,
                            text=summary,
                            owner=editor,
                            shared_with=share_lookup.get(doc_id, []),
                            source="google",
                        ),
                    )
                )

        permission_rows = document.get("permissions", [])
        if isinstance(permission_rows, list):
            for permission_index, permission in enumerate(permission_rows):
                if not isinstance(permission, dict):
                    continue
                shared_with = _permission_targets(permission)
                if not shared_with:
                    continue
                changed_at = permission.get("created") or permission.get(
                    "modified_time"
                )
                actor = _normalized_actor_id(
                    permission.get("granted_by") or owner,
                    organization_domain=organization_domain,
                    fallback=owner,
                )
                summary = (
                    f"Shared with {', '.join(shared_with[:3])}"
                    if shared_with
                    else "Permissions updated"
                )
                events.append(
                    WhatIfEvent(
                        event_id=_company_history_event_id(
                            provider="docs",
                            raw_event_id=str(permission.get("id") or ""),
                            fallback_parts=(
                                doc_id,
                                "permission",
                                str(permission_index + 1),
                            ),
                        ),
                        timestamp=timestamp_to_text(changed_at or base_timestamp),
                        timestamp_ms=_history_timestamp_ms(
                            changed_at or base_timestamp,
                            fallback_index=(
                                (document_index + 1) * 1000 + 200 + permission_index
                            ),
                        ),
                        actor_id=actor,
                        event_type="share",
                        thread_id=thread_id,
                        surface="docs",
                        conversation_anchor=doc_id,
                        subject=title,
                        snippet=summary,
                        flags=_docs_flags(
                            title=title,
                            text=summary,
                            owner=actor,
                            shared_with=shared_with,
                            source="google",
                        ),
                    )
                )

    return events


def _docs_snippet(
    *,
    title: str,
    body_text: str,
    include_content: bool,
) -> str:
    text = body_text.strip() or f"Document activity for {title}"
    if include_content:
        return text
    return _truncate_snippet(text)


def _docs_flags(
    *,
    title: str,
    text: str,
    owner: str,
    shared_with: list[str],
    source: str,
    is_reply: bool = False,
) -> WhatIfArtifactFlags:
    joined = " ".join([title, text])
    return WhatIfArtifactFlags(
        consult_legal_specialist=_contains_keyword(
            joined,
            ("legal", "counsel", "compliance", "contract"),
        ),
        consult_trading_specialist=_contains_keyword(
            joined,
            ("trading", "desk", "market"),
        ),
        has_attachment_reference=_contains_keyword(
            joined,
            ("attachment", ".pdf", ".doc", ".xlsx"),
        ),
        is_escalation=_contains_keyword(
            joined,
            ("urgent", "escalate", "executive"),
        ),
        is_reply=is_reply,
        to_count=len(shared_with) or 1,
        to_recipients=shared_with or [owner],
        subject=title,
        norm_subject=title.lower().strip(),
        source=source,
    )


def _share_lookup(drive_shares: object) -> dict[str, list[str]]:
    lookup: dict[str, list[str]] = {}
    if not isinstance(drive_shares, list):
        return lookup
    for share in drive_shares:
        if not isinstance(share, dict):
            continue
        doc_id = str(share.get("doc_id") or "").strip()
        if not doc_id:
            continue
        targets = [
            str(item).strip()
            for item in share.get("shared_with", [])
            if str(item).strip()
        ]
        if targets:
            lookup[doc_id] = targets
    return lookup


def _comment_rows(document: dict[str, Any]) -> list[dict[str, Any]]:
    comments = document.get("comments", [])
    if not isinstance(comments, list):
        return []
    return [comment for comment in comments if isinstance(comment, dict)]


def _permission_targets(permission: dict[str, Any]) -> list[str]:
    targets = permission.get("shared_with") or permission.get("emails") or []
    if isinstance(targets, list):
        return [str(item).strip() for item in targets if str(item).strip()]
    text = str(targets or "").strip()
    if not text:
        return []
    delimiter = ";" if ";" in text else ","
    return [item.strip() for item in text.split(delimiter) if item.strip()]
