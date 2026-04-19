from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parseaddr, parsedate_to_datetime
from pathlib import Path
from typing import Any, Literal, Sequence

from pydantic import BaseModel, Field

from vei.events.api import (
    ActorRef,
    CanonicalEvent,
    EventDomain,
    EventProvenance,
    InternalExternal,
    ObjectRef,
    ProvenanceRecord,
    StateDelta,
    TextHandle,
)

from .models import ContextSnapshot

TimestampQuality = Literal["exact", "derived", "unknown"]
CANONICAL_EVENTS_FILE = "canonical_events.jsonl"
CANONICAL_EVENT_INDEX_FILE = "canonical_event_index.json"

_CASE_TOKEN_PATTERN = re.compile(r"\b[A-Z][A-Z0-9]{1,12}-\d{1,8}\b")
_DOC_TOKEN_PATTERN = re.compile(r"\bDOC[A-Z0-9-]{2,}\b")
_DEAL_TOKEN_PATTERN = re.compile(r"\bDEAL-[A-Z0-9-]{2,}\b")
_IGNORED_ANCHOR_TOKENS = {
    "DOCTYPE",
    "GPT-3",
    "GPT-4",
    "GPT-5",
    "UTF-8",
    "UTF-16",
    "UTF-32",
}
_NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")


class CanonicalHistoryIndexRow(BaseModel):
    event_id: str
    timestamp: str
    ts_ms: int
    timestamp_quality: TimestampQuality = "unknown"
    surface: str
    provider: str
    kind: str
    domain: str
    case_id: str = ""
    thread_ref: str = ""
    conversation_anchor: str = ""
    actor_id: str = ""
    target_id: str = ""
    participant_ids: list[str] = Field(default_factory=list)
    subject: str = ""
    normalized_subject: str = ""
    snippet: str = ""
    search_terms: list[str] = Field(default_factory=list)
    provider_object_refs: list[str] = Field(default_factory=list)
    stitch_confidence: float = 0.0
    stitch_basis: str = ""
    internal_external: Literal["internal", "external", "unknown"] = "unknown"
    metadata: dict[str, Any] = Field(default_factory=dict)


class CanonicalHistoryIndex(BaseModel):
    version: Literal["1"] = "1"
    organization_name: str
    organization_domain: str = ""
    captured_at: str = ""
    snapshot_role: str = "company_history_bundle"
    source_providers: list[str] = Field(default_factory=list)
    event_count: int = 0
    case_count: int = 0
    surface_counts: dict[str, int] = Field(default_factory=dict)
    rows: list[CanonicalHistoryIndexRow] = Field(default_factory=list)


class CanonicalHistoryPaths(BaseModel):
    snapshot_path: Path
    events_path: Path
    index_path: Path


class CanonicalHistoryBundle(BaseModel):
    paths: CanonicalHistoryPaths
    events: list[CanonicalEvent] = Field(default_factory=list)
    index: CanonicalHistoryIndex


class CanonicalHistoryTimelineResult(BaseModel):
    available: bool = False
    organization_name: str = ""
    organization_domain: str = ""
    source_providers: list[str] = Field(default_factory=list)
    total_event_count: int = 0
    matching_event_count: int = 0
    case_count: int = 0
    surface_counts: dict[str, int] = Field(default_factory=dict)
    rows: list[CanonicalHistoryIndexRow] = Field(default_factory=list)


class CanonicalHistoryReadinessReport(BaseModel):
    available: bool = False
    organization_name: str = ""
    organization_domain: str = ""
    source_providers: list[str] = Field(default_factory=list)
    event_count: int = 0
    case_count: int = 0
    surface_count: int = 0
    exact_timestamp_count: int = 0
    stitched_event_count: int = 0
    high_confidence_stitch_count: int = 0
    surface_counts: dict[str, int] = Field(default_factory=dict)
    top_cases: list[dict[str, Any]] = Field(default_factory=list)
    readiness_label: Literal["empty", "thin", "developing", "ready", "rich"] = "empty"
    ready_for_world_modeling: bool = False
    notes: list[str] = Field(default_factory=list)


@dataclass(frozen=True)
class _NormalizedHistoryEvent:
    provider: str
    surface: str
    kind: str
    timestamp: str
    ts_ms: int
    timestamp_quality: TimestampQuality
    thread_ref: str
    conversation_anchor: str
    actor_id: str
    target_id: str
    participant_ids: list[str]
    subject: str
    snippet: str
    provider_object_refs: list[str]
    internal_external: InternalExternal
    candidate_texts: list[str]
    metadata: dict[str, Any]


def canonical_history_paths(path: str | Path) -> CanonicalHistoryPaths:
    resolved = Path(path).expanduser().resolve()
    snapshot_path = resolved
    if resolved.is_dir():
        snapshot_path = resolved / "context_snapshot.json"
    root = snapshot_path.parent
    return CanonicalHistoryPaths(
        snapshot_path=snapshot_path,
        events_path=root / CANONICAL_EVENTS_FILE,
        index_path=root / CANONICAL_EVENT_INDEX_FILE,
    )


def canonical_history_sidecars_exist(path: str | Path) -> bool:
    paths = canonical_history_paths(path)
    return paths.events_path.exists() and paths.index_path.exists()


def build_canonical_history_bundle(snapshot: ContextSnapshot) -> CanonicalHistoryBundle:
    entries: list[_NormalizedHistoryEvent] = []
    for source in snapshot.sources:
        if source.status == "error":
            continue
        provider = str(source.provider or "").strip().lower()
        payload = source.typed_data().model_dump(mode="python")
        if provider in {"gmail", "mail_archive"}:
            entries.extend(
                _mail_entries(
                    provider=provider,
                    payload=payload,
                    organization_domain=snapshot.organization_domain,
                )
            )
            continue
        if provider in {"slack", "teams"}:
            entries.extend(
                _chat_entries(
                    provider=provider,
                    payload=payload,
                    organization_domain=snapshot.organization_domain,
                )
            )
            continue
        if provider in {"jira", "linear", "github", "gitlab", "clickup"}:
            entries.extend(
                _work_entries(
                    provider=provider,
                    payload=payload,
                    organization_domain=snapshot.organization_domain,
                )
            )
            continue
        if provider in {"google", "notion", "granola"}:
            entries.extend(
                _doc_entries(
                    provider=provider,
                    payload=payload,
                    organization_domain=snapshot.organization_domain,
                )
            )
            continue
        if provider in {"crm", "salesforce"}:
            entries.extend(
                _crm_entries(
                    provider=provider,
                    payload=payload,
                    organization_domain=snapshot.organization_domain,
                )
            )

    entries.sort(
        key=lambda item: (
            item.ts_ms,
            item.provider,
            item.thread_ref,
            item.actor_id,
            item.subject,
        )
    )
    thread_tokens = _thread_primary_tokens(entries)
    rows: list[CanonicalHistoryIndexRow] = []
    events: list[CanonicalEvent] = []
    case_ids: set[str] = set()

    for entry in entries:
        case_id, stitch_confidence, stitch_basis = _case_link_for_entry(
            entry,
            thread_tokens=thread_tokens,
        )
        row = _index_row_from_entry(
            entry,
            case_id=case_id,
            stitch_confidence=stitch_confidence,
            stitch_basis=stitch_basis,
        )
        event = _canonical_event_from_row(
            organization_domain=snapshot.organization_domain,
            row=row,
        )
        rows.append(row)
        events.append(event)
        if case_id:
            case_ids.add(case_id)

    surface_counts: dict[str, int] = {}
    for row in rows:
        surface_counts[row.surface] = surface_counts.get(row.surface, 0) + 1

    paths = canonical_history_paths(Path.cwd() / "context_snapshot.json")
    index = CanonicalHistoryIndex(
        organization_name=snapshot.organization_name,
        organization_domain=snapshot.organization_domain,
        captured_at=snapshot.captured_at,
        snapshot_role=str(
            snapshot.metadata.get("snapshot_role", "company_history_bundle")
        ),
        source_providers=sorted(
            {
                str(source.provider or "").strip().lower()
                for source in snapshot.sources
                if str(source.provider or "").strip()
            }
        ),
        event_count=len(rows),
        case_count=len(case_ids),
        surface_counts=surface_counts,
        rows=rows,
    )
    return CanonicalHistoryBundle(paths=paths, events=events, index=index)


def build_canonical_history_bundle_from_rows(
    *,
    organization_name: str,
    organization_domain: str = "",
    captured_at: str = "",
    snapshot_role: str = "company_history_bundle",
    source_providers: Sequence[str] | None = None,
    rows: Sequence[CanonicalHistoryIndexRow],
) -> CanonicalHistoryBundle:
    sorted_rows = sorted(rows, key=lambda item: (item.ts_ms, item.event_id))
    case_ids = {row.case_id for row in sorted_rows if str(row.case_id or "").strip()}
    surface_counts: dict[str, int] = {}
    for row in sorted_rows:
        surface_counts[row.surface] = surface_counts.get(row.surface, 0) + 1
    index = CanonicalHistoryIndex(
        organization_name=organization_name,
        organization_domain=organization_domain,
        captured_at=captured_at,
        snapshot_role=snapshot_role,
        source_providers=sorted(
            {
                str(item).strip().lower()
                for item in (source_providers or [])
                if str(item).strip()
            }
        ),
        event_count=len(sorted_rows),
        case_count=len(case_ids),
        surface_counts=surface_counts,
        rows=list(sorted_rows),
    )
    events = [
        _canonical_event_from_row(
            organization_domain=organization_domain,
            row=row,
        )
        for row in sorted_rows
    ]
    paths = canonical_history_paths(Path.cwd() / "context_snapshot.json")
    return CanonicalHistoryBundle(paths=paths, events=events, index=index)


def write_canonical_history_sidecars(
    snapshot: ContextSnapshot,
    snapshot_path: str | Path,
) -> CanonicalHistoryPaths:
    bundle = build_canonical_history_bundle(snapshot)
    return write_canonical_history_bundle(bundle, snapshot_path)


def write_canonical_history_bundle(
    bundle: CanonicalHistoryBundle,
    snapshot_path: str | Path,
) -> CanonicalHistoryPaths:
    paths = canonical_history_paths(snapshot_path)
    bundle = bundle.model_copy(update={"paths": paths})
    paths.events_path.write_text(
        "\n".join(event.model_dump_json() for event in bundle.events)
        + ("\n" if bundle.events else ""),
        encoding="utf-8",
    )
    paths.index_path.write_text(
        bundle.index.model_dump_json(indent=2),
        encoding="utf-8",
    )
    return paths


def load_canonical_history_bundle(path: str | Path) -> CanonicalHistoryBundle | None:
    paths = canonical_history_paths(path)
    if not paths.events_path.exists() or not paths.index_path.exists():
        return None
    events: list[CanonicalEvent] = []
    raw_events = paths.events_path.read_text(encoding="utf-8").splitlines()
    for line in raw_events:
        stripped = line.strip()
        if not stripped:
            continue
        events.append(CanonicalEvent.model_validate_json(stripped))
    index = CanonicalHistoryIndex.model_validate_json(
        paths.index_path.read_text(encoding="utf-8")
    )
    return CanonicalHistoryBundle(paths=paths, events=events, index=index)


def query_canonical_history(
    path: str | Path,
    *,
    surface: str | None = None,
    actor: str | None = None,
    case_id: str | None = None,
    start: str | None = None,
    end: str | None = None,
    confidence_min: float | None = None,
    limit: int = 50,
) -> CanonicalHistoryTimelineResult:
    bundle = load_canonical_history_bundle(path)
    if bundle is None:
        return CanonicalHistoryTimelineResult()

    start_ms = _timestamp_ms(start, fallback=0)[0] if start else None
    end_ms = _timestamp_ms(end, fallback=0)[0] if end else None
    rows: list[CanonicalHistoryIndexRow] = []
    normalized_surface = str(surface or "").strip().lower()
    normalized_actor = _normalized_actor_id(actor or "")
    normalized_case = str(case_id or "").strip()

    for row in bundle.index.rows:
        if normalized_surface and row.surface != normalized_surface:
            continue
        if normalized_actor and row.actor_id != normalized_actor:
            continue
        if normalized_case and row.case_id != normalized_case:
            continue
        if start_ms is not None and row.ts_ms < start_ms:
            continue
        if end_ms is not None and row.ts_ms > end_ms:
            continue
        if confidence_min is not None and row.stitch_confidence < confidence_min:
            continue
        rows.append(row)

    rows.sort(key=lambda item: (item.ts_ms, item.event_id))
    limited_rows = rows[: max(1, int(limit))]
    return CanonicalHistoryTimelineResult(
        available=True,
        organization_name=bundle.index.organization_name,
        organization_domain=bundle.index.organization_domain,
        source_providers=list(bundle.index.source_providers),
        total_event_count=bundle.index.event_count,
        matching_event_count=len(rows),
        case_count=bundle.index.case_count,
        surface_counts=dict(bundle.index.surface_counts),
        rows=limited_rows,
    )


def build_canonical_history_readiness(
    path: str | Path,
) -> CanonicalHistoryReadinessReport:
    bundle = load_canonical_history_bundle(path)
    if bundle is None:
        return CanonicalHistoryReadinessReport()

    rows = list(bundle.index.rows)
    exact_timestamp_count = sum(1 for row in rows if row.timestamp_quality == "exact")
    stitched_rows = [row for row in rows if row.case_id]
    high_confidence_rows = [
        row for row in stitched_rows if row.stitch_confidence >= 0.7
    ]
    case_counts: dict[str, int] = {}
    for row in stitched_rows:
        case_counts[row.case_id] = case_counts.get(row.case_id, 0) + 1
    top_cases = [
        {"case_id": case_id, "event_count": count}
        for case_id, count in sorted(
            case_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[:5]
    ]
    surface_count = len(bundle.index.surface_counts)
    event_count = bundle.index.event_count
    case_count = bundle.index.case_count

    readiness_label = "empty"
    if event_count >= 500 and surface_count >= 3 and case_count >= 10:
        readiness_label = "rich"
    elif event_count >= 150 and surface_count >= 2 and case_count >= 5:
        readiness_label = "ready"
    elif event_count >= 50 and surface_count >= 2:
        readiness_label = "developing"
    elif event_count > 0:
        readiness_label = "thin"

    notes: list[str] = []
    if event_count < 50:
        notes.append(
            "Capture more dated activity before treating this as a world-model tenant."
        )
    if surface_count < 2:
        notes.append(
            "Add at least one more surface so chronology is not anchored to a single system."
        )
    if high_confidence_rows and case_count < 5:
        notes.append(
            "Case stitching is working, but there are still few recurring cases."
        )
    if exact_timestamp_count < max(10, event_count // 2):
        notes.append(
            "Many events are using derived timestamps, so timeline quality is still partial."
        )
    if not notes:
        notes.append(
            "This tenant has enough dated, cross-surface history for exploratory world-model work."
        )

    ready_for_world_modeling = readiness_label in {"ready", "rich"}
    return CanonicalHistoryReadinessReport(
        available=True,
        organization_name=bundle.index.organization_name,
        organization_domain=bundle.index.organization_domain,
        source_providers=list(bundle.index.source_providers),
        event_count=event_count,
        case_count=case_count,
        surface_count=surface_count,
        exact_timestamp_count=exact_timestamp_count,
        stitched_event_count=len(stitched_rows),
        high_confidence_stitch_count=len(high_confidence_rows),
        surface_counts=dict(bundle.index.surface_counts),
        top_cases=top_cases,
        readiness_label=readiness_label,  # type: ignore[arg-type]
        ready_for_world_modeling=ready_for_world_modeling,
        notes=notes,
    )


def _mail_entries(
    *,
    provider: str,
    payload: dict[str, Any],
    organization_domain: str,
) -> list[_NormalizedHistoryEvent]:
    threads = payload.get("threads") or []
    if not isinstance(threads, list):
        return []
    entries: list[_NormalizedHistoryEvent] = []
    for thread_index, thread in enumerate(threads):
        if not isinstance(thread, dict):
            continue
        thread_id = str(
            thread.get("thread_id")
            or thread.get("id")
            or f"{provider}-thread-{thread_index + 1}"
        ).strip()
        subject = str(thread.get("subject") or "").strip()
        messages = thread.get("messages") or []
        if not isinstance(messages, list):
            continue
        for message_index, message in enumerate(messages):
            if not isinstance(message, dict):
                continue
            sender = _normalized_actor_id(
                message.get("from") or message.get("sender") or message.get("actor")
            )
            recipients = _recipient_ids(
                message.get("to") or message.get("recipients") or []
            )
            cc_recipients = _recipient_ids(message.get("cc") or [])
            all_recipients = _dedupe(recipients + cc_recipients)
            timestamp_ms, timestamp_text, quality = _timestamp_ms(
                message.get("timestamp")
                or message.get("date")
                or message.get("sent_at")
                or "",
                fallback=(thread_index + 1) * 1000 + message_index,
            )
            message_subject = str(message.get("subject") or subject).strip()
            snippet = str(
                message.get("body_text")
                or message.get("snippet")
                or message.get("body")
                or ""
            ).strip()
            message_id = str(
                message.get("message_id") or message.get("id") or ""
            ).strip()
            entries.append(
                _NormalizedHistoryEvent(
                    provider=provider,
                    surface="mail",
                    kind="reply" if message_index else "message",
                    timestamp=timestamp_text,
                    ts_ms=timestamp_ms,
                    timestamp_quality=quality,
                    thread_ref=f"mail:{thread_id}",
                    conversation_anchor=str(message.get("thread_id") or thread_id),
                    actor_id=sender,
                    target_id=all_recipients[0] if all_recipients else "",
                    participant_ids=all_recipients,
                    subject=message_subject,
                    snippet=snippet or message_subject,
                    provider_object_refs=_dedupe(
                        [thread_id, message_id] + [ref for ref in all_recipients if ref]
                    ),
                    internal_external=_internal_external(
                        all_recipients,
                        organization_domain=organization_domain,
                    ),
                    candidate_texts=[
                        thread_id,
                        message_subject,
                        snippet,
                        *all_recipients,
                    ],
                    metadata={
                        "provider_message_id": message_id,
                        "thread_id": thread_id,
                        "cc_recipients": cc_recipients,
                    },
                )
            )
    return entries


def _chat_entries(
    *,
    provider: str,
    payload: dict[str, Any],
    organization_domain: str,
) -> list[_NormalizedHistoryEvent]:
    channels = payload.get("channels") or []
    users = payload.get("users") or []
    if not isinstance(channels, list):
        return []
    user_lookup = _user_lookup(users if isinstance(users, list) else [])
    entries: list[_NormalizedHistoryEvent] = []
    for channel_index, channel in enumerate(channels):
        if not isinstance(channel, dict):
            continue
        channel_name = str(
            channel.get("channel")
            or channel.get("channel_id")
            or f"channel-{channel_index + 1}"
        ).strip()
        messages = channel.get("messages") or []
        if not isinstance(messages, list):
            continue
        for message_index, message in enumerate(messages):
            if not isinstance(message, dict):
                continue
            timestamp_ms, timestamp_text, quality = _timestamp_ms(
                message.get("ts") or message.get("timestamp") or "",
                fallback=(channel_index + 1) * 1000 + message_index,
            )
            raw_thread = str(
                message.get("thread_ts")
                or message.get("thread_id")
                or message.get("conversation_id")
                or message.get("ts")
                or f"{channel_name}:{message_index + 1}"
            ).strip()
            thread_ref = f"slack:{channel_name}:{raw_thread}"
            actor_id = _normalized_actor_id(
                user_lookup.get(str(message.get("user") or "").lower())
                or message.get("user")
                or message.get("author")
            )
            body = str(message.get("text") or message.get("body") or "").strip()
            subject = _summarize_subject(body, fallback=channel_name)
            entries.append(
                _NormalizedHistoryEvent(
                    provider=provider,
                    surface="slack",
                    kind=(
                        "reply"
                        if str(message.get("thread_ts") or "").strip()
                        else "message"
                    ),
                    timestamp=timestamp_text,
                    ts_ms=timestamp_ms,
                    timestamp_quality=quality,
                    thread_ref=thread_ref,
                    conversation_anchor=raw_thread,
                    actor_id=actor_id,
                    target_id=channel_name,
                    participant_ids=[channel_name],
                    subject=subject,
                    snippet=body or subject,
                    provider_object_refs=_dedupe(
                        [channel_name, str(message.get("id") or ""), raw_thread]
                    ),
                    internal_external=InternalExternal.INTERNAL,
                    candidate_texts=[channel_name, subject, body],
                    metadata={"channel": channel_name},
                )
            )
    return entries


def _work_entries(
    *,
    provider: str,
    payload: dict[str, Any],
    organization_domain: str,
) -> list[_NormalizedHistoryEvent]:
    if provider == "jira":
        return _jira_entries(payload=payload, organization_domain=organization_domain)
    if provider == "linear":
        return _linear_entries(payload=payload, organization_domain=organization_domain)
    if provider == "github":
        return _github_entries(payload=payload, organization_domain=organization_domain)
    if provider == "gitlab":
        return _gitlab_entries(payload=payload, organization_domain=organization_domain)
    if provider == "clickup":
        return _clickup_entries(
            payload=payload, organization_domain=organization_domain
        )
    return []


def _jira_entries(
    *,
    payload: dict[str, Any],
    organization_domain: str,
) -> list[_NormalizedHistoryEvent]:
    issues = payload.get("issues") or []
    if not isinstance(issues, list):
        return []
    entries: list[_NormalizedHistoryEvent] = []
    for issue_index, issue in enumerate(issues):
        if not isinstance(issue, dict):
            continue
        ticket_id = str(issue.get("ticket_id") or issue.get("key") or "").strip()
        if not ticket_id:
            continue
        title = str(issue.get("title") or issue.get("summary") or ticket_id).strip()
        assignee = _normalized_actor_id(issue.get("assignee"))
        updated = issue.get("updated") or issue.get("updated_at") or ""
        timestamp_ms, timestamp_text, quality = _timestamp_ms(
            updated,
            fallback=(issue_index + 1) * 1000,
        )
        thread_ref = f"tickets:{ticket_id}"
        description = str(issue.get("description") or "").strip()
        entries.append(
            _NormalizedHistoryEvent(
                provider="jira",
                surface="tickets",
                kind=str(issue.get("status") or "issue_state").strip().lower()
                or "issue_state",
                timestamp=timestamp_text,
                ts_ms=timestamp_ms,
                timestamp_quality=quality,
                thread_ref=thread_ref,
                conversation_anchor=ticket_id,
                actor_id=assignee,
                target_id=ticket_id,
                participant_ids=[ticket_id],
                subject=title,
                snippet=description or title,
                provider_object_refs=[ticket_id],
                internal_external=InternalExternal.INTERNAL,
                candidate_texts=[ticket_id, title, description],
                metadata={"status": str(issue.get("status") or "")},
            )
        )
        comments = issue.get("comments") or []
        if not isinstance(comments, list):
            continue
        for comment_index, comment in enumerate(comments):
            if not isinstance(comment, dict):
                continue
            comment_ts, comment_text, comment_quality = _timestamp_ms(
                comment.get("created") or updated,
                fallback=(issue_index + 1) * 1000 + comment_index + 1,
            )
            body = str(comment.get("body") or "").strip()
            author = _normalized_actor_id(comment.get("author")) or assignee
            entries.append(
                _NormalizedHistoryEvent(
                    provider="jira",
                    surface="tickets",
                    kind="comment",
                    timestamp=comment_text,
                    ts_ms=comment_ts,
                    timestamp_quality=comment_quality,
                    thread_ref=thread_ref,
                    conversation_anchor=ticket_id,
                    actor_id=author,
                    target_id=ticket_id,
                    participant_ids=[ticket_id],
                    subject=title,
                    snippet=body or title,
                    provider_object_refs=_dedupe(
                        [ticket_id, str(comment.get("id") or "")]
                    ),
                    internal_external=InternalExternal.INTERNAL,
                    candidate_texts=[ticket_id, title, body],
                    metadata={"comment_id": str(comment.get("id") or "")},
                )
            )
    return entries


def _linear_entries(
    *,
    payload: dict[str, Any],
    organization_domain: str,
) -> list[_NormalizedHistoryEvent]:
    del organization_domain
    issues = payload.get("issues") or []
    if not isinstance(issues, list):
        return []
    entries: list[_NormalizedHistoryEvent] = []
    for issue_index, issue in enumerate(issues):
        if not isinstance(issue, dict):
            continue
        issue_id = str(
            issue.get("identifier")
            or issue.get("issue_id")
            or issue.get("id")
            or f"linear-{issue_index + 1}"
        ).strip()
        title = str(issue.get("title") or issue_id).strip()
        timestamp_ms, timestamp_text, quality = _timestamp_ms(
            issue.get("updatedAt")
            or issue.get("updated")
            or issue.get("createdAt")
            or "",
            fallback=(issue_index + 1) * 1000,
        )
        actor_id = _normalized_actor_id(
            issue.get("assignee") or issue.get("creator") or issue.get("owner")
        )
        description = str(issue.get("description") or issue.get("body") or "").strip()
        entries.append(
            _NormalizedHistoryEvent(
                provider="linear",
                surface="tickets",
                kind="issue",
                timestamp=timestamp_text,
                ts_ms=timestamp_ms,
                timestamp_quality=quality,
                thread_ref=f"tickets:{issue_id}",
                conversation_anchor=issue_id,
                actor_id=actor_id,
                target_id=issue_id,
                participant_ids=[issue_id],
                subject=title,
                snippet=description or title,
                provider_object_refs=[issue_id],
                internal_external=InternalExternal.INTERNAL,
                candidate_texts=[issue_id, title, description],
                metadata={"state": str(issue.get("state") or "")},
            )
        )
    return entries


def _github_entries(
    *,
    payload: dict[str, Any],
    organization_domain: str,
) -> list[_NormalizedHistoryEvent]:
    del organization_domain
    entries: list[_NormalizedHistoryEvent] = []
    entries.extend(_issue_like_entries(provider="github", payload=payload))
    return entries


def _gitlab_entries(
    *,
    payload: dict[str, Any],
    organization_domain: str,
) -> list[_NormalizedHistoryEvent]:
    del organization_domain
    entries: list[_NormalizedHistoryEvent] = []
    entries.extend(_issue_like_entries(provider="gitlab", payload=payload))
    return entries


def _clickup_entries(
    *,
    payload: dict[str, Any],
    organization_domain: str,
) -> list[_NormalizedHistoryEvent]:
    del organization_domain
    tasks = payload.get("tasks") or []
    if not isinstance(tasks, list):
        return []
    entries: list[_NormalizedHistoryEvent] = []
    for task_index, task in enumerate(tasks):
        if not isinstance(task, dict):
            continue
        task_id = str(task.get("id") or f"clickup-{task_index + 1}").strip()
        title = str(task.get("name") or task.get("title") or task_id).strip()
        body = str(task.get("description") or "").strip()
        actor_id = _normalized_actor_id(
            task.get("creator") or task.get("owner") or task.get("assignee")
        )
        timestamp_ms, timestamp_text, quality = _timestamp_ms(
            task.get("date_updated")
            or task.get("updated_at")
            or task.get("date_created")
            or "",
            fallback=(task_index + 1) * 1000,
        )
        entries.append(
            _NormalizedHistoryEvent(
                provider="clickup",
                surface="tickets",
                kind="task",
                timestamp=timestamp_text,
                ts_ms=timestamp_ms,
                timestamp_quality=quality,
                thread_ref=f"tickets:{task_id}",
                conversation_anchor=task_id,
                actor_id=actor_id,
                target_id=task_id,
                participant_ids=[task_id],
                subject=title,
                snippet=body or title,
                provider_object_refs=[task_id],
                internal_external=InternalExternal.INTERNAL,
                candidate_texts=[task_id, title, body],
                metadata={"status": str(task.get("status") or "")},
            )
        )
        comments = task.get("comments") or []
        if not isinstance(comments, list):
            continue
        for comment_index, comment in enumerate(comments):
            if not isinstance(comment, dict):
                continue
            comment_ts, comment_text, comment_quality = _timestamp_ms(
                comment.get("date") or comment.get("created_at") or "",
                fallback=(task_index + 1) * 1000 + comment_index + 1,
            )
            body_text = str(
                comment.get("comment_text") or comment.get("text") or ""
            ).strip()
            author = (
                _normalized_actor_id(comment.get("user") or comment.get("author"))
                or actor_id
            )
            entries.append(
                _NormalizedHistoryEvent(
                    provider="clickup",
                    surface="tickets",
                    kind="comment",
                    timestamp=comment_text,
                    ts_ms=comment_ts,
                    timestamp_quality=comment_quality,
                    thread_ref=f"tickets:{task_id}",
                    conversation_anchor=task_id,
                    actor_id=author,
                    target_id=task_id,
                    participant_ids=[task_id],
                    subject=title,
                    snippet=body_text or title,
                    provider_object_refs=_dedupe(
                        [task_id, str(comment.get("id") or "")]
                    ),
                    internal_external=InternalExternal.INTERNAL,
                    candidate_texts=[task_id, title, body_text],
                    metadata={"comment_id": str(comment.get("id") or "")},
                )
            )
    return entries


def _issue_like_entries(
    *,
    provider: str,
    payload: dict[str, Any],
) -> list[_NormalizedHistoryEvent]:
    items: list[dict[str, Any]] = []
    for key in ("issues", "pull_requests", "merge_requests"):
        value = payload.get(key) or []
        if isinstance(value, list):
            items.extend(item for item in value if isinstance(item, dict))
    entries: list[_NormalizedHistoryEvent] = []
    for item_index, item in enumerate(items):
        issue_id = str(
            item.get("number")
            or item.get("iid")
            or item.get("id")
            or f"{provider}-{item_index + 1}"
        ).strip()
        title = str(item.get("title") or issue_id).strip()
        actor_id = _normalized_actor_id(
            item.get("author")
            or item.get("creator")
            or item.get("user")
            or item.get("assignee")
        )
        body = str(item.get("body") or item.get("description") or "").strip()
        timestamp_ms, timestamp_text, quality = _timestamp_ms(
            item.get("updated_at")
            or item.get("updatedAt")
            or item.get("created_at")
            or item.get("createdAt")
            or "",
            fallback=(item_index + 1) * 1000,
        )
        entries.append(
            _NormalizedHistoryEvent(
                provider=provider,
                surface="tickets",
                kind=(
                    "merge_request" if item.get("merge_status") is not None else "issue"
                ),
                timestamp=timestamp_text,
                ts_ms=timestamp_ms,
                timestamp_quality=quality,
                thread_ref=f"tickets:{provider}:{issue_id}",
                conversation_anchor=issue_id,
                actor_id=actor_id,
                target_id=issue_id,
                participant_ids=[issue_id],
                subject=title,
                snippet=body or title,
                provider_object_refs=[issue_id],
                internal_external=InternalExternal.INTERNAL,
                candidate_texts=[issue_id, title, body],
                metadata={"state": str(item.get("state") or "")},
            )
        )
        comments = item.get("comments") or item.get("notes") or []
        if not isinstance(comments, list):
            continue
        for comment_index, comment in enumerate(comments):
            if not isinstance(comment, dict):
                continue
            comment_ts, comment_text, comment_quality = _timestamp_ms(
                comment.get("created_at") or comment.get("createdAt") or "",
                fallback=(item_index + 1) * 1000 + comment_index + 1,
            )
            body_text = str(comment.get("body") or comment.get("text") or "").strip()
            author = (
                _normalized_actor_id(comment.get("author") or comment.get("user"))
                or actor_id
            )
            entries.append(
                _NormalizedHistoryEvent(
                    provider=provider,
                    surface="tickets",
                    kind="comment",
                    timestamp=comment_text,
                    ts_ms=comment_ts,
                    timestamp_quality=comment_quality,
                    thread_ref=f"tickets:{provider}:{issue_id}",
                    conversation_anchor=issue_id,
                    actor_id=author,
                    target_id=issue_id,
                    participant_ids=[issue_id],
                    subject=title,
                    snippet=body_text or title,
                    provider_object_refs=_dedupe(
                        [issue_id, str(comment.get("id") or "")]
                    ),
                    internal_external=InternalExternal.INTERNAL,
                    candidate_texts=[issue_id, title, body_text],
                    metadata={"comment_id": str(comment.get("id") or "")},
                )
            )
    return entries


def _doc_entries(
    *,
    provider: str,
    payload: dict[str, Any],
    organization_domain: str,
) -> list[_NormalizedHistoryEvent]:
    if provider == "granola":
        return _granola_entries(payload=payload)
    items: list[dict[str, Any]] = []
    for key in ("documents", "pages", "blocks"):
        value = payload.get(key) or []
        if isinstance(value, list):
            items.extend(item for item in value if isinstance(item, dict))
    entries: list[_NormalizedHistoryEvent] = []
    for item_index, item in enumerate(items):
        doc_id = str(
            item.get("doc_id")
            or item.get("page_id")
            or item.get("id")
            or f"{provider}-doc-{item_index + 1}"
        ).strip()
        title = str(item.get("title") or doc_id).strip()
        body = str(item.get("body") or item.get("summary") or "").strip()
        actor_id = _normalized_actor_id(item.get("owner") or item.get("author"))
        timestamp_ms, timestamp_text, quality = _timestamp_ms(
            item.get("updated")
            or item.get("updated_at")
            or item.get("modified_time")
            or item.get("created")
            or item.get("created_at")
            or "",
            fallback=(item_index + 1) * 1000,
        )
        entries.append(
            _NormalizedHistoryEvent(
                provider=provider,
                surface="docs",
                kind="document",
                timestamp=timestamp_text,
                ts_ms=timestamp_ms,
                timestamp_quality=quality,
                thread_ref=f"docs:{doc_id}",
                conversation_anchor=doc_id,
                actor_id=actor_id,
                target_id=doc_id,
                participant_ids=[doc_id],
                subject=title,
                snippet=body or title,
                provider_object_refs=[doc_id],
                internal_external=InternalExternal.INTERNAL,
                candidate_texts=[doc_id, title, body],
                metadata={"mime_type": str(item.get("mime_type") or "")},
            )
        )
        comments = item.get("comments") or []
        if isinstance(comments, list):
            for comment_index, comment in enumerate(comments):
                if not isinstance(comment, dict):
                    continue
                comment_ts, comment_text, comment_quality = _timestamp_ms(
                    comment.get("created")
                    or comment.get("created_at")
                    or comment.get("updated_at")
                    or "",
                    fallback=(item_index + 1) * 1000 + comment_index + 1,
                )
                comment_body = str(
                    comment.get("body") or comment.get("text") or ""
                ).strip()
                comment_author = (
                    _normalized_actor_id(comment.get("author") or comment.get("user"))
                    or actor_id
                )
                comment_id = str(
                    comment.get("id") or f"{doc_id}-comment-{comment_index + 1}"
                ).strip()
                entries.append(
                    _NormalizedHistoryEvent(
                        provider=provider,
                        surface="docs",
                        kind="comment",
                        timestamp=comment_text,
                        ts_ms=comment_ts,
                        timestamp_quality=comment_quality,
                        thread_ref=f"docs:{doc_id}",
                        conversation_anchor=doc_id,
                        actor_id=comment_author,
                        target_id=doc_id,
                        participant_ids=[doc_id],
                        subject=title,
                        snippet=comment_body or title,
                        provider_object_refs=_dedupe([doc_id, comment_id]),
                        internal_external=InternalExternal.INTERNAL,
                        candidate_texts=[doc_id, title, comment_body],
                        metadata={"comment_id": comment_id},
                    )
                )
        permissions = item.get("permissions") or []
        if isinstance(permissions, list):
            for permission_index, permission in enumerate(permissions):
                if not isinstance(permission, dict):
                    continue
                permission_ts, permission_text, permission_quality = _timestamp_ms(
                    permission.get("created")
                    or permission.get("created_at")
                    or permission.get("granted_at")
                    or "",
                    fallback=(item_index + 1) * 1000 + 200 + permission_index,
                )
                shared_with = _recipient_ids(permission.get("shared_with"))
                granted_by = (
                    _normalized_actor_id(permission.get("granted_by")) or actor_id
                )
                permission_id = str(
                    permission.get("id")
                    or f"{doc_id}-permission-{permission_index + 1}"
                ).strip()
                snippet_parts = [title]
                if shared_with:
                    snippet_parts.append("shared with " + ", ".join(shared_with))
                entries.append(
                    _NormalizedHistoryEvent(
                        provider=provider,
                        surface="docs",
                        kind="share",
                        timestamp=permission_text,
                        ts_ms=permission_ts,
                        timestamp_quality=permission_quality,
                        thread_ref=f"docs:{doc_id}",
                        conversation_anchor=doc_id,
                        actor_id=granted_by,
                        target_id=doc_id,
                        participant_ids=_dedupe([doc_id, *shared_with]),
                        subject=title,
                        snippet=" ".join(snippet_parts).strip(),
                        provider_object_refs=_dedupe([doc_id, permission_id]),
                        internal_external=_internal_external(
                            shared_with or [doc_id],
                            organization_domain=organization_domain,
                        ),
                        candidate_texts=[doc_id, title, *shared_with],
                        metadata={"permission_id": permission_id},
                    )
                )
    drive_shares = payload.get("drive_shares") or []
    if isinstance(drive_shares, list):
        for share_index, share in enumerate(drive_shares):
            if not isinstance(share, dict):
                continue
            doc_id = str(
                share.get("doc_id") or f"{provider}-share-{share_index + 1}"
            ).strip()
            shared_with = _recipient_ids(share.get("shared_with"))
            share_ts, share_text, share_quality = _timestamp_ms(
                share.get("created") or share.get("created_at") or "",
                fallback=900_000 + share_index,
            )
            entries.append(
                _NormalizedHistoryEvent(
                    provider=provider,
                    surface="docs",
                    kind="share",
                    timestamp=share_text,
                    ts_ms=share_ts,
                    timestamp_quality=share_quality,
                    thread_ref=f"docs:{doc_id}",
                    conversation_anchor=doc_id,
                    actor_id=_normalized_actor_id(share.get("granted_by")),
                    target_id=doc_id,
                    participant_ids=_dedupe([doc_id, *shared_with]),
                    subject=doc_id,
                    snippet=(
                        "shared with " + ", ".join(shared_with)
                        if shared_with
                        else doc_id
                    ),
                    provider_object_refs=[doc_id],
                    internal_external=_internal_external(
                        shared_with or [doc_id],
                        organization_domain=organization_domain,
                    ),
                    candidate_texts=[doc_id, *shared_with],
                    metadata={"share_source": "drive_shares"},
                )
            )
    return entries


def _granola_entries(*, payload: dict[str, Any]) -> list[_NormalizedHistoryEvent]:
    transcripts = payload.get("transcripts") or []
    if not isinstance(transcripts, list):
        return []
    entries: list[_NormalizedHistoryEvent] = []
    for transcript_index, transcript in enumerate(transcripts):
        if not isinstance(transcript, dict):
            continue
        transcript_id = str(
            transcript.get("id") or f"granola-{transcript_index + 1}"
        ).strip()
        title = str(transcript.get("title") or transcript_id).strip()
        body = str(transcript.get("body") or transcript.get("transcript") or "").strip()
        actor_id = _normalized_actor_id(
            transcript.get("owner") or transcript.get("speaker")
        )
        timestamp_ms, timestamp_text, quality = _timestamp_ms(
            transcript.get("updated_at")
            or transcript.get("created_at")
            or transcript.get("timestamp")
            or "",
            fallback=(transcript_index + 1) * 1000,
        )
        entries.append(
            _NormalizedHistoryEvent(
                provider="granola",
                surface="docs",
                kind="meeting_note",
                timestamp=timestamp_text,
                ts_ms=timestamp_ms,
                timestamp_quality=quality,
                thread_ref=f"docs:{transcript_id}",
                conversation_anchor=transcript_id,
                actor_id=actor_id,
                target_id=transcript_id,
                participant_ids=[transcript_id],
                subject=title,
                snippet=body or title,
                provider_object_refs=[transcript_id],
                internal_external=InternalExternal.INTERNAL,
                candidate_texts=[transcript_id, title, body],
                metadata={},
            )
        )
    return entries


def _crm_entries(
    *,
    provider: str,
    payload: dict[str, Any],
    organization_domain: str,
) -> list[_NormalizedHistoryEvent]:
    del organization_domain
    entries: list[_NormalizedHistoryEvent] = []
    companies = payload.get("companies") or []
    if isinstance(companies, list):
        for company_index, company in enumerate(companies):
            if not isinstance(company, dict):
                continue
            company_id = str(
                company.get("id") or f"company-{company_index + 1}"
            ).strip()
            company_name = str(company.get("name") or company_id).strip()
            timestamp_ms, timestamp_text, quality = _timestamp_ms(
                company.get("updated_ms")
                or company.get("updated_at")
                or company.get("created_ms")
                or company.get("created_at")
                or "",
                fallback=(company_index + 1) * 1000,
            )
            entries.append(
                _NormalizedHistoryEvent(
                    provider=provider,
                    surface="crm",
                    kind="account",
                    timestamp=timestamp_text,
                    ts_ms=timestamp_ms,
                    timestamp_quality=quality,
                    thread_ref=f"crm:{company_id}",
                    conversation_anchor=company_id,
                    actor_id="",
                    target_id=company_id,
                    participant_ids=[company_id],
                    subject=company_name,
                    snippet=company_name,
                    provider_object_refs=[company_id],
                    internal_external=InternalExternal.EXTERNAL,
                    candidate_texts=[company_id, company_name],
                    metadata={},
                )
            )
    contacts = payload.get("contacts") or []
    if isinstance(contacts, list):
        for contact_index, contact in enumerate(contacts):
            if not isinstance(contact, dict):
                continue
            contact_id = str(
                contact.get("id") or f"contact-{contact_index + 1}"
            ).strip()
            email = _normalized_actor_id(contact.get("email"))
            display_name = " ".join(
                part
                for part in [
                    str(contact.get("first_name") or "").strip(),
                    str(contact.get("last_name") or "").strip(),
                ]
                if part
            ).strip()
            timestamp_ms, timestamp_text, quality = _timestamp_ms(
                contact.get("updated_ms")
                or contact.get("updated_at")
                or contact.get("created_ms")
                or contact.get("created_at")
                or "",
                fallback=(contact_index + 1) * 1000 + 200,
            )
            company_id = str(contact.get("company_id") or "").strip()
            entries.append(
                _NormalizedHistoryEvent(
                    provider=provider,
                    surface="crm",
                    kind="contact",
                    timestamp=timestamp_text,
                    ts_ms=timestamp_ms,
                    timestamp_quality=quality,
                    thread_ref=f"crm:{company_id or contact_id}",
                    conversation_anchor=contact_id,
                    actor_id=email,
                    target_id=company_id or contact_id,
                    participant_ids=_dedupe([contact_id, company_id, email]),
                    subject=display_name or email or contact_id,
                    snippet=email or display_name or contact_id,
                    provider_object_refs=_dedupe([contact_id, company_id]),
                    internal_external=InternalExternal.EXTERNAL,
                    candidate_texts=[contact_id, company_id, email, display_name],
                    metadata={},
                )
            )
    deals = payload.get("deals") or []
    if not isinstance(deals, list):
        return entries
    for deal_index, deal in enumerate(deals):
        if not isinstance(deal, dict):
            continue
        deal_id = str(deal.get("id") or f"deal-{deal_index + 1}").strip()
        name = str(deal.get("name") or deal_id).strip()
        owner = _normalized_actor_id(deal.get("owner"))
        amount = str(deal.get("amount") or "").strip()
        stage = str(deal.get("stage") or "").strip()
        stage_text = stage.replace("_", " ").strip().capitalize()
        timestamp_ms, timestamp_text, quality = _timestamp_ms(
            deal.get("updated_ms")
            or deal.get("updated_at")
            or deal.get("created_ms")
            or deal.get("created_at")
            or "",
            fallback=(deal_index + 1) * 1000,
        )
        snippet = (
            " ".join(part for part in [stage_text, amount] if part).strip() or name
        )
        entries.append(
            _NormalizedHistoryEvent(
                provider=provider,
                surface="crm",
                kind="deal",
                timestamp=timestamp_text,
                ts_ms=timestamp_ms,
                timestamp_quality=quality,
                thread_ref=f"crm:{deal_id}",
                conversation_anchor=deal_id,
                actor_id=owner,
                target_id=deal_id,
                participant_ids=[deal_id],
                subject=name,
                snippet=snippet,
                provider_object_refs=[deal_id],
                internal_external=InternalExternal.EXTERNAL,
                candidate_texts=[deal_id, name, stage_text],
                metadata={"amount": amount, "stage": stage_text},
            )
        )
        history_items = deal.get("history") or []
        if not isinstance(history_items, list):
            continue
        for history_index, history_item in enumerate(history_items):
            if not isinstance(history_item, dict):
                continue
            history_ts, history_text, history_quality = _timestamp_ms(
                history_item.get("timestamp")
                or history_item.get("updated_at")
                or history_item.get("created_at")
                or "",
                fallback=(deal_index + 1) * 1000 + history_index + 1,
            )
            field_name = str(history_item.get("field") or "field").strip()
            field_from = str(history_item.get("from") or "").strip()
            field_to = str(history_item.get("to") or "").strip()
            history_actor = (
                _normalized_actor_id(history_item.get("changed_by")) or owner
            )
            history_id = str(
                history_item.get("id") or f"{deal_id}-history-{history_index + 1}"
            ).strip()
            entries.append(
                _NormalizedHistoryEvent(
                    provider=provider,
                    surface="crm",
                    kind="deal_change",
                    timestamp=history_text,
                    ts_ms=history_ts,
                    timestamp_quality=history_quality,
                    thread_ref=f"crm:{deal_id}",
                    conversation_anchor=deal_id,
                    actor_id=history_actor,
                    target_id=deal_id,
                    participant_ids=[deal_id],
                    subject=name,
                    snippet=" ".join(
                        part for part in [field_name, field_from, field_to] if part
                    ).strip()
                    or name,
                    provider_object_refs=_dedupe([deal_id, history_id]),
                    internal_external=InternalExternal.EXTERNAL,
                    candidate_texts=[deal_id, name, field_name, field_from, field_to],
                    metadata={"history_id": history_id, "field": field_name},
                )
            )
    return entries


def _thread_primary_tokens(
    entries: Sequence[_NormalizedHistoryEvent],
) -> dict[str, str]:
    counts_by_thread: dict[str, dict[str, int]] = {}
    for entry in entries:
        tokens = _anchor_tokens(entry.candidate_texts)
        if not tokens or not entry.thread_ref:
            continue
        bucket = counts_by_thread.setdefault(entry.thread_ref, {})
        for token in tokens:
            bucket[token] = bucket.get(token, 0) + 1
    resolved: dict[str, str] = {}
    for thread_ref, counts in counts_by_thread.items():
        sorted_counts = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        if sorted_counts:
            resolved[thread_ref] = sorted_counts[0][0]
    return resolved


def _case_link_for_entry(
    entry: _NormalizedHistoryEvent,
    *,
    thread_tokens: dict[str, str],
) -> tuple[str, float, str]:
    explicit_tokens = _anchor_tokens(entry.candidate_texts)
    if explicit_tokens:
        return f"case:{explicit_tokens[0]}", 0.98, "explicit_token"
    thread_token = thread_tokens.get(entry.thread_ref, "")
    if thread_token:
        return f"case:{thread_token}", 0.72, "thread_token"
    if entry.thread_ref:
        return f"thread:{entry.thread_ref}", 0.4, "thread_ref"
    return "", 0.0, ""


def _index_row_from_entry(
    entry: _NormalizedHistoryEvent,
    *,
    case_id: str,
    stitch_confidence: float,
    stitch_basis: str,
) -> CanonicalHistoryIndexRow:
    event_id = _stable_event_id(
        entry.provider,
        entry.thread_ref,
        str(entry.ts_ms),
        entry.actor_id,
        entry.subject,
        entry.snippet,
    )
    return CanonicalHistoryIndexRow(
        event_id=event_id,
        timestamp=entry.timestamp,
        ts_ms=entry.ts_ms,
        timestamp_quality=entry.timestamp_quality,
        surface=entry.surface,
        provider=entry.provider,
        kind=entry.kind,
        domain=_domain_for_surface(entry.surface).value,
        case_id=case_id,
        thread_ref=entry.thread_ref,
        conversation_anchor=entry.conversation_anchor,
        actor_id=entry.actor_id,
        target_id=entry.target_id,
        participant_ids=list(entry.participant_ids),
        subject=entry.subject,
        normalized_subject=_normalize_subject(entry.subject),
        snippet=entry.snippet,
        search_terms=_search_terms(entry),
        provider_object_refs=list(entry.provider_object_refs),
        stitch_confidence=round(stitch_confidence, 3),
        stitch_basis=stitch_basis,
        internal_external=entry.internal_external.value,
        metadata=dict(entry.metadata),
    )


def _canonical_event_from_row(
    *,
    organization_domain: str,
    row: CanonicalHistoryIndexRow,
) -> CanonicalEvent:
    participants = [
        ActorRef(
            actor_id=participant,
            display_name=participant,
            tenant_id=organization_domain,
        )
        for participant in row.participant_ids
        if participant
    ]
    object_refs = [
        ObjectRef(
            object_id=ref,
            domain=row.domain,
            kind=row.surface,
            label=row.subject or ref,
        )
        for ref in row.provider_object_refs
        if ref
    ]
    text_value = row.snippet or row.subject
    return CanonicalEvent(
        event_id=row.event_id,
        tenant_id=organization_domain,
        case_id=row.case_id or None,
        ts_ms=row.ts_ms,
        domain=_domain_for_surface(row.surface),
        kind=f"{row.provider}.{row.kind}",
        actor_ref=(
            ActorRef(
                actor_id=row.actor_id,
                display_name=row.actor_id,
                tenant_id=organization_domain,
            )
            if row.actor_id
            else None
        ),
        participants=participants,
        object_refs=object_refs,
        internal_external=InternalExternal(row.internal_external),
        provenance=ProvenanceRecord(
            origin=EventProvenance.IMPORTED,
            source_id=row.provider,
        ),
        text_handle=(
            TextHandle.from_text(text_value, store_uri=f"history://{row.event_id}")
            if text_value
            else None
        ),
        delta=StateDelta(
            domain=_domain_for_surface(row.surface),
            delta_schema_version=0,
            data={
                "surface": row.surface,
                "thread_ref": row.thread_ref,
                "conversation_anchor": row.conversation_anchor,
                "subject": row.subject,
                "snippet": row.snippet,
                **dict(row.metadata),
            },
        ),
    ).with_hash()


def _domain_for_surface(surface: str) -> EventDomain:
    if surface == "mail" or surface == "slack":
        return EventDomain.COMM_GRAPH
    if surface == "tickets":
        return EventDomain.WORK_GRAPH
    if surface == "docs":
        return EventDomain.DOC_GRAPH
    if surface == "crm":
        return EventDomain.REVENUE_GRAPH
    if surface == "governance":
        return EventDomain.GOVERNANCE
    if surface == "market":
        return EventDomain.OBS_GRAPH
    return EventDomain.INTERNAL


def _timestamp_ms(
    value: Any,
    *,
    fallback: int,
) -> tuple[int, str, TimestampQuality]:
    text = str(value or "").strip()
    if not text:
        return fallback, _timestamp_text_from_ms(fallback), "derived"
    try:
        if text.isdigit():
            raw = int(text)
            if raw > 10_000_000_000:
                return raw, _timestamp_text_from_ms(raw), "exact"
            return raw * 1000, _timestamp_text_from_ms(raw * 1000), "exact"
        normalized = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return (
            int(dt.timestamp() * 1000),
            dt.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "exact",
        )
    except ValueError:
        try:
            dt = parsedate_to_datetime(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return (
                int(dt.timestamp() * 1000),
                dt.astimezone(UTC).isoformat().replace("+00:00", "Z"),
                "exact",
            )
        except (TypeError, ValueError):
            return fallback, _timestamp_text_from_ms(fallback), "derived"


def _timestamp_text_from_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, UTC).isoformat().replace("+00:00", "Z")


def _normalized_actor_id(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    email = parseaddr(text)[1].strip().lower()
    if email:
        return email
    normalized = _NON_ALNUM_PATTERN.sub(".", text.lower()).strip(".")
    return normalized


def _recipient_ids(value: Any) -> list[str]:
    if isinstance(value, list):
        raw_parts = [str(item or "").strip() for item in value]
    else:
        raw_parts = re.split(r"[,;]", str(value or ""))
    recipients: list[str] = []
    for raw in raw_parts:
        text = raw.strip()
        if not text:
            continue
        email = parseaddr(text)[1].strip().lower()
        recipients.append(email or _normalized_actor_id(text))
    return _dedupe(recipients)


def _internal_external(
    participant_ids: Sequence[str],
    *,
    organization_domain: str,
) -> InternalExternal:
    domain = str(organization_domain or "").strip().lower()
    if not participant_ids or not domain:
        return InternalExternal.UNKNOWN
    saw_internal = False
    saw_external = False
    for participant in participant_ids:
        lower = str(participant or "").strip().lower()
        if "@" not in lower:
            saw_internal = True
            continue
        if lower.endswith(f"@{domain}"):
            saw_internal = True
        else:
            saw_external = True
    if saw_external:
        return InternalExternal.EXTERNAL
    if saw_internal:
        return InternalExternal.INTERNAL
    return InternalExternal.UNKNOWN


def _user_lookup(users: Sequence[dict[str, Any]]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for user in users:
        if not isinstance(user, dict):
            continue
        canonical = _normalized_actor_id(
            user.get("email") or user.get("name") or user.get("real_name")
        )
        if not canonical:
            continue
        for key in (
            user.get("id"),
            user.get("name"),
            user.get("real_name"),
            user.get("email"),
        ):
            text = str(key or "").strip().lower()
            if text:
                lookup[text] = canonical
    return lookup


def _summarize_subject(text: str, *, fallback: str) -> str:
    stripped = str(text or "").strip()
    if not stripped:
        return fallback
    first_line = stripped.splitlines()[0].strip()
    if len(first_line) <= 80:
        return first_line
    return first_line[:77].rstrip() + "..."


def _anchor_tokens(values: Sequence[str]) -> list[str]:
    joined = " ".join(str(value or "") for value in values if value)
    if not joined:
        return []
    tokens = [
        *[item.upper() for item in _CASE_TOKEN_PATTERN.findall(joined)],
        *[item.upper() for item in _DOC_TOKEN_PATTERN.findall(joined)],
        *[item.upper() for item in _DEAL_TOKEN_PATTERN.findall(joined)],
    ]
    filtered = [token for token in tokens if token not in _IGNORED_ANCHOR_TOKENS]
    return _dedupe(filtered)


def _normalize_subject(value: str) -> str:
    return " ".join(str(value or "").lower().split())


def _search_terms(entry: _NormalizedHistoryEvent) -> list[str]:
    values = [
        entry.subject,
        entry.snippet,
        *entry.provider_object_refs,
        *entry.participant_ids,
        entry.actor_id,
    ]
    tokens = _anchor_tokens(values)
    for value in values:
        normalized = _normalize_subject(str(value or ""))
        if not normalized:
            continue
        for part in normalized.split():
            cleaned = part.strip()
            if len(cleaned) < 3:
                continue
            tokens.append(cleaned)
    return _dedupe(tokens)


def _dedupe(values: Sequence[str]) -> list[str]:
    deduped: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in deduped:
            continue
        deduped.append(text)
    return deduped


def _stable_event_id(*parts: str) -> str:
    payload = "|".join(str(part or "") for part in parts)
    return f"history_{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]}"


__all__ = [
    "CanonicalHistoryBundle",
    "CanonicalHistoryIndex",
    "CanonicalHistoryIndexRow",
    "CanonicalHistoryPaths",
    "CanonicalHistoryReadinessReport",
    "CanonicalHistoryTimelineResult",
    "build_canonical_history_readiness",
    "build_canonical_history_bundle",
    "canonical_history_paths",
    "canonical_history_sidecars_exist",
    "load_canonical_history_bundle",
    "query_canonical_history",
    "write_canonical_history_sidecars",
]
