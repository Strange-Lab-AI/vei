from __future__ import annotations

import json
import math
from typing import Optional

from vei.blueprint.models import (
    BlueprintKnowledgeAsset,
    BlueprintKnowledgeGraphAsset,
    BlueprintKnowledgeProvenanceAsset,
)
from vei.knowledge.api import iso_from_ms, normalize_asset_id, parse_iso_to_ms

from .models import (
    ContextSnapshot,
    GmailSourceData,
    GoogleSourceData,
    GranolaSourceData,
    JiraSourceData,
    LinearSourceData,
    MailArchiveSourceData,
    NotionSourceData,
    SlackSourceData,
)

_MIN_REASONABLE_TIME_MS = 946_684_800_000


def _coerce_time_ms(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        text = str(value).strip()
        if not text:
            return 0
        try:
            number = float(text)
        except ValueError:
            return parse_iso_to_ms(text)
    if math.isnan(number) or number <= 0:
        return 0
    if number < 10_000_000_000:
        number *= 1000
    time_ms = int(number)
    if time_ms < _MIN_REASONABLE_TIME_MS:
        return 0
    return time_ms


def _pick_time_ms(*values: object) -> int:
    for value in values:
        time_ms = _coerce_time_ms(value)
        if time_ms > 0:
            return time_ms
    return 0


def build_knowledge_graph(
    snapshot: ContextSnapshot,
    *,
    slack_data: SlackSourceData | None,
    gmail_data: GmailSourceData | None,
    mail_archive_data: MailArchiveSourceData | None,
    google_data: GoogleSourceData | None,
    jira_data: JiraSourceData | None,
    notion_data: NotionSourceData | None,
    linear_data: LinearSourceData | None,
    granola_data: GranolaSourceData | None,
) -> Optional[BlueprintKnowledgeGraphAsset]:
    assets: list[BlueprintKnowledgeAsset] = []
    captured_at_ms = parse_iso_to_ms(snapshot.captured_at)

    def _append_asset(
        *,
        kind: str,
        source: str,
        source_id: str,
        title: str,
        body: str,
        summary: str,
        tags: list[str],
        linked_object_refs: list[str],
        shelf_life_ms: int | None,
        metrics: dict[str, float | int | str] | None = None,
        source_time_ms: int = 0,
    ) -> None:
        asset_time_ms = source_time_ms or captured_at_ms
        asset_id = normalize_asset_id(f"{source}-{source_id or title}")
        assets.append(
            BlueprintKnowledgeAsset(
                asset_id=asset_id,
                kind=kind,
                title=title,
                body=body,
                summary=summary,
                tags=tags,
                provenance=BlueprintKnowledgeProvenanceAsset(
                    source=source,
                    source_id=source_id,
                    captured_at=iso_from_ms(asset_time_ms) or snapshot.captured_at,
                    shelf_life_ms=shelf_life_ms,
                    metadata={"captured_at_ms": asset_time_ms},
                ),
                linked_object_refs=linked_object_refs,
                metrics=dict(metrics or {}),
                metadata={"captured_at_ms": asset_time_ms},
            )
        )

    if google_data is not None:
        for item in google_data.documents:
            if not isinstance(item, dict):
                continue
            _append_asset(
                kind="deliverable",
                source="google",
                source_id=str(item.get("doc_id", "")),
                title=str(item.get("title", "Document")),
                body=str(item.get("body", "")),
                summary=str(item.get("body", ""))[:180],
                tags=[str(item.get("mime_type", "document"))],
                linked_object_refs=[],
                shelf_life_ms=30 * 86_400_000,
                source_time_ms=_pick_time_ms(
                    item.get("modified_time"),
                    item.get("updated_at"),
                    item.get("created_at"),
                ),
            )

    if slack_data is not None:
        for channel in slack_data.channels:
            if not isinstance(channel, dict):
                continue
            messages = [
                item
                for item in (channel.get("messages") or [])
                if isinstance(item, dict)
            ]
            if not messages:
                continue
            body = "\n".join(str(item.get("text", "")) for item in messages[-6:])
            _append_asset(
                kind="meeting_notes",
                source="slack",
                source_id=str(channel.get("channel_id") or channel.get("channel", "")),
                title=f"{channel.get('channel', 'slack')} summary",
                body=body,
                summary=body[:180],
                tags=["slack", "meeting_notes"],
                linked_object_refs=[],
                shelf_life_ms=7 * 86_400_000,
                source_time_ms=_pick_time_ms(
                    *(message.get("ts") for message in messages[::-1]),
                    channel.get("updated_at"),
                    channel.get("captured_at"),
                ),
            )

    for source_name, source_data, shelf_life_ms in (
        ("gmail", gmail_data, 14 * 86_400_000),
        ("mail_archive", mail_archive_data, 30 * 86_400_000),
    ):
        if source_data is None:
            continue
        for thread in source_data.threads:
            if not isinstance(thread, dict):
                continue
            messages = [
                item
                for item in (thread.get("messages") or [])
                if isinstance(item, dict)
            ]
            if not messages:
                continue
            body = "\n".join(
                str(
                    item.get(
                        "body_text",
                        item.get("snippet", item.get("content", "")),
                    )
                )
                for item in messages[-4:]
            )
            _append_asset(
                kind="email_summary",
                source=source_name,
                source_id=str(thread.get("thread_id", "")),
                title=str(thread.get("subject", thread.get("title", "Mail thread"))),
                body=body,
                summary=body[:180],
                tags=[str(thread.get("category", "mail"))],
                linked_object_refs=[],
                shelf_life_ms=shelf_life_ms,
                source_time_ms=_pick_time_ms(
                    *(
                        message.get("time_ms")
                        or message.get("internal_date")
                        or message.get("timestamp_ms")
                        or message.get("date")
                        for message in messages[::-1]
                    ),
                    thread.get("updated_at"),
                    thread.get("captured_at"),
                ),
            )

    if jira_data is not None and jira_data.issues:
        closed = sum(
            1
            for issue in jira_data.issues
            if isinstance(issue, dict)
            and str(issue.get("status", "")).lower() in {"done", "closed", "resolved"}
        )
        _append_asset(
            kind="metric_snapshot",
            source="jira",
            source_id="issue-rollup",
            title="Jira issue rollup",
            body="Issue rollup generated from the imported Jira snapshot.",
            summary=f"{len(jira_data.issues)} issues captured, {closed} closed or resolved.",
            tags=["jira", "metrics"],
            linked_object_refs=[],
            shelf_life_ms=14 * 86_400_000,
            metrics={
                "issue_count": len(jira_data.issues),
                "closed_issue_count": closed,
            },
            source_time_ms=_pick_time_ms(
                *(
                    issue.get("updated_at")
                    or issue.get("resolved_at")
                    or issue.get("created_at")
                    for issue in jira_data.issues
                    if isinstance(issue, dict)
                )
            ),
        )

    if notion_data is not None:
        for page in notion_data.pages:
            if not isinstance(page, dict):
                continue
            _append_asset(
                kind="deliverable",
                source="notion",
                source_id=str(page.get("page_id", "")),
                title=str(page.get("title", "Notion page")),
                body=str(page.get("body", "")),
                summary=str(page.get("body", ""))[:180],
                tags=[str(tag) for tag in (page.get("tags") or [])],
                linked_object_refs=[
                    str(ref) for ref in (page.get("linked_object_refs") or [])
                ],
                shelf_life_ms=21 * 86_400_000,
                source_time_ms=_pick_time_ms(
                    page.get("last_edited_time"),
                    page.get("updated_at"),
                    page.get("created_time"),
                    page.get("created_at"),
                ),
            )

    if linear_data is not None:
        for cycle in linear_data.cycles:
            if not isinstance(cycle, dict):
                continue
            metrics: dict[str, float | int | str] = {
                "scope_completed": int(cycle.get("scope_completed", 0) or 0),
                "scope_planned": int(cycle.get("scope_planned", 0) or 0),
            }
            _append_asset(
                kind="metric_snapshot",
                source="linear",
                source_id=str(cycle.get("id", cycle.get("name", ""))),
                title=str(cycle.get("name", "Linear cycle")),
                body=json.dumps(cycle, indent=2, sort_keys=True),
                summary=str(
                    cycle.get("summary") or cycle.get("name") or "Linear cycle"
                ),
                tags=["linear", "metrics"],
                linked_object_refs=[
                    str(ref) for ref in (cycle.get("linked_object_refs") or [])
                ],
                shelf_life_ms=14 * 86_400_000,
                metrics=metrics,
                source_time_ms=_pick_time_ms(
                    cycle.get("endedAt"),
                    cycle.get("updatedAt"),
                    cycle.get("createdAt"),
                ),
            )

    if granola_data is not None:
        for transcript in granola_data.transcripts:
            if not isinstance(transcript, dict):
                continue
            body = str(transcript.get("body", transcript.get("transcript", "")))
            _append_asset(
                kind="transcript",
                source="granola",
                source_id=str(
                    transcript.get("transcript_id", transcript.get("title", ""))
                ),
                title=str(transcript.get("title", "Transcript")),
                body=body,
                summary=body[:180],
                tags=["granola", "transcript"],
                linked_object_refs=[
                    str(ref) for ref in (transcript.get("linked_object_refs") or [])
                ],
                shelf_life_ms=30 * 86_400_000,
                source_time_ms=_pick_time_ms(
                    transcript.get("timestamp"),
                    transcript.get("recorded_at"),
                    transcript.get("updated_at"),
                    transcript.get("created_at"),
                    transcript.get("date"),
                ),
            )

    if not assets:
        return None
    return BlueprintKnowledgeGraphAsset(
        assets=assets,
        metadata={
            "captured_at": snapshot.captured_at,
            "reference_now_ms": captured_at_ms,
            "source_provider": "context_capture",
        },
    )
