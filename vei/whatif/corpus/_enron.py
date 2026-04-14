from __future__ import annotations

import json
import logging
from collections import Counter
from pathlib import Path
from typing import Any, Sequence

from ..cases import assign_case_ids, build_case_summaries
from ..models import (
    WhatIfArtifactFlags,
    WhatIfEvent,
    WhatIfScenario,
    WhatIfWorld,
    WhatIfWorldSummary,
)
from ..public_context import empty_enron_public_context, resolve_world_public_context
from ..situations import build_situation_graph
from ._aggregation import (
    build_actor_profiles,
    build_thread_summaries,
    matches_custodian_filter,
)
from ._time import safe_int, string_list, timestamp_to_ms, timestamp_to_text

logger = logging.getLogger(__name__)

ENRON_DOMAIN = "enron.com"
CONTENT_NOTICE = (
    "Historical email bodies are built from Rosetta excerpts and event metadata. "
    "They are grounded, but they are not full original messages."
)
EXECUTIVE_MARKERS = ("skilling", "lay", "fastow", "kean")


def load_enron_world(
    *,
    rosetta_dir: str | Path,
    scenarios: Sequence[WhatIfScenario] | None = None,
    time_window: tuple[str, str] | None = None,
    custodian_filter: Sequence[str] | None = None,
    max_events: int | None = None,
    include_content: bool = False,
) -> WhatIfWorld:
    from ._time import resolve_time_window

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - guarded by dependency
        raise RuntimeError(
            "pyarrow is required for `vei whatif` parquet loading"
        ) from exc

    base = Path(rosetta_dir).expanduser().resolve()
    metadata_path = base / "enron_rosetta_events_metadata.parquet"
    content_path = base / "enron_rosetta_events_content.parquet"
    if not metadata_path.exists():
        raise ValueError(f"metadata parquet not found: {metadata_path}")
    if not content_path.exists():
        raise ValueError(f"content parquet not found: {content_path}")

    metadata_rows = pq.read_table(
        metadata_path,
        columns=[
            "event_id",
            "timestamp",
            "actor_id",
            "target_id",
            "event_type",
            "thread_task_id",
            "artifacts",
        ],
    ).to_pylist()
    content_by_id = (
        load_content_by_event_ids(
            rosetta_dir=base,
            event_ids=[
                str(row.get("event_id", ""))
                for row in metadata_rows
                if str(row.get("event_id", "")).strip()
            ],
        )
        if include_content
        else {}
    )

    time_bounds = resolve_time_window(time_window)
    custodian_tokens = {item.strip().lower() for item in custodian_filter or [] if item}
    events: list[WhatIfEvent] = []
    for row in metadata_rows:
        event = build_event(row, content_by_id.get(str(row.get("event_id", "")), ""))
        if event is None:
            continue
        if time_bounds is not None and not (
            time_bounds[0] <= event.timestamp_ms <= time_bounds[1]
        ):
            continue
        if custodian_tokens and not matches_custodian_filter(event, custodian_tokens):
            continue
        events.append(event)

    events.sort(key=lambda item: (item.timestamp_ms, item.event_id))
    events = assign_case_ids(events)
    if max_events is not None:
        events = events[: max(0, int(max_events))]

    threads = build_thread_summaries(events, organization_domain=ENRON_DOMAIN)
    actors = build_actor_profiles(events, organization_domain=ENRON_DOMAIN)
    cases = build_case_summaries(events)
    situation_graph = build_situation_graph(
        threads=threads,
        cases=cases,
        events=events,
    )
    summary = WhatIfWorldSummary(
        source="enron",
        organization_name="Enron Corporation",
        organization_domain=ENRON_DOMAIN,
        event_count=len(events),
        thread_count=len(threads),
        actor_count=len(actors),
        custodian_count=len(
            {
                custodian
                for actor in actors
                for custodian in actor.custodian_ids
                if custodian
            }
        ),
        first_timestamp=events[0].timestamp if events else "",
        last_timestamp=events[-1].timestamp if events else "",
        event_type_counts=dict(Counter(event.event_type for event in events)),
        key_actor_ids=[actor.actor_id for actor in actors[:5]],
    )
    public_context = (
        resolve_world_public_context(
            source="enron",
            source_dir=base,
            organization_name=summary.organization_name,
            organization_domain=summary.organization_domain,
            window_start=summary.first_timestamp,
            window_end=summary.last_timestamp,
        )
        if events
        else empty_enron_public_context()
    )
    return WhatIfWorld(
        source="enron",
        source_dir=base,
        summary=summary,
        scenarios=list(scenarios or []),
        actors=actors,
        threads=threads,
        cases=cases,
        events=events,
        situation_graph=situation_graph,
        metadata={"content_notice": CONTENT_NOTICE},
        public_context=public_context,
    )


def load_content_by_event_ids(
    *,
    rosetta_dir: str | Path,
    event_ids: Sequence[str],
) -> dict[str, str]:
    unique_event_ids = sorted(
        {str(item).strip() for item in event_ids if str(item).strip()}
    )
    if not unique_event_ids:
        return {}
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - guarded by dependency
        raise RuntimeError(
            "pyarrow is required for `vei whatif` parquet loading"
        ) from exc

    content_path = (
        Path(rosetta_dir).expanduser().resolve()
        / "enron_rosetta_events_content.parquet"
    )
    if not content_path.exists():
        return {}
    content_rows = pq.read_table(
        content_path,
        columns=["event_id", "content"],
        filters=[("event_id", "in", unique_event_ids)],
    ).to_pylist()
    return {
        str(row.get("event_id", "")): str(row.get("content", "") or "")
        for row in content_rows
        if str(row.get("event_id", "")).strip()
    }


def hydrate_event_snippets(
    *,
    rosetta_dir: str | Path,
    events: Sequence[WhatIfEvent],
) -> list[WhatIfEvent]:
    missing_ids = [event.event_id for event in events if not event.snippet]
    if not missing_ids:
        return list(events)
    content_by_id = load_content_by_event_ids(
        rosetta_dir=rosetta_dir,
        event_ids=missing_ids,
    )
    hydrated: list[WhatIfEvent] = []
    for event in events:
        snippet = content_by_id.get(event.event_id, event.snippet)
        hydrated.append(event.model_copy(update={"snippet": snippet}))
    return hydrated


def build_event(row: dict[str, Any], content: str) -> WhatIfEvent | None:
    event_id = str(row.get("event_id", "")).strip()
    if not event_id:
        return None
    timestamp = row.get("timestamp")
    timestamp_ms_val = timestamp_to_ms(timestamp)
    timestamp_text = timestamp_to_text(timestamp)
    artifacts = artifact_flags(row.get("artifacts"))
    thread_id = str(row.get("thread_task_id", "") or event_id)
    subject = artifacts.subject or artifacts.norm_subject or thread_id
    return WhatIfEvent(
        event_id=event_id,
        timestamp=timestamp_text,
        timestamp_ms=timestamp_ms_val,
        actor_id=str(row.get("actor_id", "") or ""),
        target_id=str(row.get("target_id", "") or ""),
        event_type=str(row.get("event_type", "") or ""),
        thread_id=thread_id,
        surface="mail",
        subject=subject,
        snippet=str(content or ""),
        flags=artifacts,
    )


def artifact_flags(raw: Any) -> WhatIfArtifactFlags:
    if isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {}
    elif isinstance(raw, dict):
        payload = dict(raw)
    else:
        payload = {}

    return WhatIfArtifactFlags(
        consult_legal_specialist=bool(payload.get("consult_legal_specialist", False)),
        consult_trading_specialist=bool(
            payload.get("consult_trading_specialist", False)
        ),
        has_attachment_reference=bool(payload.get("has_attachment_reference", False)),
        is_escalation=bool(payload.get("is_escalation", False)),
        is_forward=bool(payload.get("is_forward", False)),
        is_reply=bool(payload.get("is_reply", False)),
        cc_count=safe_int(payload.get("cc_count")),
        bcc_count=safe_int(payload.get("bcc_count")),
        to_count=safe_int(payload.get("to_count")),
        to_recipients=string_list(payload.get("to_recipients")),
        cc_recipients=string_list(payload.get("cc_recipients")),
        subject=str(payload.get("subject", "") or ""),
        norm_subject=str(payload.get("norm_subject", "") or ""),
        body_sha1=str(payload.get("body_sha1", "") or ""),
        custodian_id=str(payload.get("custodian_id", "") or ""),
        message_id=str(payload.get("message_id", "") or ""),
        folder=str(payload.get("folder", "") or ""),
        source=str(payload.get("source", "") or ""),
    )


def touches_executive(event: WhatIfEvent) -> bool:
    haystack = " ".join(
        [
            event.actor_id.lower(),
            event.target_id.lower(),
            " ".join(value.lower() for value in event.flags.to_recipients),
            " ".join(value.lower() for value in event.flags.cc_recipients),
        ]
    )
    return any(marker in haystack for marker in EXECUTIVE_MARKERS)
