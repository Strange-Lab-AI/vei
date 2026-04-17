from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from vei.whatif_filenames import CONTEXT_SNAPSHOT_FILE

from .._helpers import load_episode_context as _load_episode_context
from ..models import WhatIfEpisodeManifest, WhatIfEventReference

logger = logging.getLogger(__name__)


def _history_preview_from_saved_context(
    workspace_root: Path,
    *,
    manifest: WhatIfEpisodeManifest,
    history_limit: int,
) -> list[WhatIfEventReference]:
    if manifest.history_preview:
        return list(manifest.history_preview[-max(1, history_limit) :])
    try:
        context = _load_episode_context(workspace_root)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        logger.warning(
            "whatif saved episode context load failed for %s (%s)",
            manifest.thread_id,
            type(exc).__name__,
            extra={
                "source": "episode",
                "provider": "context_snapshot",
                "file_path": str(workspace_root / CONTEXT_SNAPSHOT_FILE),
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return []

    for thread in context.get("threads", []):
        if (
            not isinstance(thread, dict)
            or thread.get("thread_id") != manifest.thread_id
        ):
            continue
        messages = thread.get("messages", [])
        if not isinstance(messages, list):
            return []
        historical_messages = [
            message
            for message in messages
            if isinstance(message, dict)
            and str(message.get("event_id") or "").strip() != manifest.branch_event_id
        ]
        preview_messages = historical_messages[-max(1, history_limit) :]
        return [
            WhatIfEventReference(
                event_id=_message_event_id(
                    message,
                    manifest=manifest,
                    index=index,
                ),
                timestamp=_message_timestamp_text(message),
                actor_id=str(message.get("from", "")),
                target_id=str(message.get("to", "")),
                event_type="history",
                thread_id=manifest.thread_id,
                subject=str(message.get("subject", manifest.thread_subject)),
                snippet=str(message.get("body_text", ""))[:600],
                to_recipients=[str(message.get("to", ""))] if message.get("to") else [],
            )
            for index, message in enumerate(preview_messages, start=1)
            if isinstance(message, dict)
        ]
    return []


def _message_event_id(
    message: dict[str, object],
    *,
    manifest: WhatIfEpisodeManifest,
    index: int,
) -> str:
    for key in ("event_id", "message_id", "id"):
        value = str(message.get(key, "") or "").strip()
        if value:
            return value
    return f"{manifest.thread_id}:history:{index}"


def _message_timestamp_text(message: dict[str, object]) -> str:
    timestamp = str(message.get("timestamp", "") or "").strip()
    if timestamp:
        return timestamp
    raw_time_ms = message.get("time_ms")
    if isinstance(raw_time_ms, (int, float)):
        return (
            datetime.fromtimestamp(float(raw_time_ms) / 1000.0, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
    return ""


__all__ = ["_history_preview_from_saved_context"]
