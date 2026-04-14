from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Sequence

from pydantic import ValidationError

from vei.context.api import ContextSnapshot

from .._helpers import (
    historical_archive_address as _historical_archive_address,
    primary_recipient as _primary_recipient,
)
from ..corpus import CONTENT_NOTICE, _load_history_snapshot
from ..models import (
    WhatIfCaseContext,
    WhatIfEvent,
    WhatIfPublicContext,
    WhatIfSituationContext,
    WhatIfWorld,
)
from ._dataset import _historical_body, _historical_chat_text

logger = logging.getLogger(__name__)


def _episode_snapshot_metadata(
    *,
    world: WhatIfWorld,
    thread_id: str,
    branch_event: WhatIfEvent,
    public_context: WhatIfPublicContext | None,
    case_context: WhatIfCaseContext | None,
    situation_context: WhatIfSituationContext | None,
    historical_business_state: Any,
) -> dict[str, Any]:
    return {
        "snapshot_role": "workspace_seed",
        "whatif": {
            "source": world.source,
            "thread_id": thread_id,
            "case_id": branch_event.case_id,
            "branch_event_id": branch_event.event_id,
            "branch_surface": branch_event.surface,
            "content_notice": str(world.metadata.get("content_notice", CONTENT_NOTICE)),
            "public_context": (
                public_context.model_dump(mode="json")
                if public_context is not None
                else None
            ),
            "case_context": (
                case_context.model_dump(mode="json")
                if case_context is not None
                else None
            ),
            "situation_context": (
                situation_context.model_dump(mode="json")
                if situation_context is not None
                else None
            ),
            "historical_business_state": (
                historical_business_state.model_dump(mode="json")
                if historical_business_state is not None
                else None
            ),
        },
    }


def _thread_actor_payload(
    world: WhatIfWorld,
    *,
    thread_history: Sequence[WhatIfEvent],
) -> list[dict[str, str]]:
    actor_ids = {
        actor_id
        for event in thread_history
        for actor_id in {event.actor_id, event.target_id}
        if actor_id
    }
    return [
        {
            "actor_id": actor.actor_id,
            "email": actor.email,
            "display_name": actor.display_name,
        }
        for actor in world.actors
        if actor.actor_id in actor_ids
    ]


def _source_snapshot_for_world(world: WhatIfWorld) -> ContextSnapshot | None:
    if world.source not in {"mail_archive", "company_history"}:
        return None
    try:
        return _load_history_snapshot(world.source_dir)
    except (OSError, json.JSONDecodeError, ValueError, ValidationError) as exc:
        logger.warning(
            "whatif source snapshot load failed for %s (%s)",
            world.source,
            type(exc).__name__,
            extra={
                "source": "episode",
                "provider": world.source,
                "file_path": str(world.source_dir),
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return None


def _persist_workspace_historical_source(
    world: WhatIfWorld,
    workspace_root: Path,
) -> None:
    del world, workspace_root
    return


def _historical_source_file(source_dir: Path) -> Path | None:
    resolved = source_dir.expanduser().resolve()
    if resolved.is_file():
        return resolved
    for filename in ("context_snapshot.json",):
        candidate = resolved / filename
        if candidate.exists():
            return candidate
    return None


def _archive_message_payload(
    event: WhatIfEvent,
    *,
    base_time_ms: int,
    organization_domain: str,
) -> dict[str, Any]:
    recipient = _primary_recipient(event)
    return {
        "from": event.actor_id
        or _historical_archive_address(organization_domain, "unknown"),
        "to": recipient,
        "subject": event.subject or event.thread_id,
        "body_text": _historical_body(event),
        "unread": False,
        "time_ms": base_time_ms,
    }


def _chat_message_ts(event: WhatIfEvent, *, fallback_index: int) -> str:
    if event.timestamp_ms > 0:
        return str(event.timestamp_ms)
    return str(max(1, fallback_index))


def _ticket_status_for_event(event: WhatIfEvent) -> str:
    if event.event_type == "approval":
        return "resolved"
    if event.event_type == "escalation":
        return "blocked"
    if event.event_type == "assignment":
        return "in_progress"
    return "open"


__all__ = [
    "_archive_message_payload",
    "_chat_message_ts",
    "_episode_snapshot_metadata",
    "_historical_body",
    "_historical_chat_text",
    "_historical_source_file",
    "_persist_workspace_historical_source",
    "_source_snapshot_for_world",
    "_thread_actor_payload",
    "_ticket_status_for_event",
]
