from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import WhatIfEvent, WhatIfEventReference


def chat_channel_name_from_reference(
    event: WhatIfEventReference,
    *,
    default: str = "#history",
) -> str:
    recipients = [item for item in event.to_recipients if item]
    if recipients:
        return recipients[0]
    if event.target_id:
        return event.target_id
    return default


def chat_channel_name(
    event: WhatIfEvent,
    *,
    default: str = "#history",
) -> str:
    recipients = [item for item in event.flags.to_recipients if item]
    if recipients:
        return recipients[0]
    if event.target_id:
        return event.target_id
    return default


def primary_recipient(
    event: WhatIfEvent,
    *,
    default: str | None = None,
) -> str:
    recipients = [item for item in event.flags.to_recipients if item]
    if recipients:
        return recipients[0]
    if event.target_id:
        return event.target_id
    if default is not None:
        return default
    return historical_archive_address("", "archive")


def reference_primary_recipient(
    event: WhatIfEventReference,
    *,
    default: str = "",
) -> str:
    recipients = [item for item in event.to_recipients if item]
    if recipients:
        return recipients[0]
    if event.target_id:
        return event.target_id
    return default


def historical_archive_address(organization_domain: str, local_part: str) -> str:
    normalized_domain = organization_domain.strip().lower()
    if not normalized_domain:
        return f"{local_part}@archive.local"
    return f"{local_part}@{normalized_domain}"


def load_episode_snapshot(root: Path) -> dict[str, Any]:
    snapshot_path = root / "context_snapshot.json"
    if not snapshot_path.exists():
        raise ValueError(f"context snapshot not found: {snapshot_path}")
    return json.loads(snapshot_path.read_text(encoding="utf-8"))


def load_episode_context(root: Path) -> dict[str, Any]:
    payload = load_episode_snapshot(root)
    sources = payload.get("sources", [])
    for source in sources:
        if not isinstance(source, dict):
            continue
        data = source.get("data", {})
        if isinstance(data, dict):
            return data
    raise ValueError("what-if episode is missing a supported context source")
