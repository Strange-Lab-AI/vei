"""Canonical timeline path discovery helpers.

Keeps WorkspaceEventStore / ingest / provenance readers aligned on JSONL lookups.
"""

from __future__ import annotations

import json
from pathlib import Path

from vei.events.models import CanonicalEvent


def canonical_event_paths(workspace: str | Path) -> list[Path]:
    """Enumerate JSONL timelines under a workspace root (dedupe by resolve())."""

    root = Path(workspace).expanduser().resolve()
    paths: list[Path] = []
    seen_resolved: set[Path] = set()

    def append_if_file(candidate: Path) -> None:
        resolved = candidate.resolve()
        if not candidate.is_file() or resolved in seen_resolved:
            return
        paths.append(candidate)
        seen_resolved.add(resolved)

    append_if_file(root / "canonical_events.jsonl")
    workspace_root = root / "workspace"
    append_if_file(workspace_root / "canonical_events.jsonl")
    if workspace_root.is_dir():
        for path in sorted(workspace_root.glob("**/canonical_events.jsonl")):
            append_if_file(path)

    agent_root = root / "provenance" / "agent_activity"
    if agent_root.exists():
        for path in sorted(agent_root.glob("*/*/canonical_events.jsonl")):
            append_if_file(path)

    return paths


def load_canonical_events_jsonl(path: Path) -> list[CanonicalEvent]:
    """Load ``CanonicalEvent`` entries from ``path`` preserving order."""

    events: list[CanonicalEvent] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                events.append(CanonicalEvent.model_validate_json(stripped))
            except ValueError:
                events.append(
                    CanonicalEvent.model_validate(json.loads(stripped)),
                )
    return events
