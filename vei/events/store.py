"""Small canonical event sink/store boundary."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable, Protocol, runtime_checkable

from .models import CanonicalEvent


@runtime_checkable
class CanonicalEventSink(Protocol):
    def append(self, event: CanonicalEvent) -> CanonicalEvent: ...

    def append_many(self, events: Iterable[CanonicalEvent]) -> list[CanonicalEvent]: ...


@runtime_checkable
class CanonicalEventStore(CanonicalEventSink, Protocol):
    def query(self) -> list[CanonicalEvent]: ...

    def get(self, event_id: str) -> CanonicalEvent | None: ...


class WorkspaceEventStore:
    """Append-only JSONL event store using the existing provenance layout."""

    def __init__(
        self,
        workspace: str | Path,
        *,
        source: str = "runtime",
        batch_id: str | None = None,
    ) -> None:
        self.workspace = Path(workspace).expanduser().resolve()
        self.source = source
        self.batch_id = batch_id or "runtime"
        self.batch_dir = (
            self.workspace / "provenance" / "agent_activity" / source / self.batch_id
        )
        self.events_path = self.batch_dir / "canonical_events.jsonl"
        self.manifest_path = self.batch_dir / "manifest.json"
        self.batch_dir.mkdir(parents=True, exist_ok=True)

    def append(self, event: CanonicalEvent) -> CanonicalEvent:
        hashed = event.with_hash()
        existing = {item.event_id for item in self.query()}
        if hashed.event_id in existing:
            return hashed
        with self.events_path.open("a", encoding="utf-8") as fh:
            fh.write(hashed.model_dump_json() + "\n")
        self._write_manifest()
        return hashed

    def append_many(self, events: Iterable[CanonicalEvent]) -> list[CanonicalEvent]:
        appended: list[CanonicalEvent] = []
        existing = {item.event_id for item in self.query()}
        with self.events_path.open("a", encoding="utf-8") as fh:
            for event in events:
                hashed = event.with_hash()
                if hashed.event_id in existing:
                    continue
                existing.add(hashed.event_id)
                appended.append(hashed)
                fh.write(hashed.model_dump_json() + "\n")
        self._write_manifest()
        return appended

    def query(self) -> list[CanonicalEvent]:
        events: list[CanonicalEvent] = []
        for path in _canonical_event_paths(self.workspace):
            events.extend(_read_events(path))
        return events

    def get(self, event_id: str) -> CanonicalEvent | None:
        for event in self.query():
            if event.event_id == event_id:
                return event
        return None

    def _write_manifest(self) -> None:
        events = _read_events(self.events_path) if self.events_path.exists() else []
        ts_values = [event.ts_ms for event in events if event.ts_ms]
        payload = {
            "source": self.source,
            "workspace": str(self.workspace),
            "batch_id": self.batch_id,
            "event_count": len(events),
            "first_ts_ms": min(ts_values) if ts_values else 0,
            "last_ts_ms": max(ts_values) if ts_values else 0,
            "source_hashes": [event.hash for event in events],
            "created_at": datetime.now(UTC).isoformat(),
        }
        self.manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _read_events(path: Path) -> list[CanonicalEvent]:
    events: list[CanonicalEvent] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(CanonicalEvent.model_validate_json(line))
            except ValueError:
                events.append(CanonicalEvent.model_validate(json.loads(line)))
    return events


def _canonical_event_paths(workspace: str | Path) -> list[Path]:
    root = Path(workspace).expanduser().resolve()
    paths: list[Path] = []
    direct = root / "canonical_events.jsonl"
    if direct.exists():
        paths.append(direct)
    workspace_direct = root / "workspace" / "canonical_events.jsonl"
    if workspace_direct.exists():
        paths.append(workspace_direct)
    activity_root = root / "provenance" / "agent_activity"
    if activity_root.exists():
        paths.extend(sorted(activity_root.glob("*/*/canonical_events.jsonl")))
    return paths
