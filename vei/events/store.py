"""Small canonical event sink/store boundary."""

from __future__ import annotations

import json
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable, Protocol, runtime_checkable

from .models import CanonicalEvent
from .paths import canonical_event_paths, load_canonical_events_jsonl


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
        self._known_event_ids: set[str] | None = None

    def append(self, event: CanonicalEvent) -> CanonicalEvent:
        hashed = event.with_hash()
        existing = self._event_id_index()
        if hashed.event_id in existing:
            return hashed
        with self.events_path.open("a", encoding="utf-8") as fh:
            fh.write(hashed.model_dump_json() + "\n")
        existing.add(hashed.event_id)
        self._write_manifest()
        return hashed

    def append_many(self, events: Iterable[CanonicalEvent]) -> list[CanonicalEvent]:
        appended: list[CanonicalEvent] = []
        existing = self._event_id_index()
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
        for path in canonical_event_paths(self.workspace):
            events.extend(load_canonical_events_jsonl(path))
        return events

    def get(self, event_id: str) -> CanonicalEvent | None:
        for event in self.query():
            if event.event_id == event_id:
                return event
        return None

    def _event_id_index(self) -> set[str]:
        if self._known_event_ids is None:
            self._known_event_ids = {item.event_id for item in self.query()}
        return self._known_event_ids

    def _write_manifest(self) -> None:
        events = (
            load_canonical_events_jsonl(self.events_path)
            if self.events_path.exists()
            else []
        )
        ts_values = [event.ts_ms for event in events if event.ts_ms]
        source_hashes = [event.hash for event in events]
        previous_batch_hash = _previous_batch_hash(self.batch_dir)
        batch_hash = _stable_hash(source_hashes)
        payload = {
            "source": self.source,
            "workspace": str(self.workspace),
            "batch_id": self.batch_id,
            "event_count": len(events),
            "batch_event_count": len(events),
            "first_ts_ms": min(ts_values) if ts_values else 0,
            "last_ts_ms": max(ts_values) if ts_values else 0,
            "source_hashes": source_hashes,
            "batch_hash": batch_hash,
            "previous_batch_hash": previous_batch_hash,
            "created_at": datetime.now(UTC).isoformat(),
        }
        payload["manifest_hash"] = _stable_hash(payload)
        self.manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _stable_hash(payload: object) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _previous_batch_hash(batch_dir: Path) -> str:
    source_dir = batch_dir.parent
    manifests = sorted(source_dir.glob("*/manifest.json"))
    previous: list[Path] = [
        path for path in manifests if path.parent != batch_dir and path.exists()
    ]
    if not previous:
        return ""
    try:
        payload = json.loads(previous[-1].read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return ""
    return str(payload.get("batch_hash") or payload.get("manifest_hash") or "")
