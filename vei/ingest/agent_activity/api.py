"""Public API for capture-first agent-activity ingest."""

from __future__ import annotations

import json
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Protocol, runtime_checkable

from pydantic import BaseModel, Field

from vei.events.api import CanonicalEvent


class RawAgentActivity(BaseModel):
    source: str
    source_record_id: str
    ts_ms: int = 0
    kind: str = ""
    actor_id: str = ""
    actor_display_name: str = ""
    tool_name: str = ""
    provider: str = ""
    model: str = ""
    status: str = ""
    source_granularity: str = "per_call"
    payload: dict[str, Any] = Field(default_factory=dict)


class AgentActivityManifest(BaseModel):
    source: str
    workspace: str
    batch_id: str
    source_granularity: str = ""
    event_count: int = 0
    skipped_duplicate_count: int = 0
    first_ts_ms: int = 0
    last_ts_ms: int = 0
    source_hashes: list[str] = Field(default_factory=list)
    batch_event_count: int = 0
    batch_hash: str = ""
    previous_batch_hash: str = ""
    manifest_hash: str = ""
    cursor: str = ""
    created_at: str = ""


class AgentActivityIngestResult(BaseModel):
    source: str
    workspace: str
    event_count: int = 0
    skipped_duplicate_count: int = 0
    manifest_path: str = ""
    events_path: str = ""
    source_granularity: str = ""


@runtime_checkable
class AgentActivityAdapter(Protocol):
    source_name: str

    def fetch(self, window: str = "") -> Iterable[RawAgentActivity]: ...

    def to_canonical_events(
        self, raw: RawAgentActivity
    ) -> Iterable[CanonicalEvent]: ...


def _agent_activity_root(workspace: str | Path) -> Path:
    return Path(workspace).expanduser().resolve() / "provenance" / "agent_activity"


def _canonical_event_paths(workspace: str | Path) -> list[Path]:
    root = Path(workspace).expanduser().resolve()
    paths: list[Path] = []
    direct = root / "canonical_events.jsonl"
    if direct.exists():
        paths.append(direct)
    for path in sorted((root / "workspace").glob("canonical_events.jsonl")):
        paths.append(path)
    activity_root = _agent_activity_root(root)
    if activity_root.exists():
        paths.extend(sorted(activity_root.glob("*/*/canonical_events.jsonl")))
    return paths


def load_workspace_canonical_events(workspace: str | Path) -> list[CanonicalEvent]:
    events: list[CanonicalEvent] = []
    for path in _canonical_event_paths(workspace):
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(CanonicalEvent.model_validate_json(line))
                except ValueError:
                    payload = json.loads(line)
                    events.append(CanonicalEvent.model_validate(payload))
    return events


def append_events_to_workspace(
    workspace: str | Path,
    *,
    source: str,
    events: Iterable[CanonicalEvent],
    cursor: str = "",
    source_granularity: str = "",
) -> AgentActivityIngestResult:
    workspace_path = Path(workspace).expanduser().resolve()
    batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    batch_dir = _agent_activity_root(workspace_path) / source / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    events_path = batch_dir / "canonical_events.jsonl"
    manifest_path = batch_dir / "manifest.json"

    existing_ids = {
        event.event_id for event in load_workspace_canonical_events(workspace_path)
    }
    event_list: list[CanonicalEvent] = []
    skipped = 0
    for event in events:
        hashed = event.with_hash()
        if hashed.event_id in existing_ids:
            skipped += 1
            continue
        existing_ids.add(hashed.event_id)
        event_list.append(hashed)

    with events_path.open("w", encoding="utf-8") as fh:
        for event in event_list:
            fh.write(event.model_dump_json() + "\n")

    ts_values = [event.ts_ms for event in event_list if event.ts_ms]
    source_hashes = [event.hash for event in event_list]
    batch_hash = _stable_hash(source_hashes)
    manifest = AgentActivityManifest(
        source=source,
        workspace=str(workspace_path),
        batch_id=batch_id,
        source_granularity=source_granularity,
        event_count=len(event_list),
        skipped_duplicate_count=skipped,
        first_ts_ms=min(ts_values) if ts_values else 0,
        last_ts_ms=max(ts_values) if ts_values else 0,
        source_hashes=source_hashes,
        batch_event_count=len(event_list),
        batch_hash=batch_hash,
        previous_batch_hash=_previous_batch_hash(batch_dir),
        cursor=cursor,
        created_at=datetime.now(UTC).isoformat(),
    )
    manifest.manifest_hash = _stable_hash(
        manifest.model_dump(mode="json", exclude={"manifest_hash"})
    )
    manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    return AgentActivityIngestResult(
        source=source,
        workspace=str(workspace_path),
        event_count=len(event_list),
        skipped_duplicate_count=skipped,
        manifest_path=str(manifest_path),
        events_path=str(events_path),
        source_granularity=source_granularity,
    )


def ingest_agent_activity(
    *,
    adapter: AgentActivityAdapter,
    workspace: str | Path,
    window: str = "",
) -> AgentActivityIngestResult:
    events: list[CanonicalEvent] = []
    granularities: set[str] = set()
    cursor = ""
    for raw in adapter.fetch(window):
        cursor = raw.source_record_id
        granularities.add(raw.source_granularity)
        events.extend(adapter.to_canonical_events(raw))
    granularity = next(iter(granularities)) if len(granularities) == 1 else "mixed"
    return append_events_to_workspace(
        workspace,
        source=adapter.source_name,
        events=events,
        cursor=cursor,
        source_granularity=granularity,
    )


def ingest_status(workspace: str | Path) -> dict[str, Any]:
    root = _agent_activity_root(workspace)
    sources: list[dict[str, Any]] = []
    if root.exists():
        for manifest_path in sorted(root.glob("*/*/manifest.json")):
            try:
                sources.append(json.loads(manifest_path.read_text(encoding="utf-8")))
            except json.JSONDecodeError:
                sources.append(
                    {"manifest_path": str(manifest_path), "error": "invalid_json"}
                )
    return {
        "workspace": str(Path(workspace).expanduser().resolve()),
        "batch_count": len(sources),
        "event_count": sum(int(item.get("event_count", 0)) for item in sources),
        "sources": sources,
    }


def _stable_hash(payload: object) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _previous_batch_hash(batch_dir: Path) -> str:
    source_dir = batch_dir.parent
    manifests = sorted(source_dir.glob("*/manifest.json"))
    previous = [path for path in manifests if path.parent != batch_dir]
    if not previous:
        return ""
    try:
        payload = json.loads(previous[-1].read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return ""
    return str(payload.get("batch_hash") or payload.get("manifest_hash") or "")
