"""Generic NDJSON landing-zone adapter for agent activity."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from vei.events.api import (
    ActorRef,
    CanonicalEvent,
    EventProvenance,
    build_llm_call_event,
    build_llm_usage_observed,
    build_tool_call_event,
    stable_event_id,
)

from .api import RawAgentActivity


class AgentActivityJsonlAdapter:
    source_name = "agent_activity_jsonl"

    def __init__(self, path: str | Path, *, tenant_id: str = "") -> None:
        self.path = Path(path).expanduser().resolve()
        self.tenant_id = tenant_id

    def fetch(self, window: str = "") -> Iterable[RawAgentActivity]:
        paths = sorted(self.path.glob("*.jsonl")) if self.path.is_dir() else [self.path]
        for path in paths:
            with path.open("r", encoding="utf-8") as fh:
                for idx, line in enumerate(fh, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    payload = json.loads(line)
                    source_record_id = str(
                        payload.get("source_record_id")
                        or payload.get("id")
                        or f"{path.name}:{idx}"
                    )
                    yield RawAgentActivity(
                        source=self.source_name,
                        source_record_id=source_record_id,
                        ts_ms=int(payload.get("ts_ms", payload.get("time_ms", 0)) or 0),
                        kind=str(payload.get("kind", payload.get("type", ""))),
                        actor_id=str(
                            payload.get("actor_id", payload.get("user_id", ""))
                        ),
                        actor_display_name=str(payload.get("actor_display_name", "")),
                        tool_name=str(
                            payload.get("tool_name", payload.get("tool", ""))
                        ),
                        provider=str(payload.get("provider", "")),
                        model=str(payload.get("model", "")),
                        status=str(payload.get("status", "")),
                        source_granularity=str(
                            payload.get("source_granularity", "per_call")
                        ),
                        payload=payload,
                    )

    def to_canonical_events(self, raw: RawAgentActivity) -> Iterable[CanonicalEvent]:
        payload = dict(raw.payload)
        actor = (
            ActorRef(
                actor_id=raw.actor_id,
                display_name=raw.actor_display_name,
                tenant_id=self.tenant_id,
            )
            if raw.actor_id
            else None
        )
        source_id = f"{self.source_name}:{raw.source_record_id}"
        if raw.kind == "llm.usage.observed" or raw.source_granularity == "aggregate":
            yield build_llm_usage_observed(
                event_id=stable_event_id(source_id, "llm.usage.observed"),
                tenant_id=self.tenant_id,
                ts_ms=raw.ts_ms,
                provider=raw.provider or str(payload.get("provider", "unknown")),
                model=raw.model,
                source_id=source_id,
                usage=dict(payload.get("usage", payload)),
            )
            return
        if raw.tool_name:
            status = raw.status or "completed"
            kind = "tool.call.failed" if status == "failed" else "tool.call.completed"
            yield build_tool_call_event(
                kind=kind,
                event_id=stable_event_id(source_id, kind),
                tenant_id=self.tenant_id,
                ts_ms=raw.ts_ms,
                actor_ref=actor,
                tool_name=raw.tool_name,
                args=payload.get("args"),
                response=payload.get("response"),
                status=status,
                error=str(payload.get("error", "")),
                source_id=source_id,
                source_granularity=raw.source_granularity,
                provenance_origin=EventProvenance.IMPORTED,
            )
            return
        yield build_llm_call_event(
            kind="llm.call.completed",
            event_id=stable_event_id(source_id, "llm.call.completed"),
            tenant_id=self.tenant_id,
            ts_ms=raw.ts_ms,
            actor_ref=actor,
            provider=raw.provider or "unknown",
            model=raw.model,
            prompt=payload.get("prompt"),
            response=payload.get("response"),
            status=raw.status or "completed",
            prompt_tokens=payload.get("prompt_tokens"),
            completion_tokens=payload.get("completion_tokens"),
            total_tokens=payload.get("total_tokens"),
            cost_usd=payload.get("cost_usd"),
            source_id=source_id,
            source_granularity=raw.source_granularity,
            provenance_origin=EventProvenance.IMPORTED,
        )
