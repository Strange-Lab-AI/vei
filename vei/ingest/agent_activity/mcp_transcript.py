"""MCP transcript adapter for JSON-RPC tool-call evidence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from vei.events.api import (
    CanonicalEvent,
    EventProvenance,
    build_tool_call_event,
    stable_event_id,
)

from .api import RawAgentActivity


class McpTranscriptAdapter:
    source_name = "mcp_transcript"

    def __init__(self, path: str | Path, *, tenant_id: str = "") -> None:
        self.path = Path(path).expanduser().resolve()
        self.tenant_id = tenant_id

    def fetch(self, window: str = "") -> Iterable[RawAgentActivity]:
        paths = sorted(self.path.glob("*.jsonl")) if self.path.is_dir() else [self.path]
        pending: dict[str, dict] = {}
        for path in paths:
            with path.open("r", encoding="utf-8") as fh:
                for idx, line in enumerate(fh, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    payload = json.loads(line)
                    msg = payload.get("message", payload)
                    method = str(msg.get("method", ""))
                    rpc_id = str(msg.get("id", payload.get("id", f"{path.name}:{idx}")))
                    if method in {"tools/call", "tool/call"}:
                        params = dict(msg.get("params", {}))
                        pending[rpc_id] = params
                        yield RawAgentActivity(
                            source=self.source_name,
                            source_record_id=f"{path.name}:{rpc_id}:requested",
                            ts_ms=int(
                                payload.get("ts_ms", payload.get("time_ms", 0)) or 0
                            ),
                            kind="tool.call.requested",
                            tool_name=str(params.get("name", params.get("tool", ""))),
                            status="requested",
                            source_granularity="transcript",
                            payload={"args": params.get("arguments", {}), **payload},
                        )
                    elif rpc_id in pending and "result" in msg:
                        params = pending.pop(rpc_id)
                        yield RawAgentActivity(
                            source=self.source_name,
                            source_record_id=f"{path.name}:{rpc_id}:completed",
                            ts_ms=int(
                                payload.get("ts_ms", payload.get("time_ms", 0)) or 0
                            ),
                            kind="tool.call.completed",
                            tool_name=str(params.get("name", params.get("tool", ""))),
                            status="completed",
                            source_granularity="transcript",
                            payload={
                                "args": params.get("arguments", {}),
                                "response": msg.get("result"),
                                **payload,
                            },
                        )
                    elif rpc_id in pending and "error" in msg:
                        params = pending.pop(rpc_id)
                        yield RawAgentActivity(
                            source=self.source_name,
                            source_record_id=f"{path.name}:{rpc_id}:failed",
                            ts_ms=int(
                                payload.get("ts_ms", payload.get("time_ms", 0)) or 0
                            ),
                            kind="tool.call.failed",
                            tool_name=str(params.get("name", params.get("tool", ""))),
                            status="failed",
                            source_granularity="transcript",
                            payload={
                                "args": params.get("arguments", {}),
                                "error": msg.get("error"),
                                **payload,
                            },
                        )

    def to_canonical_events(self, raw: RawAgentActivity) -> Iterable[CanonicalEvent]:
        source_id = f"{self.source_name}:{raw.source_record_id}"
        yield build_tool_call_event(
            kind=raw.kind,
            event_id=stable_event_id(source_id, raw.kind),
            tenant_id=self.tenant_id,
            ts_ms=raw.ts_ms,
            tool_name=raw.tool_name,
            args=raw.payload.get("args"),
            response=raw.payload.get("response"),
            status=raw.status,
            error=json.dumps(raw.payload.get("error", "")),
            source_id=source_id,
            source_granularity=raw.source_granularity,
            provenance_origin=EventProvenance.IMPORTED,
        )
