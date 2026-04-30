"""MCP transcript adapter for JSON-RPC tool-call evidence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from vei.events.api import (
    CanonicalEvent,
    EventProvenance,
    ExecutionPrincipal,
    build_tool_call_event,
    stable_event_id,
)

from .api import RawAgentActivity
from .object_refs import extract_object_refs


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
                        request_source_id = (
                            f"{self.source_name}:{path.name}:{rpc_id}:requested"
                        )
                        request_event_id = stable_event_id(
                            request_source_id, "tool.call.requested"
                        )
                        pending[rpc_id] = {
                            "params": params,
                            "request_event_id": request_event_id,
                            "request_ts_ms": int(
                                payload.get("ts_ms", payload.get("time_ms", 0)) or 0
                            ),
                        }
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
                            payload={
                                "args": params.get("arguments", {}),
                                "jsonrpc_request_id": rpc_id,
                                "mcp_method_name": method,
                                **payload,
                            },
                        )
                    elif rpc_id in pending and "result" in msg:
                        pending_item = pending.pop(rpc_id)
                        params = dict(pending_item.get("params", {}))
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
                                "jsonrpc_request_id": rpc_id,
                                "mcp_method_name": "tools/call",
                                "request_event_id": pending_item.get(
                                    "request_event_id", ""
                                ),
                                "latency_ms": _latency_ms(
                                    pending_item.get("request_ts_ms"),
                                    payload.get("ts_ms", payload.get("time_ms", 0)),
                                ),
                                **payload,
                            },
                        )
                    elif rpc_id in pending and "error" in msg:
                        pending_item = pending.pop(rpc_id)
                        params = dict(pending_item.get("params", {}))
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
                                "jsonrpc_request_id": rpc_id,
                                "mcp_method_name": "tools/call",
                                "request_event_id": pending_item.get(
                                    "request_event_id", ""
                                ),
                                "latency_ms": _latency_ms(
                                    pending_item.get("request_ts_ms"),
                                    payload.get("ts_ms", payload.get("time_ms", 0)),
                                ),
                                **payload,
                            },
                        )

    def to_canonical_events(self, raw: RawAgentActivity) -> Iterable[CanonicalEvent]:
        source_id = f"{self.source_name}:{raw.source_record_id}"
        payload = raw.payload
        request_event_id = str(payload.get("request_event_id", ""))
        link_kind = ""
        if raw.kind == "tool.call.completed" and request_event_id:
            link_kind = "completed_by"
        elif raw.kind == "tool.call.failed" and request_event_id:
            link_kind = "failed_by"
        context = ExecutionPrincipal.from_mapping(
            payload, source="mcp"
        ).to_event_context(
            source_id=source_id,
            source_granularity=raw.source_granularity,
        )
        yield build_tool_call_event(
            kind=raw.kind,
            event_id=stable_event_id(source_id, raw.kind),
            tenant_id=self.tenant_id,
            ts_ms=raw.ts_ms,
            tool_name=raw.tool_name,
            args=raw.payload.get("args"),
            response=raw.payload.get("response"),
            object_refs=extract_object_refs(
                tool_name=raw.tool_name,
                args=raw.payload.get("args"),
                response=raw.payload.get("response"),
                explicit_refs=raw.payload.get("object_refs"),
            ),
            status=raw.status,
            error=json.dumps(raw.payload.get("error", "")),
            latency_ms=raw.payload.get("latency_ms"),
            source_id=source_id,
            source_granularity=raw.source_granularity,
            provenance_origin=EventProvenance.IMPORTED,
            links=(
                [{"kind": link_kind, "event_id": request_event_id}]
                if link_kind
                else None
            ),
            link_refs=[request_event_id] if request_event_id else None,
            context=context,
        )


def _latency_ms(start: object, end: object) -> int | None:
    try:
        start_ms = int(start or 0)
        end_ms = int(end or 0)
    except (TypeError, ValueError):
        return None
    if not start_ms or not end_ms:
        return None
    return max(0, end_ms - start_ms)
