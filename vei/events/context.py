"""Context helpers for provenance events.

The ``CanonicalEvent`` envelope stays frozen at v1.  Runtime, identity, trace,
and source context therefore lives inside ``StateDelta.data["context"]``.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class EventContext(BaseModel):
    workspace_id: str = ""
    run_id: str = ""
    trace_id: str = ""
    span_id: str = ""
    parent_event_id: str = ""
    human_user_id: str = ""
    agent_id: str = ""
    agent_version: str = ""
    service_principal: str = ""
    delegated_credential_id: str = ""
    source_id: str = ""
    source_granularity: str = "per_call"
    mcp_session_id: str = ""
    mcp_client_id: str = ""
    mcp_server_id: str = ""
    mcp_protocol_version: str = ""
    mcp_transport: str = ""
    mcp_method_name: str = ""
    jsonrpc_request_id: str = ""
    extra: dict[str, Any] = Field(default_factory=dict)

    def compact(self) -> dict[str, Any]:
        data = self.model_dump(mode="json")
        extra = data.pop("extra", {}) or {}
        compacted = {
            key: value for key, value in data.items() if value not in {"", None}
        }
        if extra:
            compacted["extra"] = extra
        return compacted


def merge_event_context(
    data: dict[str, Any],
    context: EventContext | dict[str, Any] | None,
) -> dict[str, Any]:
    if context is None:
        return data
    context_data = (
        context.compact() if isinstance(context, EventContext) else dict(context)
    )
    if not context_data:
        return data
    merged = dict(data)
    existing = merged.get("context")
    if isinstance(existing, dict):
        context_data = {**existing, **context_data}
    merged["context"] = context_data
    return merged
