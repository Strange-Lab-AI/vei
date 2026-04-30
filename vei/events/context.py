"""Context helpers for provenance events.

The ``CanonicalEvent`` envelope stays frozen at v1.  Runtime, identity, trace,
and source context therefore lives inside ``StateDelta.data["context"]``.
"""

from __future__ import annotations

import os
from typing import Any

from pydantic import BaseModel, Field


class EventContext(BaseModel):
    tenant_id: str = ""
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


class ExecutionPrincipal(BaseModel):
    """Runtime identity chain that can be folded into frozen event deltas."""

    tenant_id: str = ""
    workspace_id: str = ""
    human_user_id: str = ""
    agent_id: str = ""
    agent_version: str = ""
    service_principal: str = ""
    delegated_credential_id: str = ""
    auth_subject: str = ""
    source: str = "sim"
    mcp_session_id: str = ""
    mcp_client_id: str = ""
    mcp_server_id: str = ""
    mcp_protocol_version: str = ""
    mcp_transport: str = ""
    mcp_method_name: str = ""
    jsonrpc_request_id: str = ""
    extra: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_env(cls, *, source: str | None = None) -> "ExecutionPrincipal":
        return cls(
            tenant_id=os.environ.get("VEI_TENANT_ID", ""),
            workspace_id=os.environ.get("VEI_WORKSPACE_ID", ""),
            human_user_id=os.environ.get("VEI_HUMAN_USER_ID", ""),
            agent_id=os.environ.get("VEI_AGENT_ID", ""),
            agent_version=os.environ.get("VEI_AGENT_VERSION", ""),
            service_principal=os.environ.get("VEI_SERVICE_PRINCIPAL", ""),
            delegated_credential_id=os.environ.get("VEI_DELEGATED_CREDENTIAL_ID", ""),
            auth_subject=os.environ.get("VEI_AUTH_SUBJECT", ""),
            source=source or os.environ.get("VEI_EXECUTION_SOURCE", "sim"),
            mcp_session_id=os.environ.get("VEI_MCP_SESSION_ID", ""),
            mcp_client_id=os.environ.get("VEI_MCP_CLIENT_ID", ""),
            mcp_server_id=os.environ.get("VEI_MCP_SERVER_ID", ""),
            mcp_protocol_version=os.environ.get("VEI_MCP_PROTOCOL_VERSION", ""),
            mcp_transport=os.environ.get("VEI_MCP_TRANSPORT", ""),
        )

    @classmethod
    def from_mapping(
        cls, payload: dict[str, Any] | None, *, source: str = "import"
    ) -> "ExecutionPrincipal":
        data = payload or {}
        return cls(
            tenant_id=_string(data, "tenant_id", "tenant"),
            workspace_id=_string(data, "workspace_id", "workspace"),
            human_user_id=_string(data, "human_user_id", "user_id", "owner_user_id"),
            agent_id=_string(data, "agent_id", "actor_id", "client_id"),
            agent_version=_string(data, "agent_version", "agent_revision"),
            service_principal=_string(
                data, "service_principal", "service_principal_id"
            ),
            delegated_credential_id=_string(
                data, "delegated_credential_id", "credential_id"
            ),
            auth_subject=_string(data, "auth_subject", "subject", "sub"),
            source=source,
            mcp_session_id=_string(data, "mcp_session_id", "session_id"),
            mcp_client_id=_string(data, "mcp_client_id", "client_id"),
            mcp_server_id=_string(data, "mcp_server_id", "server_id"),
            mcp_protocol_version=_string(
                data, "mcp_protocol_version", "protocol_version"
            ),
            mcp_transport=_string(data, "mcp_transport", "transport"),
            mcp_method_name=_string(data, "mcp_method_name", "method"),
            jsonrpc_request_id=_string(data, "jsonrpc_request_id", "jsonrpc_id", "id"),
            extra={
                key: value
                for key, value in {
                    "source": source,
                    "auth_subject": data.get("auth_subject") or data.get("subject"),
                }.items()
                if value not in {None, ""}
            },
        )

    def to_event_context(
        self,
        *,
        source_id: str = "",
        source_granularity: str = "per_call",
        workspace_id: str = "",
        run_id: str = "",
        trace_id: str = "",
        span_id: str = "",
        parent_event_id: str = "",
        extra: dict[str, Any] | None = None,
    ) -> EventContext:
        merged_extra = dict(self.extra)
        if extra:
            merged_extra.update(extra)
        return EventContext(
            tenant_id=self.tenant_id,
            workspace_id=workspace_id or self.workspace_id,
            run_id=run_id,
            trace_id=trace_id,
            span_id=span_id,
            parent_event_id=parent_event_id,
            human_user_id=self.human_user_id,
            agent_id=self.agent_id,
            agent_version=self.agent_version,
            service_principal=self.service_principal,
            delegated_credential_id=self.delegated_credential_id,
            source_id=source_id,
            source_granularity=source_granularity,
            mcp_session_id=self.mcp_session_id,
            mcp_client_id=self.mcp_client_id,
            mcp_server_id=self.mcp_server_id,
            mcp_protocol_version=self.mcp_protocol_version,
            mcp_transport=self.mcp_transport,
            mcp_method_name=self.mcp_method_name,
            jsonrpc_request_id=self.jsonrpc_request_id,
            extra=merged_extra,
        )


def _string(data: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = data.get(key)
        if value not in {None, ""}:
            return str(value)
    return ""


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
