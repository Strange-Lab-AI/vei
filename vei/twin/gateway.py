from . import _gateway_adapters as _adapter_impl
from . import _gateway_routes as _route_impl
from . import _runtime as _runtime_impl
from ._gateway_adapters import dispatch_request as _dispatch_request
from ._helpers import (
    _blocked_live_operations,
    _channel_for_focus,
    _contract_summary,
    _env_bool,
    _error_payload,
    _event_surface,
    _extract_jql_value,
    _find_mail_message,
    _graph_attendees,
    _graph_body_content,
    _graph_datetime_to_ms,
    _graph_email_address,
    _graph_event,
    _graph_first_recipient,
    _graph_message,
    _graph_message_summary,
    _http_exception,
    _identity_from_mirror_agent,
    _iso_now,
    _jira_issue,
    _jira_project_key,
    _jira_transitions,
    _merge_mirror_agent_identity,
    _mirror_operation_class,
    _mirror_route_error_response,
    _ms_to_iso,
    _normalize_surface,
    _object_refs,
    _provider_error_code,
    _request_agent_identity,
    _require_bearer,
    _resolve_slack_channel_name,
    _salesforce_account,
    _salesforce_contact,
    _salesforce_opportunity,
    _slack_auth_ok,
    _slack_channel,
    _slack_channel_id,
    _slack_message,
    _slack_user_id,
    _snapshot_path,
    _status_code_for_error,
    _surface_alias_set,
)
from ._runtime import TwinRuntime


def create_twin_gateway_app(root):
    _route_impl.dispatch_request = _dispatch_request
    return _runtime_impl.create_twin_gateway_app(root)


def _jira_search(runtime, request, params):
    original = _adapter_impl.dispatch_request
    _adapter_impl.dispatch_request = _dispatch_request
    try:
        return _adapter_impl.jira_search(runtime, request, params)
    finally:
        _adapter_impl.dispatch_request = original


def _salesforce_query(runtime, request, query):
    original = _adapter_impl.dispatch_request
    _adapter_impl.dispatch_request = _dispatch_request
    try:
        return _adapter_impl.salesforce_query(runtime, request, query)
    finally:
        _adapter_impl.dispatch_request = original


__all__ = [
    "TwinRuntime",
    "create_twin_gateway_app",
    "_blocked_live_operations",
    "_channel_for_focus",
    "_contract_summary",
    "_dispatch_request",
    "_env_bool",
    "_error_payload",
    "_event_surface",
    "_extract_jql_value",
    "_find_mail_message",
    "_graph_attendees",
    "_graph_body_content",
    "_graph_datetime_to_ms",
    "_graph_email_address",
    "_graph_event",
    "_graph_first_recipient",
    "_graph_message",
    "_graph_message_summary",
    "_http_exception",
    "_identity_from_mirror_agent",
    "_iso_now",
    "_jira_issue",
    "_jira_project_key",
    "_jira_search",
    "_jira_transitions",
    "_merge_mirror_agent_identity",
    "_mirror_operation_class",
    "_mirror_route_error_response",
    "_ms_to_iso",
    "_normalize_surface",
    "_object_refs",
    "_provider_error_code",
    "_request_agent_identity",
    "_require_bearer",
    "_resolve_slack_channel_name",
    "_salesforce_account",
    "_salesforce_contact",
    "_salesforce_opportunity",
    "_salesforce_query",
    "_slack_auth_ok",
    "_slack_channel",
    "_slack_channel_id",
    "_slack_message",
    "_slack_user_id",
    "_snapshot_path",
    "_status_code_for_error",
    "_surface_alias_set",
]
