from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from vei.router.errors import MCPError

from ._runtime_support import request_agent_identity

if TYPE_CHECKING:
    from ._runtime import TwinRuntime


def slack_auth_ok(request: Request, token: str) -> bool:
    auth = request.headers.get("authorization", "")
    return auth == f"Bearer {token}"


def require_bearer(request: Request, token: str) -> None:
    if request.headers.get("authorization", "") == f"Bearer {token}":
        return
    raise HTTPException(status_code=401, detail="invalid bearer token")


async def request_payload(request: Request) -> dict[str, Any]:
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        payload = await request.json()
        return payload if isinstance(payload, dict) else {}
    if (
        "application/x-www-form-urlencoded" in content_type
        or "multipart/form-data" in content_type
    ):
        form = await request.form()
        return dict(form)
    return {}


def slack_channel(channel: str) -> dict[str, Any]:
    name = channel[1:] if channel.startswith("#") else channel
    return {
        "id": slack_channel_id(channel),
        "name": name,
        "is_channel": True,
        "is_member": True,
    }


def slack_channel_id(channel: str) -> str:
    digest = hashlib.sha1(channel.encode("utf-8"), usedforsecurity=False)
    return "C" + digest.hexdigest()[:8].upper()


def slack_user_id(user: str) -> str:
    digest = hashlib.sha1(user.encode("utf-8"), usedforsecurity=False)
    return "U" + digest.hexdigest()[:8].upper()


def resolve_slack_channel_name(runtime: TwinRuntime, value: str) -> str:
    if value.startswith("#"):
        return value
    channels = runtime.peek("slack.list_channels", {})
    if isinstance(channels, list):
        for channel in channels:
            if slack_channel_id(str(channel)) == value:
                return str(channel)
    if value:
        return f"#{value}"
    raise HTTPException(status_code=400, detail="channel is required")


def slack_message(channel: str, message: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "message",
        "user": slack_user_id(str(message.get("user", "unknown"))),
        "username": str(message.get("user", "unknown")),
        "text": str(message.get("text", "")),
        "ts": str(message.get("ts", "")),
        "thread_ts": message.get("thread_ts"),
        "channel": slack_channel_id(channel),
    }


def jira_search(
    runtime: TwinRuntime,
    request: Request,
    params: Any,
) -> dict[str, Any]:
    jql = str(params.get("jql", ""))
    max_results = int(params.get("maxResults", params.get("max_results", 25)) or 25)
    start_at = int(params.get("startAt", params.get("start_at", 0)) or 0)
    args: dict[str, Any] = {"limit": max_results}
    status = extract_jql_value(jql, "status")
    assignee = extract_jql_value(jql, "assignee")
    if status:
        args["status"] = status
    if assignee:
        args["assignee"] = assignee
    try:
        payload = dispatch_request(
            runtime,
            request,
            external_tool="jira.search",
            resolved_tool="jira.list_issues",
            args=args,
            focus_hint="tickets",
        )
    except Exception as exc:  # noqa: BLE001
        raise http_exception(exc) from exc
    issues = payload if isinstance(payload, list) else payload.get("issues", [])
    sliced = issues[start_at : start_at + max_results]
    return {
        "startAt": start_at,
        "maxResults": max_results,
        "total": len(issues),
        "issues": [jira_issue(issue) for issue in sliced],
    }


def jira_issue(issue: dict[str, Any]) -> dict[str, Any]:
    issue_id = str(issue.get("issue_id", issue.get("ticket_id", "")))
    return {
        "id": issue_id,
        "key": issue_id,
        "fields": {
            "summary": issue.get("title", ""),
            "description": issue.get("description", ""),
            "status": {"name": issue.get("status", "open")},
            "assignee": {"displayName": issue.get("assignee") or "unassigned"},
            "priority": {"name": issue.get("priority", "P3")},
            "labels": issue.get("labels", []),
            "comment": {"total": issue.get("comment_count", 0)},
        },
    }


def jira_transitions(status: str) -> list[dict[str, Any]]:
    allowed = {
        "open": ["in_progress", "blocked", "resolved", "closed"],
        "in_progress": ["blocked", "resolved", "closed"],
        "blocked": ["open", "in_progress", "resolved", "closed"],
        "resolved": ["closed", "open", "in_progress"],
        "closed": ["open"],
    }
    return [{"id": item, "name": item} for item in allowed.get(status.lower(), [])]


def extract_jql_value(jql: str, key: str) -> str | None:
    pattern = re.compile(rf"{key}\s*=\s*['\"]?([^'\"]+)['\"]?", re.IGNORECASE)
    match = pattern.search(jql)
    if not match:
        return None
    return match.group(1).strip()


def graph_message_summary(message: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": message.get("id", ""),
        "subject": message.get("subj", ""),
        "from": {
            "emailAddress": {
                "address": message.get("from", ""),
                "name": message.get("from", ""),
            }
        },
        "toRecipients": [
            {
                "emailAddress": {
                    "address": message.get("to", ""),
                    "name": message.get("to", ""),
                }
            }
        ],
        "bodyPreview": message.get("body_text", ""),
        "isRead": not bool(message.get("unread", False)),
        "receivedDateTime": ms_to_iso(int(message.get("time", 0) or 0)),
    }


def graph_message(message: dict[str, Any], opened: dict[str, Any]) -> dict[str, Any]:
    summary = graph_message_summary(message)
    summary["body"] = {
        "contentType": "text",
        "content": opened.get("body_text", ""),
    }
    return summary


def find_mail_message(payload: Any, message_id: str) -> dict[str, Any]:
    messages = payload if isinstance(payload, list) else payload.get("messages", [])
    for message in messages:
        if str(message.get("id", "")) == message_id:
            return dict(message)
    return {"id": message_id}


def graph_first_recipient(payload: Any) -> str:
    if not isinstance(payload, list) or not payload:
        return ""
    first = payload[0]
    if not isinstance(first, dict):
        return ""
    return graph_email_address(first.get("emailAddress"))


def graph_email_address(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("address", ""))


def graph_body_content(payload: Any) -> str:
    if isinstance(payload, str):
        return payload
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("content", ""))


def graph_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": event.get("event_id", ""),
        "subject": event.get("title", ""),
        "start": {"dateTime": ms_to_iso(int(event.get("start_ms", 0) or 0))},
        "end": {"dateTime": ms_to_iso(int(event.get("end_ms", 0) or 0))},
        "attendees": [
            {"emailAddress": {"address": item, "name": item}}
            for item in event.get("attendees", [])
        ],
        "organizer": {
            "emailAddress": {
                "address": event.get("organizer", ""),
                "name": event.get("organizer", ""),
            }
        },
        "location": {"displayName": event.get("location", "")},
        "bodyPreview": event.get("description", ""),
        "isCancelled": str(event.get("status", "")).upper() == "CANCELED",
    }


def graph_datetime_to_ms(value: Any) -> int:
    if not value:
        return int(datetime.now(UTC).timestamp() * 1000)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def graph_attendees(payload: Any) -> list[str]:
    if not isinstance(payload, list):
        return []
    result: list[str] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        address = graph_email_address(item.get("emailAddress"))
        if address:
            result.append(address)
    return result


def salesforce_query(
    runtime: TwinRuntime,
    request: Request,
    query: str,
) -> dict[str, Any]:
    lowered = query.lower()
    limit_match = re.search(r"limit\s+(\d+)", lowered)
    limit = int(limit_match.group(1)) if limit_match else 25
    try:
        if "from opportunity" in lowered:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="salesforce.query.opportunity",
                resolved_tool="salesforce.opportunity.list",
                args={"limit": limit},
                focus_hint="crm",
            )
            rows = payload if isinstance(payload, list) else payload.get("deals", [])
            records = [salesforce_opportunity(item) for item in rows[:limit]]
            return {"totalSize": len(records), "done": True, "records": records}
        if "from contact" in lowered:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="salesforce.query.contact",
                resolved_tool="salesforce.contact.list",
                args={"limit": limit},
                focus_hint="crm",
            )
            rows = payload if isinstance(payload, list) else payload.get("contacts", [])
            records = [salesforce_contact(item) for item in rows[:limit]]
            return {"totalSize": len(records), "done": True, "records": records}
        payload = dispatch_request(
            runtime,
            request,
            external_tool="salesforce.query.account",
            resolved_tool="salesforce.account.list",
            args={"limit": limit},
            focus_hint="crm",
        )
    except Exception as exc:  # noqa: BLE001
        raise http_exception(exc) from exc
    rows = payload if isinstance(payload, list) else payload.get("companies", [])
    records = [salesforce_account(item) for item in rows[:limit]]
    return {"totalSize": len(records), "done": True, "records": records}


def salesforce_opportunity(payload: dict[str, Any]) -> dict[str, Any]:
    record_id = str(payload.get("id", ""))
    return {
        "attributes": {
            "type": "Opportunity",
            "url": f"/services/data/v60.0/sobjects/Opportunity/{record_id}",
        },
        "Id": record_id,
        "Name": payload.get("name", ""),
        "StageName": payload.get("stage", ""),
        "Amount": payload.get("amount", 0),
        "AccountId": payload.get("company_id"),
        "ContactId": payload.get("contact_id"),
    }


def salesforce_contact(payload: dict[str, Any]) -> dict[str, Any]:
    record_id = str(payload.get("id", ""))
    return {
        "attributes": {
            "type": "Contact",
            "url": f"/services/data/v60.0/sobjects/Contact/{record_id}",
        },
        "Id": record_id,
        "Email": payload.get("email", ""),
        "FirstName": payload.get("first_name", ""),
        "LastName": payload.get("last_name", ""),
        "AccountId": payload.get("company_id"),
    }


def salesforce_account(payload: dict[str, Any]) -> dict[str, Any]:
    record_id = str(payload.get("id", ""))
    return {
        "attributes": {
            "type": "Account",
            "url": f"/services/data/v60.0/sobjects/Account/{record_id}",
        },
        "Id": record_id,
        "Name": payload.get("name", ""),
        "Domain__c": payload.get("domain", ""),
    }


def http_exception(exc: Exception) -> HTTPException:
    if isinstance(exc, MCPError):
        return HTTPException(
            status_code=status_code_for_error(exc.code),
            detail={"code": exc.code, "message": exc.message},
        )
    if isinstance(exc, ValueError):
        return HTTPException(
            status_code=400,
            detail={"code": "invalid_args", "message": str(exc)},
        )
    return HTTPException(
        status_code=500,
        detail={"code": "operation_failed", "message": str(exc)},
    )


def provider_error_code(exc: Exception) -> str:
    if isinstance(exc, MCPError):
        return exc.code
    if isinstance(exc, ValueError):
        return "invalid_args"
    return "operation_failed"


def error_payload(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, MCPError):
        return {"code": exc.code, "message": exc.message}
    return {"code": exc.__class__.__name__.lower(), "message": str(exc)}


def jira_project_key(runtime: TwinRuntime) -> str:
    payload = runtime.peek("jira.list_issues", {"limit": 1})
    issues = payload if isinstance(payload, list) else payload.get("issues", [])
    if not issues:
        return "VEI"
    issue_id = str(issues[0].get("issue_id", "VEI-1"))
    return issue_id.split("-", 1)[0]


def ms_to_iso(value: int) -> str:
    if value <= 0:
        return datetime.now(UTC).isoformat()
    return datetime.fromtimestamp(value / 1000, tz=UTC).isoformat()


def dispatch_request(
    runtime: TwinRuntime,
    request: Request,
    *,
    external_tool: str,
    resolved_tool: str,
    args: dict[str, Any],
    focus_hint: str,
) -> Any:
    agent = request_agent_identity(request)
    if runtime.mirror is not None:
        if agent is None or not agent.agent_id:
            raise MCPError(
                "mirror.agent_id_required",
                "proxy requests must include X-VEI-Agent-Id",
            )
        return runtime.dispatch_proxy_request(
            external_tool=external_tool,
            resolved_tool=resolved_tool,
            args=args,
            focus_hint=focus_hint,
            agent=agent,
        )
    return runtime.dispatch(
        external_tool=external_tool,
        resolved_tool=resolved_tool,
        args=args,
        focus_hint=focus_hint,
        agent=agent,
    )


def status_code_for_error(code: str) -> int:
    if code in {
        "mirror.surface_denied",
        "mirror.profile_denied",
        "mirror.mode_denied",
        "mirror.agent_not_registered",
        "mirror.agent_inactive",
        "mirror.unknown_operation_class",
        "policy.denied",
    }:
        return 403
    if code in {"mirror.approval_required", "policy.approval_required"}:
        return 409
    if code == "mirror.rate_limited":
        return 429
    if code in {
        "mirror.unsupported_live_write",
        "mirror.connector_degraded",
        "service_unavailable",
        "slack.live_backend_unavailable",
        "mail.live_backend_unavailable",
        "calendar.live_backend_unavailable",
        "tickets.live_backend_unavailable",
        "crm.live_backend_unavailable",
    }:
        return 503
    if code == "mirror.agent_id_required":
        return 400
    return 400


def mirror_route_error_response(
    exc: Exception,
    *,
    surface: str,
) -> JSONResponse:
    if surface == "slack":
        return JSONResponse(
            {"ok": False, "error": provider_error_code(exc)},
            status_code=200,
        )
    raise http_exception(exc)
