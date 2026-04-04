from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from vei.governor import GovernorAgentSpec, GovernorIngestEvent
from vei.workforce.api import WorkforceCommandRecord, WorkforceState
from vei.run.api import build_run_timeline, get_run_surface_state

from ._gateway_adapters import (
    dispatch_request,
    find_mail_message,
    graph_attendees,
    graph_body_content,
    graph_datetime_to_ms,
    graph_email_address,
    graph_event,
    graph_first_recipient,
    graph_message,
    graph_message_summary,
    http_exception,
    jira_issue,
    jira_project_key,
    jira_search,
    jira_transitions,
    mirror_route_error_response,
    request_payload,
    require_bearer,
    resolve_slack_channel_name,
    salesforce_account,
    salesforce_contact,
    salesforce_opportunity,
    salesforce_query,
    slack_auth_ok,
    slack_channel,
    slack_channel_id,
    slack_message,
    slack_user_id,
)

if TYPE_CHECKING:
    from ._runtime import TwinRuntime


def register_gateway_routes(app: FastAPI, runtime: TwinRuntime) -> None:
    bundle = runtime.bundle

    @app.get("/")
    def root_index() -> JSONResponse:
        return JSONResponse(
            {
                "organization_name": bundle.organization_name,
                "organization_domain": bundle.organization_domain,
                "surfaces": [
                    item.model_dump(mode="json") for item in bundle.gateway.surfaces
                ],
                "status_path": "/api/twin",
            }
        )

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        return JSONResponse({"ok": True, "run_id": runtime.run_id})

    @app.get("/api/twin")
    def api_twin() -> JSONResponse:
        return JSONResponse(runtime.status_payload())

    @app.get("/api/governor")
    def api_mirror(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        return JSONResponse(runtime._mirror_snapshot_payload())

    @app.get("/api/workforce")
    def api_workforce(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        return JSONResponse(runtime._workforce_payload())

    @app.post("/api/workforce/sync")
    async def api_workforce_sync(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        body = await request_payload(request)
        state = WorkforceState.model_validate(body)
        payload = runtime.sync_workforce_state(state)
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/workforce/commands")
    async def api_workforce_command(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        body = await request_payload(request)
        command = WorkforceCommandRecord.model_validate(body)
        payload = runtime.record_workforce_command(command)
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/governor/agents")
    def api_mirror_agents(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            return JSONResponse({"agents": []})
        agents = [item.model_dump(mode="json") for item in runtime.mirror.list_agents()]
        return JSONResponse({"agents": agents})

    @app.post("/api/governor/agents")
    async def api_mirror_register_agent(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            raise HTTPException(status_code=503, detail="governor runtime unavailable")
        body = await request_payload(request)
        agent = runtime.mirror.register_agent(GovernorAgentSpec.model_validate(body))
        return JSONResponse(agent.model_dump(mode="json"), status_code=201)

    @app.patch("/api/governor/agents/{agent_id}")
    async def api_mirror_update_agent(agent_id: str, request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            raise HTTPException(status_code=503, detail="governor runtime unavailable")
        body = await request_payload(request)
        try:
            agent = runtime.mirror.update_agent(agent_id, dict(body))
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return JSONResponse(agent.model_dump(mode="json"))

    @app.delete("/api/governor/agents/{agent_id}")
    def api_mirror_remove_agent(agent_id: str, request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            raise HTTPException(status_code=503, detail="governor runtime unavailable")
        try:
            agent = runtime.mirror.remove_agent(agent_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return JSONResponse(agent.model_dump(mode="json"))

    @app.get("/api/governor/approvals")
    def api_mirror_approvals(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            return JSONResponse({"approvals": []})
        approvals = [
            item.model_dump(mode="json")
            for item in runtime.mirror.list_pending_approvals()
        ]
        return JSONResponse({"approvals": approvals})

    @app.post("/api/governor/approvals/{approval_id}/approve")
    async def api_mirror_approve(
        approval_id: str,
        request: Request,
    ) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            raise HTTPException(status_code=503, detail="governor runtime unavailable")
        body = await request_payload(request)
        resolver_agent_id = str(body.get("resolver_agent_id") or "").strip()
        if not resolver_agent_id:
            raise HTTPException(
                status_code=400,
                detail="resolver_agent_id is required to approve mirror actions",
            )
        try:
            approval = runtime.mirror.resolve_approval(
                approval_id=approval_id,
                resolver_agent_id=resolver_agent_id,
                action="approve",
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(approval.model_dump(mode="json"))

    @app.post("/api/governor/approvals/{approval_id}/reject")
    async def api_mirror_reject(
        approval_id: str,
        request: Request,
    ) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            raise HTTPException(status_code=503, detail="governor runtime unavailable")
        body = await request_payload(request)
        resolver_agent_id = str(body.get("resolver_agent_id") or "").strip()
        if not resolver_agent_id:
            raise HTTPException(
                status_code=400,
                detail="resolver_agent_id is required to reject mirror actions",
            )
        try:
            approval = runtime.mirror.resolve_approval(
                approval_id=approval_id,
                resolver_agent_id=resolver_agent_id,
                action="reject",
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(approval.model_dump(mode="json"))

    @app.post("/api/governor/events")
    async def api_mirror_ingest_event(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            raise HTTPException(status_code=503, detail="governor runtime unavailable")
        body = await request_payload(request)
        event = GovernorIngestEvent.model_validate(body).model_copy(
            update={"source_mode": "ingest"}
        )
        try:
            result = runtime.mirror.ingest_event(event)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "mirror.agent_not_registered",
                    "message": str(exc),
                },
            ) from exc
        return JSONResponse(result.model_dump(mode="json"), status_code=202)

    @app.post("/api/governor/demo/tick")
    def api_governor_demo_tick(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        if runtime.mirror is None:
            raise HTTPException(status_code=503, detail="governor runtime unavailable")
        result = runtime.mirror.demo_tick()
        if result is None:
            return JSONResponse({"ok": True, "remaining_demo_steps": 0})
        return JSONResponse(result.model_dump(mode="json"))

    @app.get("/api/twin/history")
    def api_twin_history() -> JSONResponse:
        payload = [
            item.model_dump(mode="json")
            for item in build_run_timeline(runtime.workspace_root, runtime.run_id)
        ]
        return JSONResponse(payload)

    @app.get("/api/twin/surfaces")
    def api_twin_surfaces() -> JSONResponse:
        payload = get_run_surface_state(runtime.workspace_root, runtime.run_id)
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/twin/finalize")
    def api_twin_finalize() -> JSONResponse:
        runtime.finalize()
        return JSONResponse(runtime.status_payload())

    @app.get("/slack/api/conversations.list")
    async def slack_conversations_list(request: Request) -> JSONResponse:
        if not slack_auth_ok(request, bundle.gateway.auth_token):
            return JSONResponse({"ok": False, "error": "invalid_auth"})
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="slack.conversations.list",
                resolved_tool="slack.list_channels",
                args={},
                focus_hint="slack",
            )
        except Exception as exc:  # noqa: BLE001
            return mirror_route_error_response(exc, surface="slack")
        channels = payload if isinstance(payload, list) else payload.get("channels", [])
        return JSONResponse(
            {"ok": True, "channels": [slack_channel(channel) for channel in channels]}
        )

    @app.get("/slack/api/conversations.history")
    async def slack_conversations_history(request: Request) -> JSONResponse:
        if not slack_auth_ok(request, bundle.gateway.auth_token):
            return JSONResponse({"ok": False, "error": "invalid_auth"})
        channel_arg = request.query_params.get("channel", "")
        channel_name = resolve_slack_channel_name(runtime, channel_arg)
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="slack.conversations.history",
                resolved_tool="slack.open_channel",
                args={"channel": channel_name},
                focus_hint="slack",
            )
        except Exception as exc:  # noqa: BLE001
            return mirror_route_error_response(exc, surface="slack")
        messages = payload.get("messages", []) if isinstance(payload, dict) else []
        return JSONResponse(
            {
                "ok": True,
                "messages": [
                    slack_message(channel_name, message) for message in messages
                ],
                "has_more": False,
            }
        )

    @app.get("/slack/api/conversations.replies")
    async def slack_conversations_replies(request: Request) -> JSONResponse:
        if not slack_auth_ok(request, bundle.gateway.auth_token):
            return JSONResponse({"ok": False, "error": "invalid_auth"})
        channel_name = resolve_slack_channel_name(
            runtime, request.query_params.get("channel", "")
        )
        thread_ts = request.query_params.get("ts", "")
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="slack.conversations.replies",
                resolved_tool="slack.fetch_thread",
                args={"channel": channel_name, "thread_ts": thread_ts},
                focus_hint="slack",
            )
        except Exception as exc:  # noqa: BLE001
            return mirror_route_error_response(exc, surface="slack")
        messages = payload.get("messages", []) if isinstance(payload, dict) else []
        return JSONResponse(
            {
                "ok": True,
                "messages": [
                    slack_message(channel_name, message) for message in messages
                ],
            }
        )

    @app.post("/slack/api/chat.postMessage")
    async def slack_post_message(request: Request) -> JSONResponse:
        if not slack_auth_ok(request, bundle.gateway.auth_token):
            return JSONResponse({"ok": False, "error": "invalid_auth"})
        body = await request_payload(request)
        channel_name = resolve_slack_channel_name(runtime, str(body.get("channel", "")))
        args = {
            "channel": channel_name,
            "text": str(body.get("text", "")),
            "thread_ts": body.get("thread_ts"),
        }
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="slack.chat.postMessage",
                resolved_tool="slack.send_message",
                args=args,
                focus_hint="slack",
            )
        except Exception as exc:  # noqa: BLE001
            return mirror_route_error_response(exc, surface="slack")
        ts = str(payload.get("ts", ""))
        return JSONResponse(
            {
                "ok": True,
                "channel": slack_channel_id(channel_name),
                "ts": ts,
                "message": {
                    "type": "message",
                    "text": str(args["text"]),
                    "user": slack_user_id("agent"),
                    "ts": ts,
                },
            }
        )

    @app.get("/jira/rest/api/3/project")
    async def jira_projects(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        project_key = jira_project_key(runtime)
        return JSONResponse(
            [{"id": project_key, "key": project_key, "name": bundle.organization_name}]
        )

    @app.get("/jira/rest/api/3/search")
    async def jira_search_get(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        return JSONResponse(jira_search(runtime, request, request.query_params))

    @app.post("/jira/rest/api/3/search")
    async def jira_search_post(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        body = await request_payload(request)
        return JSONResponse(jira_search(runtime, request, body))

    @app.get("/jira/rest/api/3/issue/{issue_id}")
    async def jira_issue_get(request: Request, issue_id: str) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="jira.issue.get",
                resolved_tool="jira.get_issue",
                args={"issue_id": issue_id},
                focus_hint="tickets",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return JSONResponse(jira_issue(payload))

    @app.get("/jira/rest/api/3/issue/{issue_id}/transitions")
    async def jira_issue_transitions(request: Request, issue_id: str) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="jira.issue.get",
                resolved_tool="jira.get_issue",
                args={"issue_id": issue_id},
                focus_hint="tickets",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        status = str(payload.get("status", "open"))
        return JSONResponse({"transitions": jira_transitions(status)})

    @app.post("/jira/rest/api/3/issue/{issue_id}/comment")
    async def jira_add_comment(request: Request, issue_id: str) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        body = await request_payload(request)
        args = {"issue_id": issue_id, "body": str(body.get("body", ""))}
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="jira.issue.comment",
                resolved_tool="jira.add_comment",
                args=args,
                focus_hint="tickets",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return JSONResponse(
            {"id": payload.get("comment_id"), "body": body.get("body", "")},
            status_code=201,
        )

    @app.post("/jira/rest/api/3/issue/{issue_id}/transitions")
    async def jira_transition(request: Request, issue_id: str) -> Response:
        require_bearer(request, bundle.gateway.auth_token)
        body = await request_payload(request)
        transition = (
            body.get("transition", {})
            if isinstance(body.get("transition"), dict)
            else {}
        )
        status = transition.get("id") or transition.get("name") or body.get("status")
        try:
            dispatch_request(
                runtime,
                request,
                external_tool="jira.issue.transition",
                resolved_tool="jira.transition_issue",
                args={"issue_id": issue_id, "status": str(status or "")},
                focus_hint="tickets",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return Response(status_code=204)

    @app.get("/graph/v1.0/me/messages")
    async def graph_messages(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="graph.messages.list",
                resolved_tool="mail.list",
                args={"folder": "INBOX"},
                focus_hint="mail",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        messages = payload if isinstance(payload, list) else payload.get("messages", [])
        return JSONResponse(
            {"value": [graph_message_summary(message) for message in messages]}
        )

    @app.get("/graph/v1.0/me/messages/{message_id}")
    async def graph_message_get(request: Request, message_id: str) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        try:
            summary = dispatch_request(
                runtime,
                request,
                external_tool="graph.messages.get",
                resolved_tool="mail.open",
                args={"id": message_id},
                focus_hint="mail",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        listing = runtime.peek("mail.list", {"folder": "INBOX"})
        message = find_mail_message(listing, message_id)
        return JSONResponse(graph_message(message, summary))

    @app.post("/graph/v1.0/me/sendMail")
    async def graph_send_mail(request: Request) -> Response:
        require_bearer(request, bundle.gateway.auth_token)
        body = await request_payload(request)
        message = (
            body.get("message", {}) if isinstance(body.get("message"), dict) else {}
        )
        to_address = graph_first_recipient(message.get("toRecipients"))
        subject = str(message.get("subject", ""))
        body_content = graph_body_content(message.get("body"))
        try:
            dispatch_request(
                runtime,
                request,
                external_tool="graph.messages.send",
                resolved_tool="mail.compose",
                args={"to": to_address, "subj": subject, "body_text": body_content},
                focus_hint="mail",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return Response(status_code=202)

    @app.get("/graph/v1.0/me/events")
    async def graph_events(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="graph.events.list",
                resolved_tool="calendar.list_events",
                args={},
                focus_hint="calendar",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        events = payload if isinstance(payload, list) else payload.get("events", [])
        return JSONResponse({"value": [graph_event(event) for event in events]})

    @app.post("/graph/v1.0/me/events")
    async def graph_create_event(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        body = await request_payload(request)
        args = {
            "title": str(body.get("subject", "Untitled")),
            "start_ms": graph_datetime_to_ms((body.get("start") or {}).get("dateTime")),
            "end_ms": graph_datetime_to_ms((body.get("end") or {}).get("dateTime")),
            "attendees": graph_attendees(body.get("attendees")),
            "location": ((body.get("location") or {}).get("displayName") or None),
            "description": graph_body_content(body.get("body")),
            "organizer": graph_email_address(
                (body.get("organizer") or {}).get("emailAddress")
            ),
        }
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="graph.events.create",
                resolved_tool="calendar.create_event",
                args=args,
                focus_hint="calendar",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return JSONResponse({"id": payload.get("event_id")}, status_code=201)

    @app.get("/salesforce/services/data/v60.0/query")
    async def salesforce_query_route(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        query = request.query_params.get("q", "")
        return JSONResponse(salesforce_query(runtime, request, query))

    @app.get("/salesforce/services/data/v60.0/sobjects/Opportunity/{record_id}")
    async def salesforce_opportunity_get(
        request: Request, record_id: str
    ) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="salesforce.opportunity.get",
                resolved_tool="salesforce.opportunity.get",
                args={"id": record_id},
                focus_hint="crm",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return JSONResponse(salesforce_opportunity(payload))

    @app.post("/salesforce/services/data/v60.0/sobjects/Opportunity")
    async def salesforce_opportunity_create(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        body = await request_payload(request)
        args = {
            "name": str(body.get("Name", "")),
            "amount": float(body.get("Amount", 0) or 0),
            "stage": str(body.get("StageName", "New")),
            "contact_id": body.get("ContactId"),
            "company_id": body.get("AccountId"),
            "close_date": body.get("CloseDate"),
        }
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="salesforce.opportunity.create",
                resolved_tool="salesforce.opportunity.create",
                args=args,
                focus_hint="crm",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return JSONResponse(
            {"id": payload.get("id"), "success": True, "errors": []}, status_code=201
        )

    @app.post("/salesforce/services/data/v60.0/sobjects/Task")
    async def salesforce_task_create(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        body = await request_payload(request)
        args = {
            "kind": "task",
            "deal_id": body.get("WhatId"),
            "contact_id": body.get("WhoId"),
            "note": body.get("Description") or body.get("Subject") or "",
        }
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="salesforce.task.create",
                resolved_tool="salesforce.activity.log",
                args=args,
                focus_hint="crm",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return JSONResponse(
            {"id": payload.get("id"), "success": True, "errors": []}, status_code=201
        )

    @app.post("/slack/api/reactions.add")
    async def slack_reactions_add(request: Request) -> JSONResponse:
        if not slack_auth_ok(request, bundle.gateway.auth_token):
            return JSONResponse({"ok": False, "error": "invalid_auth"})
        body = await request_payload(request)
        channel_name = resolve_slack_channel_name(runtime, str(body.get("channel", "")))
        args = {
            "channel": channel_name,
            "text": f":{body.get('name', 'thumbsup')}:",
            "thread_ts": body.get("timestamp"),
        }
        try:
            dispatch_request(
                runtime,
                request,
                external_tool="slack.reactions.add",
                resolved_tool="slack.send_message",
                args=args,
                focus_hint="slack",
            )
        except Exception as exc:  # noqa: BLE001
            return mirror_route_error_response(exc, surface="slack")
        return JSONResponse({"ok": True})

    @app.get("/slack/api/users.list")
    async def slack_users_list(request: Request) -> JSONResponse:
        if not slack_auth_ok(request, bundle.gateway.auth_token):
            return JSONResponse({"ok": False, "error": "invalid_auth"})
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="slack.users.list",
                resolved_tool="okta.list_users",
                args={},
                focus_hint="slack",
            )
        except Exception as exc:  # noqa: BLE001
            return mirror_route_error_response(exc, surface="slack")
        users = payload if isinstance(payload, list) else payload.get("users", [])
        members = [
            {
                "id": slack_user_id(str(u.get("email", u.get("user_id", "")))),
                "name": str(u.get("login", u.get("email", ""))).split("@")[0],
                "real_name": u.get("display_name", u.get("first_name", "")),
                "profile": {
                    "email": u.get("email", ""),
                    "display_name": u.get("display_name", ""),
                    "title": u.get("title", ""),
                },
            }
            for u in users
        ]
        return JSONResponse({"ok": True, "members": members})

    @app.post("/jira/rest/api/3/issue")
    async def jira_create_issue(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        body = await request_payload(request)
        fields = body.get("fields", body)
        args = {
            "title": str(fields.get("summary", "")),
            "description": str(fields.get("description", "")),
            "assignee": (
                (fields.get("assignee") or {}).get("name", "")
                if isinstance(fields.get("assignee"), dict)
                else str(fields.get("assignee", ""))
            ),
            "priority": (
                (fields.get("priority") or {}).get("name", "P3")
                if isinstance(fields.get("priority"), dict)
                else str(fields.get("priority", "P3"))
            ),
            "labels": fields.get("labels", []),
        }
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="jira.issue.create",
                resolved_tool="jira.create_issue",
                args=args,
                focus_hint="tickets",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        issue_id = str(payload.get("issue_id", payload.get("ticket_id", "")))
        return JSONResponse(
            {"id": issue_id, "key": issue_id, "self": f"/rest/api/3/issue/{issue_id}"},
            status_code=201,
        )

    @app.put("/jira/rest/api/3/issue/{issue_id}")
    async def jira_update_issue(request: Request, issue_id: str) -> Response:
        require_bearer(request, bundle.gateway.auth_token)
        body = await request_payload(request)
        fields = body.get("fields", body)
        args: dict[str, Any] = {"issue_id": issue_id}
        if "summary" in fields:
            args["title"] = str(fields["summary"])
        if "description" in fields:
            args["description"] = str(fields["description"])
        if "assignee" in fields:
            assignee = fields["assignee"]
            args["assignee"] = (
                assignee.get("name", "")
                if isinstance(assignee, dict)
                else str(assignee)
            )
        if "priority" in fields:
            priority = fields["priority"]
            args["priority"] = (
                priority.get("name", "P3")
                if isinstance(priority, dict)
                else str(priority)
            )
        if "labels" in fields:
            args["labels"] = fields["labels"]
        try:
            dispatch_request(
                runtime,
                request,
                external_tool="jira.issue.update",
                resolved_tool="jira.update_issue",
                args=args,
                focus_hint="tickets",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return Response(status_code=204)

    @app.patch("/salesforce/services/data/v60.0/sobjects/Opportunity/{record_id}")
    async def salesforce_opportunity_patch(
        request: Request, record_id: str
    ) -> Response:
        require_bearer(request, bundle.gateway.auth_token)
        body = await request_payload(request)
        args: dict[str, Any] = {"id": record_id}
        if "StageName" in body:
            args["stage"] = str(body["StageName"])
        if "Amount" in body:
            args["amount"] = float(body["Amount"] or 0)
        if "Name" in body:
            args["name"] = str(body["Name"])
        if "CloseDate" in body:
            args["close_date"] = body["CloseDate"]
        try:
            dispatch_request(
                runtime,
                request,
                external_tool="salesforce.opportunity.update",
                resolved_tool="salesforce.opportunity.update",
                args=args,
                focus_hint="crm",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return Response(status_code=204)

    @app.get("/salesforce/services/data/v60.0/sobjects/Contact/{record_id}")
    async def salesforce_contact_get(request: Request, record_id: str) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="salesforce.contact.get",
                resolved_tool="salesforce.contact.get",
                args={"id": record_id},
                focus_hint="crm",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return JSONResponse(salesforce_contact(payload))

    @app.get("/salesforce/services/data/v60.0/sobjects/Account/{record_id}")
    async def salesforce_account_get(request: Request, record_id: str) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="salesforce.account.get",
                resolved_tool="salesforce.account.get",
                args={"id": record_id},
                focus_hint="crm",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return JSONResponse(salesforce_account(payload))
