from __future__ import annotations

from typing import Any, TYPE_CHECKING

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from ._gateway_adapters import (
    dispatch_request,
    http_exception,
    jira_issue,
    jira_project_key,
    jira_search,
    jira_transitions,
    request_payload,
    require_bearer,
)

if TYPE_CHECKING:
    from ._runtime import TwinRuntime


def register_jira_gateway_routes(app: FastAPI, runtime: "TwinRuntime") -> None:
    bundle = runtime.bundle

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
