from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

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
    request_payload,
    require_bearer,
)

if TYPE_CHECKING:
    from ._runtime import TwinRuntime


def register_graph_gateway_routes(app: FastAPI, runtime: "TwinRuntime") -> None:
    bundle = runtime.bundle

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
