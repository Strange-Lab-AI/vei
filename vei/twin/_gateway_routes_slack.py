from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from ._gateway_adapters import (
    dispatch_request,
    mirror_route_error_response,
    request_payload,
    resolve_slack_channel_name,
    slack_auth_ok,
    slack_channel,
    slack_channel_id,
    slack_message,
    slack_user_id,
)

if TYPE_CHECKING:
    from ._runtime import TwinRuntime


def register_slack_gateway_routes(app: FastAPI, runtime: "TwinRuntime") -> None:
    bundle = runtime.bundle

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
                "id": slack_user_id(str(user.get("email", user.get("user_id", "")))),
                "name": str(user.get("login", user.get("email", ""))).split("@")[0],
                "real_name": user.get("display_name", user.get("first_name", "")),
                "profile": {
                    "email": user.get("email", ""),
                    "display_name": user.get("display_name", ""),
                    "title": user.get("title", ""),
                },
            }
            for user in users
        ]
        return JSONResponse({"ok": True, "members": members})
