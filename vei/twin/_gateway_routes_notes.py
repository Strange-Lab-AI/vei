from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from ._gateway_adapters import (
    dispatch_request,
    http_exception,
    request_payload,
    require_bearer,
)

if TYPE_CHECKING:
    from ._runtime import TwinRuntime


def register_notes_gateway_routes(app: FastAPI, runtime: "TwinRuntime") -> None:
    bundle = runtime.bundle

    @app.get("/notes/api/entries")
    async def notes_entries(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="notes.entries.list",
                resolved_tool="notes.list_entries",
                args={"tag": request.query_params.get("tag")},
                focus_hint="notes",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return JSONResponse({"items": payload})

    @app.get("/notes/api/entries/{entry_id}")
    async def notes_entry_get(request: Request, entry_id: str) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="notes.entry.get",
                resolved_tool="notes.get_entry",
                args={"entry_id": entry_id},
                focus_hint="notes",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return JSONResponse(payload)

    @app.post("/notes/api/entries")
    async def notes_entry_create(request: Request) -> JSONResponse:
        require_bearer(request, bundle.gateway.auth_token)
        body = await request_payload(request)
        try:
            payload = dispatch_request(
                runtime,
                request,
                external_tool="notes.entry.create",
                resolved_tool="notes.create_entry",
                args={
                    "title": str(body.get("title", "")),
                    "body": str(body.get("body", "")),
                    "tags": list(body.get("tags") or []),
                },
                focus_hint="notes",
            )
        except Exception as exc:  # noqa: BLE001
            raise http_exception(exc) from exc
        return JSONResponse(payload, status_code=201)
