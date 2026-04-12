from __future__ import annotations

from typing import Any, TYPE_CHECKING

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from ._gateway_adapters import (
    dispatch_request,
    http_exception,
    request_payload,
    require_bearer,
    salesforce_account,
    salesforce_contact,
    salesforce_opportunity,
    salesforce_query,
)

if TYPE_CHECKING:
    from ._runtime import TwinRuntime


def register_salesforce_gateway_routes(app: FastAPI, runtime: "TwinRuntime") -> None:
    bundle = runtime.bundle

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
