from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from vei.governor import GovernorAgentSpec, GovernorIngestEvent
from vei.workforce.api import WorkforceCommandRecord, WorkforceState
from vei.run.api import build_run_timeline, get_run_surface_state

from ._gateway_adapters import request_payload, require_bearer

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

    supported_surfaces = {item.name for item in bundle.gateway.surfaces}
    registered_surfaces: set[str] = set()
    for entry in runtime.session.router.facade_plugins.values():
        plugin = entry.plugin
        registrar = plugin.gateway_route_registrar
        if registrar is None:
            continue
        surface_names = {
            binding.name
            for binding in plugin.gateway_surfaces
            if binding.name in supported_surfaces
        }
        if not surface_names or surface_names <= registered_surfaces:
            continue
        registrar(app, runtime)
        registered_surfaces.update(surface_names)
