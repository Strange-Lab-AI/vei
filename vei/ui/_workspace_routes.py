from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from vei.verticals import (
    load_workspace_exports_preview,
    load_workspace_presentation,
    load_workspace_story_manifest,
)
from vei.workspace.api import show_workspace

from ._api_models import (
    ExerciseActivateRequest,
    MirrorAgentUpdateRequest,
    MirrorApprovalResolveRequest,
    OrchestratorApprovalDecisionRequest,
    OrchestratorTaskCommentRequest,
    gateway_json_request,
    load_workspace_workforce_payload,
    load_workspace_mirror_payload,
)


def register_workspace_routes(app: FastAPI, root: Path, *, deps: Any) -> None:
    @app.get("/api/workspace")
    def api_workspace() -> JSONResponse:
        return JSONResponse(show_workspace(root).model_dump(mode="json"))

    @app.get("/api/workspace/mirror")
    def api_workspace_mirror() -> JSONResponse:
        try:
            payload = gateway_json_request(root, path="/api/mirror")
        except HTTPException:
            payload = load_workspace_mirror_payload(root)
        return JSONResponse(payload or {})

    @app.get("/api/workforce")
    def api_workforce() -> JSONResponse:
        try:
            payload = gateway_json_request(root, path="/api/workforce")
        except HTTPException:
            payload = load_workspace_workforce_payload(root)
        return JSONResponse(payload or {})

    @app.post("/api/workspace/mirror/agents")
    def api_workspace_mirror_register_agent(
        request: MirrorAgentUpdateRequest,
    ) -> JSONResponse:
        payload = gateway_json_request(
            root,
            path="/api/mirror/agents",
            method="POST",
            payload=request.model_dump(exclude_none=True),
        )
        return JSONResponse(payload, status_code=201)

    @app.patch("/api/workspace/mirror/agents/{agent_id}")
    def api_workspace_mirror_update_agent(
        agent_id: str,
        request: MirrorAgentUpdateRequest,
    ) -> JSONResponse:
        payload = gateway_json_request(
            root,
            path=f"/api/mirror/agents/{agent_id}",
            method="PATCH",
            payload=request.model_dump(exclude_none=True),
        )
        return JSONResponse(payload)

    @app.delete("/api/workspace/mirror/agents/{agent_id}")
    def api_workspace_mirror_remove_agent(agent_id: str) -> JSONResponse:
        payload = gateway_json_request(
            root,
            path=f"/api/mirror/agents/{agent_id}",
            method="DELETE",
        )
        return JSONResponse(payload)

    @app.get("/api/workspace/mirror/approvals")
    def api_workspace_mirror_approvals() -> JSONResponse:
        payload = gateway_json_request(root, path="/api/mirror/approvals")
        return JSONResponse(payload)

    @app.post("/api/workspace/mirror/approvals/{approval_id}/approve")
    def api_workspace_mirror_approve(
        approval_id: str,
        request: MirrorApprovalResolveRequest,
    ) -> JSONResponse:
        payload = gateway_json_request(
            root,
            path=f"/api/mirror/approvals/{approval_id}/approve",
            method="POST",
            payload=request.model_dump(),
        )
        return JSONResponse(payload)

    @app.post("/api/workspace/mirror/approvals/{approval_id}/reject")
    def api_workspace_mirror_reject(
        approval_id: str,
        request: MirrorApprovalResolveRequest,
    ) -> JSONResponse:
        payload = gateway_json_request(
            root,
            path=f"/api/mirror/approvals/{approval_id}/reject",
            method="POST",
            payload=request.model_dump(),
        )
        return JSONResponse(payload)

    @app.get("/api/story")
    def api_story() -> JSONResponse:
        payload = load_workspace_story_manifest(root)
        return JSONResponse(payload.model_dump(mode="json") if payload else {})

    @app.get("/api/exports-preview")
    def api_exports_preview() -> JSONResponse:
        return JSONResponse(
            [
                item.model_dump(mode="json")
                for item in load_workspace_exports_preview(root)
            ]
        )

    @app.get("/api/presentation")
    def api_presentation() -> JSONResponse:
        payload = load_workspace_presentation(root)
        return JSONResponse(payload.model_dump(mode="json") if payload else {})

    @app.get("/api/pilot")
    def api_pilot() -> JSONResponse:
        try:
            payload = deps.build_pilot_status(root)
        except FileNotFoundError:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/exercise")
    def api_exercise() -> JSONResponse:
        try:
            payload = deps.build_exercise_status(root)
        except FileNotFoundError:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/exercise/activate")
    def api_exercise_activate(request: ExerciseActivateRequest) -> JSONResponse:
        try:
            payload = deps.activate_exercise(
                root,
                scenario_variant=request.scenario_variant,
                contract_variant=request.contract_variant,
            )
        except (FileNotFoundError, KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/dataset")
    def api_dataset() -> JSONResponse:
        payload = deps.load_workspace_dataset_bundle(root)
        if payload is None:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/pilot/finalize")
    def api_pilot_finalize() -> JSONResponse:
        try:
            payload = deps.finalize_pilot_run(root)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="pilot stack is not configured")
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/pilot/reset")
    def api_pilot_reset() -> JSONResponse:
        try:
            payload = deps.reset_pilot_gateway(root)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="pilot stack is not configured")
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/pilot/orchestrator/sync")
    def api_pilot_orchestrator_sync() -> JSONResponse:
        try:
            payload = deps.sync_pilot_orchestrator(root)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="pilot stack is not configured")
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/pilot/orchestrator/agents/{agent_id}/pause")
    def api_pilot_orchestrator_pause(agent_id: str) -> JSONResponse:
        try:
            payload = deps.pause_pilot_orchestrator_agent(root, agent_id)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="pilot stack is not configured")
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/pilot/orchestrator/agents/{agent_id}/resume")
    def api_pilot_orchestrator_resume(agent_id: str) -> JSONResponse:
        try:
            payload = deps.resume_pilot_orchestrator_agent(root, agent_id)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="pilot stack is not configured")
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/pilot/orchestrator/tasks/{task_id}/comment")
    def api_pilot_orchestrator_task_comment(
        task_id: str,
        request: OrchestratorTaskCommentRequest,
    ) -> JSONResponse:
        try:
            payload = deps.comment_on_pilot_orchestrator_task(
                root,
                task_id,
                body=request.body,
            )
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="pilot stack is not configured")
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/pilot/orchestrator/approvals/{approval_id}/approve")
    def api_pilot_orchestrator_approval_approve(
        approval_id: str,
        request: OrchestratorApprovalDecisionRequest,
    ) -> JSONResponse:
        try:
            payload = deps.approve_pilot_orchestrator_approval(
                root,
                approval_id,
                decision_note=request.decision_note,
            )
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="pilot stack is not configured")
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/pilot/orchestrator/approvals/{approval_id}/reject")
    def api_pilot_orchestrator_approval_reject(
        approval_id: str,
        request: OrchestratorApprovalDecisionRequest,
    ) -> JSONResponse:
        try:
            payload = deps.reject_pilot_orchestrator_approval(
                root,
                approval_id,
                decision_note=request.decision_note,
            )
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="pilot stack is not configured")
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/pilot/orchestrator/approvals/{approval_id}/request-revision")
    def api_pilot_orchestrator_approval_request_revision(
        approval_id: str,
        request: OrchestratorApprovalDecisionRequest,
    ) -> JSONResponse:
        try:
            payload = deps.request_revision_pilot_orchestrator_approval(
                root,
                approval_id,
                decision_note=request.decision_note,
            )
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="pilot stack is not configured")
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/fidelity")
    def api_fidelity() -> JSONResponse:
        try:
            payload = deps.get_or_build_workspace_fidelity_report(root)
        except ValueError:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))
