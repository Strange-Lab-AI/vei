from __future__ import annotations

from pathlib import Path
from re import sub
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from vei.whatif import (
    load_world,
    materialize_episode,
    run_counterfactual_experiment,
    search_events,
)
from vei.verticals import (
    load_workspace_exports_preview,
    load_workspace_presentation,
    load_workspace_story_manifest,
)
from vei.workspace.api import show_workspace

from ._api_models import (
    GovernorAgentUpdateRequest,
    GovernorApprovalResolveRequest,
    GovernorSituationActivateRequest,
    OrchestratorApprovalDecisionRequest,
    OrchestratorTaskCommentRequest,
    WhatIfOpenRequest,
    WhatIfRunRequest,
    WhatIfSearchRequest,
    gateway_json_request,
    load_workspace_workforce_payload,
    resolve_whatif_rosetta_dir,
)


def register_workspace_routes(app: FastAPI, root: Path, *, deps: Any) -> None:
    def _resolve_whatif_source(source: str, *, max_events: int | None = None):
        normalized = (source or "enron").strip().lower()
        if normalized != "enron":
            raise HTTPException(status_code=400, detail="only enron is supported today")
        rosetta_dir = resolve_whatif_rosetta_dir(root)
        if rosetta_dir is None:
            raise HTTPException(
                status_code=404,
                detail="enron Rosetta tables are not configured for this workspace",
            )
        try:
            world = load_world(
                source="enron",
                rosetta_dir=rosetta_dir,
                max_events=max_events,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return world, rosetta_dir

    def _whatif_artifacts_root() -> Path:
        path = root / ".artifacts" / "whatif_ui"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _slug(value: str) -> str:
        cleaned = sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
        return cleaned or "whatif"

    @app.get("/api/workspace")
    def api_workspace() -> JSONResponse:
        return JSONResponse(show_workspace(root).model_dump(mode="json"))

    @app.get("/api/workspace/whatif")
    def api_workspace_whatif_status() -> JSONResponse:
        rosetta_dir = resolve_whatif_rosetta_dir(root)
        return JSONResponse(
            {
                "available": rosetta_dir is not None,
                "source": "enron",
                "rosetta_dir": str(rosetta_dir) if rosetta_dir is not None else None,
            }
        )

    @app.post("/api/workspace/whatif/search")
    def api_workspace_whatif_search(request: WhatIfSearchRequest) -> JSONResponse:
        world, rosetta_dir = _resolve_whatif_source(
            request.source,
            max_events=request.max_events,
        )
        result = search_events(
            world,
            actor=request.actor,
            participant=request.participant,
            thread_id=request.thread_id,
            event_type=request.event_type,
            query=request.query,
            flagged_only=request.flagged_only,
            limit=request.limit,
        )
        payload = result.model_dump(mode="json")
        payload["rosetta_dir"] = str(rosetta_dir)
        return JSONResponse(payload)

    @app.post("/api/workspace/whatif/open")
    def api_workspace_whatif_open(request: WhatIfOpenRequest) -> JSONResponse:
        if not request.event_id and not request.thread_id:
            raise HTTPException(
                status_code=400,
                detail="event_id or thread_id is required",
            )
        world, rosetta_dir = _resolve_whatif_source(
            request.source,
            max_events=request.max_events,
        )
        label = request.label or request.event_id or request.thread_id or "episode"
        episode_root = _whatif_artifacts_root() / "episodes" / _slug(label)
        try:
            materialization = materialize_episode(
                world,
                root=episode_root,
                event_id=request.event_id,
                thread_id=request.thread_id,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(
            {
                "source": request.source,
                "rosetta_dir": str(rosetta_dir),
                "episode_root": str(episode_root),
                "materialization": materialization.model_dump(mode="json"),
            }
        )

    @app.post("/api/workspace/whatif/run")
    def api_workspace_whatif_run(request: WhatIfRunRequest) -> JSONResponse:
        if not request.event_id and not request.thread_id:
            raise HTTPException(
                status_code=400,
                detail="event_id or thread_id is required",
            )
        world, rosetta_dir = _resolve_whatif_source(
            request.source,
            max_events=request.max_events,
        )
        try:
            result = run_counterfactual_experiment(
                world,
                artifacts_root=_whatif_artifacts_root() / "experiments",
                label=request.label,
                counterfactual_prompt=request.prompt,
                event_id=request.event_id,
                thread_id=request.thread_id,
                mode=request.mode,
                provider=request.provider,
                model=request.model,
                ejepa_epochs=request.ejepa_epochs,
                ejepa_batch_size=request.ejepa_batch_size,
                ejepa_force_retrain=request.ejepa_force_retrain,
                ejepa_device=request.ejepa_device,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        payload = result.model_dump(mode="json")
        payload["rosetta_dir"] = str(rosetta_dir)
        return JSONResponse(payload)

    @app.get("/api/workspace/governor")
    def api_workspace_governor() -> JSONResponse:
        live_governor_payload: dict[str, Any] | None = None
        live_workforce_payload: dict[str, Any] | None = None
        try:
            live_governor_payload = gateway_json_request(root, path="/api/governor")
        except HTTPException:
            live_governor_payload = None
        try:
            live_workforce_payload = gateway_json_request(root, path="/api/workforce")
        except HTTPException:
            live_workforce_payload = None

        payload = deps.build_workspace_governor_status(
            root,
            governor_payload=live_governor_payload,
            workforce_payload=live_workforce_payload,
        )
        data = payload.model_dump(mode="json")
        governor = data.get("governor")
        if isinstance(governor, dict):
            data = {**governor, **data}
        return JSONResponse(data)

    @app.get("/api/workforce")
    def api_workforce() -> JSONResponse:
        try:
            payload = gateway_json_request(root, path="/api/workforce")
        except HTTPException:
            payload = load_workspace_workforce_payload(root)
        return JSONResponse(payload or {})

    @app.post("/api/workspace/governor/agents")
    def api_workspace_governor_register_agent(
        request: GovernorAgentUpdateRequest,
    ) -> JSONResponse:
        payload = gateway_json_request(
            root,
            path="/api/governor/agents",
            method="POST",
            payload=request.model_dump(exclude_none=True),
        )
        return JSONResponse(payload, status_code=201)

    @app.patch("/api/workspace/governor/agents/{agent_id}")
    def api_workspace_governor_update_agent(
        agent_id: str,
        request: GovernorAgentUpdateRequest,
    ) -> JSONResponse:
        payload = gateway_json_request(
            root,
            path=f"/api/governor/agents/{agent_id}",
            method="PATCH",
            payload=request.model_dump(exclude_none=True),
        )
        return JSONResponse(payload)

    @app.delete("/api/workspace/governor/agents/{agent_id}")
    def api_workspace_governor_remove_agent(agent_id: str) -> JSONResponse:
        payload = gateway_json_request(
            root,
            path=f"/api/governor/agents/{agent_id}",
            method="DELETE",
        )
        return JSONResponse(payload)

    @app.get("/api/workspace/governor/approvals")
    def api_workspace_governor_approvals() -> JSONResponse:
        payload = gateway_json_request(root, path="/api/governor/approvals")
        return JSONResponse(payload)

    @app.post("/api/workspace/governor/approvals/{approval_id}/approve")
    def api_workspace_governor_approve(
        approval_id: str,
        request: GovernorApprovalResolveRequest,
    ) -> JSONResponse:
        payload = gateway_json_request(
            root,
            path=f"/api/governor/approvals/{approval_id}/approve",
            method="POST",
            payload=request.model_dump(),
        )
        return JSONResponse(payload)

    @app.post("/api/workspace/governor/approvals/{approval_id}/reject")
    def api_workspace_governor_reject(
        approval_id: str,
        request: GovernorApprovalResolveRequest,
    ) -> JSONResponse:
        payload = gateway_json_request(
            root,
            path=f"/api/governor/approvals/{approval_id}/reject",
            method="POST",
            payload=request.model_dump(),
        )
        return JSONResponse(payload)

    @app.post("/api/workspace/governor/exercise/activate")
    def api_workspace_governor_activate_situation(
        request: GovernorSituationActivateRequest,
    ) -> JSONResponse:
        try:
            payload = deps.activate_twin_exercise(
                root,
                scenario_variant=request.scenario_variant,
                contract_variant=request.contract_variant,
            )
        except (FileNotFoundError, KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/workspace/governor/finalize")
    def api_workspace_governor_finalize() -> JSONResponse:
        try:
            payload = deps.finalize_twin(root)
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail="twin services are not configured",
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/workspace/governor/reset")
    def api_workspace_governor_reset() -> JSONResponse:
        try:
            payload = deps.reset_twin(root)
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail="twin services are not configured",
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/workspace/governor/sync")
    def api_workspace_governor_sync() -> JSONResponse:
        try:
            payload = deps.sync_twin(root)
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail="twin services are not configured",
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/workspace/governor/orchestrator/agents/{agent_id}/pause")
    def api_workspace_governor_pause_agent(agent_id: str) -> JSONResponse:
        try:
            payload = deps.pause_twin_orchestrator_agent(root, agent_id)
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail="twin services are not configured",
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/workspace/governor/orchestrator/agents/{agent_id}/resume")
    def api_workspace_governor_resume_agent(agent_id: str) -> JSONResponse:
        try:
            payload = deps.resume_twin_orchestrator_agent(root, agent_id)
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail="twin services are not configured",
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/workspace/governor/orchestrator/tasks/{task_id}/comment")
    def api_workspace_governor_comment_on_task(
        task_id: str,
        request: OrchestratorTaskCommentRequest,
    ) -> JSONResponse:
        try:
            payload = deps.comment_on_twin_orchestrator_task(
                root,
                task_id,
                body=request.body,
            )
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail="twin services are not configured",
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/workspace/governor/orchestrator/approvals/{approval_id}/approve")
    def api_workspace_governor_approve_orchestrator(
        approval_id: str,
        request: OrchestratorApprovalDecisionRequest,
    ) -> JSONResponse:
        try:
            payload = deps.approve_twin_orchestrator_approval(
                root,
                approval_id,
                decision_note=request.decision_note,
            )
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail="twin services are not configured",
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/workspace/governor/orchestrator/approvals/{approval_id}/reject")
    def api_workspace_governor_reject_orchestrator(
        approval_id: str,
        request: OrchestratorApprovalDecisionRequest,
    ) -> JSONResponse:
        try:
            payload = deps.reject_twin_orchestrator_approval(
                root,
                approval_id,
                decision_note=request.decision_note,
            )
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail="twin services are not configured",
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post(
        "/api/workspace/governor/orchestrator/approvals/{approval_id}/request-revision"
    )
    def api_workspace_governor_request_orchestrator_revision(
        approval_id: str,
        request: OrchestratorApprovalDecisionRequest,
    ) -> JSONResponse:
        try:
            payload = deps.request_twin_orchestrator_revision(
                root,
                approval_id,
                decision_note=request.decision_note,
            )
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail="twin services are not configured",
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

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

    @app.get("/api/dataset")
    def api_dataset() -> JSONResponse:
        payload = deps.load_workspace_dataset_bundle(root)
        if payload is None:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/fidelity")
    def api_fidelity() -> JSONResponse:
        try:
            payload = deps.get_or_build_workspace_fidelity_report(root)
        except ValueError:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))
