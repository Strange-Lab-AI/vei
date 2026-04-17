from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

from vei.run.api import (
    build_run_timeline,
    diff_cross_run_snapshots,
    diff_run_snapshots,
    generate_run_id,
    get_run_capability_graphs,
    get_run_knowledge_state,
    get_run_orientation,
    get_run_surface_state,
    get_workspace_run_dir,
    get_workspace_run_manifest_path,
    launch_workspace_run,
    list_run_manifests,
    list_run_snapshots,
    load_run_contract_evaluation,
    load_run_manifest,
    normalize_runner,
)

from ._api_models import RunLaunchRequest
from ._root_mode import root_has_workspace


def register_run_routes(app: FastAPI, root: Path, *, deps: Any) -> None:
    @app.get("/api/runs")
    def api_runs() -> JSONResponse:
        if not root_has_workspace(root):
            return JSONResponse([])
        manifests = [
            manifest.model_dump(mode="json") for manifest in list_run_manifests(root)
        ]
        return JSONResponse(manifests)

    @app.post("/api/runs")
    def api_start_run(request: RunLaunchRequest) -> JSONResponse:
        if not root_has_workspace(root):
            raise HTTPException(
                status_code=404,
                detail="run controls are unavailable for benchmark roots",
            )
        launch = request.model_copy(deep=True)
        try:
            normalized_runner = normalize_runner(launch.runner)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        resolved_run_id = launch.run_id or generate_run_id()
        if get_workspace_run_dir(root, resolved_run_id).exists():
            raise HTTPException(status_code=409, detail="run_id already exists")

        if normalized_runner == "llm" and not launch.model:
            raise HTTPException(status_code=400, detail="llm runner requires model")
        if normalized_runner == "bc" and not launch.bc_model:
            raise HTTPException(status_code=400, detail="bc runner requires bc_model")

        def worker() -> None:
            launch_workspace_run(
                root,
                runner=normalized_runner,
                scenario_name=launch.scenario_name,
                run_id=resolved_run_id,
                seed=launch.seed,
                branch=launch.branch,
                model=launch.model,
                provider=launch.provider,
                bc_model_path=launch.bc_model,
                task=launch.task,
                max_steps=launch.max_steps,
            )

        deps.Thread(target=worker, daemon=True).start()
        return JSONResponse(
            {"ok": True, "run_id": resolved_run_id, "runner": normalized_runner}
        )

    @app.get("/api/runs/diff-cross")
    def api_runs_diff_cross(
        run_a: str,
        snap_a: int,
        run_b: str,
        snap_b: int,
    ) -> JSONResponse:
        try:
            payload = diff_cross_run_snapshots(root, run_a, snap_a, run_b, snap_b)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload)

    @app.get("/api/runs/{run_id}")
    def api_run(run_id: str) -> JSONResponse:
        path = get_workspace_run_manifest_path(root, run_id)
        if not path.exists():
            raise HTTPException(status_code=404, detail="run not found")
        return JSONResponse(load_run_manifest(path).model_dump(mode="json"))

    @app.get("/api/runs/{run_id}/timeline")
    def api_run_timeline(run_id: str) -> JSONResponse:
        payload = [
            item.model_dump(mode="json") for item in build_run_timeline(root, run_id)
        ]
        return JSONResponse(payload)

    @app.get("/api/runs/{run_id}/events")
    def api_run_events(run_id: str) -> JSONResponse:
        return api_run_timeline(run_id)

    @app.get("/api/runs/{run_id}/orientation")
    def api_run_orientation(run_id: str) -> JSONResponse:
        return JSONResponse(get_run_orientation(root, run_id))

    @app.get("/api/runs/{run_id}/graphs")
    def api_run_graphs(run_id: str) -> JSONResponse:
        return JSONResponse(get_run_capability_graphs(root, run_id))

    @app.get("/api/runs/{run_id}/surfaces")
    def api_run_surfaces(run_id: str) -> JSONResponse:
        try:
            payload = get_run_surface_state(root, run_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/runs/{run_id}/knowledge")
    def api_run_knowledge(run_id: str) -> JSONResponse:
        return JSONResponse(get_run_knowledge_state(root, run_id))

    @app.get("/api/runs/{run_id}/snapshots")
    def api_run_snapshots(run_id: str) -> JSONResponse:
        return JSONResponse(
            [
                snapshot.model_dump(mode="json")
                for snapshot in list_run_snapshots(root, run_id)
            ]
        )

    @app.get("/api/runs/{run_id}/contract")
    def api_run_contract(run_id: str) -> JSONResponse:
        payload = load_run_contract_evaluation(root, run_id)
        if payload is None:
            payload = {"ok": None, "issues": [], "metadata": {}}
        return JSONResponse(payload)

    @app.get("/api/runs/{run_id}/receipts")
    def api_run_receipts(run_id: str) -> JSONResponse:
        events = build_run_timeline(root, run_id)
        receipts = [
            event.model_dump(mode="json") for event in events if event.kind == "receipt"
        ]
        return JSONResponse(receipts)

    @app.get("/api/runs/{run_id}/diff")
    def api_run_diff(run_id: str, snapshot_from: int, snapshot_to: int) -> JSONResponse:
        try:
            payload = diff_run_snapshots(root, run_id, snapshot_from, snapshot_to)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload)

    @app.get("/api/runs/{run_id}/stream")
    async def api_run_stream(run_id: str) -> StreamingResponse:
        manifest_path = get_workspace_run_manifest_path(root, run_id)

        async def event_iter():
            last_payload = None
            while True:
                payload: dict[str, Any] = {
                    "run_id": run_id,
                    "manifest": None,
                    "timeline": [],
                }
                if manifest_path.exists():
                    payload["manifest"] = json.loads(
                        manifest_path.read_text(encoding="utf-8")
                    )
                payload["timeline"] = [
                    item.model_dump(mode="json")
                    for item in build_run_timeline(root, run_id)
                ]
                if payload != last_payload:
                    yield f"data: {json.dumps(payload)}\n\n"
                    last_payload = payload
                manifest = payload.get("manifest") or {}
                if manifest.get("completed_at"):
                    break
                await asyncio.sleep(1.0)

        return StreamingResponse(event_iter(), media_type="text/event-stream")
