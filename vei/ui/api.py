from __future__ import annotations

import asyncio
import json
from pathlib import Path
from threading import Thread
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from vei.run.api import (
    build_run_timeline,
    diff_run_snapshots,
    generate_run_id,
    get_run_capability_graphs,
    get_run_orientation,
    launch_workspace_run,
    list_run_manifests,
    list_run_snapshots,
    load_run_contract_evaluation,
    load_run_manifest,
    normalize_runner,
)
from vei import __version__ as vei_version
from vei.verticals.demo import (
    load_workspace_exports_preview,
    load_workspace_story_manifest,
)
from vei.workspace.api import (
    activate_workspace_contract_variant,
    activate_workspace_scenario,
    activate_workspace_scenario_variant,
    list_workspace_contract_variants,
    list_workspace_source_syncs,
    list_workspace_sources,
    load_workspace_generated_scenarios,
    load_workspace_import_report,
    load_workspace_import_review,
    load_workspace_provenance,
    list_workspace_scenario_variants,
    list_workspace_scenarios,
    load_workspace_contract,
    preview_workspace_scenario,
    show_workspace,
)
from vei.workspace.identity import build_identity_flow_summary


class RunLaunchRequest(BaseModel):
    runner: str = "workflow"
    scenario_name: str | None = None
    run_id: str | None = None
    seed: int = 42042
    branch: str | None = None
    model: str | None = None
    provider: str | None = None
    bc_model: str | None = None
    task: str | None = None
    max_steps: int = 12


class ScenarioActivateRequest(BaseModel):
    scenario_name: str | None = None
    variant: str | None = None
    bootstrap_contract: bool = False


class ContractActivateRequest(BaseModel):
    variant: str


def create_ui_app(workspace_root: str | Path) -> FastAPI:
    root = Path(workspace_root).expanduser().resolve()
    static_dir = Path(__file__).with_name("static")
    app = FastAPI(title="VEI UI", version=vei_version)
    app.state.workspace_root = root
    app.state.active_runs = set()

    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    @app.get("/favicon.ico")
    def favicon() -> FileResponse:
        return FileResponse(static_dir / "favicon.svg")

    @app.get("/api/workspace")
    def api_workspace() -> JSONResponse:
        return JSONResponse(show_workspace(root).model_dump(mode="json"))

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

    @app.get("/api/imports/summary")
    def api_import_summary() -> JSONResponse:
        summary = show_workspace(root).imports
        return JSONResponse(summary.model_dump(mode="json") if summary else {})

    @app.get("/api/identity/flow")
    def api_identity_flow() -> JSONResponse:
        try:
            payload = build_identity_flow_summary(root)
        except ValueError:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/imports/sources")
    def api_import_sources() -> JSONResponse:
        return JSONResponse(
            {
                "sources": [
                    item.model_dump(mode="json")
                    for item in list_workspace_sources(root)
                ],
                "syncs": [
                    item.model_dump(mode="json")
                    for item in list_workspace_source_syncs(root)
                ],
            }
        )

    @app.get("/api/imports/normalization")
    def api_import_normalization() -> JSONResponse:
        report = load_workspace_import_report(root)
        return JSONResponse(report.model_dump(mode="json") if report else {})

    @app.get("/api/imports/review")
    def api_import_review() -> JSONResponse:
        review = load_workspace_import_review(root)
        return JSONResponse(review.model_dump(mode="json") if review else {})

    @app.get("/api/imports/scenarios")
    def api_import_scenarios() -> JSONResponse:
        return JSONResponse(
            [
                item.model_dump(mode="json")
                for item in load_workspace_generated_scenarios(root)
            ]
        )

    @app.get("/api/imports/provenance")
    def api_import_provenance(object_ref: str | None = None) -> JSONResponse:
        return JSONResponse(
            [
                item.model_dump(mode="json")
                for item in load_workspace_provenance(root, object_ref)
            ]
        )

    @app.get("/api/scenarios")
    def api_scenarios() -> JSONResponse:
        return JSONResponse(
            [item.model_dump(mode="json") for item in list_workspace_scenarios(root)]
        )

    @app.get("/api/scenario-variants")
    def api_scenario_variants() -> JSONResponse:
        return JSONResponse(list_workspace_scenario_variants(root))

    @app.post("/api/scenarios/activate")
    def api_activate_scenario(request: ScenarioActivateRequest) -> JSONResponse:
        if bool(request.scenario_name) == bool(request.variant):
            raise HTTPException(
                status_code=400,
                detail="Provide exactly one of scenario_name or variant",
            )
        try:
            if request.variant:
                scenario = activate_workspace_scenario_variant(
                    root,
                    request.variant,
                    bootstrap_contract=request.bootstrap_contract,
                )
            else:
                scenario = activate_workspace_scenario(
                    root,
                    request.scenario_name or "",
                    bootstrap_contract=request.bootstrap_contract,
                )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(scenario.model_dump(mode="json"))

    @app.get("/api/contract-variants")
    def api_contract_variants() -> JSONResponse:
        return JSONResponse(list_workspace_contract_variants(root))

    @app.post("/api/contract-variants/activate")
    def api_activate_contract_variant(
        request: ContractActivateRequest,
    ) -> JSONResponse:
        try:
            contract = activate_workspace_contract_variant(root, request.variant)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(contract.model_dump(mode="json"))

    @app.get("/api/scenarios/{scenario_name}/preview")
    def api_scenario_preview(scenario_name: str) -> JSONResponse:
        return JSONResponse(preview_workspace_scenario(root, scenario_name))

    @app.get("/api/scenarios/{scenario_name}/contract")
    def api_contract(scenario_name: str) -> JSONResponse:
        return JSONResponse(
            load_workspace_contract(root, scenario_name).model_dump(mode="json")
        )

    @app.get("/api/runs")
    def api_runs() -> JSONResponse:
        manifests = [
            manifest.model_dump(mode="json") for manifest in list_run_manifests(root)
        ]
        return JSONResponse(manifests)

    @app.post("/api/runs")
    def api_start_run(request: RunLaunchRequest) -> JSONResponse:
        launch = request.model_copy(deep=True)
        try:
            normalized_runner = normalize_runner(launch.runner)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        resolved_run_id = launch.run_id or generate_run_id()
        if (root / "runs" / resolved_run_id).exists():
            raise HTTPException(status_code=409, detail="run_id already exists")

        if normalized_runner == "llm" and not launch.model:
            raise HTTPException(status_code=400, detail="llm runner requires model")
        if normalized_runner == "bc" and not launch.bc_model:
            raise HTTPException(status_code=400, detail="bc runner requires bc_model")

        def _worker() -> None:
            app.state.active_runs.add(resolved_run_id)
            try:
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
            finally:
                app.state.active_runs.discard(resolved_run_id)

        Thread(target=_worker, daemon=True).start()
        return JSONResponse(
            {"ok": True, "run_id": resolved_run_id, "runner": normalized_runner}
        )

    @app.get("/api/runs/{run_id}")
    def api_run(run_id: str) -> JSONResponse:
        path = root / "runs" / run_id / "run_manifest.json"
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
        return JSONResponse(
            diff_run_snapshots(root, run_id, snapshot_from, snapshot_to)
        )

    @app.get("/api/runs/{run_id}/stream")
    async def api_run_stream(run_id: str) -> StreamingResponse:
        manifest_path = root / "runs" / run_id / "run_manifest.json"

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

    return app
