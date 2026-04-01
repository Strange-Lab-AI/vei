from __future__ import annotations

import asyncio
import json
from pathlib import Path
from threading import Thread
from typing import Any, Mapping

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from vei.dataset import load_workspace_dataset_bundle
from vei.exercise import activate_exercise, build_exercise_status
from vei.fidelity import get_or_build_workspace_fidelity_report
from vei.playable import (
    activate_workspace_playable_mission,
    apply_workspace_mission_move,
    branch_workspace_mission_run,
    build_mission_run_exports,
    export_mission_run,
    finish_workspace_mission_run,
    list_workspace_playable_missions,
    load_workspace_mission_state,
    load_workspace_playable_bundle,
    start_workspace_mission_run,
)
from vei.pilot import (
    build_pilot_status,
    finalize_pilot_run,
    reset_pilot_gateway,
)
from vei.run.api import (
    build_run_timeline,
    diff_run_snapshots,
    generate_run_id,
    get_run_capability_graphs,
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
from vei import __version__ as vei_version
from vei.verticals import (
    load_workspace_exports_preview,
    load_workspace_presentation,
    load_workspace_story_manifest,
)
from vei.workspace import build_identity_flow_summary
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


class MissionActivateRequest(BaseModel):
    mission_name: str
    objective_variant: str | None = None


class MissionStartRequest(BaseModel):
    mission_name: str | None = None
    objective_variant: str | None = None
    run_id: str | None = None
    seed: int = 42042


class MissionBranchRequest(BaseModel):
    branch_name: str | None = None


class ContextCaptureRequest(BaseModel):
    providers: list[str]


class ExerciseActivateRequest(BaseModel):
    scenario_variant: str
    contract_variant: str | None = None


CONTEXT_PROVIDER_ENV_VARS = {
    "slack": "VEI_SLACK_TOKEN",
    "google": "VEI_GOOGLE_TOKEN",
    "jira": "VEI_JIRA_TOKEN",
    "okta": "VEI_OKTA_TOKEN",
    "gmail": "VEI_GMAIL_TOKEN",
    "teams": "VEI_TEAMS_TOKEN",
}

CONTEXT_PROVIDER_BASE_URL_ENV_VARS = {
    "jira": "VEI_JIRA_URL",
    "okta": "VEI_OKTA_ORG_URL",
}


def _build_context_provider_status(
    provider: str,
    env: Mapping[str, str],
) -> dict[str, Any]:
    token_env = CONTEXT_PROVIDER_ENV_VARS[provider]
    if not env.get(token_env):
        return {
            "provider": provider,
            "configured": False,
            "env_var": token_env,
        }

    base_url_env = CONTEXT_PROVIDER_BASE_URL_ENV_VARS.get(provider)
    if base_url_env and not env.get(base_url_env):
        return {
            "provider": provider,
            "configured": False,
            "env_var": base_url_env,
        }

    return {
        "provider": provider,
        "configured": True,
        "env_var": token_env,
    }


def _context_capture_org_name(workspace_root: Path) -> str:
    workspace = show_workspace(workspace_root)
    return workspace.manifest.title or workspace.manifest.name or "Unknown"


def create_ui_app(workspace_root: str | Path) -> FastAPI:
    root = Path(workspace_root).expanduser().resolve()
    static_dir = Path(__file__).with_name("static")
    app = FastAPI(title="VEI UI", version=vei_version)
    app.state.workspace_root = root
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    @app.get("/pilot")
    def pilot_console() -> FileResponse:
        return FileResponse(static_dir / "pilot.html")

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

    @app.get("/api/presentation")
    def api_presentation() -> JSONResponse:
        payload = load_workspace_presentation(root)
        return JSONResponse(payload.model_dump(mode="json") if payload else {})

    @app.get("/api/playable")
    def api_playable() -> JSONResponse:
        payload = load_workspace_playable_bundle(root)
        return JSONResponse(payload or {})

    @app.get("/api/pilot")
    def api_pilot() -> JSONResponse:
        try:
            payload = build_pilot_status(root)
        except FileNotFoundError:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/exercise")
    def api_exercise() -> JSONResponse:
        try:
            payload = build_exercise_status(root)
        except FileNotFoundError:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/exercise/activate")
    def api_exercise_activate(request: ExerciseActivateRequest) -> JSONResponse:
        try:
            payload = activate_exercise(
                root,
                scenario_variant=request.scenario_variant,
                contract_variant=request.contract_variant,
            )
        except (FileNotFoundError, KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/dataset")
    def api_dataset() -> JSONResponse:
        payload = load_workspace_dataset_bundle(root)
        if payload is None:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/pilot/finalize")
    def api_pilot_finalize() -> JSONResponse:
        try:
            payload = finalize_pilot_run(root)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="pilot stack is not configured")
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/pilot/reset")
    def api_pilot_reset() -> JSONResponse:
        try:
            payload = reset_pilot_gateway(root)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="pilot stack is not configured")
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/fidelity")
    def api_fidelity() -> JSONResponse:
        try:
            payload = get_or_build_workspace_fidelity_report(root)
        except ValueError:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/missions")
    def api_missions() -> JSONResponse:
        return JSONResponse(
            [
                item.model_dump(mode="json")
                for item in list_workspace_playable_missions(root)
            ]
        )

    @app.post("/api/missions/activate")
    def api_activate_mission(request: MissionActivateRequest) -> JSONResponse:
        try:
            payload = activate_workspace_playable_mission(
                root,
                request.mission_name,
                objective_variant=request.objective_variant,
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload)

    @app.get("/api/missions/state")
    def api_mission_state(run_id: str | None = None) -> JSONResponse:
        if run_id is None:
            bundle = load_workspace_playable_bundle(root)
            if bundle is not None and not bundle.get("run_id"):
                return JSONResponse({})
        payload = load_workspace_mission_state(root, run_id)
        return JSONResponse(payload.model_dump(mode="json") if payload else {})

    @app.post("/api/missions/start")
    def api_start_mission(request: MissionStartRequest) -> JSONResponse:
        try:
            mission_name = request.mission_name
            if mission_name is None:
                missions = list_workspace_playable_missions(root)
                if not missions:
                    raise ValueError("playable missions require a vertical workspace")
                mission_name = missions[0].mission_name
            payload = start_workspace_mission_run(
                root,
                mission_name=mission_name,
                objective_variant=request.objective_variant,
                run_id=request.run_id,
                seed=request.seed,
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/missions/{run_id}/moves/{move_id}")
    def api_apply_mission_move(run_id: str, move_id: str) -> JSONResponse:
        try:
            payload = apply_workspace_mission_move(root, run_id=run_id, move_id=move_id)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/missions/{run_id}/branch")
    def api_branch_mission_run(
        run_id: str,
        request: MissionBranchRequest,
    ) -> JSONResponse:
        try:
            payload = branch_workspace_mission_run(
                root,
                run_id=run_id,
                branch_name=request.branch_name,
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/missions/{run_id}/finish")
    def api_finish_mission_run(run_id: str) -> JSONResponse:
        try:
            payload = finish_workspace_mission_run(root, run_id=run_id)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/missions/{run_id}/exports")
    def api_mission_exports(run_id: str) -> JSONResponse:
        state = load_workspace_mission_state(root, run_id)
        if state is None:
            raise HTTPException(status_code=404, detail="mission run not found")
        payload = [
            item.model_dump(mode="json")
            for item in build_mission_run_exports(root, state)
        ]
        return JSONResponse(payload)

    @app.get("/api/missions/{run_id}/exports/{export_name}")
    def api_mission_export(run_id: str, export_name: str) -> JSONResponse:
        try:
            payload = export_mission_run(root, run_id=run_id, export_format=export_name)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(payload)

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
        if get_workspace_run_dir(root, resolved_run_id).exists():
            raise HTTPException(status_code=409, detail="run_id already exists")

        if normalized_runner == "llm" and not launch.model:
            raise HTTPException(status_code=400, detail="llm runner requires model")
        if normalized_runner == "bc" and not launch.bc_model:
            raise HTTPException(status_code=400, detail="bc runner requires bc_model")

        def _worker() -> None:
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

        Thread(target=_worker, daemon=True).start()
        return JSONResponse(
            {"ok": True, "run_id": resolved_run_id, "runner": normalized_runner}
        )

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

    # --- Context capture endpoints ---

    @app.get("/api/context/status")
    def api_context_status() -> JSONResponse:
        import os

        providers = [
            _build_context_provider_status(name, os.environ)
            for name in CONTEXT_PROVIDER_ENV_VARS
        ]
        return JSONResponse({"providers": providers})

    @app.post("/api/context/capture")
    def api_context_capture(req: ContextCaptureRequest) -> JSONResponse:
        import os

        from vei.context.api import capture_context
        from vei.context.models import ContextProviderConfig

        configs = []
        for name in req.providers:
            name = name.strip().lower()
            token_env = CONTEXT_PROVIDER_ENV_VARS.get(name)
            if not token_env or not os.environ.get(token_env):
                raise HTTPException(
                    status_code=400,
                    detail=f"provider {name}: missing token ({token_env})",
                )
            base_url_env = CONTEXT_PROVIDER_BASE_URL_ENV_VARS.get(name)
            if base_url_env and not os.environ.get(base_url_env):
                raise HTTPException(
                    status_code=400,
                    detail=f"provider {name}: missing base URL ({base_url_env})",
                )
            base_url = os.environ.get(base_url_env) if base_url_env else None
            configs.append(
                ContextProviderConfig(
                    provider=name,  # type: ignore[arg-type]
                    token_env=token_env,
                    base_url=base_url,
                )
            )

        snapshot = capture_context(
            configs,
            organization_name=_context_capture_org_name(root),
            organization_domain="",
        )

        out_path = root / "context_snapshot.json"
        out_path.write_text(
            snapshot.model_dump_json(indent=2),
            encoding="utf-8",
        )

        ok_count = sum(1 for s in snapshot.sources if s.status == "ok")
        err_count = sum(1 for s in snapshot.sources if s.status == "error")
        errors = [
            {"provider": s.provider, "error": s.error}
            for s in snapshot.sources
            if s.status == "error"
        ]

        return JSONResponse(
            {
                "captured": ok_count,
                "errors": err_count,
                "error_details": errors,
                "snapshot_path": str(out_path),
                "sources": [
                    {
                        "provider": s.provider,
                        "status": s.status,
                        "record_counts": s.record_counts,
                    }
                    for s in snapshot.sources
                ],
            }
        )

    return app
