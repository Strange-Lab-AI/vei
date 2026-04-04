from __future__ import annotations

import sys
from pathlib import Path
from threading import Thread

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from vei import __version__ as vei_version
from vei.dataset import load_workspace_dataset_bundle
from vei.pilot.exercise import activate_exercise, build_exercise_status
from vei.fidelity import get_or_build_workspace_fidelity_report
from vei.pilot import (
    approve_pilot_orchestrator_approval,
    build_pilot_status,
    comment_on_pilot_orchestrator_task,
    finalize_pilot_run,
    pause_pilot_orchestrator_agent,
    reject_pilot_orchestrator_approval,
    reset_pilot_gateway,
    request_revision_pilot_orchestrator_approval,
    resume_pilot_orchestrator_agent,
    sync_pilot_orchestrator,
)

from ._api_models import (
    ContextCaptureRequest,
    ContractActivateRequest,
    ExerciseActivateRequest,
    MissionActivateRequest,
    MissionBranchRequest,
    MissionStartRequest,
    MirrorAgentUpdateRequest,
    MirrorApprovalResolveRequest,
    OrchestratorApprovalDecisionRequest,
    OrchestratorTaskCommentRequest,
    RunLaunchRequest,
    ScenarioActivateRequest,
    ServiceOpsPolicyReplayRequest,
)
from ._imports_routes import register_imports_routes
from ._playable_routes import register_playable_routes
from ._run_routes import register_run_routes
from ._workspace_routes import register_workspace_routes

# Keep these dependencies bound on the public module so route tests can patch
# them through `vei.ui.api` after the route split.
_PATCHABLE_ROUTE_DEPS = (
    Thread,
    load_workspace_dataset_bundle,
    activate_exercise,
    build_exercise_status,
    get_or_build_workspace_fidelity_report,
    approve_pilot_orchestrator_approval,
    build_pilot_status,
    comment_on_pilot_orchestrator_task,
    finalize_pilot_run,
    pause_pilot_orchestrator_agent,
    reject_pilot_orchestrator_approval,
    reset_pilot_gateway,
    request_revision_pilot_orchestrator_approval,
    resume_pilot_orchestrator_agent,
    sync_pilot_orchestrator,
)

__all__ = [
    "ContextCaptureRequest",
    "ContractActivateRequest",
    "ExerciseActivateRequest",
    "MissionActivateRequest",
    "MissionBranchRequest",
    "MissionStartRequest",
    "MirrorAgentUpdateRequest",
    "MirrorApprovalResolveRequest",
    "OrchestratorApprovalDecisionRequest",
    "OrchestratorTaskCommentRequest",
    "RunLaunchRequest",
    "ScenarioActivateRequest",
    "ServiceOpsPolicyReplayRequest",
    "create_ui_app",
]


_VALID_SKINS = {"sandbox", "mirror", "test", "train"}


def create_ui_app(workspace_root: str | Path, *, skin: str = "sandbox") -> FastAPI:
    root = Path(workspace_root).expanduser().resolve()
    static_dir = Path(__file__).with_name("static")
    resolved_skin = skin if skin in _VALID_SKINS else "sandbox"
    app = FastAPI(title="VEI UI", version=vei_version)
    app.state.workspace_root = root
    app.state.vei_skin = resolved_skin
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    deps = sys.modules[__name__]

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    @app.get("/api/skin")
    def api_skin() -> dict[str, str]:
        return {"skin": app.state.vei_skin}

    @app.get("/pilot")
    def pilot_console() -> FileResponse:
        return FileResponse(static_dir / "pilot.html")

    @app.get("/favicon.ico")
    def favicon() -> FileResponse:
        return FileResponse(static_dir / "favicon.svg")

    register_workspace_routes(app, root, deps=deps)
    register_playable_routes(app, root)
    register_run_routes(app, root, deps=deps)
    register_imports_routes(app, root)
    return app
