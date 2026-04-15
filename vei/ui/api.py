from __future__ import annotations

import sys
from pathlib import Path
from threading import Thread

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from vei import __version__ as vei_version
from vei.dataset import load_workspace_dataset_bundle
from vei.fidelity import get_or_build_workspace_fidelity_report
from vei.twin.api import (
    activate_twin_exercise,
    approve_twin_orchestrator_approval,
    build_workspace_governor_status,
    comment_on_twin_orchestrator_task,
    finalize_twin,
    pause_twin_orchestrator_agent,
    reject_twin_orchestrator_approval,
    request_twin_orchestrator_revision,
    reset_twin,
    resume_twin_orchestrator_agent,
    sync_twin,
)

from ._api_models import (
    ContextCaptureRequest,
    ContractActivateRequest,
    GovernorAgentUpdateRequest,
    GovernorApprovalResolveRequest,
    GovernorSituationActivateRequest,
    MissionActivateRequest,
    MissionBranchRequest,
    MissionStartRequest,
    OrchestratorApprovalDecisionRequest,
    OrchestratorTaskCommentRequest,
    RunLaunchRequest,
    ScenarioActivateRequest,
    ServiceOpsPolicyReplayRequest,
    WhatIfOpenRequest,
    WhatIfRunRequest,
    WhatIfSearchRequest,
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
    activate_twin_exercise,
    approve_twin_orchestrator_approval,
    build_workspace_governor_status,
    comment_on_twin_orchestrator_task,
    finalize_twin,
    get_or_build_workspace_fidelity_report,
    pause_twin_orchestrator_agent,
    reject_twin_orchestrator_approval,
    request_twin_orchestrator_revision,
    reset_twin,
    resume_twin_orchestrator_agent,
    sync_twin,
)

__all__ = [
    "ContextCaptureRequest",
    "ContractActivateRequest",
    "GovernorAgentUpdateRequest",
    "GovernorApprovalResolveRequest",
    "GovernorSituationActivateRequest",
    "MissionActivateRequest",
    "MissionBranchRequest",
    "MissionStartRequest",
    "OrchestratorApprovalDecisionRequest",
    "OrchestratorTaskCommentRequest",
    "RunLaunchRequest",
    "ScenarioActivateRequest",
    "ServiceOpsPolicyReplayRequest",
    "WhatIfOpenRequest",
    "WhatIfRunRequest",
    "WhatIfSearchRequest",
    "create_ui_app",
]


_STUDIO_MODE = "studio"
_LEGACY_SKIN_ALIASES = {
    "sandbox": _STUDIO_MODE,
    "governor": _STUDIO_MODE,
    "test": _STUDIO_MODE,
    "train": _STUDIO_MODE,
    _STUDIO_MODE: _STUDIO_MODE,
}


def create_ui_app(workspace_root: str | Path, *, skin: str = _STUDIO_MODE) -> FastAPI:
    root = Path(workspace_root).expanduser().resolve()
    static_dir = Path(__file__).with_name("static")
    resolved_skin = _LEGACY_SKIN_ALIASES.get(
        str(skin or "").strip().lower(), _STUDIO_MODE
    )
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

    @app.get("/favicon.ico")
    def favicon() -> FileResponse:
        return FileResponse(static_dir / "favicon.svg")

    register_workspace_routes(app, root, deps=deps)
    register_playable_routes(app, root)
    register_run_routes(app, root, deps=deps)
    register_imports_routes(app, root)
    return app
