from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from vei.whatif.filenames import CONTEXT_SNAPSHOT_FILE
from vei.workspace import build_identity_flow_summary
from vei.workspace.api import (
    activate_workspace_contract_variant,
    activate_workspace_scenario,
    activate_workspace_scenario_variant,
    list_workspace_contract_variants,
    list_workspace_source_syncs,
    list_workspace_sources,
    load_workspace_contract,
    load_workspace_generated_scenarios,
    load_workspace_import_report,
    load_workspace_import_review,
    load_workspace_provenance,
    list_workspace_scenario_variants,
    list_workspace_scenarios,
    preview_workspace_scenario,
    show_workspace,
)

from ._api_models import (
    CONTEXT_PROVIDER_BASE_URL_ENV_VARS,
    CONTEXT_PROVIDER_ENV_VARS,
    ContextCaptureRequest,
    ContractActivateRequest,
    ScenarioActivateRequest,
    build_context_provider_status,
    context_capture_org_name,
)
from ._root_mode import root_has_workspace


def register_imports_routes(app: FastAPI, root: Path) -> None:
    @app.get("/api/imports/summary")
    def api_import_summary() -> JSONResponse:
        if not root_has_workspace(root):
            return JSONResponse({})
        summary = show_workspace(root).imports
        return JSONResponse(summary.model_dump(mode="json") if summary else {})

    @app.get("/api/identity/flow")
    def api_identity_flow() -> JSONResponse:
        if not root_has_workspace(root):
            return JSONResponse({})
        try:
            payload = build_identity_flow_summary(root)
        except ValueError:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/imports/sources")
    def api_import_sources() -> JSONResponse:
        if not root_has_workspace(root):
            return JSONResponse({"sources": [], "syncs": []})
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
        if not root_has_workspace(root):
            return JSONResponse({})
        report = load_workspace_import_report(root)
        return JSONResponse(report.model_dump(mode="json") if report else {})

    @app.get("/api/imports/review")
    def api_import_review() -> JSONResponse:
        if not root_has_workspace(root):
            return JSONResponse({})
        review = load_workspace_import_review(root)
        return JSONResponse(review.model_dump(mode="json") if review else {})

    @app.get("/api/imports/scenarios")
    def api_import_scenarios() -> JSONResponse:
        if not root_has_workspace(root):
            return JSONResponse([])
        return JSONResponse(
            [
                item.model_dump(mode="json")
                for item in load_workspace_generated_scenarios(root)
            ]
        )

    @app.get("/api/imports/provenance")
    def api_import_provenance(object_ref: str | None = None) -> JSONResponse:
        if not root_has_workspace(root):
            return JSONResponse([])
        return JSONResponse(
            [
                item.model_dump(mode="json")
                for item in load_workspace_provenance(root, object_ref)
            ]
        )

    @app.get("/api/scenarios")
    def api_scenarios() -> JSONResponse:
        if not root_has_workspace(root):
            return JSONResponse([])
        return JSONResponse(
            [item.model_dump(mode="json") for item in list_workspace_scenarios(root)]
        )

    @app.get("/api/scenario-variants")
    def api_scenario_variants() -> JSONResponse:
        if not root_has_workspace(root):
            return JSONResponse([])
        return JSONResponse(list_workspace_scenario_variants(root))

    @app.post("/api/scenarios/activate")
    def api_activate_scenario(request: ScenarioActivateRequest) -> JSONResponse:
        if not root_has_workspace(root):
            raise HTTPException(
                status_code=404,
                detail="scenario controls are unavailable for benchmark roots",
            )
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
        if not root_has_workspace(root):
            return JSONResponse([])
        return JSONResponse(list_workspace_contract_variants(root))

    @app.post("/api/contract-variants/activate")
    def api_activate_contract_variant(
        request: ContractActivateRequest,
    ) -> JSONResponse:
        if not root_has_workspace(root):
            raise HTTPException(
                status_code=404,
                detail="contract controls are unavailable for benchmark roots",
            )
        try:
            contract = activate_workspace_contract_variant(root, request.variant)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(contract.model_dump(mode="json"))

    @app.get("/api/scenarios/{scenario_name}/preview")
    def api_scenario_preview(scenario_name: str) -> JSONResponse:
        if not root_has_workspace(root):
            raise HTTPException(
                status_code=404,
                detail="scenario previews are unavailable for benchmark roots",
            )
        return JSONResponse(preview_workspace_scenario(root, scenario_name))

    @app.get("/api/scenarios/{scenario_name}/contract")
    def api_contract(scenario_name: str) -> JSONResponse:
        if not root_has_workspace(root):
            raise HTTPException(
                status_code=404,
                detail="scenario contracts are unavailable for benchmark roots",
            )
        return JSONResponse(
            load_workspace_contract(root, scenario_name).model_dump(mode="json")
        )

    @app.get("/api/context/status")
    def api_context_status() -> JSONResponse:
        import os

        providers = [
            build_context_provider_status(name, os.environ)
            for name in CONTEXT_PROVIDER_ENV_VARS
        ]
        return JSONResponse({"providers": providers})

    @app.post("/api/context/capture")
    def api_context_capture(req: ContextCaptureRequest) -> JSONResponse:
        import os

        from vei.context.api import capture_context
        from vei.context.api import ContextProviderConfig
        from vei.context.api import write_canonical_history_sidecars

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
            organization_name=context_capture_org_name(root),
            organization_domain="",
        )

        out_path = root / CONTEXT_SNAPSHOT_FILE
        out_path.write_text(
            snapshot.model_dump_json(indent=2),
            encoding="utf-8",
        )
        write_canonical_history_sidecars(snapshot, out_path)

        ok_count = sum(1 for source in snapshot.sources if source.status == "ok")
        err_count = sum(1 for source in snapshot.sources if source.status == "error")
        errors = [
            {"provider": source.provider, "error": source.error}
            for source in snapshot.sources
            if source.status == "error"
        ]

        return JSONResponse(
            {
                "captured": ok_count,
                "errors": err_count,
                "error_details": errors,
                "snapshot_path": str(out_path),
                "sources": [
                    {
                        "provider": source.provider,
                        "status": source.status,
                        "record_counts": source.record_counts,
                    }
                    for source in snapshot.sources
                ],
            }
        )
