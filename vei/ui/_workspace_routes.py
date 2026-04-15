from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
from re import sub
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from vei.whatif import (
    build_decision_scene,
    build_saved_decision_scene,
    default_forecast_backend,
    list_objective_packs,
    load_branch_point_benchmark_build_result,
    load_branch_point_benchmark_judge_result,
    load_world,
    materialize_episode,
    run_counterfactual_experiment,
    run_ranked_counterfactual_experiment,
    search_events,
)
from vei.whatif.models import (
    WhatIfAuditRecord,
    WhatIfCandidateIntervention,
    WhatIfJudgedPairwiseComparison,
)
from vei.verticals import (
    load_workspace_exports_preview,
    load_workspace_presentation,
    load_workspace_story_manifest,
)

from ._api_models import (
    AuditSubmitRequest,
    GovernorAgentUpdateRequest,
    GovernorApprovalResolveRequest,
    GovernorSituationActivateRequest,
    OrchestratorApprovalDecisionRequest,
    OrchestratorTaskCommentRequest,
    WhatIfOpenRequest,
    WhatIfRankRequest,
    WhatIfRunRequest,
    WhatIfSceneRequest,
    WhatIfSearchRequest,
    gateway_json_request,
    load_workspace_historical_summary,
    load_workspace_workforce_payload,
    resolve_whatif_rosetta_dir,
    resolve_whatif_source_path,
)
from ._root_mode import load_ui_workspace_summary


def register_workspace_routes(app: FastAPI, root: Path, *, deps: Any) -> None:
    def _saved_whatif_bundle_root() -> Path | None:
        if root.name != "workspace":
            return None
        bundle_root = root.parent
        if not (bundle_root / "whatif_experiment_result.json").exists():
            return None
        return bundle_root

    def _load_saved_whatif_json(filename: str) -> dict[str, Any] | None:
        bundle_root = _saved_whatif_bundle_root()
        if bundle_root is None:
            return None
        path = bundle_root / filename
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def _saved_historical_request_matches(
        *,
        event_id: str | None = None,
        thread_id: str | None = None,
    ) -> Any | None:
        historical = load_workspace_historical_summary(root)
        if historical is None:
            return None
        if event_id and event_id != historical.branch_event_id:
            return None
        if thread_id and thread_id != historical.thread_id:
            return None
        return historical

    def _can_use_saved_enron_bundle(
        *,
        event_id: str | None = None,
        thread_id: str | None = None,
    ) -> bool:
        historical = _saved_historical_request_matches(
            event_id=event_id,
            thread_id=thread_id,
        )
        if historical is None or historical.source != "enron":
            return False
        if resolve_whatif_rosetta_dir(root) is not None:
            return False
        return _saved_whatif_bundle_root() is not None

    def _saved_source_dir_text() -> str:
        saved_snapshot = root / "context_snapshot.json"
        if saved_snapshot.exists():
            return str(saved_snapshot.resolve())
        return str(root)

    def _build_saved_ranked_result_payload() -> dict[str, Any] | None:
        comparison_payload = _load_saved_whatif_json(
            "whatif_business_state_comparison.json"
        )
        experiment_payload = _load_saved_whatif_json("whatif_experiment_result.json")
        if comparison_payload is None:
            return None
        objective_pack = next(
            (
                pack.model_dump(mode="json")
                for pack in list_objective_packs()
                if pack.pack_id == "contain_exposure"
            ),
            {
                "pack_id": "contain_exposure",
                "title": "Contain Exposure",
                "summary": "Saved business-state comparison",
                "weights": {},
                "evidence_labels": [],
            },
        )
        transformed_candidates: list[dict[str, Any]] = []
        for index, candidate in enumerate(
            comparison_payload.get("candidates", []),
            start=1,
        ):
            if not isinstance(candidate, dict):
                continue
            business_state_change = candidate.get("business_state_change") or {}
            if not isinstance(business_state_change, dict):
                business_state_change = {}
            net_effect_score = business_state_change.get("net_effect_score", 0.0)
            transformed_candidates.append(
                {
                    "intervention": {
                        "label": candidate.get("label") or f"Candidate {index}",
                        "prompt": candidate.get("prompt") or "",
                    },
                    "rank": candidate.get("rank") or index,
                    "rollout_count": 0,
                    "saved_result": True,
                    "average_outcome_signals": {},
                    "outcome_score": {
                        "objective_pack_id": objective_pack["pack_id"],
                        "overall_score": net_effect_score,
                        "components": {},
                        "evidence": [],
                    },
                    "reason": (
                        ((candidate.get("forecast") or {}).get("summary"))
                        or business_state_change.get("summary")
                        or ""
                    ),
                    "rollouts": [],
                    "business_state_change": business_state_change,
                }
            )
        return {
            "version": "1",
            "label": (
                comparison_payload.get("label")
                or "saved_enron_business_state_comparison"
            ),
            "objective_pack": objective_pack,
            "selection": (
                experiment_payload.get("selection", {})
                if isinstance(experiment_payload, dict)
                else {}
            ),
            "materialization": (
                experiment_payload.get("materialization", {})
                if isinstance(experiment_payload, dict)
                else {}
            ),
            "baseline": (
                experiment_payload.get("baseline", {})
                if isinstance(experiment_payload, dict)
                else {}
            ),
            "recommended_candidate_label": (
                transformed_candidates[0]["intervention"]["label"]
                if transformed_candidates
                else ""
            ),
            "candidates": transformed_candidates,
            "artifacts": {
                "root": ".",
                "result_json_path": "whatif_business_state_comparison.json",
                "overview_markdown_path": "whatif_business_state_comparison.md",
            },
        }

    def _resolve_whatif_source(source: str, *, max_events: int | None = None):
        resolved = resolve_whatif_source_path(root, requested_source=source)
        if resolved is None:
            raise HTTPException(
                status_code=404,
                detail="historical source is not configured for this workspace",
            )
        resolved_source, source_dir = resolved
        try:
            world = load_world(
                source=resolved_source,
                source_dir=source_dir,
                max_events=max_events,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return world, source_dir

    def _whatif_artifacts_root() -> Path:
        path = root / ".artifacts" / "whatif_ui"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _slug(value: str) -> str:
        cleaned = sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
        return cleaned or "whatif"

    def _iso_now() -> str:
        return datetime.now(UTC).isoformat().replace("+00:00", "Z")

    @app.get("/api/workspace")
    def api_workspace() -> JSONResponse:
        payload = load_ui_workspace_summary(root)
        if payload is None:
            raise HTTPException(
                status_code=404,
                detail="workspace root is not configured",
            )
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/workspace/historical")
    def api_workspace_historical() -> JSONResponse:
        payload = load_workspace_historical_summary(root)
        return JSONResponse(payload.model_dump(mode="json") if payload else {})

    @app.get("/api/workspace/whatif")
    def api_workspace_whatif_status() -> JSONResponse:
        resolved = resolve_whatif_source_path(root)
        source = resolved[0] if resolved is not None else "auto"
        source_dir = resolved[1] if resolved is not None else None
        return JSONResponse(
            {
                "available": resolved is not None,
                "source": source,
                "source_dir": str(source_dir) if source_dir is not None else None,
                "objective_packs": [
                    pack.model_dump(mode="json") for pack in list_objective_packs()
                ],
            }
        )

    @app.post("/api/workspace/whatif/search")
    def api_workspace_whatif_search(request: WhatIfSearchRequest) -> JSONResponse:
        world, source_dir = _resolve_whatif_source(
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
        payload["source_dir"] = str(source_dir)
        return JSONResponse(payload)

    @app.post("/api/workspace/whatif/open")
    def api_workspace_whatif_open(request: WhatIfOpenRequest) -> JSONResponse:
        if _can_use_saved_enron_bundle(
            event_id=request.event_id,
            thread_id=request.thread_id,
        ):
            experiment_payload = _load_saved_whatif_json(
                "whatif_experiment_result.json"
            )
            if isinstance(experiment_payload, dict):
                materialization = experiment_payload.get("materialization")
                if isinstance(materialization, dict):
                    return JSONResponse(
                        {
                            "source": "enron",
                            "source_dir": _saved_source_dir_text(),
                            "episode_root": str(root),
                            "materialization": materialization,
                        }
                    )
        world, source_dir = _resolve_whatif_source(
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
                "source": world.source,
                "source_dir": str(source_dir),
                "episode_root": str(episode_root),
                "materialization": materialization.model_dump(mode="json"),
            }
        )

    @app.post("/api/workspace/whatif/scene")
    def api_workspace_whatif_scene(request: WhatIfSceneRequest) -> JSONResponse:
        historical = load_workspace_historical_summary(root)
        matches_saved_branch = historical is not None and (
            (not request.event_id or request.event_id == historical.branch_event_id)
            and (not request.thread_id or request.thread_id == historical.thread_id)
        )
        if matches_saved_branch:
            try:
                scene = build_saved_decision_scene(root)
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return JSONResponse(scene.model_dump(mode="json"))
        world, source_dir = _resolve_whatif_source(
            request.source,
            max_events=request.max_events,
        )
        try:
            scene = build_decision_scene(
                world,
                event_id=request.event_id,
                thread_id=request.thread_id,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        payload = scene.model_dump(mode="json")
        payload["source_dir"] = str(source_dir)
        return JSONResponse(payload)

    @app.post("/api/workspace/whatif/run")
    def api_workspace_whatif_run(request: WhatIfRunRequest) -> JSONResponse:
        if _can_use_saved_enron_bundle(
            event_id=request.event_id,
            thread_id=request.thread_id,
        ):
            experiment_payload = _load_saved_whatif_json(
                "whatif_experiment_result.json"
            )
            if isinstance(experiment_payload, dict):
                saved_payload = dict(experiment_payload)
                saved_payload["source_dir"] = _saved_source_dir_text()
                return JSONResponse(saved_payload)
        world, source_dir = _resolve_whatif_source(
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
        payload["source_dir"] = str(source_dir)
        return JSONResponse(payload)

    @app.post("/api/workspace/whatif/rank")
    def api_workspace_whatif_rank(request: WhatIfRankRequest) -> JSONResponse:
        if not request.candidates:
            raise HTTPException(
                status_code=400,
                detail="at least one candidate is required",
            )
        if _can_use_saved_enron_bundle(
            event_id=request.event_id,
            thread_id=request.thread_id,
        ):
            payload = _build_saved_ranked_result_payload()
            if payload is not None:
                payload["source_dir"] = _saved_source_dir_text()
                return JSONResponse(payload)
        world, source_dir = _resolve_whatif_source(
            request.source,
            max_events=request.max_events,
        )
        normalized_shadow_backend = request.shadow_forecast_backend.strip().lower()
        if normalized_shadow_backend not in {"auto", "e_jepa", "e_jepa_proxy"}:
            raise HTTPException(
                status_code=400,
                detail="shadow_forecast_backend must be auto, e_jepa, or e_jepa_proxy",
            )
        try:
            result = run_ranked_counterfactual_experiment(
                world,
                artifacts_root=_whatif_artifacts_root() / "ranked",
                label=request.label,
                objective_pack_id=request.objective_pack_id,
                candidate_interventions=[
                    WhatIfCandidateIntervention(
                        label=(candidate.label or candidate.prompt[:40]).strip(),
                        prompt=candidate.prompt,
                    )
                    for candidate in request.candidates
                ],
                event_id=request.event_id,
                thread_id=request.thread_id,
                rollout_count=request.rollout_count,
                provider=request.provider,
                model=request.model,
                shadow_forecast_backend=(
                    default_forecast_backend()
                    if normalized_shadow_backend == "auto"
                    else normalized_shadow_backend
                ),
                ejepa_epochs=request.ejepa_epochs,
                ejepa_batch_size=request.ejepa_batch_size,
                ejepa_force_retrain=request.ejepa_force_retrain,
                ejepa_device=request.ejepa_device,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        payload = result.model_dump(mode="json")
        payload["source_dir"] = str(source_dir)
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
        if load_workspace_historical_summary(root) is not None:
            return JSONResponse({})
        try:
            payload = deps.get_or_build_workspace_fidelity_report(root)
        except ValueError:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))

    # ------------------------------------------------------------------
    # Audit routes — human review of LLM judge benchmark rankings
    # ------------------------------------------------------------------

    def _resolve_benchmark_root() -> Path:
        env_root = os.environ.get("VEI_BENCHMARK_ROOT", "").strip()
        if env_root:
            candidate = Path(env_root).expanduser().resolve()
        else:
            candidate = root
        if not (candidate / "judge_result.json").exists():
            raise HTTPException(
                status_code=404,
                detail="No judge_result.json found. Run `vei whatif benchmark judge` first.",
            )
        return candidate

    def _completed_audit_path(benchmark_root: Path) -> Path:
        return benchmark_root / "completed_audit_records.json"

    def _load_completed_audits(benchmark_root: Path) -> list[WhatIfAuditRecord]:
        path = _completed_audit_path(benchmark_root)
        if not path.exists():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
        return [WhatIfAuditRecord.model_validate(item) for item in payload]

    def _save_completed_audits(
        benchmark_root: Path,
        records: list[WhatIfAuditRecord],
    ) -> None:
        path = _completed_audit_path(benchmark_root)
        path.write_text(
            json.dumps(
                [record.model_dump(mode="json") for record in records],
                indent=2,
            ),
            encoding="utf-8",
        )

    @app.get("/api/workspace/whatif/audit")
    def api_audit_queue() -> JSONResponse:
        try:
            benchmark_root = _resolve_benchmark_root()
        except HTTPException as exc:
            if exc.status_code == 404:
                return JSONResponse({"items": [], "total": 0})
            raise
        judge_result = load_branch_point_benchmark_judge_result(benchmark_root)
        build = load_branch_point_benchmark_build_result(benchmark_root)
        completed = _load_completed_audits(benchmark_root)
        completed_keys = {
            (record.case_id, record.objective_pack_id)
            for record in completed
            if record.status == "completed"
        }
        case_by_id = {case.case_id: case for case in build.cases}
        items = []
        for audit in judge_result.audit_queue:
            key = (audit.case_id, audit.objective_pack_id)
            case = case_by_id.get(audit.case_id)
            if case is None:
                continue
            # Read the dossier text for this case+objective
            dossier_path = case.objective_dossier_paths.get(audit.objective_pack_id)
            dossier_text = ""
            if dossier_path and Path(dossier_path).exists():
                dossier_text = Path(dossier_path).read_text(encoding="utf-8")
            # Find the judge's judgment for reveal after submission
            judge_ranking = None
            for judgment in judge_result.judgments:
                if (
                    judgment.case_id == audit.case_id
                    and judgment.objective_pack_id == audit.objective_pack_id
                ):
                    judge_ranking = judgment
                    break
            items.append(
                {
                    "case_id": audit.case_id,
                    "objective_pack_id": audit.objective_pack_id,
                    "status": "completed" if key in completed_keys else "pending",
                    "case_title": case.title,
                    "case_summary": case.summary,
                    "dossier_text": dossier_text,
                    "candidates": [
                        {
                            "candidate_id": c.candidate_id,
                            "label": c.label,
                            "prompt": c.prompt,
                        }
                        for c in case.candidates
                    ],
                    "judge_confidence": (
                        judge_ranking.confidence if judge_ranking else None
                    ),
                    "judge_uncertainty_flag": (
                        judge_ranking.uncertainty_flag if judge_ranking else False
                    ),
                }
            )
        return JSONResponse({"items": items, "total": len(items)})

    @app.post("/api/workspace/whatif/audit/{case_id}/{objective_pack_id}")
    def api_audit_submit(
        case_id: str,
        objective_pack_id: str,
        request: AuditSubmitRequest,
    ) -> JSONResponse:
        benchmark_root = _resolve_benchmark_root()
        build = load_branch_point_benchmark_build_result(benchmark_root)
        judge_result = load_branch_point_benchmark_judge_result(benchmark_root)
        case_by_id = {case.case_id: case for case in build.cases}
        case = case_by_id.get(case_id)
        if case is None:
            raise HTTPException(status_code=404, detail="benchmark case not found")
        candidate_ids = [candidate.candidate_id for candidate in case.candidates]
        candidate_id_set = set(candidate_ids)
        if (
            len(request.ordered_candidate_ids) != len(candidate_ids)
            or set(request.ordered_candidate_ids) != candidate_id_set
        ):
            raise HTTPException(
                status_code=400,
                detail="ordered_candidate_ids must contain each candidate exactly once",
            )

        # Find the judge's ranking for agreement check and reveal
        judge_ranking = None
        for judgment in judge_result.judgments:
            if (
                judgment.case_id == case_id
                and judgment.objective_pack_id == objective_pack_id
            ):
                judge_ranking = judgment
                break
        if judge_ranking is None:
            raise HTTPException(
                status_code=404,
                detail="judge ranking not found for case/objective",
            )

        expected_pairs = {
            tuple(sorted((left_id, right_id)))
            for index, left_id in enumerate(candidate_ids)
            for right_id in candidate_ids[index + 1 :]
        }
        seen_pairs: set[tuple[str, str]] = set()
        for comparison in request.pairwise_comparisons:
            pair_key = tuple(
                sorted((comparison.left_candidate_id, comparison.right_candidate_id))
            )
            if pair_key in seen_pairs:
                raise HTTPException(
                    status_code=400,
                    detail="pairwise_comparisons contains duplicate pairs",
                )
            seen_pairs.add(pair_key)
            pair_ids = {
                comparison.left_candidate_id,
                comparison.right_candidate_id,
            }
            if not pair_ids.issubset(candidate_id_set):
                raise HTTPException(
                    status_code=400,
                    detail="pairwise_comparisons contains unknown candidate ids",
                )
            if comparison.preferred_candidate_id not in pair_ids:
                raise HTTPException(
                    status_code=400,
                    detail="preferred_candidate_id must match one side of the pair",
                )
        if seen_pairs != expected_pairs:
            raise HTTPException(
                status_code=400,
                detail="pairwise_comparisons must cover every candidate pair once",
            )

        agreement = None
        agreement = list(request.ordered_candidate_ids) == list(
            judge_ranking.ordered_candidate_ids
        )

        record = WhatIfAuditRecord(
            case_id=case_id,
            objective_pack_id=objective_pack_id,
            submission_id=uuid4().hex,
            submitted_at=_iso_now(),
            reviewer_id=request.reviewer_id,
            ordered_candidate_ids=request.ordered_candidate_ids,
            pairwise_comparisons=[
                WhatIfJudgedPairwiseComparison.model_validate(
                    comp.model_dump(mode="json")
                )
                for comp in request.pairwise_comparisons
            ],
            confidence=request.confidence,
            status="completed",
            agreement_with_judge=agreement,
            notes=request.notes,
        )

        # Append to completed audits file
        completed = _load_completed_audits(benchmark_root)
        completed.append(record)
        _save_completed_audits(benchmark_root, completed)

        # Build reveal payload with judge's comparisons
        reveal: dict[str, Any] = {
            "submitted": record.model_dump(mode="json"),
            "agreement_with_judge": agreement,
        }
        if judge_ranking is not None:
            reveal["judge_ranking"] = {
                "ordered_candidate_ids": judge_ranking.ordered_candidate_ids,
                "pairwise_comparisons": [
                    comp.model_dump(mode="json")
                    for comp in judge_ranking.pairwise_comparisons
                ],
                "confidence": judge_ranking.confidence,
                "notes": judge_ranking.notes,
            }
        return JSONResponse(reveal)
