from __future__ import annotations

from datetime import datetime
from typing import Sequence

from vei.dynamics.api import get_backend, register_backend
from vei.dynamics.models import (
    BackendInfo,
    BusinessHeads,
    CandidateAction,
    CompanyGraphSlice,
    DeterminismManifest,
    DynamicsRequest,
    DynamicsResponse,
    PointInterval,
)
from vei.events.models import (
    ActorRef,
    CanonicalEvent,
    EventDomain,
    EventProvenance,
    InternalExternal,
    ProvenanceRecord,
    StateDelta,
)

from .models import (
    WhatIfCounterfactualEstimateDelta,
    WhatIfCounterfactualEstimateResult,
    WhatIfEpisodeMaterialization,
    WhatIfForecastBackend,
    WhatIfHistoricalScore,
    WhatIfLLMGeneratedMessage,
    WhatIfWorld,
)


class _WhatIfHeuristicBackend:
    def forecast(self, request: DynamicsRequest) -> DynamicsResponse:
        from .counterfactual import estimate_counterfactual_delta

        whatif_context = request.company_graph_slice.metadata["whatif"]
        result = estimate_counterfactual_delta(
            whatif_context["workspace_root"],
            prompt=whatif_context["prompt"],
        )
        return DynamicsResponse(
            backend_id="heuristic_baseline",
            backend_version="1.0.0",
            business_heads=BusinessHeads(
                risk=PointInterval(point=result.delta.risk_score_delta),
                spread=PointInterval(point=float(result.delta.external_event_delta)),
                escalation=PointInterval(point=float(result.delta.escalation_delta)),
                approval=PointInterval(point=float(result.delta.approval_delta)),
                load=PointInterval(point=float(result.delta.future_event_delta)),
                drag=PointInterval(point=float(result.delta.assignment_delta)),
            ),
            state_delta_summary={"whatif_result": result.model_dump(mode="json")},
        )

    def describe(self) -> BackendInfo:
        return BackendInfo(
            name="heuristic_baseline",
            version="1.0.0",
            backend_type="legacy_whatif_adapter",
            deterministic=True,
        )

    def determinism_manifest(self) -> DeterminismManifest:
        return DeterminismManifest(
            backend_id="heuristic_baseline",
            backend_version="1.0.0",
            notes=["Wraps vei.whatif.counterfactual.estimate_counterfactual_delta."],
        )


class _WhatIfEjepaBackend:
    def forecast(self, request: DynamicsRequest) -> DynamicsResponse:
        from .ejepa import run_ejepa_counterfactual

        whatif_context = request.company_graph_slice.metadata["whatif"]
        llm_messages = [
            WhatIfLLMGeneratedMessage.model_validate(item)
            for item in whatif_context.get("llm_messages", [])
            if isinstance(item, dict)
        ]
        result = run_ejepa_counterfactual(
            whatif_context["workspace_root"],
            prompt=whatif_context["prompt"],
            source=whatif_context["source"],
            source_dir=whatif_context["source_dir"],
            thread_id=whatif_context["thread_id"],
            branch_event_id=whatif_context["branch_event_id"],
            llm_messages=llm_messages,
            epochs=int(whatif_context["ejepa_epochs"]),
            batch_size=int(whatif_context["ejepa_batch_size"]),
            force_retrain=bool(whatif_context["ejepa_force_retrain"]),
            device=whatif_context["ejepa_device"],
        )
        return DynamicsResponse(
            backend_id="e_jepa",
            backend_version="1.0.0",
            business_heads=BusinessHeads(
                risk=PointInterval(point=result.delta.risk_score_delta),
                spread=PointInterval(point=float(result.delta.external_event_delta)),
                escalation=PointInterval(point=float(result.delta.escalation_delta)),
                approval=PointInterval(point=float(result.delta.approval_delta)),
                load=PointInterval(point=float(result.delta.future_event_delta)),
                drag=PointInterval(point=float(result.delta.assignment_delta)),
            ),
            state_delta_summary={"whatif_result": result.model_dump(mode="json")},
        )

    def describe(self) -> BackendInfo:
        return BackendInfo(
            name="e_jepa",
            version="1.0.0",
            backend_type="legacy_whatif_adapter",
            deterministic=False,
        )

    def determinism_manifest(self) -> DeterminismManifest:
        return DeterminismManifest(
            backend_id="e_jepa",
            backend_version="1.0.0",
            notes=["Wraps vei.whatif.ejepa.run_ejepa_counterfactual."],
        )


def run_dynamics_counterfactual(
    *,
    world: WhatIfWorld,
    materialization: WhatIfEpisodeMaterialization,
    prompt: str,
    forecast_backend: WhatIfForecastBackend,
    allow_proxy_fallback: bool,
    llm_messages: Sequence[WhatIfLLMGeneratedMessage] | None,
    seed: int,
    ejepa_epochs: int,
    ejepa_batch_size: int,
    ejepa_force_retrain: bool,
    ejepa_device: str | None,
) -> WhatIfCounterfactualEstimateResult:
    backend_name = _normalize_backend_name(forecast_backend)
    _ensure_backend_registered(backend_name)
    request = _build_request(
        world=world,
        materialization=materialization,
        prompt=prompt,
        llm_messages=llm_messages,
        seed=seed,
        ejepa_epochs=ejepa_epochs,
        ejepa_batch_size=ejepa_batch_size,
        ejepa_force_retrain=ejepa_force_retrain,
        ejepa_device=ejepa_device,
    )
    response = get_backend(backend_name).forecast(request)
    result = _whatif_result_from_response(
        backend_name=backend_name,
        prompt=prompt,
        baseline=materialization.forecast,
        response=response,
    )
    if backend_name == "e_jepa" and result.status == "error" and allow_proxy_fallback:
        proxy_response = get_backend("heuristic_baseline").forecast(request)
        proxy_result = _whatif_result_from_response(
            backend_name="heuristic_baseline",
            prompt=prompt,
            baseline=materialization.forecast,
            response=proxy_response,
        )
        proxy_result.notes.insert(
            0,
            "Real E-JEPA forecast failed, so this experiment fell back to the proxy forecast.",
        )
        if result.error:
            proxy_result.notes.append(f"Original E-JEPA error: {result.error}")
        return proxy_result
    return result


def _ensure_backend_registered(backend_name: str) -> None:
    if backend_name == "heuristic_baseline":
        try:
            current = get_backend("heuristic_baseline")
        except KeyError:
            register_backend("heuristic_baseline", _WhatIfHeuristicBackend)
            return
        if current.__class__.__module__ == "vei.dynamics.backends.heuristic":
            register_backend("heuristic_baseline", _WhatIfHeuristicBackend)
        return
    if backend_name != "e_jepa":
        return
    try:
        get_backend("e_jepa")
    except KeyError:
        register_backend("e_jepa", _WhatIfEjepaBackend)


def _build_request(
    *,
    world: WhatIfWorld,
    materialization: WhatIfEpisodeMaterialization,
    prompt: str,
    llm_messages: Sequence[WhatIfLLMGeneratedMessage] | None,
    seed: int,
    ejepa_epochs: int,
    ejepa_batch_size: int,
    ejepa_force_retrain: bool,
    ejepa_device: str | None,
) -> DynamicsRequest:
    metadata = {
        "whatif": {
            "workspace_root": str(materialization.workspace_root),
            "prompt": prompt,
            "source": world.source,
            "source_dir": str(world.source_dir),
            "thread_id": materialization.thread_id,
            "branch_event_id": materialization.branch_event_id,
            "baseline_forecast": materialization.forecast.model_dump(mode="json"),
            "llm_messages": [
                message.model_dump(mode="json") for message in (llm_messages or [])
            ],
            "ejepa_epochs": ejepa_epochs,
            "ejepa_batch_size": ejepa_batch_size,
            "ejepa_force_retrain": ejepa_force_retrain,
            "ejepa_device": ejepa_device,
        }
    }
    return DynamicsRequest(
        company_graph_slice=CompanyGraphSlice(
            tenant_id=materialization.organization_domain,
            domains=[materialization.surface],
            metadata=metadata,
        ),
        recent_events=_canonical_recent_events(materialization),
        candidate_action=CandidateAction(
            action_id=materialization.branch_event_id,
            label=prompt[:80],
            description=prompt,
        ),
        horizon=max(1, int(materialization.future_event_count or 1)),
        seed=seed,
    )


def _canonical_recent_events(
    materialization: WhatIfEpisodeMaterialization,
) -> list[CanonicalEvent]:
    history = list(materialization.history_preview) + [materialization.branch_event]
    return [
        _canonical_event_from_reference(
            reference=reference,
            organization_domain=materialization.organization_domain,
        )
        for reference in history
    ]


def _canonical_event_from_reference(
    *,
    reference,
    organization_domain: str,
) -> CanonicalEvent:
    surface = str(reference.surface or "mail").strip().lower() or "mail"
    payload: dict[str, object]
    if surface == "slack":
        payload = {
            "target": "slack",
            "channel": reference.target_id or "#procurement",
            "text": reference.snippet or reference.subject,
            "thread_ts": reference.conversation_anchor or None,
            "user": reference.actor_id,
        }
    elif surface == "tickets":
        payload = {
            "target": "tickets",
            "ticket_id": reference.thread_id.split(":", 1)[-1],
            "comment": reference.snippet or reference.subject,
            "author": reference.actor_id,
            "title": reference.subject or reference.thread_id,
        }
    elif surface == "docs":
        payload = {
            "target": "docs",
            "title": reference.subject or reference.thread_id,
            "body": reference.snippet or reference.subject,
        }
    else:
        payload = {
            "target": "mail",
            "from": reference.actor_id,
            "to": list(reference.to_recipients)
            or [reference.target_id or "me@example"],
            "subj": reference.subject or reference.thread_id,
            "body_text": reference.snippet or reference.subject,
            "thread_id": reference.thread_id,
        }
    return CanonicalEvent(
        event_id=reference.event_id,
        tenant_id=organization_domain,
        case_id=reference.case_id or None,
        ts_ms=_timestamp_ms(reference.timestamp),
        domain=_domain_for_surface(surface),
        kind=f"{surface}.{reference.event_type}",
        actor_ref=ActorRef(
            actor_id=reference.actor_id,
            display_name=reference.actor_id,
            tenant_id=organization_domain,
        ),
        participants=[
            ActorRef(
                actor_id=recipient,
                display_name=recipient,
                tenant_id=organization_domain,
            )
            for recipient in reference.to_recipients
            if recipient
        ],
        internal_external=_internal_external(
            reference.to_recipients, organization_domain
        ),
        provenance=ProvenanceRecord(origin=EventProvenance.IMPORTED),
        delta=StateDelta(
            domain=_domain_for_surface(surface),
            delta_schema_version=0,
            data=payload,
        ),
    )


def _whatif_result_from_response(
    *,
    backend_name: str,
    prompt: str,
    baseline: WhatIfHistoricalScore,
    response,
) -> WhatIfCounterfactualEstimateResult:
    raw_result = response.state_delta_summary.get("whatif_result")
    if isinstance(raw_result, dict):
        return WhatIfCounterfactualEstimateResult.model_validate(raw_result)

    risk_delta = float(response.business_heads.risk.point)
    future_event_delta = _count_delta(
        response.business_heads.load.point,
        baseline.future_event_count,
    )
    escalation_delta = _count_delta(
        response.business_heads.escalation.point,
        baseline.future_escalation_count,
    )
    approval_delta = _count_delta(
        response.business_heads.approval.point,
        baseline.future_approval_count,
    )
    external_event_delta = _count_delta(
        response.business_heads.spread.point,
        baseline.future_external_event_count,
    )
    assignment_delta = _count_delta(
        response.business_heads.drag.point,
        baseline.future_assignment_count,
    )
    predicted = baseline.model_copy(
        update={
            "backend": backend_name,
            "risk_score": _clamp_probability(baseline.risk_score + risk_delta),
            "future_event_count": max(
                0,
                baseline.future_event_count + future_event_delta,
            ),
            "future_escalation_count": max(
                0,
                baseline.future_escalation_count + escalation_delta,
            ),
            "future_assignment_count": max(
                0,
                baseline.future_assignment_count + assignment_delta,
            ),
            "future_approval_count": max(
                0,
                baseline.future_approval_count + approval_delta,
            ),
            "future_external_event_count": max(
                0,
                baseline.future_external_event_count + external_event_delta,
            ),
        }
    )
    delta = WhatIfCounterfactualEstimateDelta(
        risk_score_delta=round(predicted.risk_score - baseline.risk_score, 3),
        future_event_delta=predicted.future_event_count - baseline.future_event_count,
        escalation_delta=(
            predicted.future_escalation_count - baseline.future_escalation_count
        ),
        assignment_delta=(
            predicted.future_assignment_count - baseline.future_assignment_count
        ),
        approval_delta=predicted.future_approval_count - baseline.future_approval_count,
        external_event_delta=(
            predicted.future_external_event_count - baseline.future_external_event_count
        ),
    )
    error = str(response.state_delta_summary.get("error") or "").strip() or None
    return WhatIfCounterfactualEstimateResult(
        status="error" if error else "ok",
        backend=backend_name,
        prompt=prompt,
        summary=error or f"{backend_name} forecast completed.",
        baseline=baseline,
        predicted=predicted,
        delta=delta,
        notes=[],
        error=error,
    )


def _normalize_backend_name(name: str) -> str:
    if name == "e_jepa_proxy":
        return "heuristic_baseline"
    return name


def _timestamp_ms(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return int(parsed.timestamp() * 1000)


def _domain_for_surface(surface: str) -> EventDomain:
    if surface in {"mail", "slack", "calendar"}:
        return EventDomain.COMM_GRAPH
    if surface == "tickets":
        return EventDomain.WORK_GRAPH
    if surface == "docs":
        return EventDomain.DOC_GRAPH
    return EventDomain.INTERNAL


def _internal_external(
    recipients: Sequence[str],
    organization_domain: str,
) -> InternalExternal:
    domain = str(organization_domain or "").strip().lower()
    if not recipients:
        return InternalExternal.UNKNOWN
    if any(
        domain and "@" in recipient and not recipient.endswith(f"@{domain}")
        for recipient in recipients
    ):
        return InternalExternal.EXTERNAL
    return InternalExternal.INTERNAL


def _clamp_probability(value: float) -> float:
    return max(0.0, min(1.0, round(float(value), 3)))


def _count_delta(point: float, baseline_count: int) -> int:
    scale = max(1, int(baseline_count or 1))
    return int(round(float(point) * scale))
