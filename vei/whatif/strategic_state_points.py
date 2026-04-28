from __future__ import annotations

import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Sequence

import numpy as np
from pydantic import BaseModel, Field

from vei.llm.codex_cli import run_codex_json
from vei.score_frontier import run_llm_json_prompt

from .benchmark import (
    _build_pre_branch_contract,
    outcome_targets_to_signals,
    summarize_observed_targets,
)
from .benchmark_business import (
    evidence_to_business_outcomes,
    summarize_future_state_heads,
    summarize_observed_evidence,
)
from .benchmark_runtime import run_branch_point_benchmark_predictions
from .doctrine import build_doctrine_packet, doctrine_packet_text
from .models import (
    WhatIfActionSchema,
    WhatIfArtifactFlags,
    WhatIfBenchmarkDatasetRow,
    WhatIfEvent,
    WhatIfWorld,
)

StrategicProposalMode = Literal["llm", "template"]
DEFAULT_STRATEGIC_PROPOSAL_MODEL = "gpt-5.4"
OPERATOR_SCORE_FORMULA_VERSION = "balanced_operator_v1"
OPERATOR_SCORE_FORMULA = (
    "mean(1-enterprise_risk, commercial_position, 1-org_strain, "
    "stakeholder_trust, 1-execution_drag)"
)
_NEAR_TIE_MARGIN = 0.01
_DELTA_EPSILON = 0.005
_BUSINESS_HEADS: tuple[tuple[str, str, bool], ...] = (
    ("predicted_enterprise_risk", "risk", False),
    ("predicted_commercial_position", "commercial", True),
    ("predicted_stakeholder_trust", "trust", True),
    ("predicted_execution_drag", "drag", False),
    ("predicted_org_strain", "strain", False),
)
_DOMAIN_RISK_HEADS: tuple[tuple[str, str, bool], ...] = (
    ("predicted_regulatory_exposure", "regulatory", False),
    ("predicted_accounting_control_pressure", "accounting_control", False),
    ("predicted_liquidity_stress", "liquidity", False),
    ("predicted_external_confidence_pressure", "external_confidence", False),
)
_TELEMETRY_HEADS: tuple[tuple[str, str], ...] = (
    ("predicted_external_spread", "external_spread"),
    ("predicted_participant_fanout", "participant_fanout"),
    ("predicted_governance_response", "governance_response"),
    ("predicted_evidence_control", "evidence_control"),
)
_DELTA_HEADS = _BUSINESS_HEADS + _DOMAIN_RISK_HEADS
PARETO_BASIS_VERSION = "operator_utility_plus_domain_risk_v1"
PREDICTION_HEAD_VERSION = "business_future_heads_v1"
PREDICTION_PROBE_VERSION = "linear_heads_v1"


class StrategicCandidateInput(BaseModel):
    label: str
    action: str
    candidate_type: str = ""
    success_observable: str = ""
    failure_observable: str = ""
    time_to_signal: str = ""
    next_decision_trigger: str = ""
    falsifying_evidence: str = ""


class StrategicDecisionInput(BaseModel):
    title: str
    decision_question: str
    why_selected: str
    as_of: str = ""
    topic: str = ""
    candidates: list[StrategicCandidateInput] = Field(default_factory=list)


class StrategicStatePointArtifacts(BaseModel):
    root: Path
    proposal_manifest_path: Path
    result_json_path: Path
    result_csv_path: Path
    result_markdown_path: Path


class StrategicStatePointRunResult(BaseModel):
    version: str = "1"
    label: str
    source_count: int
    decision_count: int
    candidate_count: int
    proposal_mode: StrategicProposalMode
    proposal_model: str
    artifacts: StrategicStatePointArtifacts
    notes: list[str] = Field(default_factory=list)


@dataclass(frozen=True)
class StrategicStatePointSource:
    tenant_id: str
    world: WhatIfWorld
    display_name: str = ""
    as_of: str = ""


@dataclass(frozen=True)
class _StrategicStatePoint:
    tenant_id: str
    display_name: str
    as_of: str
    branch_event: WhatIfEvent
    history_events: list[WhatIfEvent]
    future_events: list[WhatIfEvent]
    evidence_events: list[WhatIfEvent]
    doctrine_text: str
    decision: StrategicDecisionInput
    proposal_source: str
    proposal_model: str
    prompt_hash: str
    evidence_hash: str


def run_strategic_state_point_counterfactuals(
    sources: Sequence[StrategicStatePointSource],
    *,
    checkpoint_path: str | Path,
    artifacts_root: str | Path,
    label: str,
    decisions_per_source: int = 3,
    candidates_per_decision: int = 8,
    proposal_mode: StrategicProposalMode = "llm",
    proposal_model: str = DEFAULT_STRATEGIC_PROPOSAL_MODEL,
    future_horizon_days: int = 120,
    max_history_events: int = 260,
    max_evidence_events: int = 24,
    device: str | None = None,
    runtime_root: str | Path | None = None,
) -> StrategicStatePointRunResult:
    if not sources:
        raise ValueError("at least one strategic state-point source is required")
    root = Path(artifacts_root).expanduser().resolve() / _slug(label)
    root.mkdir(parents=True, exist_ok=True)
    proposal_manifest_path = root / "strategic_state_point_proposals.json"
    result_json_path = root / "strategic_state_point_results.json"
    result_csv_path = root / "strategic_state_point_results.csv"
    result_markdown_path = root / "strategic_state_point_results.md"

    state_points: list[_StrategicStatePoint] = []
    proposal_manifest: list[dict[str, Any]] = []
    for source in sources:
        generated, manifest = _build_state_points_for_source(
            source,
            root=root,
            decisions_per_source=decisions_per_source,
            candidates_per_decision=candidates_per_decision,
            proposal_mode=proposal_mode,
            proposal_model=proposal_model,
            future_horizon_days=future_horizon_days,
            max_history_events=max_history_events,
            max_evidence_events=max_evidence_events,
        )
        state_points.extend(generated)
        proposal_manifest.append(manifest)

    rows: list[dict[str, Any]] = []
    for state_point in state_points:
        rows.extend(
            _score_state_point(
                state_point,
                checkpoint_path=Path(checkpoint_path).expanduser().resolve(),
                organization_domain=state_point.branch_event.target_id,
                device=device,
                runtime_root=runtime_root,
                prediction_output_root=root
                / "prediction_runtime"
                / state_point.tenant_id
                / _slug(state_point.decision.title),
            )
        )
    rows = _rank_rows(rows)
    result_payload = {
        "version": "1",
        "label": label,
        "proposal_mode": proposal_mode,
        "proposal_model": proposal_model,
        "no_future_context_for_proposals": True,
        "decision_count": len(state_points),
        "candidate_count": len(rows),
        "state_points": [
            _state_point_payload(state_point) for state_point in state_points
        ],
        "candidates": rows,
    }
    proposal_manifest_path.write_text(
        json.dumps(proposal_manifest, indent=2),
        encoding="utf-8",
    )
    result_json_path.write_text(json.dumps(result_payload, indent=2), encoding="utf-8")
    _write_rows_csv(rows, result_csv_path)
    _write_markdown_result(
        rows=rows, state_points=state_points, path=result_markdown_path
    )
    return StrategicStatePointRunResult(
        label=label,
        source_count=len(sources),
        decision_count=len(state_points),
        candidate_count=len(rows),
        proposal_mode=proposal_mode,
        proposal_model=proposal_model,
        artifacts=StrategicStatePointArtifacts(
            root=root,
            proposal_manifest_path=proposal_manifest_path,
            result_json_path=result_json_path,
            result_csv_path=result_csv_path,
            result_markdown_path=result_markdown_path,
        ),
        notes=[
            "Strategic state-point run: decision points may be proposed, not historical branch events.",
            "LLM/template proposal uses only pre-as-of evidence and archive-derived doctrine.",
            "JEPA predicts future heads for each candidate action; ranks are deterministic over those predictions.",
        ],
    )


def _build_state_points_for_source(
    source: StrategicStatePointSource,
    *,
    root: Path,
    decisions_per_source: int,
    candidates_per_decision: int,
    proposal_mode: StrategicProposalMode,
    proposal_model: str,
    future_horizon_days: int,
    max_history_events: int,
    max_evidence_events: int,
) -> tuple[list[_StrategicStatePoint], dict[str, Any]]:
    world = source.world
    ordered = sorted(
        world.events, key=lambda event: (event.timestamp_ms, event.event_id)
    )
    if not ordered:
        raise ValueError(f"source {source.tenant_id!r} has no events")
    as_of_dt = (
        _parse_datetime(source.as_of) if source.as_of else _default_as_of(ordered)
    )
    horizon_dt = as_of_dt + timedelta(days=future_horizon_days)
    history_events = [
        event for event in ordered if _parse_datetime(event.timestamp) <= as_of_dt
    ][-max_history_events:]
    if not history_events:
        raise ValueError(
            f"source {source.tenant_id!r} has no pre-as-of events for {as_of_dt.isoformat()}"
        )
    future_events = [
        event
        for event in ordered
        if as_of_dt < _parse_datetime(event.timestamp) <= horizon_dt
    ]
    evidence_events = _select_evidence_events(
        history_events, max_events=max_evidence_events
    )
    evidence_lines = [_event_evidence_line(event) for event in evidence_events]
    doctrine_packet = build_doctrine_packet(
        tenant_id=source.tenant_id,
        display_name=source.display_name
        or world.summary.organization_name
        or source.tenant_id,
        source="strategic_state_point_prebranch_archive",
        evidence=evidence_lines,
    )
    doctrine_text = doctrine_packet_text(doctrine_packet)
    prompt = build_strategic_state_point_proposal_prompt(
        tenant_id=source.tenant_id,
        display_name=source.display_name
        or world.summary.organization_name
        or source.tenant_id,
        as_of=as_of_dt,
        doctrine_text=doctrine_text,
        evidence_events=evidence_events,
        decisions_per_source=decisions_per_source,
        candidates_per_decision=candidates_per_decision,
    )
    prompt_hash = sha256(prompt.encode("utf-8")).hexdigest()
    evidence_hash = sha256("\n".join(evidence_lines).encode("utf-8")).hexdigest()
    proposal_source = "template"
    raw_response = ""
    try:
        if proposal_mode == "llm":
            payload = _run_proposal_json_prompt(
                prompt,
                model=proposal_model,
                max_tokens=6400,
                output_schema=_proposal_schema(
                    decisions_per_source=decisions_per_source,
                    candidates_per_decision=candidates_per_decision,
                ),
                temperature=None if proposal_model.startswith("gpt-5") else 0.0,
            )
            raw_response = json.dumps(payload, indent=2, sort_keys=True)
            proposal_source = "llm"
        else:
            payload = _template_proposal_payload(
                source=source,
                as_of=as_of_dt,
                decisions_per_source=decisions_per_source,
                candidates_per_decision=candidates_per_decision,
            )
            raw_response = json.dumps(payload, indent=2)
    except Exception as exc:  # pragma: no cover - exercised in live Codex/API runs.
        payload = _template_proposal_payload(
            source=source,
            as_of=as_of_dt,
            decisions_per_source=decisions_per_source,
            candidates_per_decision=candidates_per_decision,
        )
        raw_response = json.dumps(
            {"template_fallback_error": str(exc), **payload},
            indent=2,
        )
        proposal_source = "template_fallback"
    decisions = _decisions_from_payload(
        payload,
        as_of=as_of_dt,
        decisions_per_source=decisions_per_source,
        candidates_per_decision=candidates_per_decision,
    )
    prompt_path = root / "proposal_prompts" / f"{source.tenant_id}.txt"
    response_path = root / "proposal_responses" / f"{source.tenant_id}.json"
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    response_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text(prompt, encoding="utf-8")
    response_path.write_text(raw_response, encoding="utf-8")
    state_points: list[_StrategicStatePoint] = []
    for index, decision in enumerate(decisions, start=1):
        branch_event = _synthetic_branch_event(
            source=source,
            world=world,
            as_of=as_of_dt,
            decision=decision,
            index=index,
        )
        state_points.append(
            _StrategicStatePoint(
                tenant_id=source.tenant_id,
                display_name=source.display_name
                or world.summary.organization_name
                or source.tenant_id,
                as_of=branch_event.timestamp,
                branch_event=branch_event,
                history_events=history_events,
                future_events=future_events,
                evidence_events=evidence_events,
                doctrine_text=doctrine_text,
                decision=decision,
                proposal_source=proposal_source,
                proposal_model=proposal_model,
                prompt_hash=prompt_hash,
                evidence_hash=evidence_hash,
            )
        )
    manifest = {
        "tenant_id": source.tenant_id,
        "display_name": source.display_name or world.summary.organization_name,
        "as_of": as_of_dt.isoformat().replace("+00:00", "Z"),
        "proposal_source": proposal_source,
        "proposal_model": proposal_model,
        "prompt_path": str(prompt_path),
        "response_path": str(response_path),
        "generation_prompt_sha256": prompt_hash,
        "pre_branch_evidence_sha256": evidence_hash,
        "no_future_context": True,
        "decision_count": len(state_points),
    }
    return state_points, manifest


def build_strategic_state_point_proposal_prompt(
    *,
    tenant_id: str,
    display_name: str,
    as_of: datetime,
    doctrine_text: str,
    evidence_events: Sequence[WhatIfEvent],
    decisions_per_source: int,
    candidates_per_decision: int,
) -> str:
    evidence = "\n".join(
        f"- {event.timestamp} | {event.subject} | {event.snippet[:420]}"
        for event in evidence_events
    )
    return f"""You are proposing strategic state-point decisions for VEI.

Boundary:
- Use only the archive state shown below, dated on or before {as_of.date().isoformat()}.
- Do not infer or mention future outcomes after that date.
- The decision point does not need to be a real email, ticket, article, or Slack message.
- It may be a CEO/operator/editor/regulator-style question such as "as of this date, should we enter a new market, pitch a major partner, change product direction, warn the public, or escalate a governance issue?"
- The JEPA world model will later score the candidate actions. You are only proposing realistic decision points and concrete actions.

Tenant: {tenant_id}
Name: {display_name}

Doctrine packet:
{doctrine_text}

Pre-as-of evidence:
{evidence}

Return exactly {decisions_per_source} decision points. For each decision point:
- write a plain-English title a senior operator would understand
- write the decision question being tested
- explain why this is an important decision to test from the pre-as-of evidence
- generate exactly {candidates_per_decision} broad, concrete candidate actions
- for every candidate, include concrete observables: success_observable, failure_observable, time_to_signal, next_decision_trigger, and falsifying_evidence
- include at least one upside/exploit action, one focused pilot, one fast move, one hold/review, one escalation, one coordination move, one commercial/market reset, and one trust/privacy/evidence-control path where applicable
- actions must be concrete enough that a manager could choose them
- observables must be specific to the candidate and useful for checking the branch later
- avoid minor wording variants
"""


def _run_proposal_json_prompt(
    prompt: str,
    *,
    model: str,
    max_tokens: int,
    output_schema: dict[str, Any],
    temperature: float | None,
) -> dict[str, Any]:
    if _proposal_uses_codex():
        return dict(
            run_codex_json(
                model=model,
                prompt=prompt,
                output_schema=output_schema,
                cwd=Path.cwd(),
            )
        )
    return run_llm_json_prompt(
        prompt,
        model=model,
        max_tokens=max_tokens,
        output_schema=output_schema,
        temperature=temperature,
    )


def _proposal_uses_codex() -> bool:
    backend = os.environ.get("VEI_STRATEGIC_PROPOSAL_BACKEND", "codex").strip().lower()
    return backend not in {"api", "direct", "provider"}


def _score_state_point(
    state_point: _StrategicStatePoint,
    *,
    checkpoint_path: Path,
    organization_domain: str,
    device: str | None,
    runtime_root: str | Path | None,
    prediction_output_root: Path,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pending_rows: list[
        tuple[int, StrategicCandidateInput, str, WhatIfBenchmarkDatasetRow]
    ] = []
    evidence = summarize_observed_evidence(
        branch_event=state_point.branch_event,
        future_events=state_point.future_events,
    )
    observed_targets = summarize_observed_targets(
        branch_event=state_point.branch_event,
        future_events=state_point.future_events,
        organization_domain=organization_domain,
    )
    for index, candidate in enumerate(state_point.decision.candidates, start=1):
        candidate_type = candidate.candidate_type or _infer_candidate_type(
            candidate.action
        )
        action_schema = _action_schema_for_candidate(
            action=candidate.action,
            candidate_type=candidate_type,
        )
        contract = _build_pre_branch_contract(
            case_id=state_point.branch_event.case_id,
            thread_id=state_point.branch_event.thread_id,
            branch_event=state_point.branch_event,
            history_events=state_point.history_events,
            organization_domain=organization_domain,
            action_schema=action_schema,
            doctrine_context=state_point.doctrine_text,
            notes=[
                "Strategic state-point row.",
                "no_future_context=true",
                "state_point_not_historical_branch_event=true",
                "decision_point_proposed_by_llm_or_template=true",
            ],
        )
        row = WhatIfBenchmarkDatasetRow(
            row_id=f"{state_point.branch_event.event_id}:candidate_{index}",
            split="heldout",
            thread_id=state_point.branch_event.thread_id,
            branch_event_id=state_point.branch_event.event_id,
            contract=contract,
            observed_evidence_heads=evidence,
            observed_business_outcomes=evidence_to_business_outcomes(evidence),
            observed_future_state=summarize_future_state_heads(
                future_events=state_point.future_events,
                evidence=evidence,
            ),
            observed_targets=observed_targets,
            observed_outcome_signals=outcome_targets_to_signals(observed_targets),
        )
        pending_rows.append((index, candidate, candidate_type, row))
    predictions = run_branch_point_benchmark_predictions(
        checkpoint_path=checkpoint_path,
        rows=[item[3] for item in pending_rows],
        device=device,
        runtime_root=runtime_root,
        output_root=prediction_output_root,
    )
    for (index, candidate, candidate_type, _row), prediction in zip(
        pending_rows,
        predictions,
        strict=True,
    ):
        business = dict(prediction["business_heads"])
        future_state_heads = dict(prediction["future_state_heads"])
        evidence_heads = dict(prediction["evidence_heads"])
        observables = _candidate_observables(candidate, state_point.decision.title)
        latent_vector = prediction.get("latent_future_vector") or []
        rows.append(
            {
                "group": state_point.tenant_id,
                "company_or_corpus": state_point.display_name,
                "as_of": state_point.as_of,
                "decision_point": state_point.decision.title,
                "decision_question": state_point.decision.decision_question,
                "why_this_decision_was_proposed": state_point.decision.why_selected,
                "decision_point_source": state_point.proposal_source,
                "decision_point_model": state_point.proposal_model,
                "decision_point_not_historical_event": True,
                "counterfactual_action": candidate.action,
                "candidate_label": candidate.label,
                "candidate_type": candidate_type,
                "candidate_generation_source": state_point.proposal_source,
                "candidate_generation_model": state_point.proposal_model,
                "candidate_scoring_model": str(prediction.get("model_id", "")),
                "learned_model_output": "predicted future vector",
                "model_output_kind": "predicted_future_vector",
                "jepa_model_id": str(prediction.get("model_id", "")),
                "jepa_checkpoint_id": str(prediction.get("jepa_checkpoint_id", "")),
                "jepa_encoder_versions": json.dumps(
                    prediction.get("encoder_versions", {}),
                    sort_keys=True,
                ),
                "prediction_head_version": str(
                    prediction.get("prediction_head_version", PREDICTION_HEAD_VERSION)
                ),
                "prediction_probe_version": str(
                    prediction.get("prediction_probe_version", PREDICTION_PROBE_VERSION)
                ),
                "latent_future_id": str(prediction.get("latent_future_id", "")),
                "latent_future_norm": prediction.get("latent_future_norm", ""),
                "_latent_future_vector": latent_vector,
                "balanced_operator_score": _balanced_operator_score(business),
                "score_output_kind": "operator_utility_readout",
                "operator_score_formula_version": OPERATOR_SCORE_FORMULA_VERSION,
                "operator_score_formula": OPERATOR_SCORE_FORMULA,
                "operator_score_is_learned": False,
                "operator_utility_heads": _operator_utility_heads_string(business),
                "domain_risk_heads": _domain_risk_heads_string(future_state_heads),
                "telemetry_heads": _telemetry_heads_string(
                    evidence_heads=evidence_heads,
                    future_state_heads=future_state_heads,
                ),
                "predicted_enterprise_risk": business["enterprise_risk"],
                "predicted_commercial_position": business["commercial_position_proxy"],
                "predicted_org_strain": business["org_strain_proxy"],
                "predicted_stakeholder_trust": business["stakeholder_trust"],
                "predicted_execution_drag": business["execution_drag"],
                "predicted_future_vector": _future_vector_string_from_business(
                    business,
                    future_state_heads=future_state_heads,
                ),
                "predicted_external_spread": evidence_heads["any_external_spread"],
                "predicted_participant_fanout": evidence_heads["participant_fanout"],
                "predicted_regulatory_exposure": future_state_heads[
                    "regulatory_exposure"
                ],
                "predicted_accounting_control_pressure": future_state_heads[
                    "accounting_control_pressure"
                ],
                "predicted_liquidity_stress": future_state_heads["liquidity_stress"],
                "predicted_governance_response": future_state_heads[
                    "governance_response"
                ],
                "predicted_evidence_control": future_state_heads["evidence_control"],
                "predicted_external_confidence_pressure": future_state_heads[
                    "external_confidence_pressure"
                ],
                "success_observable": observables["success_observable"],
                "failure_observable": observables["failure_observable"],
                "time_to_signal": observables["time_to_signal"],
                "next_decision_trigger": observables["next_decision_trigger"],
                "falsifying_evidence": observables["falsifying_evidence"],
                "observable_source": observables["observable_source"],
                "no_future_context": True,
                "pre_branch_evidence_sha256": state_point.evidence_hash,
                "generation_prompt_sha256": state_point.prompt_hash,
                "ranking_basis": (
                    "Pareto frontier over JEPA-predicted operator utility and domain-risk heads; "
                    "balanced operator score is a non-learned sorting aid"
                ),
                "pareto_basis_version": PARETO_BASIS_VERSION,
                "prediction_uncertainty_available": False,
                "actual_outcome_vector_available": False,
                "prediction_error_available": False,
                "not_used_as_ground_truth": (
                    "decision proposal reason, candidate type, and any operator lens"
                ),
                "case_id": state_point.branch_event.case_id,
            }
        )
    return rows


def _rank_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["case_id"]), []).append(row)
    ranked: list[dict[str, Any]] = []
    for case_rows in grouped.values():
        for row in case_rows:
            row["is_pareto_efficient"] = True
        for row in case_rows:
            for other in case_rows:
                if row is other:
                    continue
                if _dominates(other, row):
                    row["is_pareto_efficient"] = False
                    break
        _attach_baseline_deltas(case_rows)
        _attach_latent_distances(case_rows)
        score_ordered = sorted(
            case_rows,
            key=lambda item: (
                -float(item["balanced_operator_score"]),
                float(item["predicted_enterprise_risk"]),
                str(item["candidate_label"]).lower(),
            ),
        )
        for index, row in enumerate(score_ordered, start=1):
            row["operator_score_rank"] = index
        for index, row in enumerate(score_ordered):
            next_row = (
                score_ordered[index + 1] if index + 1 < len(score_ordered) else None
            )
            if next_row is None:
                row["operator_score_margin_to_next_score_rank"] = ""
                row["operator_score_near_tie_next"] = False
            else:
                margin = round(
                    float(row["balanced_operator_score"])
                    - float(next_row["balanced_operator_score"]),
                    6,
                )
                row["operator_score_margin_to_next_score_rank"] = margin
                row["operator_score_near_tie_next"] = margin < _NEAR_TIE_MARGIN
        frontier_ordered = sorted(
            [row for row in case_rows if bool(row["is_pareto_efficient"])],
            key=lambda item: (
                -float(item["balanced_operator_score"]),
                float(item["predicted_enterprise_risk"]),
                str(item["candidate_label"]).lower(),
            ),
        )
        frontier_ids = {
            id(row): index for index, row in enumerate(frontier_ordered, start=1)
        }
        for row in case_rows:
            row["pareto_frontier_group"] = (
                "frontier" if bool(row["is_pareto_efficient"]) else "dominated"
            )
            row["frontier_rank"] = frontier_ids.get(id(row), "")
        display_ordered = sorted(
            case_rows,
            key=lambda item: (
                not bool(item["is_pareto_efficient"]),
                int(item["frontier_rank"]) if item["frontier_rank"] else 10_000,
                int(item["operator_score_rank"]),
                str(item["candidate_label"]).lower(),
            ),
        )
        for index, row in enumerate(display_ordered, start=1):
            row["display_rank"] = index
        for index, row in enumerate(display_ordered):
            next_row = (
                display_ordered[index + 1] if index + 1 < len(display_ordered) else None
            )
            row["display_neighbor_score_delta"] = (
                ""
                if next_row is None
                else round(
                    float(row["balanced_operator_score"])
                    - float(next_row["balanced_operator_score"]),
                    6,
                )
            )
            row["ranking_caveat"] = (
                "near score tie; inspect frontier membership, deltas, and observables"
                if bool(row["operator_score_near_tie_next"])
                else "frontier/readout view; inspect predicted vector before choosing"
            )
        _drop_internal_fields(display_ordered)
        ranked.extend(display_ordered)
    ranked.sort(
        key=lambda item: (
            str(item["group"]),
            str(item["as_of"]),
            str(item["decision_point"]).lower(),
            int(item["display_rank"]),
        )
    )
    return ranked


def _dominates(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_values = _future_axes(left)
    right_values = _future_axes(right)
    return all(left_values[name] >= right_values[name] for name in left_values) and any(
        left_values[name] > right_values[name] for name in left_values
    )


def _future_axes(row: dict[str, Any]) -> dict[str, float]:
    return {
        "risk_inverse": 1.0 - float(row["predicted_enterprise_risk"]),
        "commercial": float(row["predicted_commercial_position"]),
        "strain_inverse": 1.0 - float(row["predicted_org_strain"]),
        "trust": float(row["predicted_stakeholder_trust"]),
        "drag_inverse": 1.0 - float(row["predicted_execution_drag"]),
        "regulatory_inverse": 1.0 - float(row["predicted_regulatory_exposure"]),
        "accounting_control_inverse": 1.0
        - float(row["predicted_accounting_control_pressure"]),
        "liquidity_inverse": 1.0 - float(row["predicted_liquidity_stress"]),
        "external_confidence_inverse": 1.0
        - float(row["predicted_external_confidence_pressure"]),
    }


def _balanced_operator_score(business: dict[str, Any]) -> float:
    values = [
        1.0 - float(business["enterprise_risk"]),
        float(business["commercial_position_proxy"]),
        1.0 - float(business["org_strain_proxy"]),
        float(business["stakeholder_trust"]),
        1.0 - float(business["execution_drag"]),
    ]
    return round(sum(values) / len(values), 6)


def _operator_utility_heads_string(business: dict[str, Any]) -> str:
    return (
        f"risk {float(business['enterprise_risk']):.3f}; "
        f"commercial {float(business['commercial_position_proxy']):.3f}; "
        f"trust {float(business['stakeholder_trust']):.3f}; "
        f"drag {float(business['execution_drag']):.3f}; "
        f"strain {float(business['org_strain_proxy']):.3f}"
    )


def _domain_risk_heads_string(future_state_heads: dict[str, Any]) -> str:
    return (
        f"regulatory {float(future_state_heads['regulatory_exposure']):.3f}; "
        "accounting control "
        f"{float(future_state_heads['accounting_control_pressure']):.3f}; "
        f"liquidity {float(future_state_heads['liquidity_stress']):.3f}; "
        "external confidence "
        f"{float(future_state_heads['external_confidence_pressure']):.3f}"
    )


def _telemetry_heads_string(
    *,
    evidence_heads: dict[str, Any],
    future_state_heads: dict[str, Any],
) -> str:
    return (
        f"external spread {float(evidence_heads['any_external_spread']):.3f}; "
        f"participant fanout {float(evidence_heads['participant_fanout']):.3f}; "
        f"governance response {float(future_state_heads['governance_response']):.3f}; "
        f"evidence control {float(future_state_heads['evidence_control']):.3f}"
    )


def _attach_baseline_deltas(case_rows: list[dict[str, Any]]) -> None:
    baseline = _select_baseline_row(case_rows)
    baseline_vector = _comparison_vector(baseline)
    baseline_basis = _baseline_basis(baseline)
    for row in case_rows:
        row["baseline_action_label"] = baseline["candidate_label"]
        row["baseline_candidate_type"] = baseline["candidate_type"]
        row["baseline_basis"] = baseline_basis
        row["baseline_future_vector"] = _future_vector_string(baseline)
        delta_values: dict[str, float] = {}
        for key, short_name, _higher_is_better in _DELTA_HEADS:
            delta = round(float(row[key]) - baseline_vector[key], 6)
            delta_values[short_name] = delta
            row[f"delta_{short_name}_vs_baseline"] = delta
        row["operator_score_delta_vs_baseline"] = round(
            float(row["balanced_operator_score"])
            - float(baseline["balanced_operator_score"]),
            6,
        )
        row["predicted_delta_vector"] = _delta_vector_string(delta_values)
        row["tradeoff_summary"] = _tradeoff_summary(delta_values)


def _attach_latent_distances(case_rows: list[dict[str, Any]]) -> None:
    baseline = _select_baseline_row(case_rows)
    baseline_vector = _latent_vector(baseline)
    for row in case_rows:
        vector = _latent_vector(row)
        if vector is None or baseline_vector is None:
            row["latent_distance_available"] = False
            row["latent_future_l2_distance_from_baseline"] = ""
            row["latent_future_cosine_distance_from_baseline"] = ""
            continue
        row["latent_distance_available"] = True
        row["latent_future_l2_distance_from_baseline"] = _latent_l2(
            vector, baseline_vector
        )
        row["latent_future_cosine_distance_from_baseline"] = _latent_cosine_distance(
            vector, baseline_vector
        )
    for row in case_rows:
        vector = _latent_vector(row)
        nearest_label = ""
        nearest_distance: float | str = ""
        if vector is not None:
            for other in case_rows:
                if other is row:
                    continue
                other_vector = _latent_vector(other)
                if other_vector is None:
                    continue
                distance = _latent_cosine_distance(vector, other_vector)
                if distance == "":
                    continue
                if nearest_distance == "" or float(distance) < float(nearest_distance):
                    nearest_distance = distance
                    nearest_label = str(other["candidate_label"])
        row["nearest_latent_candidate_label"] = nearest_label
        row["latent_future_distance_to_nearest_candidate"] = nearest_distance


def _latent_vector(row: dict[str, Any]) -> np.ndarray | None:
    values = row.get("_latent_future_vector")
    if not values:
        return None
    array = np.asarray(values, dtype=float)
    if array.ndim != 1 or array.size == 0:
        return None
    return array


def _latent_l2(left: np.ndarray, right: np.ndarray) -> float | str:
    if left.shape != right.shape:
        return ""
    return round(float(np.linalg.norm(left - right)), 6)


def _latent_cosine_distance(left: np.ndarray, right: np.ndarray) -> float | str:
    if left.shape != right.shape:
        return ""
    denominator = float(np.linalg.norm(left) * np.linalg.norm(right))
    if denominator == 0.0:
        return ""
    similarity = float(np.dot(left, right) / denominator)
    return round(1.0 - similarity, 6)


def _drop_internal_fields(rows: Sequence[dict[str, Any]]) -> None:
    for row in rows:
        row.pop("_latent_future_vector", None)


def _select_baseline_row(case_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    return sorted(
        case_rows,
        key=lambda row: (
            _baseline_priority(row),
            float(row["balanced_operator_score"]),
            str(row["candidate_label"]).lower(),
        ),
    )[0]


def _baseline_priority(row: dict[str, Any]) -> int:
    text = " ".join(
        [
            str(row.get("candidate_type", "")),
            str(row.get("candidate_label", "")),
            str(row.get("counterfactual_action", "")),
        ]
    ).lower()
    if "hold" in text or "pause" in text or "defer" in text:
        return 0
    if "review" in text:
        return 1
    return 2


def _baseline_basis(row: dict[str, Any]) -> str:
    priority = _baseline_priority(row)
    if priority == 0:
        return "hold_or_pause_candidate_predicted_future"
    if priority == 1:
        return "review_candidate_predicted_future"
    return "lowest_operator_score_candidate_predicted_future"


def _comparison_vector(row: dict[str, Any]) -> dict[str, float]:
    return {key: float(row[key]) for key, _short_name, _higher in _DELTA_HEADS}


def _future_vector_string(row: dict[str, Any]) -> str:
    return (
        f"risk {float(row['predicted_enterprise_risk']):.3f}; "
        f"commercial {float(row['predicted_commercial_position']):.3f}; "
        f"trust {float(row['predicted_stakeholder_trust']):.3f}; "
        f"drag {float(row['predicted_execution_drag']):.3f}; "
        f"strain {float(row['predicted_org_strain']):.3f}; "
        f"regulatory {float(row['predicted_regulatory_exposure']):.3f}; "
        "accounting control "
        f"{float(row['predicted_accounting_control_pressure']):.3f}; "
        f"liquidity {float(row['predicted_liquidity_stress']):.3f}; "
        "external confidence "
        f"{float(row['predicted_external_confidence_pressure']):.3f}; "
        f"governance {float(row['predicted_governance_response']):.3f}; "
        f"evidence control {float(row['predicted_evidence_control']):.3f}"
    )


def _future_vector_string_from_business(
    business: dict[str, Any],
    *,
    future_state_heads: dict[str, Any],
) -> str:
    return (
        f"risk {float(business['enterprise_risk']):.3f}; "
        f"commercial {float(business['commercial_position_proxy']):.3f}; "
        f"trust {float(business['stakeholder_trust']):.3f}; "
        f"drag {float(business['execution_drag']):.3f}; "
        f"strain {float(business['org_strain_proxy']):.3f}; "
        f"regulatory {float(future_state_heads['regulatory_exposure']):.3f}; "
        "accounting control "
        f"{float(future_state_heads['accounting_control_pressure']):.3f}; "
        f"liquidity {float(future_state_heads['liquidity_stress']):.3f}; "
        "external confidence "
        f"{float(future_state_heads['external_confidence_pressure']):.3f}; "
        f"governance {float(future_state_heads['governance_response']):.3f}; "
        f"evidence control {float(future_state_heads['evidence_control']):.3f}"
    )


def _delta_vector_string(delta_values: dict[str, float]) -> str:
    return "; ".join(f"{name} {value:+.3f}" for name, value in delta_values.items())


def _tradeoff_summary(delta_values: dict[str, float]) -> str:
    gains: list[str] = []
    costs: list[str] = []
    for _key, short_name, higher_is_better in _DELTA_HEADS:
        delta = delta_values[short_name]
        if abs(delta) < _DELTA_EPSILON:
            continue
        is_gain = delta > 0 if higher_is_better else delta < 0
        entry = f"{short_name} {delta:+.3f}"
        if is_gain:
            gains.append(entry)
        else:
            costs.append(entry)
    if not gains and not costs:
        return "Near baseline across main predicted heads."
    parts: list[str] = []
    if gains:
        parts.append("Gains: " + ", ".join(gains[:3]))
    if costs:
        parts.append("Costs: " + ", ".join(costs[:3]))
    return "; ".join(parts) + "."


def _proposal_schema(
    *,
    decisions_per_source: int,
    candidates_per_decision: int,
) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "decisions": {
                "type": "array",
                "minItems": decisions_per_source,
                "maxItems": decisions_per_source,
                "items": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "decision_question": {"type": "string"},
                        "why_selected": {"type": "string"},
                        "topic": {"type": "string"},
                        "candidates": {
                            "type": "array",
                            "minItems": candidates_per_decision,
                            "maxItems": candidates_per_decision,
                            "items": {
                                "type": "object",
                                "properties": {
                                    "label": {"type": "string"},
                                    "candidate_type": {"type": "string"},
                                    "action": {"type": "string"},
                                    "success_observable": {"type": "string"},
                                    "failure_observable": {"type": "string"},
                                    "time_to_signal": {"type": "string"},
                                    "next_decision_trigger": {"type": "string"},
                                    "falsifying_evidence": {"type": "string"},
                                },
                                "required": [
                                    "label",
                                    "candidate_type",
                                    "action",
                                    "success_observable",
                                    "failure_observable",
                                    "time_to_signal",
                                    "next_decision_trigger",
                                    "falsifying_evidence",
                                ],
                                "additionalProperties": False,
                            },
                        },
                    },
                    "required": [
                        "title",
                        "decision_question",
                        "why_selected",
                        "topic",
                        "candidates",
                    ],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["decisions"],
        "additionalProperties": False,
    }


def _decisions_from_payload(
    payload: dict[str, Any],
    *,
    as_of: datetime,
    decisions_per_source: int,
    candidates_per_decision: int,
) -> list[StrategicDecisionInput]:
    decisions: list[StrategicDecisionInput] = []
    for raw in list(payload.get("decisions") or [])[:decisions_per_source]:
        candidates = [
            StrategicCandidateInput(
                label=str(item.get("label") or f"Candidate {index}").strip(),
                candidate_type=str(item.get("candidate_type") or "").strip(),
                action=str(item.get("action") or "").strip(),
                success_observable=str(item.get("success_observable") or "").strip(),
                failure_observable=str(item.get("failure_observable") or "").strip(),
                time_to_signal=str(item.get("time_to_signal") or "").strip(),
                next_decision_trigger=str(
                    item.get("next_decision_trigger") or ""
                ).strip(),
                falsifying_evidence=str(item.get("falsifying_evidence") or "").strip(),
            )
            for index, item in enumerate(
                list(raw.get("candidates") or [])[:candidates_per_decision],
                start=1,
            )
            if str(item.get("action") or "").strip()
        ]
        if len(candidates) < candidates_per_decision:
            continue
        decisions.append(
            StrategicDecisionInput(
                title=str(raw.get("title") or "Strategic state point").strip(),
                decision_question=str(raw.get("decision_question") or "").strip(),
                why_selected=str(raw.get("why_selected") or "").strip(),
                topic=str(raw.get("topic") or "").strip(),
                as_of=as_of.isoformat().replace("+00:00", "Z"),
                candidates=candidates,
            )
        )
    if len(decisions) != decisions_per_source:
        raise ValueError(
            f"expected {decisions_per_source} strategic decisions, got {len(decisions)}"
        )
    return decisions


def _template_proposal_payload(
    *,
    source: StrategicStatePointSource,
    as_of: datetime,
    decisions_per_source: int,
    candidates_per_decision: int,
) -> dict[str, Any]:
    name = (
        source.display_name
        or source.world.summary.organization_name
        or source.tenant_id
    )
    templates = [
        (
            "Strategic focus and market wedge",
            f"As of {as_of.date().isoformat()}, should {name} narrow around the strongest wedge, broaden distribution, or hold for more proof?",
        ),
        (
            "External proof and partner push",
            f"As of {as_of.date().isoformat()}, should {name} push a major external proof point or keep execution internal?",
        ),
        (
            "Trust, evidence, and governance posture",
            f"As of {as_of.date().isoformat()}, should {name} make a stronger trust/evidence move before scaling the opportunity?",
        ),
    ][:decisions_per_source]
    return {
        "decisions": [
            {
                "title": title,
                "decision_question": question,
                "why_selected": (
                    "The pre-as-of archive shows enough product, commercial, external-stakeholder, "
                    "or governance signal to make this a useful strategic state point."
                ),
                "topic": _slug(title),
                "candidates": _template_candidates(
                    name=name,
                    title=title,
                    candidates_per_decision=candidates_per_decision,
                ),
            }
            for title, question in templates
        ]
    }


def _template_candidates(
    *,
    name: str,
    title: str,
    candidates_per_decision: int,
) -> list[dict[str, str]]:
    base = [
        (
            "exploit_upside",
            "Exploit the upside",
            f"Make the bold move on '{title}': name the executive owner, pick the highest-upside customer or partner target, send a concrete proposal this week, and define the proof metric.",
        ),
        (
            "narrow_pilot",
            "Run focused pilot",
            f"Run '{title}' as a bounded pilot with one owner, one cohort or customer, explicit success criteria, privacy/review guardrails, and a two-week decision date.",
        ),
        (
            "fast_move",
            "Move fast with review",
            f"Ship the smallest credible version of '{title}' now, keep the scope narrow, do one review pass, and report the result to leadership within 48 hours.",
        ),
        (
            "hold_review",
            "Hold for review",
            f"Hold '{title}' until leadership reviews risk, evidence, customer promise, and resource cost; do not widen the loop until a written decision is made.",
        ),
        (
            "executive_escalation",
            "Escalate executive decision",
            f"Escalate '{title}' to the CEO/founder with a one-page memo covering upside, risk, resource ask, owner, and a go/no-go call.",
        ),
        (
            "coordination_move",
            "Coordinate cross-functionally",
            f"Create a time-boxed {name} coordination room for '{title}' across product, commercial, trust, and operations with daily decisions and named handoffs.",
        ),
        (
            "commercial_reset",
            "Reset commercial path",
            f"Reset the commercial path for '{title}': choose the buyer/user segment, state what will not be promised, and make the next external ask specific.",
        ),
        (
            "trust_evidence_path",
            "Strengthen trust evidence",
            f"Before scaling '{title}', create the evidence trail: customer language, consent/privacy basis, QA or governance check, owner, and follow-up trigger.",
        ),
    ]
    candidates: list[dict[str, str]] = []
    for kind, label, action in base[:candidates_per_decision]:
        candidates.append(
            {
                "candidate_type": kind,
                "label": label,
                "action": action,
                **_fallback_observable_fields(label=label, title=title),
            }
        )
    return candidates


def _candidate_observables(
    candidate: StrategicCandidateInput,
    decision_title: str,
) -> dict[str, str]:
    fields = {
        "success_observable": candidate.success_observable.strip(),
        "failure_observable": candidate.failure_observable.strip(),
        "time_to_signal": candidate.time_to_signal.strip(),
        "next_decision_trigger": candidate.next_decision_trigger.strip(),
        "falsifying_evidence": candidate.falsifying_evidence.strip(),
    }
    if all(fields.values()):
        return {**fields, "observable_source": "candidate_generation_model"}
    fallback = _fallback_observable_fields(
        label=candidate.label,
        title=decision_title,
    )
    return {
        key: fields[key] or fallback[key]
        for key in (
            "success_observable",
            "failure_observable",
            "time_to_signal",
            "next_decision_trigger",
            "falsifying_evidence",
        )
    } | {"observable_source": "deterministic_observable_fallback_v1"}


def _fallback_observable_fields(*, label: str, title: str) -> dict[str, str]:
    label_text = label.strip() or "candidate"
    title_text = title.strip() or "the decision"
    return {
        "success_observable": (
            f"{label_text}: named owner, measurable next step, and stakeholder "
            f"response for {title_text} are visible before the signal window closes."
        ),
        "failure_observable": (
            f"{label_text}: no owner or measurable response appears, or the branch "
            "creates new escalation without clearer evidence."
        ),
        "time_to_signal": "5-10 business days",
        "next_decision_trigger": (
            "If the signal window closes without the named success observable, "
            "reassess the branch and stop expanding scope."
        ),
        "falsifying_evidence": (
            "Archive evidence shows the assumed owner, buyer, regulator, customer, "
            "or operational constraint does not exist."
        ),
    }


def _synthetic_branch_event(
    *,
    source: StrategicStatePointSource,
    world: WhatIfWorld,
    as_of: datetime,
    decision: StrategicDecisionInput,
    index: int,
) -> WhatIfEvent:
    domain = world.summary.organization_domain or f"{source.tenant_id}.local"
    event_id = f"strategic_state:{source.tenant_id}:{_slug(decision.title)}:{as_of.date().isoformat()}"
    return WhatIfEvent(
        event_id=event_id,
        timestamp=as_of.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        timestamp_ms=int(as_of.timestamp() * 1000),
        actor_id="vei.strategy_agent",
        target_id=domain,
        event_type="strategic_state_point",
        thread_id=f"strategic_state:{source.tenant_id}",
        case_id=f"strategic_state:{source.tenant_id}:{index}:{_slug(decision.title)}",
        surface="state",
        conversation_anchor=f"strategic_state:{source.tenant_id}",
        subject=decision.title,
        snippet=decision.decision_question,
        flags=WhatIfArtifactFlags(
            to_recipients=[domain],
            to_count=1,
            subject=decision.title,
        ),
    )


def _action_schema_for_candidate(
    *,
    action: str,
    candidate_type: str,
) -> WhatIfActionSchema:
    lowered = action.lower()
    external = any(
        token in lowered
        for token in (
            "customer",
            "client",
            "buyer",
            "partner",
            "investor",
            "vc",
            "sam altman",
            "openai",
            "market",
            "public",
            "press",
            "external",
            "stakeholder",
        )
    )
    broad = any(
        token in lowered
        for token in (
            "cross-functional",
            "war room",
            "company",
            "leadership",
            "team",
            "coordinate",
            "daily",
            "broad",
        )
    )
    hold = any(token in lowered for token in ("hold", "pause", "defer", "do not"))
    review = any(
        token in lowered
        for token in ("review", "legal", "privacy", "consent", "governance", "qa")
    )
    escalate = any(
        token in lowered
        for token in ("ceo", "founder", "executive", "leadership", "escalate")
    )
    tags = {candidate_type}
    if external:
        tags.add("external_action")
    if broad:
        tags.add("coordination")
    if hold:
        tags.add("hold")
    if review:
        tags.add("review")
    if escalate:
        tags.add("escalation")
    return WhatIfActionSchema(
        event_type="strategic_state_point",
        action_text=action,
        recipient_scope=(
            "mixed" if broad and external else ("external" if external else "internal")
        ),
        external_recipient_count=2 if broad and external else (1 if external else 0),
        attachment_policy="sanitized" if external else "none",
        hold_required=hold,
        legal_review_required="legal" in lowered
        or "privacy" in lowered
        or "consent" in lowered,
        trading_review_required=False,
        escalation_level="executive" if escalate else ("manager" if review else "none"),
        owner_clarity=(
            "single_owner" if "owner" in lowered or "ceo" in lowered else "unclear"
        ),
        reassurance_style=(
            "high" if "trust" in lowered or "status" in lowered else "low"
        ),
        review_path=(
            "cross_functional" if broad else ("business_owner" if review else "none")
        ),
        coordination_breadth=(
            "broad" if broad else ("targeted" if external else "single_owner")
        ),
        outside_sharing_posture="limited_external" if external else "internal_only",
        decision_posture="hold" if hold else ("escalate" if escalate else "resolve"),
        action_tags=sorted(tag for tag in tags if tag),
    )


def _infer_candidate_type(action: str) -> str:
    lowered = action.lower()
    if any(token in lowered for token in ("pilot", "bounded", "cohort")):
        return "narrow_pilot"
    if any(token in lowered for token in ("hold", "pause", "defer")):
        return "hold_review"
    if any(token in lowered for token in ("ceo", "founder", "executive", "escalate")):
        return "executive_escalation"
    if any(
        token in lowered for token in ("coordinate", "war room", "cross-functional")
    ):
        return "coordination_move"
    if any(token in lowered for token in ("customer", "buyer", "commercial", "market")):
        return "commercial_reset"
    if any(token in lowered for token in ("trust", "privacy", "consent", "evidence")):
        return "trust_evidence_path"
    if any(token in lowered for token in ("ship", "send", "now", "fast")):
        return "fast_move"
    return "exploit_upside"


def _select_evidence_events(
    history_events: Sequence[WhatIfEvent],
    *,
    max_events: int,
) -> list[WhatIfEvent]:
    scored = sorted(
        history_events,
        key=lambda event: (
            -_strategic_signal_score(event),
            -event.timestamp_ms,
            event.event_id,
        ),
    )
    selected = sorted(
        scored[:max_events],
        key=lambda event: (event.timestamp_ms, event.event_id),
    )
    return selected


def _strategic_signal_score(event: WhatIfEvent) -> int:
    text = _event_text(event)
    terms = (
        "customer",
        "client",
        "buyer",
        "partner",
        "pilot",
        "proof",
        "launch",
        "risk",
        "legal",
        "privacy",
        "consent",
        "governance",
        "data",
        "research",
        "investor",
        "market",
        "bank",
        "credit",
        "treasury",
        "ceo",
        "founder",
        "decision",
    )
    return sum(1 for term in terms if term in text)


def _event_text(event: WhatIfEvent) -> str:
    return " ".join([event.subject, event.snippet, event.target_id]).lower()


def _event_evidence_line(event: WhatIfEvent) -> str:
    return (
        f"{event.timestamp} | {event.surface} | {event.subject} | "
        f"{event.snippet[:500]}"
    )


def _default_as_of(events: Sequence[WhatIfEvent]) -> datetime:
    index = max(0, min(len(events) - 1, int((len(events) - 1) * 0.85)))
    return _parse_datetime(events[index].timestamp)


def _parse_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "T" not in text:
        text = f"{text}T00:00:00+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _state_point_payload(state_point: _StrategicStatePoint) -> dict[str, Any]:
    return {
        "tenant_id": state_point.tenant_id,
        "display_name": state_point.display_name,
        "as_of": state_point.as_of,
        "state_event_id": state_point.branch_event.event_id,
        "state_point_not_historical_branch_event": True,
        "no_future_context_for_state": True,
        "decision_title": state_point.decision.title,
        "decision_question": state_point.decision.decision_question,
        "why_selected": state_point.decision.why_selected,
        "proposal_source": state_point.proposal_source,
        "proposal_model": state_point.proposal_model,
        "history_event_count": len(state_point.history_events),
        "future_event_count": len(state_point.future_events),
        "evidence_events": [
            {
                "event_id": event.event_id,
                "timestamp": event.timestamp,
                "subject": event.subject,
                "snippet": event.snippet,
                "surface": event.surface,
            }
            for event in state_point.evidence_events
        ],
    }


def _write_rows_csv(rows: Sequence[dict[str, Any]], path: Path) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown_result(
    *,
    rows: Sequence[dict[str, Any]],
    state_points: Sequence[_StrategicStatePoint],
    path: Path,
) -> None:
    lines = [
        "# Strategic State-Point World Model Results",
        "",
        "This run uses proposed strategic decision points, not only historical branch events.",
        "",
        "Read each section as: as-of state -> proposed decision -> candidate action -> JEPA-predicted future vector -> optional operator readout.",
        "",
        "What is learned: JEPA predicts future heads for each candidate action. The operator score, proposal reason, and candidate type are reporting scaffolding, not learned scores.",
        "",
        "Score direction: the operator readout rewards higher Commercial and Trust and lower Risk, Drag, and Strain. Inspect the vector and deltas before treating the rank as a decision.",
        "",
    ]
    rows_by_case: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        rows_by_case.setdefault(str(row["case_id"]), []).append(row)
    for state_point in state_points:
        case_rows = rows_by_case.get(state_point.branch_event.case_id, [])
        if not case_rows:
            continue
        ordered = sorted(case_rows, key=lambda row: int(row["display_rank"]))
        top = ordered[0]
        baseline = top.get("baseline_action_label", "")
        lines.extend(
            [
                f"## {_md(state_point.display_name)}: {_md(state_point.decision.title)}",
                "",
                f"- As of: `{state_point.as_of}`",
                "- Decision source: "
                f"`{state_point.proposal_source}` using `{state_point.proposal_model}`",
                "- Historical event? `no, synthetic strategic state point`",
                f"- Why proposed: {_md(state_point.decision.why_selected)}",
                f"- Decision question: {_md(state_point.decision.decision_question)}",
                f"- Baseline action for deltas: **{_md(str(baseline))}**",
                "- Frontier basis: Pareto over JEPA-predicted operator utility and "
                f"domain-risk heads (`{PARETO_BASIS_VERSION}`).",
                "- Score basis: "
                f"`{OPERATOR_SCORE_FORMULA_VERSION}` is a non-learned operator "
                "sorting aid over five predicted heads.",
                f"- Shortlist lead: **{_md(str(top['candidate_label']))}**",
                "",
                "| Display | Frontier | Score rank | Candidate action | Operator score | Predicted future vector | Delta vs baseline | Tradeoff summary | Success observable |",
                "|---:|---|---:|---|---:|---|---|---|---|",
            ]
        )
        for row in ordered:
            lines.append(
                "| {display} | {frontier} | {score_rank} | {action} | {score:.3f} | {future} | {delta} | {tradeoff} | {success} |".format(
                    display=row["display_rank"],
                    frontier=_md(str(row["pareto_frontier_group"])),
                    score_rank=row["operator_score_rank"],
                    action=_md(str(row["counterfactual_action"])),
                    score=float(row["balanced_operator_score"]),
                    future=_md(str(row["predicted_future_vector"])),
                    delta=_md(str(row["predicted_delta_vector"])),
                    tradeoff=_md(str(row["tradeoff_summary"])),
                    success=_md(str(row["success_observable"])),
                )
            )
        lines.append("")
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _md(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    return slug or "strategic_state_point"


__all__ = [
    "StrategicCandidateInput",
    "StrategicDecisionInput",
    "StrategicProposalMode",
    "StrategicStatePointArtifacts",
    "StrategicStatePointRunResult",
    "StrategicStatePointSource",
    "build_strategic_state_point_proposal_prompt",
    "run_strategic_state_point_counterfactuals",
]
