from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Sequence

from vei.whatif import build_saved_decision_scene, estimate_counterfactual_delta
from vei.whatif.filenames import (
    BUSINESS_STATE_COMPARISON_FILE,
    BUSINESS_STATE_COMPARISON_OVERVIEW_FILE,
    WORKSPACE_DIRECTORY,
)

DEFAULT_EXAMPLE_ROOT = Path("docs/examples/enron-master-agreement-public-context")
PUBLIC_OBJECTIVE_PACK = {
    "pack_id": "protect_company_default",
    "title": "Protect Company",
    "summary": (
        "Favor moves that lower legal exposure, preserve trust, limit commercial damage, "
        "and keep internal execution drag manageable."
    ),
    "outcomes": [
        "Legal and regulatory exposure",
        "Disclosure and stakeholder trust",
        "Commercial damage",
        "Internal execution drag",
    ],
}


def _render_markdown(payload: dict[str, Any]) -> str:
    public_summary = payload.get("public_summary") or {}
    lines = [
        f"# {payload['label']}",
        "",
        "## Branch Point",
        f"- {public_summary.get('branch_point') or payload.get('thread_subject') or ''}",
        "",
        "## What Actually Happened",
        f"- {public_summary.get('actual_happened') or payload.get('thread_subject') or ''}",
        "",
        "## Actions We Can Take",
    ]
    for candidate in payload["candidates"]:
        candidate_summary = candidate.get("public_summary") or {}
        lines.extend(
            [
                f"### Rank {candidate_summary.get('overall_rank') or candidate.get('rank')}: {candidate['label']}",
                f"- {candidate_summary.get('branch_explanation') or candidate.get('prompt')}",
            ]
        )
        outcome_rows = candidate_summary.get("public_outcomes") or []
        for outcome in outcome_rows:
            lines.append(
                f"- {outcome['label']}: {outcome['summary']} "
                f"({outcome['baseline_value']} -> {outcome['predicted_value']})"
            )
        lines.append(f"- Why: {candidate_summary.get('short_explanation') or ''}")
        lines.append("")
    lines.extend(["## Predicted Effect On The Company", ""])
    top_candidate = (
        (public_summary.get("candidates") or [{}])[0]
        if isinstance(public_summary.get("candidates"), list)
        else {}
    )
    if isinstance(top_candidate, dict) and top_candidate:
        lines.append(
            f"- Best current move: {top_candidate.get('action_label') or top_candidate.get('label')}"
        )
        lines.append(
            f"- Recorded future events after the historical branch: {top_candidate.get('recorded_future_event_count')}"
        )
        lines.append(f"- Readout: {top_candidate.get('short_explanation') or ''}")
    return "\n".join(lines).rstrip() + "\n"


def _candidate_payloads(
    scene,
    *,
    candidates: Sequence[dict[str, str]] | None,
) -> list[dict[str, str]]:
    if candidates is not None:
        payloads: list[dict[str, str]] = []
        for candidate in candidates:
            label = str(candidate.get("label") or "").strip()
            prompt = str(candidate.get("prompt") or "").strip()
            explanation = str(candidate.get("explanation") or "").strip()
            if not label or not prompt:
                continue
            payloads.append(
                {
                    "label": label,
                    "prompt": prompt,
                    "explanation": explanation,
                }
            )
        return payloads
    return [
        {
            "label": option.label,
            "prompt": option.prompt,
            "explanation": option.summary,
        }
        for option in scene.candidate_options
        if option.label.strip() and option.prompt.strip()
    ]


def _public_outcome_rows(change: dict[str, Any]) -> list[dict[str, Any]]:
    baseline = dict(change.get("baseline") or {})
    predicted = dict(change.get("predicted") or {})

    legal_baseline = _round(
        (float(baseline.get("exposure") or 0.0) * 0.7)
        + (float(baseline.get("governance_pressure") or 0.0) * 0.3)
    )
    legal_predicted = _round(
        (float(predicted.get("exposure") or 0.0) * 0.7)
        + (float(predicted.get("governance_pressure") or 0.0) * 0.3)
    )
    trust_baseline = _round(float(baseline.get("trust") or 0.0))
    trust_predicted = _round(float(predicted.get("trust") or 0.0))
    commercial_baseline = _round(1.0 - float(baseline.get("deal_position") or 0.0))
    commercial_predicted = _round(1.0 - float(predicted.get("deal_position") or 0.0))
    drag_baseline = _round(
        (
            float(baseline.get("coordination_load") or 0.0)
            + float(baseline.get("execution_delay") or 0.0)
        )
        / 2.0
    )
    drag_predicted = _round(
        (
            float(predicted.get("coordination_load") or 0.0)
            + float(predicted.get("execution_delay") or 0.0)
        )
        / 2.0
    )

    return [
        _outcome_row(
            label="Legal and regulatory exposure",
            baseline_value=legal_baseline,
            predicted_value=legal_predicted,
            lower_is_better=True,
        ),
        _outcome_row(
            label="Disclosure and stakeholder trust",
            baseline_value=trust_baseline,
            predicted_value=trust_predicted,
            lower_is_better=False,
        ),
        _outcome_row(
            label="Commercial damage",
            baseline_value=commercial_baseline,
            predicted_value=commercial_predicted,
            lower_is_better=True,
        ),
        _outcome_row(
            label="Internal execution drag",
            baseline_value=drag_baseline,
            predicted_value=drag_predicted,
            lower_is_better=True,
        ),
    ]


def _outcome_row(
    *,
    label: str,
    baseline_value: float,
    predicted_value: float,
    lower_is_better: bool,
) -> dict[str, Any]:
    delta = _round(predicted_value - baseline_value)
    if lower_is_better:
        better = predicted_value < baseline_value
    else:
        better = predicted_value > baseline_value
    if predicted_value == baseline_value:
        effect = "flat"
        summary = "stays flat"
    elif better:
        effect = "better"
        summary = "improves"
    else:
        effect = "worse"
        summary = "worsens"
    return {
        "label": label,
        "baseline_value": baseline_value,
        "predicted_value": predicted_value,
        "delta": delta,
        "effect": effect,
        "summary": summary,
    }


def _public_score(outcome_rows: Sequence[dict[str, Any]]) -> float:
    values = {row["label"]: row for row in outcome_rows}
    legal_predicted = float(
        (values.get("Legal and regulatory exposure") or {}).get("predicted_value")
        or 0.0
    )
    trust_predicted = float(
        (values.get("Disclosure and stakeholder trust") or {}).get("predicted_value")
        or 0.0
    )
    commercial_predicted = float(
        (values.get("Commercial damage") or {}).get("predicted_value") or 0.0
    )
    drag_predicted = float(
        (values.get("Internal execution drag") or {}).get("predicted_value") or 0.0
    )
    return _round(
        ((1.0 - legal_predicted) * 0.35)
        + (trust_predicted * 0.25)
        + ((1.0 - commercial_predicted) * 0.25)
        + ((1.0 - drag_predicted) * 0.15)
    )


def _round(value: float) -> float:
    return round(float(value), 3)


def build_example(
    output_root: Path = DEFAULT_EXAMPLE_ROOT,
    *,
    label: str | None = None,
    objective_pack_id: str = "contain_exposure",
    candidates: Sequence[dict[str, str]] | None = None,
    public_objective_pack_id: str = "protect_company_default",
    bundle_role: str = "proof",
    branch_point: str | None = None,
    actual_happened: str | None = None,
) -> None:
    resolved_root = output_root.expanduser().resolve()
    workspace_root = resolved_root / WORKSPACE_DIRECTORY
    scene = build_saved_decision_scene(workspace_root)
    candidate_rows: list[dict[str, Any]] = []
    for input_order, option in enumerate(
        _candidate_payloads(scene, candidates=candidates),
        start=1,
    ):
        forecast_result = estimate_counterfactual_delta(
            workspace_root,
            prompt=option["prompt"],
        )
        change = forecast_result.business_state_change
        if change is None:
            continue
        change_payload = change.model_dump(mode="json")
        public_outcomes = _public_outcome_rows(change_payload)
        candidate_rows.append(
            {
                "label": option["label"],
                "prompt": option["prompt"],
                "explanation": option["explanation"],
                "forecast": {
                    "backend": forecast_result.backend,
                    "summary": forecast_result.summary,
                    "baseline_risk_score": forecast_result.baseline.risk_score,
                    "predicted_risk_score": forecast_result.predicted.risk_score,
                    "external_event_delta": forecast_result.delta.external_event_delta,
                    "escalation_delta": forecast_result.delta.escalation_delta,
                    "baseline_stock_return_5d": forecast_result.baseline.stock_return_5d,
                    "predicted_stock_return_5d": forecast_result.predicted.stock_return_5d,
                    "baseline_credit_action_30d": forecast_result.baseline.credit_action_30d,
                    "predicted_credit_action_30d": forecast_result.predicted.credit_action_30d,
                    "baseline_ferc_action_180d": forecast_result.baseline.ferc_action_180d,
                    "predicted_ferc_action_180d": forecast_result.predicted.ferc_action_180d,
                },
                "business_state_change": change_payload,
                "_input_order": input_order,
                "public_summary": {
                    "action_label": option["label"],
                    "branch_explanation": option["explanation"] or option["prompt"],
                    "actual_historical_action": actual_happened
                    or scene.historical_action_summary
                    or "",
                    "recorded_future_event_count": scene.future_event_count,
                    "public_outcomes": public_outcomes,
                    "overall_rank": 0,
                    "short_explanation": change.summary,
                    "public_score": _public_score(public_outcomes),
                },
            }
        )
    candidate_rows.sort(
        key=lambda item: (
            -float((item.get("public_summary") or {}).get("public_score") or 0.0),
            -float(
                (item.get("business_state_change") or {}).get("net_effect_score") or 0.0
            ),
            int(item.get("_input_order") or 0),
        )
    )
    for index, candidate in enumerate(candidate_rows, start=1):
        candidate.pop("_input_order", None)
        candidate["rank"] = index
        candidate["public_summary"]["overall_rank"] = index

    payload = {
        "label": label or "enron_master_agreement_business_state_comparison_20260419",
        "objective_pack_id": public_objective_pack_id,
        "internal_objective_pack_id": objective_pack_id,
        "objective_pack": PUBLIC_OBJECTIVE_PACK,
        "thread_id": scene.thread_id,
        "branch_event_id": scene.branch_event_id,
        "thread_subject": scene.thread_subject,
        "historical_business_state": (
            scene.historical_business_state.model_dump(mode="json")
            if scene.historical_business_state is not None
            else {}
        ),
        "candidates": candidate_rows,
        "public_summary": {
            "bundle_role": bundle_role,
            "branch_point": branch_point
            or scene.branch_summary
            or scene.thread_subject,
            "actual_happened": actual_happened or scene.historical_action_summary or "",
            "actions_we_can_take": [
                {
                    "label": candidate["label"],
                    "branch_explanation": candidate["public_summary"][
                        "branch_explanation"
                    ],
                }
                for candidate in candidate_rows
            ],
            "predicted_effect_summary": (
                candidate_rows[0]["public_summary"]["short_explanation"]
                if candidate_rows
                else ""
            ),
            "recorded_future_event_count": scene.future_event_count,
            "candidates": [candidate["public_summary"] for candidate in candidate_rows],
        },
    }
    json_path = resolved_root / BUSINESS_STATE_COMPARISON_FILE
    markdown_path = resolved_root / BUSINESS_STATE_COMPARISON_OVERVIEW_FILE
    json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")


if __name__ == "__main__":
    build_example()
