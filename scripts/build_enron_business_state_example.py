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


def _render_markdown(payload: dict[str, Any]) -> str:
    objective_pack_id = str(payload.get("objective_pack_id") or "contain_exposure")
    lines = [
        f"# {payload['label']}",
        "",
        f"Objective pack: `{objective_pack_id}`",
        f"Thread: `{payload['thread_id']}`",
        f"Branch event: `{payload['branch_event_id']}`",
        f"Historical subject: {payload['thread_subject']}",
        "",
        "## Recorded Historical Path",
        f"- {payload['historical_business_state']['summary']}",
    ]
    for item in payload["historical_business_state"].get("implications", [])[:3]:
        lines.append(f"- {item}")
    lines.extend(["", "## Candidate Moves"])
    for candidate in payload["candidates"]:
        lines.extend(
            [
                f"### Rank {candidate['rank']}: {candidate['label']}",
                f"- Prompt: {candidate['prompt']}",
                f"- Business state: {candidate['business_state_change']['summary']}",
                f"- Net effect score: {candidate['business_state_change']['net_effect_score']}",
                f"- Forecast summary: {candidate['forecast']['summary']}",
                (
                    "- Risk change: "
                    f"{candidate['forecast']['baseline_risk_score']} -> "
                    f"{candidate['forecast']['predicted_risk_score']}"
                ),
                f"- External-send delta: {candidate['forecast']['external_event_delta']}",
            ]
        )
        if candidate["forecast"].get("baseline_stock_return_5d") is not None or candidate[
            "forecast"
        ].get("predicted_stock_return_5d") is not None:
            lines.append(
                "- Stock return (5d): "
                f"{candidate['forecast'].get('baseline_stock_return_5d')} -> "
                f"{candidate['forecast'].get('predicted_stock_return_5d')}"
            )
        if candidate["forecast"].get("baseline_credit_action_30d") is not None or candidate[
            "forecast"
        ].get("predicted_credit_action_30d") is not None:
            lines.append(
                "- Credit action (30d): "
                f"{candidate['forecast'].get('baseline_credit_action_30d')} -> "
                f"{candidate['forecast'].get('predicted_credit_action_30d')}"
            )
        if candidate["forecast"].get("baseline_ferc_action_180d") is not None or candidate[
            "forecast"
        ].get("predicted_ferc_action_180d") is not None:
            lines.append(
                "- FERC action (180d): "
                f"{candidate['forecast'].get('baseline_ferc_action_180d')} -> "
                f"{candidate['forecast'].get('predicted_ferc_action_180d')}"
            )
        for consequence in candidate["business_state_change"].get(
            "consequence_estimates",
            [],
        )[:4]:
            lines.append(f"- {consequence['summary']}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _candidate_payloads(
    scene,
    *,
    candidates: Sequence[dict[str, str]] | None,
) -> list[dict[str, str]]:
    if candidates is not None:
        return [
            {
                "label": str(candidate.get("label") or "").strip(),
                "prompt": str(candidate.get("prompt") or "").strip(),
            }
            for candidate in candidates
            if str(candidate.get("label") or "").strip()
            and str(candidate.get("prompt") or "").strip()
        ]
    return [
        {"label": option.label, "prompt": option.prompt}
        for option in scene.candidate_options
        if option.label.strip() and option.prompt.strip()
    ]


def build_example(
    output_root: Path = DEFAULT_EXAMPLE_ROOT,
    *,
    label: str | None = None,
    objective_pack_id: str = "contain_exposure",
    candidates: Sequence[dict[str, str]] | None = None,
) -> None:
    resolved_root = output_root.expanduser().resolve()
    workspace_root = resolved_root / WORKSPACE_DIRECTORY
    scene = build_saved_decision_scene(workspace_root)
    candidate_rows: list[dict[str, Any]] = []
    for option in _candidate_payloads(scene, candidates=candidates):
        forecast_result = estimate_counterfactual_delta(
            workspace_root,
            prompt=option["prompt"],
        )
        change = forecast_result.business_state_change
        if change is None:
            continue
        candidate_rows.append(
            {
                "label": option["label"],
                "prompt": option["prompt"],
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
                "business_state_change": change.model_dump(mode="json"),
            }
        )
    candidate_rows.sort(
        key=lambda item: item["business_state_change"]["net_effect_score"],
        reverse=True,
    )
    for index, candidate in enumerate(candidate_rows, start=1):
        candidate["rank"] = index
    payload = {
        "label": label or "enron_master_agreement_business_state_comparison_20260412",
        "objective_pack_id": objective_pack_id,
        "thread_id": scene.thread_id,
        "branch_event_id": scene.branch_event_id,
        "thread_subject": scene.thread_subject,
        "historical_business_state": (
            scene.historical_business_state.model_dump(mode="json")
            if scene.historical_business_state is not None
            else {}
        ),
        "candidates": candidate_rows,
    }
    json_path = resolved_root / BUSINESS_STATE_COMPARISON_FILE
    markdown_path = resolved_root / BUSINESS_STATE_COMPARISON_OVERVIEW_FILE
    json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")


if __name__ == "__main__":
    build_example()
