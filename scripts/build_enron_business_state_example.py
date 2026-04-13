from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from vei.whatif import build_saved_decision_scene, estimate_counterfactual_delta

DEFAULT_EXAMPLE_ROOT = Path("docs/examples/enron-master-agreement-public-context")


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# {payload['label']}",
        "",
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
        for consequence in candidate["business_state_change"].get(
            "consequence_estimates",
            [],
        )[:4]:
            lines.append(f"- {consequence['summary']}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_example(output_root: Path = DEFAULT_EXAMPLE_ROOT) -> None:
    resolved_root = output_root.expanduser().resolve()
    workspace_root = resolved_root / "workspace"
    scene = build_saved_decision_scene(workspace_root)
    candidates: list[dict[str, Any]] = []
    for option in scene.candidate_options:
        forecast_result = estimate_counterfactual_delta(
            workspace_root,
            prompt=option.prompt,
        )
        change = forecast_result.business_state_change
        if change is None:
            continue
        candidates.append(
            {
                "label": option.label,
                "prompt": option.prompt,
                "forecast": {
                    "backend": forecast_result.backend,
                    "summary": forecast_result.summary,
                    "baseline_risk_score": forecast_result.baseline.risk_score,
                    "predicted_risk_score": forecast_result.predicted.risk_score,
                    "external_event_delta": forecast_result.delta.external_event_delta,
                    "escalation_delta": forecast_result.delta.escalation_delta,
                },
                "business_state_change": change.model_dump(mode="json"),
            }
        )
    candidates.sort(
        key=lambda item: item["business_state_change"]["net_effect_score"],
        reverse=True,
    )
    for index, candidate in enumerate(candidates, start=1):
        candidate["rank"] = index
    payload = {
        "label": "enron_master_agreement_business_state_comparison_20260412",
        "thread_id": scene.thread_id,
        "branch_event_id": scene.branch_event_id,
        "thread_subject": scene.thread_subject,
        "historical_business_state": (
            scene.historical_business_state.model_dump(mode="json")
            if scene.historical_business_state is not None
            else {}
        ),
        "candidates": candidates,
    }
    json_path = resolved_root / "whatif_business_state_comparison.json"
    markdown_path = resolved_root / "whatif_business_state_comparison.md"
    json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")


if __name__ == "__main__":
    build_example()
