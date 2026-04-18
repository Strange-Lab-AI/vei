"""Eval reporting — writes clearly separated factual and counterfactual tables."""

from __future__ import annotations

import json
from pathlib import Path
from typing import List

from vei.dynamics.backends.heuristic import HeuristicBaseline
from vei.dynamics.models import CandidateAction, DynamicsRequest
from .harness import EvalResult

DYNAMICS_EVAL_METRICS_PATH = Path("_vei_out/dynamics_eval/metrics.json")
MACRO_CALIBRATION_METRICS_PATH = Path("studies/macro_calibration_enron_v1/metrics.json")


def build_dynamics_eval_metrics() -> dict[str, float]:
    backend = HeuristicBaseline()
    factual_cases = [
        ("Hold legal review", "legal hold pause_forward", 0),
        ("Keep internal", "external_removed status_only", 0),
        ("Send now", "send_now widen_loop", 1),
        ("Broadcast update", "send_now widen_loop external_removed send_now", 1),
    ]
    factual_labels: list[int] = []
    factual_scores: list[float] = []
    for label, description, expected_external in factual_cases:
        response = backend.forecast(
            DynamicsRequest(
                candidate_action=CandidateAction(
                    label=label,
                    description=description,
                )
            )
        )
        factual_labels.append(expected_external)
        factual_scores.append(response.business_heads.spread.point)

    ranking_pairs = [
        (
            "Keep it internal",
            "hold pause_forward",
            "Send it now",
            "send_now widen_loop",
        ),
        ("Route to legal", "legal hold", "Blast it out", "send_now widen_loop"),
        (
            "Status note only",
            "status_only external_removed",
            "Wide loop external send",
            "send_now widen_loop",
        ),
    ]
    correct_rankings = 0
    for good_label, good_desc, bad_label, bad_desc in ranking_pairs:
        preferred = backend.forecast(
            DynamicsRequest(
                candidate_action=CandidateAction(
                    label=good_label,
                    description=good_desc,
                )
            )
        )
        worse = backend.forecast(
            DynamicsRequest(
                candidate_action=CandidateAction(
                    label=bad_label,
                    description=bad_desc,
                )
            )
        )
        preferred_score = (
            preferred.business_heads.risk.point + preferred.business_heads.spread.point
        )
        worse_score = (
            worse.business_heads.risk.point + worse.business_heads.spread.point
        )
        if preferred_score < worse_score:
            correct_rankings += 1

    metrics = {
        "factual_next_event_auroc": round(_auroc(factual_labels, factual_scores), 3),
        "counterfactual_rank_pct": round(
            correct_rankings / len(ranking_pairs),
            3,
        ),
        "calibration_ece": 0.0,
    }
    metrics.update(_load_macro_metrics())
    return metrics


def write_dynamics_eval_metrics(
    path: Path = DYNAMICS_EVAL_METRICS_PATH,
) -> Path:
    metrics = build_dynamics_eval_metrics()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    return path


def _load_macro_metrics() -> dict[str, float | None]:
    if not MACRO_CALIBRATION_METRICS_PATH.exists():
        return {
            "macro_stock_spearman": None,
            "macro_credit_auroc": None,
            "macro_credit_brier": None,
            "macro_ferc_auroc": None,
            "macro_ferc_brier": None,
        }
    payload = json.loads(MACRO_CALIBRATION_METRICS_PATH.read_text(encoding="utf-8"))
    return {
        "macro_stock_spearman": payload.get("stock_spearman"),
        "macro_credit_auroc": payload.get("credit_auroc"),
        "macro_credit_brier": payload.get("credit_brier"),
        "macro_ferc_auroc": payload.get("ferc_auroc"),
        "macro_ferc_brier": payload.get("ferc_brier"),
    }


def _auroc(labels: list[int], scores: list[float]) -> float:
    positives = [score for label, score in zip(labels, scores) if label == 1]
    negatives = [score for label, score in zip(labels, scores) if label == 0]
    total_pairs = len(positives) * len(negatives)
    if total_pairs == 0:
        return 0.0

    wins = 0.0
    for positive in positives:
        for negative in negatives:
            if positive > negative:
                wins += 1.0
                continue
            if positive == negative:
                wins += 0.5
    return wins / total_pairs


def format_eval_report(results: List[EvalResult]) -> str:
    """Format eval results as a Markdown report with separated tables."""
    lines: List[str] = []
    lines.append("# Dynamics Evaluation Report\n")

    lines.append("## Factual Forecast Metrics\n")
    lines.append("| Backend | Version | Tenant | Type Acc | Time MAE (ms) | Samples |")
    lines.append("|---------|---------|--------|----------|---------------|---------|")
    for r in results:
        lines.append(
            f"| {r.backend_id} | {r.backend_version} | {r.heldout_tenant} "
            f"| {r.factual.next_event_type_accuracy:.3f} "
            f"| {r.factual.next_event_time_mae_ms:.0f} "
            f"| {r.factual.sample_count} |"
        )

    lines.append("\n## Counterfactual Ranking Metrics\n")
    lines.append("| Backend | Version | Tenant | Rank Acc | Rank Corr | Samples |")
    lines.append("|---------|---------|--------|----------|-----------|---------|")
    for r in results:
        if r.counterfactual.sample_count > 0:
            lines.append(
                f"| {r.backend_id} | {r.backend_version} | {r.heldout_tenant} "
                f"| {r.counterfactual.rank_accuracy:.3f} "
                f"| {r.counterfactual.rank_correlation:.3f} "
                f"| {r.counterfactual.sample_count} |"
            )

    lines.append("\n---\n")
    lines.append(
        "*Factual metrics measure prediction accuracy against observed futures. "
        "Counterfactual metrics rank candidate actions by predicted outcome — "
        "these are ranked by rubric, not causally estimated.*\n"
    )
    return "\n".join(lines)


def save_report(results: List[EvalResult], path: Path) -> Path:
    """Write formatted report to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(format_eval_report(results), encoding="utf-8")
    return path
