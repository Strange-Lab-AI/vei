"""Eval reporting — writes clearly separated factual and counterfactual tables."""

from __future__ import annotations

from pathlib import Path
from typing import List

from .harness import EvalResult


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
