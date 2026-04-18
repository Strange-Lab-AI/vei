from __future__ import annotations

from pathlib import Path

from ._benchmark_constants import (
    BUSINESS_OBJECTIVE_PACK_IDS as _BUSINESS_OBJECTIVE_PACK_IDS,
)
from .benchmark_business import get_business_judge_rubric
from .macro_outcomes import (
    MACRO_CALIBRATION_METRICS,
    MACRO_CALIBRATION_REPORT_PATH,
    preview_macro_outcomes_for_prompt,
)
from .models import WhatIfBenchmarkCase, WhatIfBusinessObjectivePackId


def build_dossier_files(
    case: WhatIfBenchmarkCase,
    *,
    dossier_root: Path,
) -> dict[str, str]:
    paths: dict[str, str] = {}
    for objective_pack_id in _BUSINESS_OBJECTIVE_PACK_IDS:
        rubric = get_business_judge_rubric(objective_pack_id)
        dossier_path = dossier_root / f"{objective_pack_id}.md"
        dossier_path.write_text(
            render_case_dossier(case, objective_pack_id=objective_pack_id),
            encoding="utf-8",
        )
        paths[objective_pack_id] = str(dossier_path)
        rubric_path = dossier_root / f"{objective_pack_id}.rubric.json"
        rubric_path.write_text(rubric.model_dump_json(indent=2), encoding="utf-8")
    return paths


def render_case_dossier(
    case: WhatIfBenchmarkCase,
    *,
    objective_pack_id: WhatIfBusinessObjectivePackId,
) -> str:
    rubric = get_business_judge_rubric(objective_pack_id)
    lines = [
        f"# {case.title}",
        "",
        case.summary or "Held-out Enron branch-point case.",
        "",
        "## Objective",
        f"- {rubric.title}",
        f"- Question: {rubric.question}",
        f"- Decision rule: {rubric.decision_rule}",
        "",
        "## Criteria",
    ]
    for criterion in rubric.criteria:
        lines.append(f"- {criterion}")
    lines.extend(
        [
            (
                "- Macro outcomes are advisory only. Keep the email-path evidence "
                "primary when the calibration numbers are weak."
            ),
            "",
            "## Macro Calibration",
            f"- Report: `{MACRO_CALIBRATION_REPORT_PATH}`",
            (
                "- Stock return (5d) Spearman: "
                f"{MACRO_CALIBRATION_METRICS['stock_spearman']}"
            ),
            (
                "- Credit action (30d) AUROC/Brier: "
                f"{MACRO_CALIBRATION_METRICS['credit_auroc']} / "
                f"{MACRO_CALIBRATION_METRICS['credit_brier']}"
            ),
            (
                "- FERC action (180d) AUROC/Brier: "
                f"{MACRO_CALIBRATION_METRICS['ferc_auroc']} / "
                f"{MACRO_CALIBRATION_METRICS['ferc_brier']}"
            ),
        ]
    )
    lines.extend(
        [
            "",
            "## Branch Event",
            f"- Event id: `{case.event_id}`",
            f"- Thread id: `{case.thread_id}`",
            f"- Sender: `{case.branch_event.actor_id}`",
            (
                "- Recipients: "
                f"{', '.join(case.branch_event.to_recipients) or case.branch_event.target_id or '(none)'}"
            ),
            f"- Subject: {case.branch_event.subject}",
        ]
    )
    if case.branch_event.snippet:
        lines.append(f"- Excerpt: {case.branch_event.snippet}")
    lines.extend(["", "## Pre-Branch History"])
    for event in case.history_preview:
        lines.append(
            f"- `{event.event_id}` {event.timestamp} {event.event_type} "
            f"from `{event.actor_id}`: {event.subject}"
        )
    lines.extend(["", "## Public Company Context"])
    if case.public_context and case.public_context.financial_snapshots:
        lines.append("### Financial Checkpoints")
        for snapshot in case.public_context.financial_snapshots:
            lines.append(
                f"- {snapshot.as_of[:10]} {snapshot.label}: {snapshot.summary}"
            )
    if case.public_context and case.public_context.public_news_events:
        lines.append("### Public News")
        for event in case.public_context.public_news_events:
            lines.append(f"- {event.timestamp[:10]} {event.headline}: {event.summary}")
    if case.public_context and case.public_context.stock_history:
        lines.append("### Market Checkpoints")
        for row in case.public_context.stock_history:
            summary = row.summary or row.label
            lines.append(f"- {row.as_of[:10]} close {row.close:.2f}: {summary}")
    if case.public_context and case.public_context.credit_history:
        lines.append("### Credit Checkpoints")
        for event in case.public_context.credit_history:
            headline = event.headline or f"{event.agency} rating action"
            lines.append(f"- {event.as_of[:10]} {headline}: {event.summary}")
    if case.public_context and case.public_context.ferc_history:
        lines.append("### Regulatory Checkpoints")
        for event in case.public_context.ferc_history:
            lines.append(f"- {event.timestamp[:10]} {event.headline}: {event.summary}")
    if not case.public_context or (
        not case.public_context.financial_snapshots
        and not case.public_context.public_news_events
        and not case.public_context.stock_history
        and not case.public_context.credit_history
        and not case.public_context.ferc_history
    ):
        lines.append("- No public company context attached.")
    lines.extend(["", "## Candidate Decisions"])
    for candidate in case.candidates:
        baseline_macro, predicted_macro, macro_delta = (
            preview_macro_outcomes_for_prompt(
                candidate.prompt,
                organization_domain=(
                    case.public_context.organization_domain
                    if case.public_context is not None
                    else "enron.com"
                ),
                branch_timestamp=case.branch_event.timestamp,
                public_context=case.public_context,
            )
        )
        lines.extend(
            [
                f"### {candidate.label}",
                f"- Candidate id: `{candidate.candidate_id}`",
                f"- Prompt: {candidate.prompt}",
                (
                    "- Action tags: "
                    f"{', '.join(candidate.action_schema.action_tags) or '(none)'}"
                ),
                f"- Review path: {candidate.action_schema.review_path}",
                (
                    "- Coordination breadth: "
                    f"{candidate.action_schema.coordination_breadth}"
                ),
                (
                    "- Outside sharing posture: "
                    f"{candidate.action_schema.outside_sharing_posture}"
                ),
                (
                    "- Macro stock return (5d): "
                    f"{baseline_macro.stock_return_5d} -> "
                    f"{predicted_macro.stock_return_5d} "
                    f"(delta {macro_delta['stock_return_5d_delta']})"
                ),
                (
                    "- Macro credit action (30d): "
                    f"{baseline_macro.credit_action_30d} -> "
                    f"{predicted_macro.credit_action_30d} "
                    f"(delta {macro_delta['credit_action_30d_delta']})"
                ),
                (
                    "- Macro FERC action (180d): "
                    f"{baseline_macro.ferc_action_180d} -> "
                    f"{predicted_macro.ferc_action_180d} "
                    f"(delta {macro_delta['ferc_action_180d_delta']})"
                ),
            ]
        )
        for pack_id, label in candidate.expected_hypotheses.items():
            lines.append(f"- {pack_id}: {label}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
