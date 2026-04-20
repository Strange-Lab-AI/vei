from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any

from vei.whatif.benchmark import (
    _build_audit_queue,
    _judge_case_objective,
    evaluate_branch_point_benchmark_model,
    load_branch_point_benchmark_build_result,
    load_branch_point_benchmark_judge_result,
)

try:
    from scripts.enron_example_specs import spec_by_case_id
except ModuleNotFoundError:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from enron_example_specs import spec_by_case_id

DEFAULT_BUILD_ROOT = Path(
    "_vei_out/reference_backend_enron_benchmark/enron_reference_backend_v1"
)
DEFAULT_CHECKPOINT_ROOT = Path("data/enron/reference_backend")
DEFAULT_OUTPUT_ROOT = Path("_vei_out/enron_flagship_proof_pass")
DEFAULT_JUDGE_MODEL = "gpt-4.1-mini"
DEFAULT_JUDGE_ID = "flagship_llm_judge"
DEFAULT_MODEL_ID = "full_context_transformer"
DEFAULT_PACK_ID = "enron_proof_flagship_v2"
PUBLIC_OBJECTIVE_PACK_ID = "minimize_enterprise_risk"
CASE_PACKS: dict[str, tuple[str, ...]] = {
    "enron_proof_flagship_v2": (
        "master_agreement",
        "pg_e_power_deal",
        "california_crisis_order",
        "baxter_press_release",
        "braveheart_forward",
        "q3_disclosure_review",
        "btu_weekly",
        "credit_derivatives_confidentiality",
    ),
    "enron_hard_subset_v2": (
        "pg_e_power_deal",
        "baxter_press_release",
        "braveheart_forward",
        "q3_disclosure_review",
    ),
}
OBJECTIVE_PACK_IDS = (
    "minimize_enterprise_risk",
    "protect_commercial_position",
    "reduce_org_strain",
    "preserve_stakeholder_trust",
    "maintain_execution_velocity",
)


def _selected_cases(build_root: Path, *, pack_id: str) -> list[object]:
    build = load_branch_point_benchmark_build_result(build_root)
    cases_by_id = {case.case_id: case for case in build.cases}
    selected: list[object] = []
    missing: list[str] = []
    for case_id in CASE_PACKS[pack_id]:
        case = cases_by_id.get(case_id)
        if case is None:
            missing.append(case_id)
            continue
        selected.append(case)
    if missing:
        raise ValueError(f"missing benchmark cases: {', '.join(sorted(missing))}")
    return selected


def _judge_pack_cases(
    *,
    build_root: Path,
    output_root: Path,
    pack_id: str,
    judge_model: str,
    judge_id: str,
) -> Path:
    judgments = []
    for case in _selected_cases(build_root, pack_id=pack_id):
        for objective_pack_id in OBJECTIVE_PACK_IDS:
            judgments.append(
                _judge_case_objective(
                    build_root=build_root,
                    case=case,
                    objective_pack_id=objective_pack_id,
                    model=judge_model,
                    judge_id=judge_id,
                )
            )

    audit_queue = _build_audit_queue(judgments)
    judge_result_path = output_root / "judge_result.json"
    audit_queue_path = output_root / "audit_queue.json"
    judge_result_path.write_text(
        json.dumps(
            {
                "version": "1",
                "build_root": str(build_root),
                "judge_model": judge_model,
                "judgments": [item.model_dump(mode="json") for item in judgments],
                "audit_queue": [item.model_dump(mode="json") for item in audit_queue],
                "notes": [
                    f"pack_id={pack_id}",
                    f"selected_cases={len(CASE_PACKS[pack_id])}",
                    f"judgments={len(judgments)}",
                ],
                "artifacts": {
                    "root": str(output_root),
                    "result_path": str(judge_result_path),
                    "audit_queue_path": str(audit_queue_path),
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    audit_queue_path.write_text(
        json.dumps([item.model_dump(mode="json") for item in audit_queue], indent=2)
        + "\n",
        encoding="utf-8",
    )
    return judge_result_path


def _prepare_runtime_root(*, checkpoint_root: Path, output_root: Path) -> Path:
    runtime_root = output_root / "runtime_checkpoint"
    runtime_root.mkdir(parents=True, exist_ok=True)
    for filename in ("model.pt", "metadata.json"):
        shutil.copy2(checkpoint_root / filename, runtime_root / filename)
    return runtime_root


def _objective_by_key(eval_result) -> dict[tuple[str, str], object]:
    result: dict[tuple[str, str], object] = {}
    for case_eval in eval_result.cases:
        for objective in case_eval.objectives:
            result[(case_eval.case.case_id, objective.objective_pack.pack_id)] = (
                objective
            )
    return result


def _case_by_id(eval_result) -> dict[str, object]:
    return {case_eval.case.case_id: case_eval.case for case_eval in eval_result.cases}


def _candidate_label_by_id(case) -> dict[str, str]:
    return {candidate.candidate_id: candidate.label for candidate in case.candidates}


def _model_candidate_order(objective) -> list[str]:
    ranked = sorted(
        objective.candidates,
        key=lambda item: (int(item.rank), item.candidate.label.lower()),
    )
    return [item.candidate.candidate_id for item in ranked]


def _pairwise_accuracy(model_order: list[str], judge_order: list[str]) -> float | None:
    if len(model_order) < 2 or len(judge_order) < 2:
        return None
    judge_position = {
        candidate_id: index for index, candidate_id in enumerate(judge_order)
    }
    compared = 0
    hits = 0
    for left_index, left_candidate in enumerate(model_order):
        if left_candidate not in judge_position:
            continue
        for right_candidate in model_order[left_index + 1 :]:
            if right_candidate not in judge_position:
                continue
            compared += 1
            left_before = (
                judge_position[left_candidate] < judge_position[right_candidate]
            )
            if left_before:
                hits += 1
    if compared == 0:
        return None
    return round(hits / compared, 3)


def _summary_from_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "judgment_count": 0,
            "top_choice_agreement": None,
            "pairwise_accuracy": None,
        }
    top_choice_hits = sum(1 for row in rows if row["matched"])
    pairwise_values = [
        row["pairwise_accuracy"] for row in rows if row["pairwise_accuracy"] is not None
    ]
    return {
        "judgment_count": len(rows),
        "top_choice_agreement": round(top_choice_hits / len(rows), 3),
        "pairwise_accuracy": (
            round(sum(pairwise_values) / len(pairwise_values), 3)
            if pairwise_values
            else None
        ),
    }


def _actual_happened(case_id: str, case) -> str:
    try:
        return spec_by_case_id(case_id).actual_happened
    except KeyError:
        return case.summary or case.title


def _default_objective_rows(
    eval_result,
    *,
    judge_result,
    pack_case_ids: tuple[str, ...],
) -> list[dict[str, Any]]:
    objective_by_key = _objective_by_key(eval_result)
    case_by_id = _case_by_id(eval_result)
    rows: list[dict[str, Any]] = []
    for judgment in judge_result.judgments:
        if judgment.objective_pack_id != PUBLIC_OBJECTIVE_PACK_ID:
            continue
        if judgment.case_id not in pack_case_ids:
            continue
        objective = objective_by_key.get((judgment.case_id, judgment.objective_pack_id))
        case = case_by_id.get(judgment.case_id)
        if objective is None or case is None:
            continue
        labels_by_id = _candidate_label_by_id(case)
        model_order = _model_candidate_order(objective)
        model_top_id = model_order[0] if model_order else ""
        judge_top_id = (
            judgment.ordered_candidate_ids[0] if judgment.ordered_candidate_ids else ""
        )
        rows.append(
            {
                "case_id": judgment.case_id,
                "case_title": case.title,
                "actual_happened": _actual_happened(judgment.case_id, case),
                "model_top_pick": labels_by_id.get(
                    model_top_id, objective.recommended_candidate_label
                ),
                "judge_top_pick": labels_by_id.get(judge_top_id, judge_top_id),
                "matched": bool(
                    model_top_id and judge_top_id and model_top_id == judge_top_id
                ),
                "pairwise_accuracy": _pairwise_accuracy(
                    model_order, judgment.ordered_candidate_ids
                ),
                "note": judgment.notes or case.summary,
            }
        )
    rows.sort(key=lambda item: item["case_title"].lower())
    return rows


def _render_overview(
    *,
    pack_id: str,
    eval_result,
    proof_summary: dict[str, Any],
    hard_summary: dict[str, Any] | None,
    case_rows: list[dict[str, Any]],
) -> str:
    observed = eval_result.observed_metrics
    lines = [
        f"# Enron Proof Pass · {pack_id}",
        "",
        "## Headline",
        f"- Top-choice agreement: {proof_summary['top_choice_agreement']}",
        f"- Pairwise accuracy: {proof_summary['pairwise_accuracy']}",
    ]
    if hard_summary is not None:
        lines.append(
            f"- Hard-subset pairwise accuracy: {hard_summary['pairwise_accuracy']}"
        )
    if observed.auroc_any_external_spread is not None:
        lines.extend(
            [
                "",
                "## Factual Forecasting",
                f"- AUROC any external spread: {observed.auroc_any_external_spread}",
                f"- Brier any external spread: {observed.brier_any_external_spread}",
                f"- Calibration error: {observed.calibration_error_any_external_spread}",
            ]
        )
    else:
        lines.extend(
            [
                "",
                "## Factual Forecasting",
                "- Use the shipped reference metrics card for the factual headline. This proof pass is focused on judged ranking agreement over the repo sample-backed held-out pack.",
            ]
        )
    lines.extend(
        [
            "",
            "## Per-Case Table",
            "",
            "| Case | What actually happened | Model top pick | Judge top pick | Match | Note |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in case_rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    row["case_title"],
                    row["actual_happened"].replace("|", "/"),
                    row["model_top_pick"].replace("|", "/"),
                    row["judge_top_pick"].replace("|", "/"),
                    "yes" if row["matched"] else "no",
                    str(row["note"]).replace("|", "/"),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Appendix",
            f"- Judged rankings: {eval_result.judge_summary.judgment_count if eval_result.judge_summary.available else 0}",
            f"- Kendall tau: {eval_result.judge_summary.kendall_tau}",
            (
                "- Dominance checks: "
                f"{eval_result.dominance_summary.passed_checks}/"
                f"{eval_result.dominance_summary.total_checks} "
                f"({eval_result.dominance_summary.pass_rate:.3f})"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def _write_summary(
    *,
    output_root: Path,
    publish_root: Path | None,
    pack_id: str,
    eval_result,
    judge_result,
) -> None:
    case_rows = _default_objective_rows(
        eval_result,
        judge_result=judge_result,
        pack_case_ids=CASE_PACKS[pack_id],
    )
    proof_summary = _summary_from_rows(case_rows)
    hard_summary = None
    if pack_id == "enron_proof_flagship_v2":
        hard_case_rows = [
            row
            for row in case_rows
            if row["case_id"] in CASE_PACKS["enron_hard_subset_v2"]
        ]
        hard_summary = _summary_from_rows(hard_case_rows)

    summary = {
        "pack_id": pack_id,
        "selected_case_ids": list(CASE_PACKS[pack_id]),
        "public_objective_pack_id": PUBLIC_OBJECTIVE_PACK_ID,
        "observed_metrics": eval_result.observed_metrics.model_dump(mode="json"),
        "dominance_summary": eval_result.dominance_summary.model_dump(mode="json"),
        "judge_summary": eval_result.judge_summary.model_dump(mode="json"),
        "audit_summary": eval_result.audit_summary.model_dump(mode="json"),
        "proof_summary": proof_summary,
        "hard_subset_summary": hard_summary,
        "per_case_rows": case_rows,
    }
    (output_root / "proof_pass_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )
    (output_root / "proof_pass_overview.md").write_text(
        _render_overview(
            pack_id=pack_id,
            eval_result=eval_result,
            proof_summary=proof_summary,
            hard_summary=hard_summary,
            case_rows=case_rows,
        ),
        encoding="utf-8",
    )
    if publish_root is None:
        return

    publish_root.mkdir(parents=True, exist_ok=True)
    compact_metrics = {
        "pack_id": pack_id,
        "selected_case_ids": list(CASE_PACKS[pack_id]),
        "public_objective_pack_id": PUBLIC_OBJECTIVE_PACK_ID,
        "proof_summary": proof_summary,
        "hard_subset_summary": hard_summary,
        "observed_metrics": {
            "auroc_any_external_spread": eval_result.observed_metrics.auroc_any_external_spread,
            "brier_any_external_spread": eval_result.observed_metrics.brier_any_external_spread,
            "calibration_error_any_external_spread": eval_result.observed_metrics.calibration_error_any_external_spread,
        },
        "judge_summary": eval_result.judge_summary.model_dump(mode="json"),
        "dominance_summary": eval_result.dominance_summary.model_dump(mode="json"),
        "per_case_rows": case_rows,
    }
    (publish_root / "metrics.json").write_text(
        json.dumps(compact_metrics, indent=2) + "\n",
        encoding="utf-8",
    )
    (publish_root / "overview.md").write_text(
        _render_overview(
            pack_id=pack_id,
            eval_result=eval_result,
            proof_summary=proof_summary,
            hard_summary=hard_summary,
            case_rows=case_rows,
        ),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a judged Enron proof pass over a named benchmark pack."
    )
    parser.add_argument(
        "--pack",
        choices=sorted(CASE_PACKS),
        default=DEFAULT_PACK_ID,
        help="Named held-out case pack to score.",
    )
    parser.add_argument(
        "--build-root",
        type=Path,
        default=DEFAULT_BUILD_ROOT,
        help="Existing branch-point benchmark build root.",
    )
    parser.add_argument(
        "--checkpoint-root",
        type=Path,
        default=DEFAULT_CHECKPOINT_ROOT,
        help="Reference backend checkpoint directory.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Directory for judged rankings and summary artifacts.",
    )
    parser.add_argument(
        "--publish-root",
        type=Path,
        help="Optional checked-in summary directory.",
    )
    parser.add_argument(
        "--judge-model",
        default=DEFAULT_JUDGE_MODEL,
        help="OpenAI model used for the locked judge pass.",
    )
    parser.add_argument(
        "--judge-id",
        default=DEFAULT_JUDGE_ID,
        help="Judge id written into the ranking artifacts.",
    )
    parser.add_argument(
        "--model-id",
        default=DEFAULT_MODEL_ID,
        help="Benchmark model id for the shipped reference checkpoint.",
    )
    args = parser.parse_args()

    build_root = args.build_root.expanduser().resolve()
    checkpoint_root = args.checkpoint_root.expanduser().resolve()
    output_root = args.output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    judge_result_path = _judge_pack_cases(
        build_root=build_root,
        output_root=output_root,
        pack_id=args.pack,
        judge_model=args.judge_model,
        judge_id=args.judge_id,
    )
    runtime_root = _prepare_runtime_root(
        checkpoint_root=checkpoint_root,
        output_root=output_root,
    )
    eval_result = evaluate_branch_point_benchmark_model(
        build_root,
        model_id=args.model_id,
        judged_rankings_path=judge_result_path,
        output_root=runtime_root,
    )
    judge_result = load_branch_point_benchmark_judge_result(judge_result_path)
    _write_summary(
        output_root=output_root,
        publish_root=(
            args.publish_root.expanduser().resolve()
            if args.publish_root is not None
            else None
        ),
        pack_id=args.pack,
        eval_result=eval_result,
        judge_result=judge_result,
    )

    summary_payload = json.loads(
        (output_root / "proof_pass_summary.json").read_text(encoding="utf-8")
    )
    print(
        json.dumps(
            {
                "pack_id": args.pack,
                "judge_result_path": str(judge_result_path),
                "eval_result_path": str(eval_result.artifacts.eval_result_path),
                "overview_path": str(output_root / "proof_pass_overview.md"),
                "top_choice_agreement": summary_payload["proof_summary"][
                    "top_choice_agreement"
                ],
                "pairwise_accuracy": summary_payload["proof_summary"][
                    "pairwise_accuracy"
                ],
                "hard_subset_pairwise_accuracy": (
                    (summary_payload.get("hard_subset_summary") or {}).get(
                        "pairwise_accuracy"
                    )
                ),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
