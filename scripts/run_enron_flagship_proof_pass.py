from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from vei.whatif.benchmark import (
    _build_audit_queue,
    _judge_case_objective,
    evaluate_branch_point_benchmark_model,
    load_branch_point_benchmark_build_result,
)
from vei.whatif.render import render_benchmark_eval

DEFAULT_BUILD_ROOT = Path(
    "_vei_out/reference_backend_enron_benchmark/enron_reference_backend_v1"
)
DEFAULT_CHECKPOINT_ROOT = Path("data/enron/reference_backend")
DEFAULT_OUTPUT_ROOT = Path("_vei_out/enron_flagship_proof_pass")
DEFAULT_JUDGE_MODEL = "gpt-4.1-mini"
DEFAULT_JUDGE_ID = "flagship_llm_judge"
DEFAULT_MODEL_ID = "full_context_transformer"
FLAGSHIP_CASE_IDS = (
    "master_agreement",
    "watkins_followup_questions",
    "california_crisis_order",
    "pg_e_power_deal",
)
OBJECTIVE_PACK_IDS = (
    "minimize_enterprise_risk",
    "protect_commercial_position",
    "reduce_org_strain",
    "preserve_stakeholder_trust",
    "maintain_execution_velocity",
)


def _selected_cases(build_root: Path) -> list[object]:
    build = load_branch_point_benchmark_build_result(build_root)
    cases_by_id = {case.case_id: case for case in build.cases}

    selected: list[object] = []
    missing: list[str] = []
    for case_id in FLAGSHIP_CASE_IDS:
        case = cases_by_id.get(case_id)
        if case is None:
            missing.append(case_id)
            continue
        selected.append(case)

    if missing:
        raise ValueError(f"missing flagship cases: {', '.join(sorted(missing))}")
    return selected


def _judge_flagship_cases(
    *,
    build_root: Path,
    output_root: Path,
    judge_model: str,
    judge_id: str,
) -> Path:
    judgments = []
    for case in _selected_cases(build_root):
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
                    f"flagship_cases={len(FLAGSHIP_CASE_IDS)}",
                    f"judgments={len(judgments)}",
                    f"audit_queue={len(audit_queue)}",
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
        source = checkpoint_root / filename
        target = runtime_root / filename
        shutil.copy2(source, target)
    return runtime_root


def _write_summary(
    *,
    output_root: Path,
    eval_result,
) -> None:
    summary = {
        "selected_case_ids": list(FLAGSHIP_CASE_IDS),
        "observed_metrics": eval_result.observed_metrics.model_dump(mode="json"),
        "dominance_summary": eval_result.dominance_summary.model_dump(mode="json"),
        "judge_summary": eval_result.judge_summary.model_dump(mode="json"),
        "audit_summary": eval_result.audit_summary.model_dump(mode="json"),
    }
    (output_root / "proof_pass_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )
    (output_root / "proof_pass_overview.md").write_text(
        render_benchmark_eval(eval_result) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a judged Enron proof pass over the four flagship cases."
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

    judge_result_path = _judge_flagship_cases(
        build_root=build_root,
        output_root=output_root,
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
    _write_summary(output_root=output_root, eval_result=eval_result)

    print(
        json.dumps(
            {
                "judge_result_path": str(judge_result_path),
                "eval_result_path": str(eval_result.artifacts.eval_result_path),
                "overview_path": str(output_root / "proof_pass_overview.md"),
                "judge_top1_agreement": eval_result.judge_summary.top1_agreement,
                "judge_pairwise_accuracy": eval_result.judge_summary.pairwise_accuracy,
                "judge_kendall_tau": eval_result.judge_summary.kendall_tau,
                "dominance_pass_rate": eval_result.dominance_summary.pass_rate,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
