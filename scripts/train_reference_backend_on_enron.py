from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from vei.whatif._enron_dataset import require_full_enron_rosetta_dir
from vei.whatif.api import (
    build_branch_point_benchmark,
    load_world,
    resolve_whatif_rosetta_dir,
)

DEFAULT_OUTPUT_ROOT = Path("data/enron/reference_backend")
DEFAULT_BENCHMARK_ROOT = Path("_vei_out/reference_backend_enron_benchmark")
SHIPPED_REFERENCE_FILES = {
    "model.pt",
    "metadata.json",
    "train_result.json",
    "eval_result.json",
    "metrics_card.md",
}


def _resolve_rosetta_dir(value: str | None) -> Path:
    if value:
        return require_full_enron_rosetta_dir(
            Path(value).expanduser().resolve(),
            purpose="reference backend training",
        )
    resolved = resolve_whatif_rosetta_dir(Path.cwd())
    if resolved is None:
        raise RuntimeError(
            "No Enron Rosetta dataset found. Run `make fetch-enron-full`, "
            "set VEI_WHATIF_ROSETTA_DIR, or pass --rosetta-dir."
        )
    return require_full_enron_rosetta_dir(
        resolved,
        purpose="reference backend training",
    )


def _run_bridge_command(
    *,
    command: str,
    request: dict[str, Any],
    request_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    request_path.write_text(json.dumps(request, indent=2), encoding="utf-8")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "vei.whatif.benchmark_bridge",
            command,
            "--request",
            str(request_path),
            "--output",
            str(output_path),
        ],
        check=True,
    )
    return json.loads(output_path.read_text(encoding="utf-8"))


def _write_metrics_card(
    *,
    output_root: Path,
    model_id: str,
    train_result: dict[str, Any],
    eval_result: dict[str, Any],
    rosetta_dir: Path,
) -> None:
    observed = eval_result.get("observed_metrics", {}) or {}
    judge_summary = eval_result.get("judge_summary", {}) or {}
    panel_summary = eval_result.get("panel_summary", {}) or {}
    dominance_summary = eval_result.get("dominance_summary", {}) or {}
    case_count = len(eval_result.get("cases") or [])
    lines = [
        "# Reference Backend Metrics",
        "",
        f"- Model: `{model_id}`",
        f"- Rosetta archive: `{rosetta_dir}`",
        f"- Train rows: `{train_result.get('train_row_count', 0)}`",
        f"- Validation rows: `{train_result.get('validation_row_count', 0)}`",
        f"- Epochs: `{train_result.get('epoch_count', 0)}`",
        f"- Train loss: `{train_result.get('train_loss', 'n/a')}`",
        f"- Validation loss: `{train_result.get('validation_loss', 'n/a')}`",
        f"- Held-out cases: `{case_count}`",
        f"- Factual next-event AUROC: `{observed.get('auroc_any_external_spread', 'n/a')}`",
        f"- Factual next-event Brier: `{observed.get('brier_any_external_spread', 'n/a')}`",
        f"- Calibration ECE: `{observed.get('calibration_error_any_external_spread', 'n/a')}`",
        f"- Held-out dominance pass rate: `{dominance_summary.get('pass_rate', 'n/a')}`",
        f"- Held-out judge top-1 agreement: `{judge_summary.get('top1_agreement', 'n/a')}`",
        f"- Held-out judge pairwise accuracy: `{judge_summary.get('pairwise_accuracy', 'n/a')}`",
        f"- Held-out panel top-1 agreement: `{panel_summary.get('top1_agreement', 'n/a')}`",
    ]
    (output_root / "metrics_card.md").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


def _prune_output_root(output_root: Path, eval_result: dict[str, Any]) -> dict[str, Any]:
    artifacts = dict(eval_result.get("artifacts") or {})
    prediction_path = str(artifacts.get("prediction_jsonl_path") or "").strip()
    if prediction_path:
        candidate = Path(prediction_path).expanduser().resolve()
        if candidate.exists():
            candidate.unlink()
        artifacts["prediction_jsonl_path"] = ""
        eval_result["artifacts"] = artifacts

    for filename in ("train_request.json", "eval_request.json"):
        candidate = output_root / filename
        if candidate.exists():
            candidate.unlink()
    for child in output_root.iterdir():
        if child.name in SHIPPED_REFERENCE_FILES:
            continue
        if child.is_file():
            child.unlink()
    return eval_result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build and train the repo-local Enron reference backend checkpoint."
    )
    parser.add_argument(
        "--rosetta-dir",
        default=None,
        help="Optional Enron Rosetta parquet directory override.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Directory where model.pt and metrics are written.",
    )
    parser.add_argument(
        "--benchmark-root",
        type=Path,
        default=DEFAULT_BENCHMARK_ROOT,
        help="Scratch root for the benchmark build artifacts.",
    )
    parser.add_argument(
        "--label",
        default="enron_reference_backend_v1",
        help="Benchmark build label.",
    )
    parser.add_argument(
        "--model-id",
        default="full_context_transformer",
        help="Benchmark bridge model id.",
    )
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--learning-rate", type=float, default=1e-3)
    parser.add_argument("--device", default="cpu")
    args = parser.parse_args()

    rosetta_dir = _resolve_rosetta_dir(args.rosetta_dir)
    output_root = args.output_root.expanduser().resolve()
    benchmark_root = args.benchmark_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    benchmark_root.mkdir(parents=True, exist_ok=True)

    world = load_world(
        source="enron",
        source_dir=rosetta_dir,
        include_content=False,
    )
    build = build_branch_point_benchmark(
        world,
        artifacts_root=benchmark_root,
        label=args.label,
    )

    train_result = _run_bridge_command(
        command="train",
        request={
            "build_root": str(build.artifacts.root),
            "output_root": str(output_root),
            "model_id": args.model_id,
            "epochs": args.epochs,
            "batch_size": args.batch_size,
            "learning_rate": args.learning_rate,
            "device": args.device,
        },
        request_path=output_root / "train_request.json",
        output_path=output_root / "train_result.json",
    )
    eval_result = _run_bridge_command(
        command="eval",
        request={
            "build_root": str(build.artifacts.root),
            "output_root": str(output_root),
            "model_id": args.model_id,
            "device": args.device,
        },
        request_path=output_root / "eval_request.json",
        output_path=output_root / "eval_result.json",
    )
    eval_result = _prune_output_root(output_root, eval_result)
    (output_root / "eval_result.json").write_text(
        json.dumps(eval_result, indent=2),
        encoding="utf-8",
    )
    _write_metrics_card(
        output_root=output_root,
        model_id=args.model_id,
        train_result=train_result,
        eval_result=eval_result,
        rosetta_dir=rosetta_dir,
    )
    print(json.dumps({"output_root": str(output_root), "build_root": str(build.artifacts.root)}, indent=2))


if __name__ == "__main__":
    main()
