from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from .ejepa import resolve_ejepa_runtime
from .models import (
    WhatIfBenchmarkDatasetRow,
    WhatIfBenchmarkEvalResult,
    WhatIfBenchmarkModelId,
    WhatIfBenchmarkTrainResult,
)


def run_branch_point_benchmark_training(
    *,
    build_root: str | Path,
    model_id: WhatIfBenchmarkModelId,
    epochs: int = 12,
    batch_size: int = 64,
    learning_rate: float = 1e-3,
    seed: int = 42042,
    device: str | None = None,
    runtime_root: str | Path | None = None,
    output_root: str | Path | None = None,
    train_splits: list[str] | None = None,
    validation_splits: list[str] | None = None,
) -> WhatIfBenchmarkTrainResult:
    model_root = (
        Path(output_root).expanduser().resolve()
        if output_root is not None
        else Path(build_root).expanduser().resolve() / "model_runs" / str(model_id)
    )
    model_root.mkdir(parents=True, exist_ok=True)
    request = {
        "build_root": str(Path(build_root).expanduser().resolve()),
        "model_id": str(model_id),
        "output_root": str(model_root),
        "epochs": int(epochs),
        "batch_size": int(batch_size),
        "learning_rate": float(learning_rate),
        "seed": int(seed),
        "device": device or "",
        "train_splits": list(train_splits or ["train"]),
        "validation_splits": list(validation_splits or ["validation"]),
    }
    response_path = _run_bridge_command(
        command_name="train",
        request=request,
        output_root=model_root,
        runtime_root=runtime_root,
    )
    return WhatIfBenchmarkTrainResult.model_validate_json(
        response_path.read_text(encoding="utf-8")
    )


def run_branch_point_benchmark_evaluation(
    *,
    build_root: str | Path,
    model_id: WhatIfBenchmarkModelId,
    device: str | None = None,
    runtime_root: str | Path | None = None,
    output_root: str | Path | None = None,
) -> WhatIfBenchmarkEvalResult:
    model_root = (
        Path(output_root).expanduser().resolve()
        if output_root is not None
        else Path(build_root).expanduser().resolve() / "model_runs" / str(model_id)
    )
    model_root.mkdir(parents=True, exist_ok=True)
    request = {
        "build_root": str(Path(build_root).expanduser().resolve()),
        "model_id": str(model_id),
        "output_root": str(model_root),
        "device": device or "",
    }
    response_path = _run_bridge_command(
        command_name="eval",
        request=request,
        output_root=model_root,
        runtime_root=runtime_root,
    )
    return WhatIfBenchmarkEvalResult.model_validate_json(
        response_path.read_text(encoding="utf-8")
    )


def run_branch_point_benchmark_prediction(
    *,
    checkpoint_path: str | Path,
    row: WhatIfBenchmarkDatasetRow,
    device: str | None = None,
    runtime_root: str | Path | None = None,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    checkpoint = Path(checkpoint_path).expanduser().resolve()
    request_output_root = (
        Path(output_root).expanduser().resolve()
        if output_root is not None
        else _default_prediction_runtime_root(checkpoint)
    )
    request = {
        "checkpoint_path": str(checkpoint),
        "row": row.model_dump(mode="json"),
        "device": device or "",
    }
    response_path = _run_bridge_command(
        command_name="predict",
        request=request,
        output_root=request_output_root,
        runtime_root=runtime_root,
    )
    return json.loads(response_path.read_text(encoding="utf-8"))


def run_branch_point_benchmark_predictions(
    *,
    checkpoint_path: str | Path,
    rows: list[WhatIfBenchmarkDatasetRow],
    device: str | None = None,
    runtime_root: str | Path | None = None,
    output_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    checkpoint = Path(checkpoint_path).expanduser().resolve()
    request_output_root = (
        Path(output_root).expanduser().resolve()
        if output_root is not None
        else _default_prediction_runtime_root(checkpoint)
    )
    request = {
        "checkpoint_path": str(checkpoint),
        "rows": [row.model_dump(mode="json") for row in rows],
        "device": device or "",
    }
    response_path = _run_bridge_command(
        command_name="predict-batch",
        request=request,
        output_root=request_output_root,
        runtime_root=runtime_root,
    )
    payload = json.loads(response_path.read_text(encoding="utf-8"))
    raw_predictions = payload.get("predictions")
    if not isinstance(raw_predictions, list):
        raise RuntimeError("benchmark bridge predict-batch returned no predictions")
    if len(raw_predictions) != len(rows):
        raise RuntimeError(
            "benchmark bridge predict-batch returned mismatched prediction count "
            f"(expected {len(rows)}, got {len(raw_predictions)})"
        )
    predictions: list[dict[str, Any]] = []
    for index, (row, item) in enumerate(
        zip(rows, raw_predictions, strict=True), start=1
    ):
        if not isinstance(item, dict):
            raise RuntimeError(
                "benchmark bridge predict-batch returned invalid prediction "
                f"at index {index}"
            )
        prediction = dict(item)
        if prediction.get("row_id") != row.row_id:
            raise RuntimeError(
                "benchmark bridge predict-batch returned out-of-order prediction "
                f"at index {index} (expected row_id={row.row_id!r}, "
                f"got {prediction.get('row_id')!r})"
            )
        predictions.append(prediction)
    return predictions


def _default_prediction_runtime_root(checkpoint: Path) -> Path:
    return (
        Path.cwd().resolve()
        / "_vei_out"
        / "reference_backend_runtime"
        / checkpoint.parent.name
    )


def _run_bridge_command(
    *,
    command_name: str,
    request: dict[str, object],
    output_root: Path,
    runtime_root: str | Path | None,
) -> Path:
    runtime = resolve_ejepa_runtime(runtime_root)
    if runtime is None:
        raise RuntimeError(
            "No torch runtime was found. Install `.[jepa]`, set VEI_EJEPA_ROOT, or place ARP_Jepa_exp next to digital-enterprise-twin."
        )
    runtime_dir, python_path = runtime
    request_root = output_root / ".benchmark_runtime"
    request_root.mkdir(parents=True, exist_ok=True)
    request_path = request_root / f"{command_name}_request.json"
    response_path = request_root / f"{command_name}_response.json"
    request_path.write_text(json.dumps(request, indent=2), encoding="utf-8")

    env = os.environ.copy()
    pythonpath_entries = [str(Path(__file__).resolve().parents[2])]
    runtime_src = runtime_dir / "src"
    if runtime_src.exists():
        pythonpath_entries.append(str(runtime_src))
    pythonpath_entries.extend(_current_site_package_entries())
    existing_pythonpath = env.get("PYTHONPATH", "")
    if existing_pythonpath:
        pythonpath_entries.append(existing_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)

    command = [
        str(python_path),
        "-m",
        "vei.whatif.benchmark_bridge",
        command_name,
        "--request",
        str(request_path),
        "--output",
        str(response_path),
    ]
    completed = subprocess.run(
        command,
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        error_text = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(
            error_text or f"benchmark bridge {command_name} subprocess failed"
        )
    if not response_path.exists():
        raise RuntimeError(
            f"benchmark bridge {command_name} did not write {response_path}"
        )
    return response_path


def _current_site_package_entries() -> list[str]:
    entries: list[str] = []
    for value in sys.path:
        if "site-packages" not in value:
            continue
        resolved = Path(value).expanduser().resolve()
        if not resolved.exists():
            continue
        entries.append(str(resolved))
    deduped: list[str] = []
    for entry in entries:
        if entry in deduped:
            continue
        deduped.append(entry)
    return deduped


__all__ = [
    "run_branch_point_benchmark_evaluation",
    "run_branch_point_benchmark_prediction",
    "run_branch_point_benchmark_predictions",
    "run_branch_point_benchmark_training",
]
