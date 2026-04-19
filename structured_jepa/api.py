from __future__ import annotations

from pathlib import Path

from .proof import (
    ablation_timeseries,
    benchmark_timeseries,
    benchmark_vei_demo,
    publish_bundle,
    write_brief,
)
from .schema import (
    EvaluationResult,
    LinearProbeResult,
    ModelConfig,
    TrainConfig,
    TrainingArtifacts,
)
from .storage import PreparedDataset, load_processed_dataset
from .timeseries import prepare_timeseries_dataset
from .training import evaluate_model, fit_linear_probe, fit_summary_decoder, train_model
from .vei_context import prepare_vei_context_dataset
from .vei_runs import prepare_vei_runs_dataset

__all__ = [
    "EvaluationResult",
    "LinearProbeResult",
    "ModelConfig",
    "PreparedDataset",
    "TrainConfig",
    "TrainingArtifacts",
    "ablation_timeseries",
    "benchmark_timeseries",
    "benchmark_vei_demo",
    "evaluate_model",
    "fit_linear_probe",
    "fit_summary_decoder",
    "publish_bundle",
    "load_processed_dataset",
    "prepare_timeseries_dataset",
    "prepare_vei_context_dataset",
    "prepare_vei_runs_dataset",
    "train_model",
    "write_brief",
]


def resolve_dataset_root(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()
