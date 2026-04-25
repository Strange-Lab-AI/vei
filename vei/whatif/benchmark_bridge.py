from __future__ import annotations

import argparse
import importlib
import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np

from vei.whatif.benchmark import load_branch_point_benchmark_build_result
from vei.whatif.benchmark_business import (
    evidence_to_business_outcomes,
    list_business_objective_packs,
    score_business_objective,
)
from vei.whatif.models import (
    WhatIfActionSchema,
    WhatIfBenchmarkCase,
    WhatIfBenchmarkCaseEvaluation,
    WhatIfBenchmarkDatasetRow,
    WhatIfBenchmarkEvalArtifacts,
    WhatIfBenchmarkEvalResult,
    WhatIfBenchmarkModelId,
    WhatIfBenchmarkTrainArtifacts,
    WhatIfBenchmarkTrainResult,
    WhatIfCounterfactualCandidatePrediction,
    WhatIfCounterfactualObjectiveEvaluation,
    WhatIfObservedForecastMetrics,
    WhatIfObservedEvidenceHeads,
    WhatIfFutureStateHeads,
)

_RANDOM_SEED = 42042
_HOLDOUT_BATCH_SIZE = 256
_RECIPIENT_SCOPE_VALUES = ("internal", "external", "mixed", "unknown")
_ATTACHMENT_POLICY_VALUES = ("none", "present", "sanitized")
_ESCALATION_LEVEL_VALUES = ("none", "manager", "executive")
_OWNER_CLARITY_VALUES = ("unclear", "single_owner", "multi_owner")
_REASSURANCE_STYLE_VALUES = ("low", "medium", "high")
_REVIEW_PATH_VALUES = (
    "none",
    "internal_legal",
    "outside_counsel",
    "business_owner",
    "cross_functional",
    "hr",
    "executive",
)
_COORDINATION_BREADTH_VALUES = ("single_owner", "narrow", "targeted", "broad")
_OUTSIDE_SHARING_POSTURE_VALUES = (
    "internal_only",
    "status_only",
    "limited_external",
    "broad_external",
)
_DECISION_POSTURE_VALUES = ("hold", "review", "resolve", "escalate")
_EVIDENCE_TARGET_NAMES = (
    "outside_recipient_count",
    "outside_forward_count",
    "outside_attachment_spread_count",
    "legal_follow_up_count",
    "review_loop_count",
    "markup_loop_count",
    "executive_escalation_count",
    "executive_mention_count",
    "urgency_spike_count",
    "participant_fanout",
    "cc_expansion_count",
    "cross_functional_loop_count",
    "time_to_first_follow_up_ms",
    "time_to_thread_end_ms",
    "review_delay_burden_ms",
    "reassurance_count",
    "apology_repair_count",
    "commitment_clarity_count",
    "blame_pressure_count",
    "internal_disagreement_count",
    "attachment_recirculation_count",
    "version_turn_count",
)
_BUSINESS_TARGET_NAMES = (
    "enterprise_risk",
    "commercial_position_proxy",
    "org_strain_proxy",
    "stakeholder_trust",
    "execution_drag",
)
_OBJECTIVE_TARGET_NAMES = (
    "minimize_enterprise_risk",
    "protect_commercial_position",
    "reduce_org_strain",
    "preserve_stakeholder_trust",
    "maintain_execution_velocity",
)
_FUTURE_STATE_TARGET_NAMES = (
    "regulatory_exposure",
    "accounting_control_pressure",
    "liquidity_stress",
    "governance_response",
    "evidence_control",
    "external_confidence_pressure",
)
_PHASE_VALUES = ("history", "branch", "generated", "historical_future")
_SEQUENCE_TOKEN_LIMIT = 12
_SEQUENCE_NUMERIC_WIDTH = 12


@dataclass(frozen=True)
class _TrainConfig:
    epochs: int
    batch_size: int
    learning_rate: float
    seed: int
    device: str


@dataclass(frozen=True)
class _RowEncoding:
    summary_values: np.ndarray
    action_values: np.ndarray
    token_categorical: np.ndarray
    token_numeric: np.ndarray
    binary_target: float | None
    regression_target: np.ndarray | None
    business_target: np.ndarray | None
    objective_target: np.ndarray | None
    future_state_target: np.ndarray | None
    row: WhatIfBenchmarkDatasetRow


@dataclass(frozen=True)
class _BatchTensors:
    summary: Any
    action: Any
    token_categorical: Any
    token_numeric: Any
    target_binary: Any | None = None
    target_regression: Any | None = None
    target_business: Any | None = None
    target_objective: Any | None = None
    target_future_state: Any | None = None


@dataclass(frozen=True)
class _PredictionBatch:
    binary_probability: np.ndarray
    regression_values: np.ndarray
    business_values: np.ndarray | None = None
    objective_values: np.ndarray | None = None
    future_state_values: np.ndarray | None = None


@dataclass(frozen=True)
class _RowPrediction:
    binary_probability: float
    regression_values: np.ndarray
    business_values: np.ndarray | None = None
    objective_values: np.ndarray | None = None
    future_state_values: np.ndarray | None = None


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    train_parser = subparsers.add_parser("train")
    train_parser.add_argument("--request", required=True)
    train_parser.add_argument("--output", required=True)

    eval_parser = subparsers.add_parser("eval")
    eval_parser.add_argument("--request", required=True)
    eval_parser.add_argument("--output", required=True)

    predict_parser = subparsers.add_parser("predict")
    predict_parser.add_argument("--request", required=True)
    predict_parser.add_argument("--output", required=True)

    args = parser.parse_args()
    if args.command == "train":
        result = _train_from_request(Path(args.request))
    elif args.command == "eval":
        result = _eval_from_request(Path(args.request))
    else:
        result = _predict_from_request(Path(args.request))

    output_path = Path(args.output)
    if hasattr(result, "model_dump_json"):
        output_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    else:
        output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return 0


def _train_from_request(path: Path) -> WhatIfBenchmarkTrainResult:
    request = json.loads(path.read_text(encoding="utf-8"))
    output_root = Path(request["output_root"]).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    build = load_branch_point_benchmark_build_result(request["build_root"])
    dataset = _load_dataset_rows(build.dataset.split_paths)
    train_split_names = _dataset_split_names(
        request.get("train_splits"),
        default=("train",),
        allowed=("train", "validation"),
    )
    validation_split_names = _dataset_split_names(
        request.get("validation_splits"),
        default=("validation",),
        allowed=("validation", "test"),
    )
    source_train_rows = _rows_for_splits(dataset, train_split_names)
    source_validation_rows = _rows_for_splits(dataset, validation_split_names)
    preprocessor = _fit_preprocessor(
        train_rows=source_train_rows,
        validation_rows=source_validation_rows,
        test_rows=dataset["test"],
        heldout_rows=dataset["heldout"],
        cases=build.cases,
    )
    if request["model_id"] == "heuristic_baseline":
        return _train_heuristic_baseline(
            output_root=output_root,
            build=build,
            dataset=dataset,
            preprocessor=preprocessor,
            train_row_count=len(source_train_rows),
            validation_row_count=len(source_validation_rows),
            train_split_names=train_split_names,
            validation_split_names=validation_split_names,
        )

    train_rows = [preprocessor.encode_row(row) for row in source_train_rows]
    validation_rows = [preprocessor.encode_row(row) for row in source_validation_rows]

    config = _TrainConfig(
        epochs=int(request.get("epochs", 12)),
        batch_size=int(request.get("batch_size", 64)),
        learning_rate=float(request.get("learning_rate", 1e-3)),
        seed=int(request.get("seed", _RANDOM_SEED)),
        device=_resolve_device(str(request.get("device", "") or "")),
    )
    trainer = TorchTrainer(model_id=request["model_id"], preprocessor=preprocessor)
    trained = trainer.train(
        train_rows=train_rows,
        validation_rows=validation_rows,
        config=config,
    )

    model_path = output_root / "model.pt"
    metadata_path = output_root / "metadata.json"
    train_result_path = output_root / "train_result.json"
    trained.torch.save(
        {
            "state_dict": trained.model.state_dict(),
            "metadata": preprocessor.to_metadata(),
            "model_id": request["model_id"],
        },
        model_path,
    )
    metadata_path.write_text(
        json.dumps(preprocessor.to_metadata(), indent=2),
        encoding="utf-8",
    )
    result = WhatIfBenchmarkTrainResult(
        model_id=request["model_id"],
        dataset_root=build.dataset.root,
        train_loss=round(trained.train_loss, 6),
        validation_loss=round(trained.validation_loss, 6),
        epoch_count=config.epochs,
        train_row_count=len(train_rows),
        validation_row_count=len(validation_rows),
        notes=[
            f"device={config.device}",
            f"seed={config.seed}",
            f"train_splits={','.join(train_split_names)}",
            f"validation_splits={','.join(validation_split_names)}",
            f"test_rows={len(dataset['test'])}",
            f"heldout_rows={len(dataset['heldout'])}",
            f"summary_features={len(preprocessor.summary_feature_names)}",
            f"action_tags={len(preprocessor.action_tag_names)}",
            f"event_types={len(preprocessor.event_type_names)}",
            f"business_heads={len(_BUSINESS_TARGET_NAMES)}",
            f"objective_heads={len(_OBJECTIVE_TARGET_NAMES)}",
            f"future_state_heads={len(_FUTURE_STATE_TARGET_NAMES)}",
        ],
        artifacts=WhatIfBenchmarkTrainArtifacts(
            root=output_root,
            model_path=model_path,
            metadata_path=metadata_path,
            train_result_path=train_result_path,
        ),
    )
    train_result_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    return result


def _eval_from_request(path: Path) -> WhatIfBenchmarkEvalResult:
    request = json.loads(path.read_text(encoding="utf-8"))
    output_root = Path(request["output_root"]).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    build = load_branch_point_benchmark_build_result(request["build_root"])
    dataset = _load_dataset_rows(build.dataset.split_paths)
    if request["model_id"] == "heuristic_baseline":
        return _eval_heuristic_baseline(
            output_root=output_root,
            build=build,
            dataset=dataset,
        )

    checkpoint = load_checkpoint(output_root / "model.pt")
    preprocessor = BenchmarkPreprocessor.from_metadata(checkpoint["metadata"])
    trainer = TorchTrainer(model_id=request["model_id"], preprocessor=preprocessor)
    device = _resolve_device(str(request.get("device", "") or ""))
    model = trainer.build_model(device=device)
    _load_compatible_state_dict(model, checkpoint["state_dict"])
    model.eval()

    test_rows = [preprocessor.encode_row(row) for row in dataset["test"]]
    factual_predictions = predict_rows(
        model=model,
        rows=test_rows,
        batch_size=_HOLDOUT_BATCH_SIZE,
        device=device,
        torch_module=trainer.torch,
    )
    observed_metrics = _compute_observed_metrics(
        rows=test_rows,
        predictions=factual_predictions,
        preprocessor=preprocessor,
    )
    heldout_rows = [preprocessor.encode_row(row) for row in dataset["heldout"]]
    base_contract_by_case = {
        encoded.row.contract.case_id: encoded.row.contract for encoded in heldout_rows
    }
    case_evaluations = _evaluate_heldout_cases(
        model=model,
        build_cases=build.cases,
        base_contract_by_case=base_contract_by_case,
        preprocessor=preprocessor,
        device=device,
        torch_module=trainer.torch,
    )

    prediction_jsonl_path = output_root / "predictions.jsonl"
    _write_prediction_rows(
        path=prediction_jsonl_path,
        factual_rows=test_rows,
        factual_predictions=factual_predictions,
        case_evaluations=case_evaluations,
    )
    eval_result_path = output_root / "eval_result.json"
    result = WhatIfBenchmarkEvalResult(
        model_id=request["model_id"],
        dataset_root=build.dataset.root,
        observed_metrics=observed_metrics,
        cases=case_evaluations,
        notes=[
            f"device={device}",
            f"test_rows={len(test_rows)}",
            f"heldout_cases={len(build.cases)}",
        ],
        artifacts=WhatIfBenchmarkEvalArtifacts(
            root=output_root,
            eval_result_path=eval_result_path,
            prediction_jsonl_path=prediction_jsonl_path,
        ),
    )
    eval_result_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    return result


def _train_heuristic_baseline(
    *,
    output_root: Path,
    build,
    dataset: dict[str, list[WhatIfBenchmarkDatasetRow]],
    preprocessor: "BenchmarkPreprocessor",
    train_row_count: int | None = None,
    validation_row_count: int | None = None,
    train_split_names: Sequence[str] = ("train",),
    validation_split_names: Sequence[str] = ("validation",),
) -> WhatIfBenchmarkTrainResult:
    model_path = output_root / "model.pt"
    metadata_path = output_root / "metadata.json"
    train_result_path = output_root / "train_result.json"
    model_path.write_text(
        json.dumps(
            {
                "model_id": "heuristic_baseline",
                "note": "deterministic action-schema baseline; no learned weights",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    metadata_path.write_text(
        json.dumps(preprocessor.to_metadata(), indent=2),
        encoding="utf-8",
    )
    result = WhatIfBenchmarkTrainResult(
        model_id="heuristic_baseline",
        dataset_root=build.dataset.root,
        train_loss=0.0,
        validation_loss=0.0,
        epoch_count=0,
        train_row_count=(
            train_row_count if train_row_count is not None else len(dataset["train"])
        ),
        validation_row_count=(
            validation_row_count
            if validation_row_count is not None
            else len(dataset["validation"])
        ),
        notes=[
            "heuristic_baseline uses action-schema rules and trains no weights",
            f"train_splits={','.join(train_split_names)}",
            f"validation_splits={','.join(validation_split_names)}",
            f"test_rows={len(dataset['test'])}",
            f"heldout_rows={len(dataset['heldout'])}",
        ],
        artifacts=WhatIfBenchmarkTrainArtifacts(
            root=output_root,
            model_path=model_path,
            metadata_path=metadata_path,
            train_result_path=train_result_path,
        ),
    )
    train_result_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    return result


def _eval_heuristic_baseline(
    *,
    output_root: Path,
    build,
    dataset: dict[str, list[WhatIfBenchmarkDatasetRow]],
) -> WhatIfBenchmarkEvalResult:
    metadata_path = output_root / "metadata.json"
    if metadata_path.exists():
        preprocessor = BenchmarkPreprocessor.from_metadata(
            json.loads(metadata_path.read_text(encoding="utf-8"))
        )
    else:
        preprocessor = _fit_preprocessor(
            train_rows=dataset["train"],
            validation_rows=dataset["validation"],
            test_rows=dataset["test"],
            heldout_rows=dataset["heldout"],
            cases=build.cases,
        )
    test_rows = [preprocessor.encode_row(row) for row in dataset["test"]]
    factual_predictions = _heuristic_predict_rows(
        rows=test_rows,
        preprocessor=preprocessor,
    )
    observed_metrics = _compute_observed_metrics(
        rows=test_rows,
        predictions=factual_predictions,
        preprocessor=preprocessor,
    )
    heldout_rows = [preprocessor.encode_row(row) for row in dataset["heldout"]]
    base_contract_by_case = {
        encoded.row.contract.case_id: encoded.row.contract for encoded in heldout_rows
    }
    case_evaluations = _evaluate_heldout_cases_heuristic(
        build_cases=build.cases,
        base_contract_by_case=base_contract_by_case,
        preprocessor=preprocessor,
    )

    prediction_jsonl_path = output_root / "predictions.jsonl"
    _write_prediction_rows(
        path=prediction_jsonl_path,
        factual_rows=test_rows,
        factual_predictions=factual_predictions,
        case_evaluations=case_evaluations,
    )
    eval_result_path = output_root / "eval_result.json"
    result = WhatIfBenchmarkEvalResult(
        model_id="heuristic_baseline",
        dataset_root=build.dataset.root,
        observed_metrics=observed_metrics,
        cases=case_evaluations,
        notes=[
            "heuristic action-schema baseline",
            f"test_rows={len(test_rows)}",
            f"heldout_cases={len(build.cases)}",
        ],
        artifacts=WhatIfBenchmarkEvalArtifacts(
            root=output_root,
            eval_result_path=eval_result_path,
            prediction_jsonl_path=prediction_jsonl_path,
        ),
    )
    eval_result_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    return result


def _predict_from_request(path: Path) -> dict[str, Any]:
    request = json.loads(path.read_text(encoding="utf-8"))
    checkpoint_path = Path(request["checkpoint_path"]).expanduser().resolve()
    checkpoint = load_checkpoint(checkpoint_path)
    preprocessor = BenchmarkPreprocessor.from_metadata(checkpoint["metadata"])
    trainer = TorchTrainer(model_id=checkpoint["model_id"], preprocessor=preprocessor)
    device = _resolve_device(str(request.get("device", "") or ""))
    model = trainer.build_model(device=device)
    _load_compatible_state_dict(model, checkpoint["state_dict"])
    model.eval()

    row = WhatIfBenchmarkDatasetRow.model_validate(request["row"])
    encoded = preprocessor.encode_row(row)
    predictions = predict_rows(
        model=model,
        rows=[encoded],
        batch_size=1,
        device=device,
        torch_module=trainer.torch,
    )
    flat_predictions = _flatten_prediction_batches(predictions)
    if not flat_predictions:
        raise RuntimeError("benchmark bridge predict produced no rows")
    prediction = flat_predictions[0]
    evidence_heads = preprocessor.decode_targets(
        binary_probability=prediction.binary_probability,
        regression_values=prediction.regression_values,
    )
    business_heads = preprocessor.decode_business(
        prediction.business_values,
        fallback_evidence=evidence_heads,
    )
    future_state_heads = preprocessor.decode_future_state(
        prediction.future_state_values,
    )
    return {
        "model_id": checkpoint["model_id"],
        "binary_probability": prediction.binary_probability,
        "regression_values": prediction.regression_values.tolist(),
        "evidence_heads": evidence_heads.model_dump(mode="json"),
        "business_heads": business_heads.model_dump(mode="json"),
        "future_state_heads": future_state_heads.model_dump(mode="json"),
        "objective_scores": preprocessor.decode_objective_scores(
            prediction.objective_values
        ),
    }


def _load_dataset_rows(
    split_paths: dict[str, str],
) -> dict[str, list[WhatIfBenchmarkDatasetRow]]:
    result: dict[str, list[WhatIfBenchmarkDatasetRow]] = {}
    for split_name in ("train", "validation", "test", "heldout"):
        path = Path(split_paths[split_name]).expanduser().resolve()
        rows: list[WhatIfBenchmarkDatasetRow] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rows.append(WhatIfBenchmarkDatasetRow.model_validate_json(line))
        result[split_name] = rows
    return result


def _dataset_split_names(
    raw: Any,
    *,
    default: Sequence[str],
    allowed: Sequence[str],
) -> list[str]:
    if raw is None or raw == "":
        values = list(default)
    elif isinstance(raw, str):
        values = [part.strip() for part in raw.split(",")]
    elif isinstance(raw, Sequence):
        values = []
        for item in raw:
            values.extend(str(item).split(","))
        values = [part.strip() for part in values]
    else:
        raise ValueError("dataset split names must be a string or sequence")

    allowed_set = set(allowed)

    result: list[str] = []
    for value in values:
        if not value:
            continue
        if value not in allowed_set:
            choices = ", ".join(sorted(allowed_set))
            raise ValueError(
                f"unsupported dataset split {value!r}; choose one of: {choices}"
            )
        if value not in result:
            result.append(value)
    if not result:
        raise ValueError("at least one dataset split is required")
    return result


def _rows_for_splits(
    dataset: dict[str, list[WhatIfBenchmarkDatasetRow]],
    split_names: Sequence[str],
) -> list[WhatIfBenchmarkDatasetRow]:
    rows: list[WhatIfBenchmarkDatasetRow] = []
    for split_name in split_names:
        rows.extend(dataset.get(split_name, []))
    return rows


def load_checkpoint(path: Path) -> dict[str, Any]:
    torch = importlib.import_module("torch")
    checkpoint = torch.load(path, map_location="cpu")
    if not isinstance(checkpoint, dict):
        raise RuntimeError(f"invalid checkpoint payload in {path}")
    return checkpoint


def _load_compatible_state_dict(model: Any, state_dict: dict[str, Any]) -> None:
    """Load older checkpoints even when newer optional heads have been added."""

    current = model.state_dict()
    compatible = {
        key: value
        for key, value in state_dict.items()
        if key in current and tuple(value.shape) == tuple(current[key].shape)
    }
    model.load_state_dict(compatible, strict=False)


class BenchmarkPreprocessor:
    def __init__(
        self,
        *,
        summary_feature_names: Sequence[str],
        summary_mean: Sequence[float],
        summary_std: Sequence[float],
        action_tag_names: Sequence[str],
        event_type_names: Sequence[str],
        target_mean: Sequence[float],
        target_std: Sequence[float],
        business_mean: Sequence[float] | None = None,
        business_std: Sequence[float] | None = None,
        objective_mean: Sequence[float] | None = None,
        objective_std: Sequence[float] | None = None,
        future_state_mean: Sequence[float] | None = None,
        future_state_std: Sequence[float] | None = None,
    ) -> None:
        self.summary_feature_names = list(summary_feature_names)
        self.summary_index = {
            name: index for index, name in enumerate(self.summary_feature_names)
        }
        self.summary_mean = np.asarray(summary_mean, dtype=np.float32)
        self.summary_std = np.asarray(summary_std, dtype=np.float32)
        self.action_tag_names = list(action_tag_names)
        self.action_tag_index = {
            name: index for index, name in enumerate(self.action_tag_names)
        }
        self.event_type_names = list(event_type_names)
        self.event_type_index = {
            name: index for index, name in enumerate(self.event_type_names)
        }
        self.target_mean = np.asarray(target_mean, dtype=np.float32)
        self.target_std = np.asarray(target_std, dtype=np.float32)
        self.business_mean = np.asarray(
            (
                business_mean
                if business_mean is not None
                else np.zeros(len(_BUSINESS_TARGET_NAMES))
            ),
            dtype=np.float32,
        )
        self.business_std = np.asarray(
            (
                business_std
                if business_std is not None
                else np.ones(len(_BUSINESS_TARGET_NAMES))
            ),
            dtype=np.float32,
        )
        self.objective_mean = np.asarray(
            (
                objective_mean
                if objective_mean is not None
                else np.zeros(len(_OBJECTIVE_TARGET_NAMES))
            ),
            dtype=np.float32,
        )
        self.objective_std = np.asarray(
            (
                objective_std
                if objective_std is not None
                else np.ones(len(_OBJECTIVE_TARGET_NAMES))
            ),
            dtype=np.float32,
        )
        self.future_state_mean = np.asarray(
            (
                future_state_mean
                if future_state_mean is not None
                else np.zeros(len(_FUTURE_STATE_TARGET_NAMES))
            ),
            dtype=np.float32,
        )
        self.future_state_std = np.asarray(
            (
                future_state_std
                if future_state_std is not None
                else np.ones(len(_FUTURE_STATE_TARGET_NAMES))
            ),
            dtype=np.float32,
        )

    @classmethod
    def from_metadata(cls, payload: dict[str, Any]) -> "BenchmarkPreprocessor":
        return cls(
            summary_feature_names=payload["summary_feature_names"],
            summary_mean=payload["summary_mean"],
            summary_std=payload["summary_std"],
            action_tag_names=payload["action_tag_names"],
            event_type_names=payload["event_type_names"],
            target_mean=payload["target_mean"],
            target_std=payload["target_std"],
            business_mean=payload.get("business_mean"),
            business_std=payload.get("business_std"),
            objective_mean=payload.get("objective_mean"),
            objective_std=payload.get("objective_std"),
            future_state_mean=payload.get("future_state_mean"),
            future_state_std=payload.get("future_state_std"),
        )

    def to_metadata(self) -> dict[str, Any]:
        return {
            "summary_feature_names": self.summary_feature_names,
            "summary_mean": self.summary_mean.tolist(),
            "summary_std": self.summary_std.tolist(),
            "action_tag_names": self.action_tag_names,
            "event_type_names": self.event_type_names,
            "target_mean": self.target_mean.tolist(),
            "target_std": self.target_std.tolist(),
            "business_target_names": list(_BUSINESS_TARGET_NAMES),
            "business_mean": self.business_mean.tolist(),
            "business_std": self.business_std.tolist(),
            "objective_target_names": list(_OBJECTIVE_TARGET_NAMES),
            "objective_mean": self.objective_mean.tolist(),
            "objective_std": self.objective_std.tolist(),
            "future_state_target_names": list(_FUTURE_STATE_TARGET_NAMES),
            "future_state_mean": self.future_state_mean.tolist(),
            "future_state_std": self.future_state_std.tolist(),
        }

    def encode_row(self, row: WhatIfBenchmarkDatasetRow) -> _RowEncoding:
        summary_values = self._encode_summary(row.contract.summary_features)
        action_values = self._encode_action(row.contract.action_schema)
        token_categorical, token_numeric = self._encode_sequence(
            row.contract.sequence_steps,
            row.contract.action_schema,
            summary_values,
        )
        if row.split == "heldout":
            return _RowEncoding(
                summary_values=summary_values,
                action_values=action_values,
                token_categorical=token_categorical,
                token_numeric=token_numeric,
                binary_target=None,
                regression_target=None,
                business_target=None,
                objective_target=None,
                future_state_target=None,
                row=row,
            )
        return _RowEncoding(
            summary_values=summary_values,
            action_values=action_values,
            token_categorical=token_categorical,
            token_numeric=token_numeric,
            binary_target=float(row.observed_evidence_heads.any_external_spread),
            regression_target=self._encode_targets(row.observed_evidence_heads),
            business_target=self._encode_business(row.observed_business_outcomes),
            objective_target=self._encode_objectives(row),
            future_state_target=self._encode_future_state(row.observed_future_state),
            row=row,
        )

    def encode_counterfactual(
        self,
        row: WhatIfBenchmarkDatasetRow,
        *,
        action_schema: WhatIfActionSchema,
    ) -> _RowEncoding:
        contract = row.contract.model_copy(update={"action_schema": action_schema})
        counterfactual_row = row.model_copy(update={"contract": contract})
        return self.encode_row(counterfactual_row)

    def decode_targets(
        self,
        *,
        binary_probability: float,
        regression_values: Sequence[float],
    ) -> WhatIfObservedEvidenceHeads:
        regression = np.asarray(regression_values, dtype=np.float32)
        # Small-data study runs can push the regression heads into extreme
        # ranges or non-finite values. Normalize those cases before expm1 so
        # benchmark decoding stays finite and comparable instead of crashing.
        logged = np.nan_to_num(
            (regression * self.target_std) + self.target_mean,
            nan=0.0,
            posinf=20.0,
            neginf=0.0,
        )
        logged = np.clip(
            logged,
            a_min=0.0,
            a_max=20.0,
        )
        restored = np.expm1(logged)
        clipped = [max(0, int(round(value))) for value in restored.tolist()]
        payload = {
            name: clipped[index] for index, name in enumerate(_EVIDENCE_TARGET_NAMES)
        }
        return WhatIfObservedEvidenceHeads(
            any_external_spread=binary_probability >= 0.5,
            outside_recipient_count=payload["outside_recipient_count"],
            outside_forward_count=payload["outside_forward_count"],
            outside_attachment_spread_count=payload["outside_attachment_spread_count"],
            legal_follow_up_count=payload["legal_follow_up_count"],
            review_loop_count=payload["review_loop_count"],
            markup_loop_count=payload["markup_loop_count"],
            executive_escalation_count=payload["executive_escalation_count"],
            executive_mention_count=payload["executive_mention_count"],
            urgency_spike_count=payload["urgency_spike_count"],
            participant_fanout=payload["participant_fanout"],
            cc_expansion_count=payload["cc_expansion_count"],
            cross_functional_loop_count=payload["cross_functional_loop_count"],
            time_to_first_follow_up_ms=payload["time_to_first_follow_up_ms"],
            time_to_thread_end_ms=payload["time_to_thread_end_ms"],
            review_delay_burden_ms=payload["review_delay_burden_ms"],
            reassurance_count=payload["reassurance_count"],
            apology_repair_count=payload["apology_repair_count"],
            commitment_clarity_count=payload["commitment_clarity_count"],
            blame_pressure_count=payload["blame_pressure_count"],
            internal_disagreement_count=payload["internal_disagreement_count"],
            attachment_recirculation_count=payload["attachment_recirculation_count"],
            version_turn_count=payload["version_turn_count"],
        )

    def decode_business(
        self,
        business_values: Sequence[float] | None,
        *,
        fallback_evidence: WhatIfObservedEvidenceHeads,
    ):
        if business_values is None:
            return evidence_to_business_outcomes(fallback_evidence)
        from .models import WhatIfBusinessOutcomeHeads

        values = np.nan_to_num(
            (np.asarray(business_values, dtype=np.float32) * self.business_std)
            + self.business_mean,
            nan=0.0,
            posinf=1.0,
            neginf=0.0,
        )
        values = np.clip(values, 0.0, 1.0)
        payload = {
            name: round(float(values[index]), 3)
            for index, name in enumerate(_BUSINESS_TARGET_NAMES)
        }
        return WhatIfBusinessOutcomeHeads(**payload)

    def decode_objective_scores(
        self,
        objective_values: Sequence[float] | None,
    ) -> dict[str, float]:
        if objective_values is None:
            return {}
        values = np.nan_to_num(
            (np.asarray(objective_values, dtype=np.float32) * self.objective_std)
            + self.objective_mean,
            nan=0.0,
            posinf=1.0,
            neginf=0.0,
        )
        values = np.clip(values, 0.0, 1.0)
        return {
            name: round(float(values[index]), 3)
            for index, name in enumerate(_OBJECTIVE_TARGET_NAMES)
        }

    def decode_future_state(
        self,
        future_state_values: Sequence[float] | None,
    ) -> WhatIfFutureStateHeads:
        if future_state_values is None:
            return WhatIfFutureStateHeads()
        values = np.nan_to_num(
            (np.asarray(future_state_values, dtype=np.float32) * self.future_state_std)
            + self.future_state_mean,
            nan=0.0,
            posinf=1.0,
            neginf=0.0,
        )
        values = np.clip(values, 0.0, 1.0)
        payload = {
            name: round(float(values[index]), 3)
            for index, name in enumerate(_FUTURE_STATE_TARGET_NAMES)
        }
        return WhatIfFutureStateHeads(**payload)

    def _encode_summary(self, features: Sequence[Any]) -> np.ndarray:
        values = np.zeros(len(self.summary_feature_names), dtype=np.float32)
        for feature in features:
            index = self.summary_index.get(feature.name)
            if index is None:
                continue
            values[index] = float(feature.value)
        return (values - self.summary_mean) / self.summary_std

    def _encode_action(self, action_schema: WhatIfActionSchema) -> np.ndarray:
        values: list[float] = []
        values.extend(_one_hot(action_schema.recipient_scope, _RECIPIENT_SCOPE_VALUES))
        values.extend(
            _one_hot(action_schema.attachment_policy, _ATTACHMENT_POLICY_VALUES)
        )
        values.extend(
            _one_hot(action_schema.escalation_level, _ESCALATION_LEVEL_VALUES)
        )
        values.extend(_one_hot(action_schema.owner_clarity, _OWNER_CLARITY_VALUES))
        values.extend(
            _one_hot(action_schema.reassurance_style, _REASSURANCE_STYLE_VALUES)
        )
        values.extend(_one_hot(action_schema.review_path, _REVIEW_PATH_VALUES))
        values.extend(
            _one_hot(
                action_schema.coordination_breadth,
                _COORDINATION_BREADTH_VALUES,
            )
        )
        values.extend(
            _one_hot(
                action_schema.outside_sharing_posture,
                _OUTSIDE_SHARING_POSTURE_VALUES,
            )
        )
        values.extend(
            _one_hot(action_schema.decision_posture, _DECISION_POSTURE_VALUES)
        )
        values.extend(
            [
                float(action_schema.external_recipient_count) / 5.0,
                float(action_schema.hold_required),
                float(action_schema.legal_review_required),
                float(action_schema.trading_review_required),
            ]
        )
        tag_values = np.zeros(len(self.action_tag_names), dtype=np.float32)
        for tag in action_schema.action_tags:
            index = self.action_tag_index.get(tag)
            if index is None:
                continue
            tag_values[index] = 1.0
        return np.concatenate(
            [
                np.asarray(values, dtype=np.float32),
                tag_values,
            ]
        )

    def _encode_sequence(
        self,
        steps: Sequence[Any],
        action_schema: WhatIfActionSchema,
        summary_values: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray]:
        token_categorical = np.zeros((_SEQUENCE_TOKEN_LIMIT, 3), dtype=np.int64)
        token_numeric = np.zeros(
            (_SEQUENCE_TOKEN_LIMIT, _SEQUENCE_NUMERIC_WIDTH),
            dtype=np.float32,
        )
        trimmed_steps = list(steps)[-_SEQUENCE_TOKEN_LIMIT + 2 :]
        for index, step in enumerate(trimmed_steps):
            token_categorical[index, 0] = _safe_index(step.phase, _PHASE_VALUES)
            token_categorical[index, 1] = self.event_type_index.get(step.event_type, 0)
            token_categorical[index, 2] = _safe_index(
                step.recipient_scope,
                _RECIPIENT_SCOPE_VALUES,
            )
            token_numeric[index, :] = np.asarray(
                [
                    float(step.delay_ms) / 86_400_000.0,
                    float(step.external_recipient_count) / 5.0,
                    float(step.cc_recipient_count) / 5.0,
                    float(step.attachment_flag),
                    float(step.escalation_flag),
                    float(step.approval_flag),
                    float(step.legal_flag),
                    float(step.trading_flag),
                    float(step.review_flag),
                    float(step.urgency_flag),
                    float(step.conflict_flag),
                    float(_text_feature_count(step.subject)),
                ],
                dtype=np.float32,
            )
        action_index = len(trimmed_steps)
        token_categorical[action_index, 0] = _safe_index("branch", _PHASE_VALUES)
        token_categorical[action_index, 1] = self.event_type_index.get(
            action_schema.event_type,
            0,
        )
        token_categorical[action_index, 2] = _safe_index(
            action_schema.recipient_scope,
            _RECIPIENT_SCOPE_VALUES,
        )
        token_numeric[action_index, :] = np.asarray(
            [
                float(action_schema.external_recipient_count) / 5.0,
                float(action_schema.hold_required),
                float(action_schema.legal_review_required),
                float(action_schema.trading_review_required),
                float(_safe_index(action_schema.review_path, _REVIEW_PATH_VALUES))
                / max(len(_REVIEW_PATH_VALUES) - 1, 1),
                float(
                    _safe_index(
                        action_schema.coordination_breadth,
                        _COORDINATION_BREADTH_VALUES,
                    )
                )
                / max(len(_COORDINATION_BREADTH_VALUES) - 1, 1),
                float(
                    _safe_index(
                        action_schema.outside_sharing_posture,
                        _OUTSIDE_SHARING_POSTURE_VALUES,
                    )
                )
                / max(len(_OUTSIDE_SHARING_POSTURE_VALUES) - 1, 1),
                float(
                    _safe_index(
                        action_schema.decision_posture, _DECISION_POSTURE_VALUES
                    )
                )
                / max(len(_DECISION_POSTURE_VALUES) - 1, 1),
                float(
                    _safe_index(
                        action_schema.reassurance_style, _REASSURANCE_STYLE_VALUES
                    )
                )
                / 3.0,
                float(_safe_index(action_schema.owner_clarity, _OWNER_CLARITY_VALUES))
                / 3.0,
                float(action_schema.external_recipient_count > 0),
                float(len(action_schema.action_tags)) / 6.0,
            ],
            dtype=np.float32,
        )
        summary_index = min(action_index + 1, _SEQUENCE_TOKEN_LIMIT - 1)
        token_categorical[summary_index, 0] = _safe_index("generated", _PHASE_VALUES)
        token_categorical[summary_index, 1] = self.event_type_index.get(
            "__summary__", 0
        )
        token_numeric[summary_index, :] = _summary_token(summary_values)
        return token_categorical, token_numeric

    def _encode_targets(self, targets: WhatIfObservedEvidenceHeads) -> np.ndarray:
        raw_values = np.asarray(
            [getattr(targets, name) for name in _EVIDENCE_TARGET_NAMES],
            dtype=np.float32,
        )
        logged = np.log1p(raw_values)
        return (logged - self.target_mean) / self.target_std

    def _encode_business(self, targets: Any) -> np.ndarray:
        raw_values = np.asarray(
            [float(getattr(targets, name)) for name in _BUSINESS_TARGET_NAMES],
            dtype=np.float32,
        )
        return (raw_values - self.business_mean) / self.business_std

    def _encode_objectives(self, row: WhatIfBenchmarkDatasetRow) -> np.ndarray:
        values: list[float] = []
        for pack in list_business_objective_packs():
            score = score_business_objective(
                pack=pack,
                outcomes=row.observed_business_outcomes,
                evidence=row.observed_evidence_heads,
            )
            values.append(float(score.overall_score))
        raw_values = np.asarray(values, dtype=np.float32)
        return (raw_values - self.objective_mean) / self.objective_std

    def _encode_future_state(self, targets: WhatIfFutureStateHeads) -> np.ndarray:
        raw_values = np.asarray(
            [float(getattr(targets, name)) for name in _FUTURE_STATE_TARGET_NAMES],
            dtype=np.float32,
        )
        return (raw_values - self.future_state_mean) / self.future_state_std


def _fit_preprocessor(
    *,
    train_rows: Sequence[WhatIfBenchmarkDatasetRow],
    validation_rows: Sequence[WhatIfBenchmarkDatasetRow],
    test_rows: Sequence[WhatIfBenchmarkDatasetRow],
    heldout_rows: Sequence[WhatIfBenchmarkDatasetRow],
    cases: Sequence[WhatIfBenchmarkCase],
) -> BenchmarkPreprocessor:
    summary_names = sorted(
        {
            feature.name
            for row in list(train_rows)
            + list(validation_rows)
            + list(test_rows)
            + list(heldout_rows)
            for feature in row.contract.summary_features
        }
    )
    if not summary_names:
        summary_names = ["history_event_count"]
    summary_matrix = np.asarray(
        [_summary_vector(row, summary_names) for row in train_rows],
        dtype=np.float32,
    )
    summary_mean = (
        summary_matrix.mean(axis=0)
        if len(summary_matrix)
        else np.zeros(len(summary_names))
    )
    summary_std = (
        summary_matrix.std(axis=0)
        if len(summary_matrix)
        else np.ones(len(summary_names))
    )
    summary_std = np.where(summary_std < 1e-6, 1.0, summary_std)

    action_tags = sorted(
        {
            tag
            for row in list(train_rows)
            + list(validation_rows)
            + list(test_rows)
            + list(heldout_rows)
            for tag in row.contract.action_schema.action_tags
        }
        | {
            tag
            for case in cases
            for candidate in case.candidates
            for tag in candidate.action_schema.action_tags
        }
    )
    event_types = {"__summary__"}
    for row in (
        list(train_rows) + list(validation_rows) + list(test_rows) + list(heldout_rows)
    ):
        for step in row.contract.sequence_steps:
            event_types.add(step.event_type)
        event_types.add(row.contract.action_schema.event_type)
    for case in cases:
        for candidate in case.candidates:
            event_types.add(candidate.action_schema.event_type)
    target_matrix = np.asarray(
        [
            np.log1p(
                [
                    getattr(row.observed_evidence_heads, name)
                    for name in _EVIDENCE_TARGET_NAMES
                ]
            )
            for row in train_rows
        ],
        dtype=np.float32,
    )
    target_mean = (
        target_matrix.mean(axis=0)
        if len(target_matrix)
        else np.zeros(len(_EVIDENCE_TARGET_NAMES))
    )
    target_std = (
        target_matrix.std(axis=0)
        if len(target_matrix)
        else np.ones(len(_EVIDENCE_TARGET_NAMES))
    )
    target_std = np.where(target_std < 1e-6, 1.0, target_std)
    business_matrix = np.asarray(
        [
            [
                float(getattr(row.observed_business_outcomes, name))
                for name in _BUSINESS_TARGET_NAMES
            ]
            for row in train_rows
        ],
        dtype=np.float32,
    )
    business_mean = (
        business_matrix.mean(axis=0)
        if len(business_matrix)
        else np.zeros(len(_BUSINESS_TARGET_NAMES))
    )
    business_std = (
        business_matrix.std(axis=0)
        if len(business_matrix)
        else np.ones(len(_BUSINESS_TARGET_NAMES))
    )
    business_std = np.where(business_std < 1e-6, 1.0, business_std)
    objective_matrix = np.asarray(
        [
            [
                float(
                    score_business_objective(
                        pack=pack,
                        outcomes=row.observed_business_outcomes,
                        evidence=row.observed_evidence_heads,
                    ).overall_score
                )
                for pack in list_business_objective_packs()
            ]
            for row in train_rows
        ],
        dtype=np.float32,
    )
    objective_mean = (
        objective_matrix.mean(axis=0)
        if len(objective_matrix)
        else np.zeros(len(_OBJECTIVE_TARGET_NAMES))
    )
    objective_std = (
        objective_matrix.std(axis=0)
        if len(objective_matrix)
        else np.ones(len(_OBJECTIVE_TARGET_NAMES))
    )
    objective_std = np.where(objective_std < 1e-6, 1.0, objective_std)
    future_state_matrix = np.asarray(
        [
            [
                float(getattr(row.observed_future_state, name))
                for name in _FUTURE_STATE_TARGET_NAMES
            ]
            for row in train_rows
        ],
        dtype=np.float32,
    )
    future_state_mean = (
        future_state_matrix.mean(axis=0)
        if len(future_state_matrix)
        else np.zeros(len(_FUTURE_STATE_TARGET_NAMES))
    )
    future_state_std = (
        future_state_matrix.std(axis=0)
        if len(future_state_matrix)
        else np.ones(len(_FUTURE_STATE_TARGET_NAMES))
    )
    future_state_std = np.where(future_state_std < 1e-6, 1.0, future_state_std)
    return BenchmarkPreprocessor(
        summary_feature_names=summary_names,
        summary_mean=summary_mean.tolist(),
        summary_std=summary_std.tolist(),
        action_tag_names=action_tags,
        event_type_names=sorted(event_types),
        target_mean=target_mean.tolist(),
        target_std=target_std.tolist(),
        business_mean=business_mean.tolist(),
        business_std=business_std.tolist(),
        objective_mean=objective_mean.tolist(),
        objective_std=objective_std.tolist(),
        future_state_mean=future_state_mean.tolist(),
        future_state_std=future_state_std.tolist(),
    )


class TorchTrainer:
    def __init__(
        self,
        *,
        model_id: WhatIfBenchmarkModelId,
        preprocessor: BenchmarkPreprocessor,
    ) -> None:
        self.model_id = model_id
        self.preprocessor = preprocessor
        self.torch = importlib.import_module("torch")
        self.nn = importlib.import_module("torch.nn")
        self.F = importlib.import_module("torch.nn.functional")

    def build_model(self, *, device: str) -> Any:
        if self.model_id == "jepa_latent":
            model = self._build_jepa_model()
        elif self.model_id == "full_context_transformer":
            model = self._build_full_context_model()
        elif self.model_id == "ft_transformer":
            model = self._build_ft_model()
        elif self.model_id == "sequence_transformer":
            model = self._build_sequence_model()
        elif self.model_id == "treatment_transformer":
            model = self._build_treatment_model()
        else:
            raise ValueError(f"unsupported benchmark model id: {self.model_id}")
        model.to(device)
        return model

    def train(
        self,
        *,
        train_rows: Sequence[_RowEncoding],
        validation_rows: Sequence[_RowEncoding],
        config: _TrainConfig,
    ) -> Any:
        torch = self.torch
        _seed_everything(torch, config.seed)
        model = self.build_model(device=config.device)
        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=config.learning_rate,
            weight_decay=1e-4,
        )
        best_state = None
        best_validation = float("inf")
        last_train = float("inf")

        for _epoch in range(config.epochs):
            model.train()
            epoch_losses: list[float] = []
            for batch in _iter_batches(
                train_rows,
                batch_size=config.batch_size,
                device=config.device,
                torch_module=torch,
            ):
                optimizer.zero_grad()
                outputs = _model_outputs(model, batch)
                loss = _training_loss(
                    outputs=outputs,
                    batch=batch,
                    functional=self.F,
                )
                loss.backward()
                optimizer.step()
                epoch_losses.append(float(loss.item()))
            last_train = sum(epoch_losses) / max(len(epoch_losses), 1)
            validation_loss = self._validation_loss(
                model=model,
                rows=validation_rows,
                batch_size=config.batch_size,
                device=config.device,
            )
            if validation_loss < best_validation:
                best_validation = validation_loss
                best_state = {
                    key: value.detach().cpu().clone()
                    for key, value in model.state_dict().items()
                }
        if best_state is not None:
            model.load_state_dict(best_state)
        return type(
            "TrainedModel",
            (),
            {
                "model": model,
                "train_loss": last_train,
                "validation_loss": best_validation,
                "torch": torch,
            },
        )()

    def _validation_loss(
        self,
        *,
        model: Any,
        rows: Sequence[_RowEncoding],
        batch_size: int,
        device: str,
    ) -> float:
        if not rows:
            return 0.0
        model.eval()
        losses: list[float] = []
        with self.torch.no_grad():
            for batch in _iter_batches(
                rows,
                batch_size=batch_size,
                device=device,
                torch_module=self.torch,
            ):
                outputs = _model_outputs(model, batch)
                loss = _training_loss(
                    outputs=outputs,
                    batch=batch,
                    functional=self.F,
                )
                losses.append(float(loss.item()))
        return sum(losses) / max(len(losses), 1)

    def _build_jepa_model(self) -> Any:
        nn = self.nn
        summary_dim = len(self.preprocessor.summary_feature_names)
        action_dim = _action_vector_width(self.preprocessor)
        target_dim = len(_EVIDENCE_TARGET_NAMES)
        business_dim = len(_BUSINESS_TARGET_NAMES)
        objective_dim = len(_OBJECTIVE_TARGET_NAMES)
        future_state_dim = len(_FUTURE_STATE_TARGET_NAMES)
        latent_dim = 128
        event_type_count = max(len(self.preprocessor.event_type_names), 1)

        class JEPAOutcomeModel(nn.Module):
            def __init__(self) -> None:
                super().__init__()
                self.phase_embedding = nn.Embedding(len(_PHASE_VALUES), latent_dim)
                self.event_embedding = nn.Embedding(event_type_count, latent_dim)
                self.scope_embedding = nn.Embedding(
                    len(_RECIPIENT_SCOPE_VALUES),
                    latent_dim,
                )
                self.numeric_projection = nn.Linear(
                    _SEQUENCE_NUMERIC_WIDTH,
                    latent_dim,
                )
                encoder_layer = nn.TransformerEncoderLayer(
                    d_model=latent_dim,
                    nhead=4,
                    dim_feedforward=256,
                    batch_first=True,
                    dropout=0.1,
                )
                self.sequence_encoder = nn.TransformerEncoder(
                    encoder_layer,
                    num_layers=2,
                )
                self.summary_action_encoder = nn.Sequential(
                    nn.Linear(summary_dim + action_dim, 256),
                    nn.ReLU(),
                    nn.Linear(256, latent_dim),
                )
                self.context_encoder = nn.Sequential(
                    nn.Linear(latent_dim * 2, 256),
                    nn.ReLU(),
                    nn.Linear(256, latent_dim),
                )
                self.target_encoder = nn.Sequential(
                    nn.Linear(
                        target_dim
                        + business_dim
                        + objective_dim
                        + future_state_dim
                        + 1,
                        256,
                    ),
                    nn.ReLU(),
                    nn.Linear(256, latent_dim),
                )
                self.predictor = nn.Sequential(
                    nn.Linear(latent_dim, latent_dim),
                    nn.ReLU(),
                    nn.Linear(latent_dim, latent_dim),
                )
                self.binary_head = nn.Linear(latent_dim, 1)
                self.regression_head = nn.Linear(latent_dim, target_dim)
                self.business_head = nn.Linear(latent_dim, business_dim)
                self.objective_head = nn.Linear(latent_dim, objective_dim)
                self.future_state_head = nn.Linear(latent_dim, future_state_dim)

            def forward(
                self,
                summary: Any,
                action: Any,
                token_categorical: Any,
                token_numeric: Any,
                target_binary: Any | None = None,
                target_regression: Any | None = None,
                target_business: Any | None = None,
                target_objective: Any | None = None,
                target_future_state: Any | None = None,
            ) -> dict[str, Any]:
                summary_action = self.summary_action_encoder(
                    self._concat([summary, action], dim=1)
                )
                sequence_tokens = (
                    self.phase_embedding(token_categorical[:, :, 0])
                    + self.event_embedding(token_categorical[:, :, 1])
                    + self.scope_embedding(token_categorical[:, :, 2])
                    + self.numeric_projection(token_numeric)
                )
                sequence_state = self.sequence_encoder(sequence_tokens).mean(dim=1)
                context = self.context_encoder(
                    self._concat([summary_action, sequence_state], dim=1)
                )
                predicted_latent = self.predictor(context)
                result = {
                    "binary_logits": self.binary_head(predicted_latent).squeeze(-1),
                    "regression": self.regression_head(predicted_latent),
                    "business": self.business_head(predicted_latent),
                    "objective": self.objective_head(predicted_latent),
                    "future_state": self.future_state_head(predicted_latent),
                }
                if (
                    target_binary is None
                    or target_regression is None
                    or target_business is None
                    or target_objective is None
                    or target_future_state is None
                ):
                    result["latent_loss"] = None
                    return result
                target = self.target_encoder(
                    self._concat(
                        [
                            target_binary.unsqueeze(-1),
                            target_regression,
                            target_business,
                            target_objective,
                            target_future_state,
                        ],
                        dim=1,
                    )
                ).detach()
                result["latent_loss"] = ((predicted_latent - target) ** 2).mean()
                return result

            @staticmethod
            def _concat(values: Sequence[Any], *, dim: int) -> Any:
                return importlib.import_module("torch").cat(values, dim=dim)

        return JEPAOutcomeModel()

    def _build_full_context_model(self) -> Any:
        nn = self.nn
        summary_dim = len(self.preprocessor.summary_feature_names)
        action_dim = _action_vector_width(self.preprocessor)
        target_dim = len(_EVIDENCE_TARGET_NAMES)
        business_dim = len(_BUSINESS_TARGET_NAMES)
        objective_dim = len(_OBJECTIVE_TARGET_NAMES)
        future_state_dim = len(_FUTURE_STATE_TARGET_NAMES)
        model_dim = 96
        event_type_count = max(len(self.preprocessor.event_type_names), 1)

        class FullContextTransformerModel(nn.Module):
            def __init__(self) -> None:
                super().__init__()
                self.phase_embedding = nn.Embedding(len(_PHASE_VALUES), model_dim)
                self.event_embedding = nn.Embedding(event_type_count, model_dim)
                self.scope_embedding = nn.Embedding(
                    len(_RECIPIENT_SCOPE_VALUES),
                    model_dim,
                )
                self.numeric_projection = nn.Linear(
                    _SEQUENCE_NUMERIC_WIDTH,
                    model_dim,
                )
                self.summary_action_projection = nn.Sequential(
                    nn.Linear(summary_dim + action_dim, 192),
                    nn.ReLU(),
                    nn.Linear(192, model_dim),
                )
                encoder_layer = nn.TransformerEncoderLayer(
                    d_model=model_dim,
                    nhead=4,
                    dim_feedforward=192,
                    batch_first=True,
                    dropout=0.1,
                )
                self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=2)
                self.binary_head = nn.Linear(model_dim, 1)
                self.regression_head = nn.Linear(model_dim, target_dim)
                self.business_head = nn.Linear(model_dim, business_dim)
                self.objective_head = nn.Linear(model_dim, objective_dim)
                self.future_state_head = nn.Linear(model_dim, future_state_dim)

            def forward(
                self,
                summary: Any,
                action: Any,
                token_categorical: Any,
                token_numeric: Any,
                target_binary: Any | None = None,
                target_regression: Any | None = None,
                target_business: Any | None = None,
                target_objective: Any | None = None,
                target_future_state: Any | None = None,
            ) -> dict[str, Any]:
                del (
                    target_binary,
                    target_regression,
                    target_business,
                    target_objective,
                    target_future_state,
                )
                summary_action_token = self.summary_action_projection(
                    self._concat([summary, action], dim=1)
                ).unsqueeze(1)
                sequence_tokens = (
                    self.phase_embedding(token_categorical[:, :, 0])
                    + self.event_embedding(token_categorical[:, :, 1])
                    + self.scope_embedding(token_categorical[:, :, 2])
                    + self.numeric_projection(token_numeric)
                )
                encoded = self.encoder(
                    self._concat([summary_action_token, sequence_tokens], dim=1)
                )
                pooled = encoded[:, 0, :]
                return {
                    "binary_logits": self.binary_head(pooled).squeeze(-1),
                    "regression": self.regression_head(pooled),
                    "business": self.business_head(pooled),
                    "objective": self.objective_head(pooled),
                    "future_state": self.future_state_head(pooled),
                    "latent_loss": None,
                }

            @staticmethod
            def _concat(values: Sequence[Any], *, dim: int) -> Any:
                return importlib.import_module("torch").cat(values, dim=dim)

        return FullContextTransformerModel()

    def _build_ft_model(self) -> Any:
        nn = self.nn
        input_dim = len(self.preprocessor.summary_feature_names) + _action_vector_width(
            self.preprocessor
        )
        model_dim = 96
        target_dim = len(_EVIDENCE_TARGET_NAMES)
        business_dim = len(_BUSINESS_TARGET_NAMES)
        objective_dim = len(_OBJECTIVE_TARGET_NAMES)

        class FTTransformerModel(nn.Module):
            def __init__(self) -> None:
                super().__init__()
                self.feature_embedding = nn.Embedding(input_dim, model_dim)
                self.value_projection = nn.Linear(1, model_dim)
                encoder_layer = nn.TransformerEncoderLayer(
                    d_model=model_dim,
                    nhead=4,
                    dim_feedforward=192,
                    batch_first=True,
                    dropout=0.1,
                )
                self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=2)
                self.binary_head = nn.Linear(model_dim, 1)
                self.regression_head = nn.Linear(model_dim, target_dim)
                self.business_head = nn.Linear(model_dim, business_dim)
                self.objective_head = nn.Linear(model_dim, objective_dim)

            def forward(
                self,
                summary: Any,
                action: Any,
                token_categorical: Any,
                token_numeric: Any,
                target_binary: Any | None = None,
                target_regression: Any | None = None,
                target_business: Any | None = None,
                target_objective: Any | None = None,
                target_future_state: Any | None = None,
            ) -> dict[str, Any]:
                del (
                    token_categorical,
                    token_numeric,
                    target_binary,
                    target_regression,
                    target_business,
                    target_objective,
                    target_future_state,
                )
                features = self._concat([summary, action], dim=1)
                indices = self._indices(features)
                tokens = self.feature_embedding(indices) + self.value_projection(
                    features.unsqueeze(-1)
                )
                encoded = self.encoder(tokens)
                pooled = encoded.mean(dim=1)
                return {
                    "binary_logits": self.binary_head(pooled).squeeze(-1),
                    "regression": self.regression_head(pooled),
                    "business": self.business_head(pooled),
                    "objective": self.objective_head(pooled),
                    "future_state": self.future_state_head(pooled),
                    "latent_loss": None,
                }

            @staticmethod
            def _concat(values: Sequence[Any], *, dim: int) -> Any:
                return importlib.import_module("torch").cat(values, dim=dim)

            @staticmethod
            def _indices(features: Any) -> Any:
                torch = importlib.import_module("torch")
                batch_size, feature_count = features.shape
                return (
                    torch.arange(feature_count, device=features.device)
                    .unsqueeze(0)
                    .repeat(batch_size, 1)
                )

        return FTTransformerModel()

    def _build_sequence_model(self) -> Any:
        nn = self.nn
        model_dim = 96
        target_dim = len(_EVIDENCE_TARGET_NAMES)
        business_dim = len(_BUSINESS_TARGET_NAMES)
        objective_dim = len(_OBJECTIVE_TARGET_NAMES)
        future_state_dim = len(_FUTURE_STATE_TARGET_NAMES)
        event_type_count = max(len(self.preprocessor.event_type_names), 1)

        class SequenceTransformerModel(nn.Module):
            def __init__(self) -> None:
                super().__init__()
                self.phase_embedding = nn.Embedding(len(_PHASE_VALUES), model_dim)
                self.event_embedding = nn.Embedding(event_type_count, model_dim)
                self.scope_embedding = nn.Embedding(
                    len(_RECIPIENT_SCOPE_VALUES), model_dim
                )
                self.numeric_projection = nn.Linear(_SEQUENCE_NUMERIC_WIDTH, model_dim)
                encoder_layer = nn.TransformerEncoderLayer(
                    d_model=model_dim,
                    nhead=4,
                    dim_feedforward=192,
                    batch_first=True,
                    dropout=0.1,
                )
                self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=2)
                self.binary_head = nn.Linear(model_dim, 1)
                self.regression_head = nn.Linear(model_dim, target_dim)
                self.business_head = nn.Linear(model_dim, business_dim)
                self.objective_head = nn.Linear(model_dim, objective_dim)
                self.future_state_head = nn.Linear(model_dim, future_state_dim)

            def forward(
                self,
                summary: Any,
                action: Any,
                token_categorical: Any,
                token_numeric: Any,
                target_binary: Any | None = None,
                target_regression: Any | None = None,
                target_business: Any | None = None,
                target_objective: Any | None = None,
                target_future_state: Any | None = None,
            ) -> dict[str, Any]:
                del (
                    summary,
                    action,
                    target_binary,
                    target_regression,
                    target_business,
                    target_objective,
                    target_future_state,
                )
                tokens = (
                    self.phase_embedding(token_categorical[:, :, 0])
                    + self.event_embedding(token_categorical[:, :, 1])
                    + self.scope_embedding(token_categorical[:, :, 2])
                    + self.numeric_projection(token_numeric)
                )
                encoded = self.encoder(tokens)
                pooled = encoded.mean(dim=1)
                return {
                    "binary_logits": self.binary_head(pooled).squeeze(-1),
                    "regression": self.regression_head(pooled),
                    "business": self.business_head(pooled),
                    "objective": self.objective_head(pooled),
                    "future_state": self.future_state_head(pooled),
                    "latent_loss": None,
                }

        return SequenceTransformerModel()

    def _build_treatment_model(self) -> Any:
        nn = self.nn
        summary_dim = len(self.preprocessor.summary_feature_names)
        action_dim = _action_vector_width(self.preprocessor)
        target_dim = len(_EVIDENCE_TARGET_NAMES)
        business_dim = len(_BUSINESS_TARGET_NAMES)
        objective_dim = len(_OBJECTIVE_TARGET_NAMES)
        future_state_dim = len(_FUTURE_STATE_TARGET_NAMES)
        model_dim = 96

        class TreatmentTransformerModel(nn.Module):
            def __init__(self) -> None:
                super().__init__()
                self.summary_projection = nn.Linear(summary_dim, model_dim)
                self.action_projection = nn.Linear(1, model_dim)
                self.feature_embedding = nn.Embedding(action_dim, model_dim)
                encoder_layer = nn.TransformerEncoderLayer(
                    d_model=model_dim,
                    nhead=4,
                    dim_feedforward=192,
                    batch_first=True,
                    dropout=0.1,
                )
                self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=2)
                self.binary_head = nn.Linear(model_dim, 1)
                self.regression_head = nn.Linear(model_dim, target_dim)
                self.business_head = nn.Linear(model_dim, business_dim)
                self.objective_head = nn.Linear(model_dim, objective_dim)
                self.future_state_head = nn.Linear(model_dim, future_state_dim)

            def forward(
                self,
                summary: Any,
                action: Any,
                token_categorical: Any,
                token_numeric: Any,
                target_binary: Any | None = None,
                target_regression: Any | None = None,
                target_business: Any | None = None,
                target_objective: Any | None = None,
                target_future_state: Any | None = None,
            ) -> dict[str, Any]:
                del (
                    token_categorical,
                    token_numeric,
                    target_binary,
                    target_regression,
                    target_business,
                    target_objective,
                    target_future_state,
                )
                summary_token = self.summary_projection(summary).unsqueeze(1)
                feature_indices = self._indices(action)
                action_tokens = self.feature_embedding(
                    feature_indices
                ) + self.action_projection(action.unsqueeze(-1))
                encoded = self.encoder(
                    self._concat([summary_token, action_tokens], dim=1)
                )
                pooled = encoded.mean(dim=1)
                return {
                    "binary_logits": self.binary_head(pooled).squeeze(-1),
                    "regression": self.regression_head(pooled),
                    "business": self.business_head(pooled),
                    "objective": self.objective_head(pooled),
                    "future_state": self.future_state_head(pooled),
                    "latent_loss": None,
                }

            @staticmethod
            def _concat(values: Sequence[Any], *, dim: int) -> Any:
                return importlib.import_module("torch").cat(values, dim=dim)

            @staticmethod
            def _indices(values: Any) -> Any:
                torch = importlib.import_module("torch")
                batch_size, feature_count = values.shape
                return (
                    torch.arange(feature_count, device=values.device)
                    .unsqueeze(0)
                    .repeat(batch_size, 1)
                )

        return TreatmentTransformerModel()


def _iter_batches(
    rows: Sequence[_RowEncoding],
    *,
    batch_size: int,
    device: str,
    torch_module: Any,
    shuffle: bool = True,
) -> Iterable[_BatchTensors]:
    if not rows:
        return
    indices = list(range(len(rows)))
    if shuffle:
        random.shuffle(indices)
    for start in range(0, len(indices), batch_size):
        batch_rows = [rows[index] for index in indices[start : start + batch_size]]
        summary = torch_module.tensor(
            np.stack([row.summary_values for row in batch_rows]),
            dtype=torch_module.float32,
            device=device,
        )
        action = torch_module.tensor(
            np.asarray([row.action_values for row in batch_rows], dtype=np.float32),
            dtype=torch_module.float32,
            device=device,
        )
        token_categorical = torch_module.tensor(
            np.stack([row.token_categorical for row in batch_rows]),
            dtype=torch_module.long,
            device=device,
        )
        token_numeric = torch_module.tensor(
            np.stack([row.token_numeric for row in batch_rows]),
            dtype=torch_module.float32,
            device=device,
        )
        if (
            batch_rows[0].binary_target is None
            or batch_rows[0].regression_target is None
            or batch_rows[0].business_target is None
            or batch_rows[0].objective_target is None
            or batch_rows[0].future_state_target is None
        ):
            yield _BatchTensors(
                summary=summary,
                action=action,
                token_categorical=token_categorical,
                token_numeric=token_numeric,
            )
            continue
        yield _BatchTensors(
            summary=summary,
            action=action,
            token_categorical=token_categorical,
            token_numeric=token_numeric,
            target_binary=torch_module.tensor(
                [row.binary_target for row in batch_rows],
                dtype=torch_module.float32,
                device=device,
            ),
            target_regression=torch_module.tensor(
                np.stack([row.regression_target for row in batch_rows]),
                dtype=torch_module.float32,
                device=device,
            ),
            target_business=torch_module.tensor(
                np.stack([row.business_target for row in batch_rows]),
                dtype=torch_module.float32,
                device=device,
            ),
            target_objective=torch_module.tensor(
                np.stack([row.objective_target for row in batch_rows]),
                dtype=torch_module.float32,
                device=device,
            ),
            target_future_state=torch_module.tensor(
                np.stack([row.future_state_target for row in batch_rows]),
                dtype=torch_module.float32,
                device=device,
            ),
        )


def _model_outputs(model: Any, batch: _BatchTensors) -> dict[str, Any]:
    return model(
        batch.summary,
        batch.action,
        batch.token_categorical,
        batch.token_numeric,
        target_binary=batch.target_binary,
        target_regression=batch.target_regression,
        target_business=batch.target_business,
        target_objective=batch.target_objective,
        target_future_state=batch.target_future_state,
    )


def _training_loss(
    *,
    outputs: dict[str, Any],
    batch: _BatchTensors,
    functional: Any,
) -> Any:
    binary_loss = functional.binary_cross_entropy_with_logits(
        outputs["binary_logits"],
        batch.target_binary,
    )
    regression_loss = functional.mse_loss(
        outputs["regression"],
        batch.target_regression,
    )
    business_loss = functional.mse_loss(outputs["business"], batch.target_business)
    objective_loss = functional.mse_loss(outputs["objective"], batch.target_objective)
    future_state_loss = functional.mse_loss(
        outputs["future_state"],
        batch.target_future_state,
    )
    latent_loss = outputs.get("latent_loss")
    supervised_loss = (
        binary_loss
        + (0.5 * regression_loss)
        + business_loss
        + (1.25 * objective_loss)
        + (1.35 * future_state_loss)
    )
    if latent_loss is None:
        return supervised_loss
    return supervised_loss + (0.25 * latent_loss)


def predict_rows(
    *,
    model: Any,
    rows: Sequence[_RowEncoding],
    batch_size: int,
    device: str,
    torch_module: Any,
) -> list[_PredictionBatch]:
    if not rows:
        return []
    batches: list[_PredictionBatch] = []
    model.eval()
    with torch_module.no_grad():
        for batch in _iter_batches(
            rows,
            batch_size=batch_size,
            device=device,
            torch_module=torch_module,
            shuffle=False,
        ):
            outputs = _model_outputs(model, batch)
            probability = (
                torch_module.sigmoid(outputs["binary_logits"]).detach().cpu().numpy()
            )
            regression = outputs["regression"].detach().cpu().numpy()
            business = (
                outputs.get("business").detach().cpu().numpy()
                if outputs.get("business") is not None
                else None
            )
            objective = (
                outputs.get("objective").detach().cpu().numpy()
                if outputs.get("objective") is not None
                else None
            )
            future_state = (
                outputs.get("future_state").detach().cpu().numpy()
                if outputs.get("future_state") is not None
                else None
            )
            batches.append(
                _PredictionBatch(
                    binary_probability=probability,
                    regression_values=regression,
                    business_values=business,
                    objective_values=objective,
                    future_state_values=future_state,
                )
            )
    return batches


def _compute_observed_metrics(
    *,
    rows: Sequence[_RowEncoding],
    predictions: Sequence[_PredictionBatch],
    preprocessor: BenchmarkPreprocessor,
) -> WhatIfObservedForecastMetrics:
    actual_binary: list[float] = []
    predicted_binary: list[float] = []
    actual_regression: dict[str, list[float]] = {
        name: [] for name in _EVIDENCE_TARGET_NAMES
    }
    predicted_regression: dict[str, list[float]] = {
        name: [] for name in _EVIDENCE_TARGET_NAMES
    }
    business_errors: dict[str, list[float]] = {
        name: []
        for name in (
            "enterprise_risk",
            "commercial_position_proxy",
            "org_strain_proxy",
            "stakeholder_trust",
            "execution_drag",
        )
    }
    objective_errors: dict[str, list[float]] = {
        pack.pack_id: [] for pack in list_business_objective_packs()
    }
    future_state_errors: dict[str, list[float]] = {
        name: [] for name in _FUTURE_STATE_TARGET_NAMES
    }

    flat_predictions = _flatten_prediction_batches(predictions)
    for row, predicted in zip(rows, flat_predictions, strict=False):
        actual_targets = row.row.observed_evidence_heads
        actual_binary.append(float(actual_targets.any_external_spread))
        predicted_binary.append(predicted.binary_probability)
        predicted_targets = preprocessor.decode_targets(
            binary_probability=predicted.binary_probability,
            regression_values=predicted.regression_values,
        )
        predicted_objective_scores = preprocessor.decode_objective_scores(
            predicted.objective_values
        )
        predicted_future_state = preprocessor.decode_future_state(
            predicted.future_state_values
        )
        for name in _EVIDENCE_TARGET_NAMES:
            actual_regression[name].append(float(getattr(actual_targets, name)))
            predicted_regression[name].append(float(getattr(predicted_targets, name)))
        actual_business = row.row.observed_business_outcomes
        predicted_business = preprocessor.decode_business(
            predicted.business_values,
            fallback_evidence=predicted_targets,
        )
        for name in business_errors:
            business_errors[name].append(
                abs(
                    float(getattr(actual_business, name))
                    - float(getattr(predicted_business, name))
                )
            )
        for pack in list_business_objective_packs():
            actual_score = score_business_objective(
                pack=pack,
                outcomes=actual_business,
                evidence=actual_targets,
            )
            predicted_score = score_business_objective(
                pack=pack,
                outcomes=predicted_business,
                evidence=predicted_targets,
            )
            predicted_overall = predicted_objective_scores.get(
                pack.pack_id,
                predicted_score.overall_score,
            )
            objective_errors[pack.pack_id].append(
                abs(actual_score.overall_score - predicted_overall)
            )
        actual_future_state = row.row.observed_future_state
        for name in future_state_errors:
            future_state_errors[name].append(
                abs(
                    float(getattr(actual_future_state, name))
                    - float(getattr(predicted_future_state, name))
                )
            )
    return WhatIfObservedForecastMetrics(
        auroc_any_external_spread=_auroc(actual_binary, predicted_binary),
        brier_any_external_spread=round(
            _mean_squared_error(actual_binary, predicted_binary), 6
        ),
        calibration_error_any_external_spread=round(
            _expected_calibration_error(actual_binary, predicted_binary),
            6,
        ),
        evidence_head_mae={
            key: round(_mae(actual_regression[key], predicted_regression[key]), 3)
            for key in _EVIDENCE_TARGET_NAMES
        },
        business_head_mae={
            key: round(sum(values) / max(len(values), 1), 3)
            for key, values in business_errors.items()
        },
        objective_score_mae={
            key: round(sum(values) / max(len(values), 1), 3)
            for key, values in objective_errors.items()
        },
        future_state_head_mae={
            key: round(sum(values) / max(len(values), 1), 3)
            for key, values in future_state_errors.items()
        },
    )


def _evaluate_heldout_cases(
    *,
    model: Any,
    build_cases: Sequence[WhatIfBenchmarkCase],
    base_contract_by_case: dict[str, Any],
    preprocessor: BenchmarkPreprocessor,
    device: str,
    torch_module: Any,
) -> list[WhatIfBenchmarkCaseEvaluation]:
    results: list[WhatIfBenchmarkCaseEvaluation] = []
    for case in build_cases:
        base_contract = base_contract_by_case.get(case.case_id)
        if base_contract is None:
            continue
        base_row = WhatIfBenchmarkDatasetRow(
            row_id=f"{case.case_id}:candidate",
            split="heldout",
            thread_id=case.thread_id,
            branch_event_id=case.event_id,
            contract=base_contract,
        )
        objective_results: list[WhatIfCounterfactualObjectiveEvaluation] = []
        for objective_pack in list_business_objective_packs():
            encoded_candidates = [
                preprocessor.encode_counterfactual(
                    base_row,
                    action_schema=candidate.action_schema,
                )
                for candidate in case.candidates
            ]
            predictions = _flatten_prediction_batches(
                predict_rows(
                    model=model,
                    rows=encoded_candidates,
                    batch_size=_HOLDOUT_BATCH_SIZE,
                    device=device,
                    torch_module=torch_module,
                )
            )
            candidate_predictions: list[WhatIfCounterfactualCandidatePrediction] = []
            for candidate, prediction in zip(
                case.candidates, predictions, strict=False
            ):
                predicted_evidence_heads = preprocessor.decode_targets(
                    binary_probability=float(prediction.binary_probability),
                    regression_values=prediction.regression_values,
                )
                predicted_business_outcomes = preprocessor.decode_business(
                    prediction.business_values,
                    fallback_evidence=predicted_evidence_heads,
                )
                outcome_score = score_business_objective(
                    pack=objective_pack,
                    outcomes=predicted_business_outcomes,
                    evidence=predicted_evidence_heads,
                )
                direct_objective_scores = preprocessor.decode_objective_scores(
                    prediction.objective_values
                )
                if objective_pack.pack_id in direct_objective_scores:
                    outcome_score = outcome_score.model_copy(
                        update={
                            "overall_score": direct_objective_scores[
                                objective_pack.pack_id
                            ]
                        }
                    )
                candidate_predictions.append(
                    WhatIfCounterfactualCandidatePrediction(
                        candidate=candidate,
                        expected_hypothesis=candidate.expected_hypotheses.get(
                            objective_pack.pack_id,
                            "middle_expected",
                        ),
                        predicted_evidence_heads=predicted_evidence_heads,
                        predicted_business_outcomes=predicted_business_outcomes,
                        predicted_objective_score=outcome_score,
                    )
                )
            ordered = sorted(
                candidate_predictions,
                key=lambda item: (
                    -item.predicted_objective_score.overall_score,
                    item.predicted_business_outcomes.enterprise_risk,
                    item.candidate.label.lower(),
                ),
            )
            for index, item in enumerate(ordered, start=1):
                item.rank = index
            objective_results.append(
                WhatIfCounterfactualObjectiveEvaluation(
                    objective_pack=objective_pack,
                    recommended_candidate_label=(
                        ordered[0].candidate.label if ordered else ""
                    ),
                    candidates=ordered,
                    expected_order_ok=_expected_order_ok(ordered),
                )
            )
        results.append(
            WhatIfBenchmarkCaseEvaluation(
                case=case,
                objectives=objective_results,
            )
        )
    return results


def _evaluate_heldout_cases_heuristic(
    *,
    build_cases: Sequence[WhatIfBenchmarkCase],
    base_contract_by_case: dict[str, Any],
    preprocessor: BenchmarkPreprocessor,
) -> list[WhatIfBenchmarkCaseEvaluation]:
    results: list[WhatIfBenchmarkCaseEvaluation] = []
    for case in build_cases:
        base_contract = base_contract_by_case.get(case.case_id)
        if base_contract is None:
            continue
        base_row = WhatIfBenchmarkDatasetRow(
            row_id=f"{case.case_id}:candidate",
            split="heldout",
            thread_id=case.thread_id,
            branch_event_id=case.event_id,
            contract=base_contract,
        )
        objective_results: list[WhatIfCounterfactualObjectiveEvaluation] = []
        for objective_pack in list_business_objective_packs():
            encoded_candidates = [
                preprocessor.encode_counterfactual(
                    base_row,
                    action_schema=candidate.action_schema,
                )
                for candidate in case.candidates
            ]
            predictions = _flatten_prediction_batches(
                _heuristic_predict_rows(
                    rows=encoded_candidates,
                    preprocessor=preprocessor,
                )
            )
            candidate_predictions: list[WhatIfCounterfactualCandidatePrediction] = []
            for candidate, prediction in zip(
                case.candidates, predictions, strict=False
            ):
                predicted_evidence_heads = preprocessor.decode_targets(
                    binary_probability=float(prediction.binary_probability),
                    regression_values=prediction.regression_values,
                )
                predicted_business_outcomes = preprocessor.decode_business(
                    prediction.business_values,
                    fallback_evidence=predicted_evidence_heads,
                )
                outcome_score = score_business_objective(
                    pack=objective_pack,
                    outcomes=predicted_business_outcomes,
                    evidence=predicted_evidence_heads,
                )
                direct_objective_scores = preprocessor.decode_objective_scores(
                    prediction.objective_values
                )
                if objective_pack.pack_id in direct_objective_scores:
                    outcome_score = outcome_score.model_copy(
                        update={
                            "overall_score": direct_objective_scores[
                                objective_pack.pack_id
                            ]
                        }
                    )
                candidate_predictions.append(
                    WhatIfCounterfactualCandidatePrediction(
                        candidate=candidate,
                        expected_hypothesis=candidate.expected_hypotheses.get(
                            objective_pack.pack_id,
                            "middle_expected",
                        ),
                        predicted_evidence_heads=predicted_evidence_heads,
                        predicted_business_outcomes=predicted_business_outcomes,
                        predicted_objective_score=outcome_score,
                    )
                )
            ordered = sorted(
                candidate_predictions,
                key=lambda item: (
                    -item.predicted_objective_score.overall_score,
                    item.predicted_business_outcomes.enterprise_risk,
                    item.candidate.label.lower(),
                ),
            )
            for index, item in enumerate(ordered, start=1):
                item.rank = index
            objective_results.append(
                WhatIfCounterfactualObjectiveEvaluation(
                    objective_pack=objective_pack,
                    recommended_candidate_label=(
                        ordered[0].candidate.label if ordered else ""
                    ),
                    candidates=ordered,
                    expected_order_ok=_expected_order_ok(ordered),
                )
            )
        results.append(
            WhatIfBenchmarkCaseEvaluation(
                case=case,
                objectives=objective_results,
            )
        )
    return results


def _heuristic_predict_rows(
    *,
    rows: Sequence[_RowEncoding],
    preprocessor: BenchmarkPreprocessor,
) -> list[_PredictionBatch]:
    if not rows:
        return []
    predictions = [
        _heuristic_prediction_for_row(row, preprocessor=preprocessor) for row in rows
    ]
    return [
        _PredictionBatch(
            binary_probability=np.asarray(
                [item.binary_probability for item in predictions],
                dtype=np.float32,
            ),
            regression_values=np.vstack(
                [item.regression_values for item in predictions]
            ).astype(np.float32),
            business_values=None,
            objective_values=None,
        )
    ]


def _heuristic_prediction_for_row(
    row: _RowEncoding,
    *,
    preprocessor: BenchmarkPreprocessor,
) -> _RowPrediction:
    action = row.row.contract.action_schema
    probability = 0.12
    if action.recipient_scope in {"external", "mixed"}:
        probability += 0.35
    probability += min(0.25, max(0, action.external_recipient_count) * 0.08)
    if action.outside_sharing_posture == "broad_external":
        probability += 0.25
    elif action.outside_sharing_posture == "limited_external":
        probability += 0.12
    elif action.outside_sharing_posture == "status_only":
        probability += 0.05
    if action.hold_required or action.decision_posture == "hold":
        probability -= 0.2
    if action.legal_review_required:
        probability -= 0.08
    probability = float(min(0.98, max(0.02, probability)))

    review_loop_count = 1 if action.review_path != "none" else 0
    if action.coordination_breadth in {"targeted", "broad"}:
        review_loop_count += 1
    participant_fanout = max(1, action.external_recipient_count + 1)
    if action.coordination_breadth == "broad":
        participant_fanout += 3
    elif action.coordination_breadth == "targeted":
        participant_fanout += 2
    predicted = WhatIfObservedEvidenceHeads(
        any_external_spread=probability >= 0.5,
        outside_recipient_count=max(0, action.external_recipient_count),
        outside_forward_count=(
            1 if action.recipient_scope in {"external", "mixed"} else 0
        ),
        outside_attachment_spread_count=(
            1 if action.attachment_policy == "present" and probability >= 0.5 else 0
        ),
        legal_follow_up_count=(
            1
            if action.legal_review_required
            or action.review_path in {"internal_legal", "outside_counsel"}
            else 0
        ),
        review_loop_count=review_loop_count,
        markup_loop_count=(
            1 if action.attachment_policy in {"present", "sanitized"} else 0
        ),
        executive_escalation_count=1 if action.escalation_level == "executive" else 0,
        executive_mention_count=1 if action.escalation_level == "executive" else 0,
        urgency_spike_count=1 if action.decision_posture == "resolve" else 0,
        participant_fanout=participant_fanout,
        cc_expansion_count=max(0, participant_fanout - 2),
        cross_functional_loop_count=(
            1 if action.coordination_breadth in {"targeted", "broad"} else 0
        ),
        time_to_first_follow_up_ms=3_600_000 if action.hold_required else 900_000,
        time_to_thread_end_ms=7_200_000 if action.hold_required else 1_800_000,
        review_delay_burden_ms=3_600_000 if review_loop_count else 0,
        reassurance_count=1 if action.reassurance_style in {"medium", "high"} else 0,
        apology_repair_count=0,
        commitment_clarity_count=1 if action.owner_clarity == "single_owner" else 0,
        blame_pressure_count=1 if action.decision_posture == "escalate" else 0,
        internal_disagreement_count=1 if action.coordination_breadth == "broad" else 0,
        attachment_recirculation_count=(
            1 if action.attachment_policy == "present" and probability >= 0.5 else 0
        ),
        version_turn_count=(
            1 if action.attachment_policy in {"present", "sanitized"} else 0
        ),
    )
    return _RowPrediction(
        binary_probability=probability,
        regression_values=preprocessor._encode_targets(predicted),
    )


def _write_prediction_rows(
    *,
    path: Path,
    factual_rows: Sequence[_RowEncoding],
    factual_predictions: Sequence[_PredictionBatch],
    case_evaluations: Sequence[WhatIfBenchmarkCaseEvaluation],
) -> None:
    lines: list[str] = []
    flat_predictions = _flatten_prediction_batches(factual_predictions)
    for row, prediction in zip(factual_rows, flat_predictions, strict=False):
        lines.append(
            json.dumps(
                {
                    "kind": "factual",
                    "row_id": row.row.row_id,
                    "thread_id": row.row.thread_id,
                    "branch_event_id": row.row.branch_event_id,
                    "binary_probability": round(
                        float(prediction.binary_probability), 6
                    ),
                    "regression_values": [
                        round(float(value), 6)
                        for value in prediction.regression_values.tolist()
                    ],
                }
            )
        )
    for case in case_evaluations:
        for objective in case.objectives:
            for candidate in objective.candidates:
                lines.append(
                    json.dumps(
                        {
                            "kind": "counterfactual",
                            "case_id": case.case.case_id,
                            "objective_pack_id": objective.objective_pack.pack_id,
                            "candidate_id": candidate.candidate.candidate_id,
                            "rank": candidate.rank,
                            "overall_score": candidate.predicted_objective_score.overall_score,
                            "predicted_evidence_heads": candidate.predicted_evidence_heads.model_dump(
                                mode="json"
                            ),
                            "predicted_business_outcomes": candidate.predicted_business_outcomes.model_dump(
                                mode="json"
                            ),
                        }
                    )
                )
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _flatten_prediction_batches(
    batches: Sequence[_PredictionBatch],
) -> list[_RowPrediction]:
    flattened: list[_RowPrediction] = []
    for batch in batches:
        for index in range(len(batch.binary_probability)):
            flattened.append(
                _RowPrediction(
                    binary_probability=float(batch.binary_probability[index]),
                    regression_values=np.asarray(batch.regression_values[index]),
                    business_values=(
                        np.asarray(batch.business_values[index])
                        if batch.business_values is not None
                        else None
                    ),
                    objective_values=(
                        np.asarray(batch.objective_values[index])
                        if batch.objective_values is not None
                        else None
                    ),
                    future_state_values=(
                        np.asarray(batch.future_state_values[index])
                        if batch.future_state_values is not None
                        else None
                    ),
                )
            )
    return flattened


def _expected_order_ok(
    candidates: Sequence[WhatIfCounterfactualCandidatePrediction],
) -> bool:
    best = None
    worst = None
    for candidate in candidates:
        if candidate.expected_hypothesis == "best_expected":
            best = candidate
        if candidate.expected_hypothesis == "worst_expected":
            worst = candidate
    if best is None or worst is None:
        return False
    return (
        best.predicted_objective_score.overall_score
        > worst.predicted_objective_score.overall_score
    )


def _summary_vector(
    row: WhatIfBenchmarkDatasetRow,
    feature_names: Sequence[str],
) -> np.ndarray:
    feature_map = {
        feature.name: float(feature.value) for feature in row.contract.summary_features
    }
    return np.asarray(
        [feature_map.get(name, 0.0) for name in feature_names], dtype=np.float32
    )


def _one_hot(value: str, allowed: Sequence[str]) -> list[float]:
    return [1.0 if value == item else 0.0 for item in allowed]


def _safe_index(value: str, allowed: Sequence[str]) -> int:
    try:
        return list(allowed).index(value)
    except ValueError:
        return 0


def _summary_token(summary_values: np.ndarray) -> np.ndarray:
    if len(summary_values) == 0:
        return np.zeros(_SEQUENCE_NUMERIC_WIDTH, dtype=np.float32)
    base = np.asarray(
        [
            float(summary_values.mean()),
            float(summary_values.std()),
            float(summary_values.min()),
            float(summary_values.max()),
            float(np.percentile(summary_values, 25)),
            float(np.percentile(summary_values, 75)),
        ],
        dtype=np.float32,
    )
    if len(base) >= _SEQUENCE_NUMERIC_WIDTH:
        return base[:_SEQUENCE_NUMERIC_WIDTH]
    padded = np.zeros(_SEQUENCE_NUMERIC_WIDTH, dtype=np.float32)
    padded[: len(base)] = base
    return padded


def _text_feature_count(text: str) -> int:
    lowered = text.lower()
    return sum(
        1
        for token in ("legal", "review", "draft", "urgent", "confirm", "update")
        if token in lowered
    )


def _action_vector_width(preprocessor: BenchmarkPreprocessor) -> int:
    fixed = (
        len(_RECIPIENT_SCOPE_VALUES)
        + len(_ATTACHMENT_POLICY_VALUES)
        + len(_ESCALATION_LEVEL_VALUES)
        + len(_OWNER_CLARITY_VALUES)
        + len(_REASSURANCE_STYLE_VALUES)
        + len(_REVIEW_PATH_VALUES)
        + len(_COORDINATION_BREADTH_VALUES)
        + len(_OUTSIDE_SHARING_POSTURE_VALUES)
        + len(_DECISION_POSTURE_VALUES)
        + 4
    )
    return fixed + len(preprocessor.action_tag_names)


def _resolve_device(requested: str) -> str:
    if requested:
        return requested
    torch = importlib.import_module("torch")
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return "mps"
    if torch.cuda.is_available():
        return "cuda"
    return "cpu"


def _seed_everything(torch_module: Any, seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch_module.manual_seed(seed)
    if torch_module.cuda.is_available():
        torch_module.cuda.manual_seed_all(seed)


def _mae(actual: Sequence[float], predicted: Sequence[float]) -> float:
    if not actual:
        return 0.0
    return sum(
        abs(left - right) for left, right in zip(actual, predicted, strict=False)
    ) / len(actual)


def _mean_squared_error(actual: Sequence[float], predicted: Sequence[float]) -> float:
    if not actual:
        return 0.0
    return sum(
        (left - right) ** 2 for left, right in zip(actual, predicted, strict=False)
    ) / len(actual)


def _auroc(actual: Sequence[float], predicted: Sequence[float]) -> float | None:
    positive = sum(1 for value in actual if value >= 0.5)
    negative = len(actual) - positive
    if positive == 0 or negative == 0:
        return None
    ranked = sorted(zip(predicted, actual, strict=False), key=lambda item: item[0])
    rank_sum = 0.0
    for index, (_, label) in enumerate(ranked, start=1):
        if label >= 0.5:
            rank_sum += index
    return round(
        (rank_sum - (positive * (positive + 1) / 2)) / (positive * negative), 6
    )


def _expected_calibration_error(
    actual: Sequence[float],
    predicted: Sequence[float],
    *,
    bins: int = 10,
) -> float:
    if not actual:
        return 0.0
    total = 0.0
    for index in range(bins):
        lower = index / bins
        upper = (index + 1) / bins
        members = [
            (truth, score)
            for truth, score in zip(actual, predicted, strict=False)
            if lower <= score < upper or (index == bins - 1 and score == upper)
        ]
        if not members:
            continue
        avg_truth = sum(item[0] for item in members) / len(members)
        avg_score = sum(item[1] for item in members) / len(members)
        total += (len(members) / len(actual)) * abs(avg_truth - avg_score)
    return total


if __name__ == "__main__":
    raise SystemExit(main())
