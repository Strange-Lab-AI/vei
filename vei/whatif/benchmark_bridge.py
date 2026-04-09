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
_PHASE_VALUES = ("history", "branch", "generated", "historical_future")
_SEQUENCE_TOKEN_LIMIT = 12
_SEQUENCE_NUMERIC_WIDTH = 12


@dataclass(frozen=True)
class _TrainConfig:
    epochs: int
    batch_size: int
    learning_rate: float
    device: str


@dataclass(frozen=True)
class _RowEncoding:
    summary_values: np.ndarray
    action_values: np.ndarray
    token_categorical: np.ndarray
    token_numeric: np.ndarray
    binary_target: float | None
    regression_target: np.ndarray | None
    row: WhatIfBenchmarkDatasetRow


@dataclass(frozen=True)
class _BatchTensors:
    summary: Any
    action: Any
    token_categorical: Any
    token_numeric: Any
    target_binary: Any | None = None
    target_regression: Any | None = None


@dataclass(frozen=True)
class _PredictionBatch:
    binary_probability: np.ndarray
    regression_values: np.ndarray


@dataclass(frozen=True)
class _RowPrediction:
    binary_probability: float
    regression_values: np.ndarray


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    train_parser = subparsers.add_parser("train")
    train_parser.add_argument("--request", required=True)
    train_parser.add_argument("--output", required=True)

    eval_parser = subparsers.add_parser("eval")
    eval_parser.add_argument("--request", required=True)
    eval_parser.add_argument("--output", required=True)

    args = parser.parse_args()
    if args.command == "train":
        result = _train_from_request(Path(args.request))
    else:
        result = _eval_from_request(Path(args.request))
    Path(args.output).write_text(result.model_dump_json(indent=2), encoding="utf-8")
    return 0


def _train_from_request(path: Path) -> WhatIfBenchmarkTrainResult:
    request = json.loads(path.read_text(encoding="utf-8"))
    output_root = Path(request["output_root"]).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    build = load_branch_point_benchmark_build_result(request["build_root"])
    dataset = _load_dataset_rows(build.dataset.split_paths)
    preprocessor = _fit_preprocessor(
        train_rows=dataset["train"],
        validation_rows=dataset["validation"],
        test_rows=dataset["test"],
        heldout_rows=dataset["heldout"],
        cases=build.cases,
    )
    train_rows = [preprocessor.encode_row(row) for row in dataset["train"]]
    validation_rows = [preprocessor.encode_row(row) for row in dataset["validation"]]

    config = _TrainConfig(
        epochs=int(request.get("epochs", 12)),
        batch_size=int(request.get("batch_size", 64)),
        learning_rate=float(request.get("learning_rate", 1e-3)),
        device=_resolve_device(str(request.get("device", "") or "")),
    )
    trainer = _TorchTrainer(model_id=request["model_id"], preprocessor=preprocessor)
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
            f"summary_features={len(preprocessor.summary_feature_names)}",
            f"action_tags={len(preprocessor.action_tag_names)}",
            f"event_types={len(preprocessor.event_type_names)}",
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
    checkpoint = _load_checkpoint(output_root / "model.pt")
    preprocessor = _BenchmarkPreprocessor.from_metadata(checkpoint["metadata"])
    trainer = _TorchTrainer(model_id=request["model_id"], preprocessor=preprocessor)
    device = _resolve_device(str(request.get("device", "") or ""))
    model = trainer.build_model(device=device)
    model.load_state_dict(checkpoint["state_dict"])
    model.eval()

    test_rows = [preprocessor.encode_row(row) for row in dataset["test"]]
    factual_predictions = _predict_rows(
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


def _load_checkpoint(path: Path) -> dict[str, Any]:
    torch = importlib.import_module("torch")
    checkpoint = torch.load(path, map_location="cpu")
    if not isinstance(checkpoint, dict):
        raise RuntimeError(f"invalid checkpoint payload in {path}")
    return checkpoint


class _BenchmarkPreprocessor:
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

    @classmethod
    def from_metadata(cls, payload: dict[str, Any]) -> "_BenchmarkPreprocessor":
        return cls(
            summary_feature_names=payload["summary_feature_names"],
            summary_mean=payload["summary_mean"],
            summary_std=payload["summary_std"],
            action_tag_names=payload["action_tag_names"],
            event_type_names=payload["event_type_names"],
            target_mean=payload["target_mean"],
            target_std=payload["target_std"],
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
                row=row,
            )
        return _RowEncoding(
            summary_values=summary_values,
            action_values=action_values,
            token_categorical=token_categorical,
            token_numeric=token_numeric,
            binary_target=float(row.observed_evidence_heads.any_external_spread),
            regression_target=self._encode_targets(row.observed_evidence_heads),
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
        restored = np.expm1((regression * self.target_std) + self.target_mean)
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


def _fit_preprocessor(
    *,
    train_rows: Sequence[WhatIfBenchmarkDatasetRow],
    validation_rows: Sequence[WhatIfBenchmarkDatasetRow],
    test_rows: Sequence[WhatIfBenchmarkDatasetRow],
    heldout_rows: Sequence[WhatIfBenchmarkDatasetRow],
    cases: Sequence[WhatIfBenchmarkCase],
) -> _BenchmarkPreprocessor:
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
    return _BenchmarkPreprocessor(
        summary_feature_names=summary_names,
        summary_mean=summary_mean.tolist(),
        summary_std=summary_std.tolist(),
        action_tag_names=action_tags,
        event_type_names=sorted(event_types),
        target_mean=target_mean.tolist(),
        target_std=target_std.tolist(),
    )


class _TorchTrainer:
    def __init__(
        self,
        *,
        model_id: WhatIfBenchmarkModelId,
        preprocessor: _BenchmarkPreprocessor,
    ) -> None:
        self.model_id = model_id
        self.preprocessor = preprocessor
        self.torch = importlib.import_module("torch")
        self.nn = importlib.import_module("torch.nn")
        self.F = importlib.import_module("torch.nn.functional")

    def build_model(self, *, device: str) -> Any:
        if self.model_id == "jepa_latent":
            model = self._build_jepa_model()
        elif self.model_id == "ft_transformer":
            model = self._build_ft_model()
        elif self.model_id == "sequence_transformer":
            model = self._build_sequence_model()
        else:
            model = self._build_treatment_model()
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
        _seed_everything(torch)
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
                outputs = model(
                    batch.summary,
                    batch.action,
                    batch.token_categorical,
                    batch.token_numeric,
                )
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
                outputs = model(
                    batch.summary,
                    batch.action,
                    batch.token_categorical,
                    batch.token_numeric,
                )
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
        latent_dim = 96

        class JEPAOutcomeModel(nn.Module):
            def __init__(self) -> None:
                super().__init__()
                self.context_encoder = nn.Sequential(
                    nn.Linear(summary_dim + action_dim, 192),
                    nn.ReLU(),
                    nn.Linear(192, latent_dim),
                )
                self.target_encoder = nn.Sequential(
                    nn.Linear(target_dim + 1, 192),
                    nn.ReLU(),
                    nn.Linear(192, latent_dim),
                )
                self.predictor = nn.Sequential(
                    nn.Linear(latent_dim, latent_dim),
                    nn.ReLU(),
                    nn.Linear(latent_dim, latent_dim),
                )
                self.binary_head = nn.Linear(latent_dim, 1)
                self.regression_head = nn.Linear(latent_dim, target_dim)

            def forward(
                self,
                summary: Any,
                action: Any,
                token_categorical: Any,
                token_numeric: Any,
                target_binary: Any | None = None,
                target_regression: Any | None = None,
            ) -> dict[str, Any]:
                del token_categorical, token_numeric
                context = self.context_encoder(self._concat([summary, action], dim=1))
                predicted_latent = self.predictor(context)
                result = {
                    "binary_logits": self.binary_head(predicted_latent).squeeze(-1),
                    "regression": self.regression_head(predicted_latent),
                }
                if target_binary is None or target_regression is None:
                    result["latent_loss"] = None
                    return result
                target = self.target_encoder(
                    self._concat(
                        [
                            target_binary.unsqueeze(-1),
                            target_regression,
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

    def _build_ft_model(self) -> Any:
        nn = self.nn
        input_dim = len(self.preprocessor.summary_feature_names) + _action_vector_width(
            self.preprocessor
        )
        model_dim = 96
        target_dim = len(_EVIDENCE_TARGET_NAMES)

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

            def forward(
                self,
                summary: Any,
                action: Any,
                token_categorical: Any,
                token_numeric: Any,
            ) -> dict[str, Any]:
                del token_categorical, token_numeric
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

            def forward(
                self,
                summary: Any,
                action: Any,
                token_categorical: Any,
                token_numeric: Any,
            ) -> dict[str, Any]:
                del summary, action
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
                    "latent_loss": None,
                }

        return SequenceTransformerModel()

    def _build_treatment_model(self) -> Any:
        nn = self.nn
        summary_dim = len(self.preprocessor.summary_feature_names)
        action_dim = _action_vector_width(self.preprocessor)
        target_dim = len(_EVIDENCE_TARGET_NAMES)
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

            def forward(
                self,
                summary: Any,
                action: Any,
                token_categorical: Any,
                token_numeric: Any,
            ) -> dict[str, Any]:
                del token_categorical, token_numeric
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
    latent_loss = outputs.get("latent_loss")
    if latent_loss is None:
        return binary_loss + regression_loss
    return binary_loss + regression_loss + (0.25 * latent_loss)


def _predict_rows(
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
            outputs = model(
                batch.summary,
                batch.action,
                batch.token_categorical,
                batch.token_numeric,
            )
            probability = (
                torch_module.sigmoid(outputs["binary_logits"]).detach().cpu().numpy()
            )
            regression = outputs["regression"].detach().cpu().numpy()
            batches.append(
                _PredictionBatch(
                    binary_probability=probability,
                    regression_values=regression,
                )
            )
    return batches


def _compute_observed_metrics(
    *,
    rows: Sequence[_RowEncoding],
    predictions: Sequence[_PredictionBatch],
    preprocessor: _BenchmarkPreprocessor,
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

    flat_predictions = _flatten_prediction_batches(predictions)
    for row, predicted in zip(rows, flat_predictions, strict=False):
        actual_targets = row.row.observed_evidence_heads
        actual_binary.append(float(actual_targets.any_external_spread))
        predicted_binary.append(predicted.binary_probability)
        predicted_targets = preprocessor.decode_targets(
            binary_probability=predicted.binary_probability,
            regression_values=predicted.regression_values,
        )
        for name in _EVIDENCE_TARGET_NAMES:
            actual_regression[name].append(float(getattr(actual_targets, name)))
            predicted_regression[name].append(float(getattr(predicted_targets, name)))
        actual_business = row.row.observed_business_outcomes
        predicted_business = evidence_to_business_outcomes(predicted_targets)
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
            objective_errors[pack.pack_id].append(
                abs(actual_score.overall_score - predicted_score.overall_score)
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
    )


def _evaluate_heldout_cases(
    *,
    model: Any,
    build_cases: Sequence[WhatIfBenchmarkCase],
    base_contract_by_case: dict[str, Any],
    preprocessor: _BenchmarkPreprocessor,
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
                _predict_rows(
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
                predicted_business_outcomes = evidence_to_business_outcomes(
                    predicted_evidence_heads
                )
                outcome_score = score_business_objective(
                    pack=objective_pack,
                    outcomes=predicted_business_outcomes,
                    evidence=predicted_evidence_heads,
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


def _action_vector_width(preprocessor: _BenchmarkPreprocessor) -> int:
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


def _seed_everything(torch_module: Any) -> None:
    random.seed(_RANDOM_SEED)
    np.random.seed(_RANDOM_SEED)
    torch_module.manual_seed(_RANDOM_SEED)
    if torch_module.cuda.is_available():
        torch_module.cuda.manual_seed_all(_RANDOM_SEED)


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
