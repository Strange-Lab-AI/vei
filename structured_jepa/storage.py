from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset

from .schema import (
    CategoricalFeatureSpec,
    DatasetSchema,
    NumericFeatureSpec,
    StepBatch,
    dataset_paths,
)
from .utils import ensure_directory, json_dump

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "episode_id",
    "step_idx",
    "timestamp",
    "delta_t_s",
    "done",
    "split",
    "action_name",
]


@dataclass
class PreparedDataset:
    frame: pd.DataFrame
    schema: DatasetSchema
    root: Path


def finalize_processed_dataset(
    *,
    raw_steps: pd.DataFrame,
    output_dir: str | Path,
    dataset_kind: str,
    observation_numeric_columns: list[str],
    observation_categorical_columns: list[str],
    action_numeric_columns: list[str],
    action_categorical_columns: list[str],
    auxiliary_numeric_targets: list[str] | None = None,
    metadata_columns: list[str] | None = None,
    notes: dict[str, object] | None = None,
    context_length: int = 16,
) -> PreparedDataset:
    steps = raw_steps.copy()
    auxiliary_targets = auxiliary_numeric_targets or []
    metadata = metadata_columns or []
    _validate_required_columns(steps.columns)

    processed = steps[REQUIRED_COLUMNS + metadata].copy()

    observation_numeric_frame, observation_numeric_specs = _normalize_numeric_group(
        steps=steps,
        columns=observation_numeric_columns,
        prefix="obs_num__",
        group="observation",
    )
    action_numeric_frame, action_numeric_specs = _normalize_numeric_group(
        steps=steps,
        columns=action_numeric_columns,
        prefix="act_num__",
        group="action",
    )
    observation_categorical_frame, observation_categorical_specs = _encode_categorical_group(
        steps=steps,
        columns=observation_categorical_columns,
        prefix="obs_cat__",
        group="observation",
    )
    action_categorical_frame, action_categorical_specs = _encode_categorical_group(
        steps=steps,
        columns=action_categorical_columns,
        prefix="act_cat__",
        group="action",
    )

    auxiliary_frame = pd.DataFrame(index=processed.index)
    for column in auxiliary_targets:
        auxiliary_frame[f"aux_num__{column}"] = pd.to_numeric(
            steps[column], errors="coerce"
        ).fillna(0.0)

    feature_frames = [
        processed,
        observation_numeric_frame,
        action_numeric_frame,
        observation_categorical_frame,
        action_categorical_frame,
        auxiliary_frame,
    ]
    processed = pd.concat(
        [frame for frame in feature_frames if not frame.empty],
        axis=1,
    )

    dataset_root = ensure_directory(output_dir)
    schema = DatasetSchema(
        dataset_kind=dataset_kind,  # type: ignore[arg-type]
        context_length=context_length,
        row_count=len(processed),
        episode_count=processed["episode_id"].nunique(),
        metadata_columns=metadata,
        observation_numeric=observation_numeric_specs,
        observation_categorical=observation_categorical_specs,
        action_numeric=action_numeric_specs,
        action_categorical=action_categorical_specs,
        auxiliary_numeric_targets=[f"aux_num__{column}" for column in auxiliary_targets],
        notes=dict(notes or {}),
    )

    steps_path, schema_path = dataset_paths(dataset_root)
    processed.to_parquet(steps_path, index=False)
    json_dump(schema_path, schema.model_dump(mode="json"))

    logger.info(
        "dataset_written",
        extra={
            "kind": dataset_kind,
            "rows": len(processed),
            "episodes": processed["episode_id"].nunique(),
            "path": str(dataset_root),
        },
    )
    return PreparedDataset(frame=processed, schema=schema, root=dataset_root)


def load_processed_dataset(root: str | Path) -> PreparedDataset:
    steps_path, schema_path = dataset_paths(root)
    frame = pd.read_parquet(steps_path)
    schema = DatasetSchema.model_validate_json(schema_path.read_text(encoding="utf-8"))
    logger.info(
        "dataset_loaded",
        extra={"rows": len(frame), "path": str(steps_path.parent)},
    )
    return PreparedDataset(frame=frame, schema=schema, root=steps_path.parent)


class WindowDataset(Dataset[StepBatch]):
    def __init__(self, prepared: PreparedDataset, split: str) -> None:
        self.prepared = prepared
        self.frame = (
            prepared.frame.loc[prepared.frame["split"] == split]
            .sort_values(["episode_id", "step_idx"])
            .reset_index(drop=True)
        )
        self.schema = prepared.schema
        self.window_size = self.schema.window_size
        self._windows = self._build_windows()
        self._observation_cat_maps = [
            spec.to_id_map() for spec in self.schema.observation_categorical
        ]
        self._action_cat_maps = [spec.to_id_map() for spec in self.schema.action_categorical]

    def __len__(self) -> int:
        return len(self._windows)

    def __getitem__(self, index: int) -> StepBatch:
        window = self._windows[index]
        rows = self.frame.iloc[window]
        return rows_to_step_batch(rows, self.schema)

    def _build_windows(self) -> list[list[int]]:
        windows: list[list[int]] = []
        grouped = self.frame.groupby("episode_id", sort=False)
        for _, group in grouped:
            indices = group.index.tolist()
            if len(indices) < self.window_size:
                continue
            for start in range(0, len(indices) - self.window_size + 1):
                windows.append(indices[start : start + self.window_size])
        return windows


def collate_step_batches(items: Iterable[StepBatch]) -> StepBatch:
    batches = list(items)
    return StepBatch(
        observation_numeric=torch.stack([item.observation_numeric for item in batches]),
        observation_masks=torch.stack([item.observation_masks for item in batches]),
        observation_categorical=torch.stack([item.observation_categorical for item in batches]),
        action_numeric=torch.stack([item.action_numeric for item in batches]),
        action_masks=torch.stack([item.action_masks for item in batches]),
        action_categorical=torch.stack([item.action_categorical for item in batches]),
        auxiliary_numeric_targets=torch.stack([item.auxiliary_numeric_targets for item in batches]),
        valid_mask=torch.stack([item.valid_mask for item in batches]),
    )


def rows_to_step_batch(rows: pd.DataFrame, schema: DatasetSchema) -> StepBatch:
    observation_cat_maps = [spec.to_id_map() for spec in schema.observation_categorical]
    action_cat_maps = [spec.to_id_map() for spec in schema.action_categorical]
    return StepBatch(
        observation_numeric=_frame_to_tensor(rows, schema.observation_numeric_columns),
        observation_masks=_frame_to_tensor(rows, schema.observation_mask_columns),
        observation_categorical=_frame_to_categorical_tensor(
            rows,
            schema.observation_categorical_columns,
            observation_cat_maps,
        ),
        action_numeric=_frame_to_tensor(rows, schema.action_numeric_columns),
        action_masks=_frame_to_tensor(rows, schema.action_mask_columns),
        action_categorical=_frame_to_categorical_tensor(
            rows,
            schema.action_categorical_columns,
            action_cat_maps,
        ),
        auxiliary_numeric_targets=_frame_to_tensor(rows, schema.auxiliary_numeric_targets),
        valid_mask=torch.ones(len(rows), dtype=torch.float32),
    )


def _validate_required_columns(columns: Iterable[str]) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")


def _normalize_numeric_group(
    *,
    steps: pd.DataFrame,
    columns: list[str],
    prefix: str,
    group: str,
) -> tuple[pd.DataFrame, list[NumericFeatureSpec]]:
    specs: list[NumericFeatureSpec] = []
    feature_columns: dict[str, pd.Series] = {}
    if not columns:
        return pd.DataFrame(index=steps.index), specs

    train_rows = steps.loc[steps["split"] == "train"]
    for column in columns:
        series = pd.to_numeric(steps[column], errors="coerce")
        train_series = pd.to_numeric(train_rows[column], errors="coerce")
        mean = float(train_series.mean()) if train_series.notna().any() else 0.0
        std = float(train_series.std()) if train_series.notna().any() else 1.0
        if std == 0.0 or pd.isna(std):
            std = 1.0

        feature_name = f"{prefix}{column}"
        mask_name = f"mask__{feature_name}"
        feature_columns[mask_name] = series.isna().astype(float)
        normalized = ((series - mean) / std).fillna(0.0)
        feature_columns[feature_name] = normalized.astype(float)

        specs.append(
            NumericFeatureSpec(
                name=feature_name,
                source_column=column,
                group=group,  # type: ignore[arg-type]
                mean=mean,
                std=std,
                mask_column=mask_name,
            )
        )
    features = pd.DataFrame(feature_columns, index=steps.index)
    return features, specs


def _encode_categorical_group(
    *,
    steps: pd.DataFrame,
    columns: list[str],
    prefix: str,
    group: str,
) -> tuple[pd.DataFrame, list[CategoricalFeatureSpec]]:
    specs: list[CategoricalFeatureSpec] = []
    feature_columns: dict[str, pd.Series] = {}
    if not columns:
        return pd.DataFrame(index=steps.index), specs

    train_rows = steps.loc[steps["split"] == "train"]
    for column in columns:
        feature_name = f"{prefix}{column}"
        train_values = (
            train_rows[column]
            .fillna("__missing__")
            .astype(str)
            .str.strip()
            .replace("", "__missing__")
        )
        vocab = ["__missing__", "__unknown__"] + sorted(
            value for value in train_values.unique().tolist() if value != "__missing__"
        )
        values = (
            steps[column].fillna("__missing__").astype(str).str.strip().replace("", "__missing__")
        )
        feature_columns[feature_name] = values
        specs.append(
            CategoricalFeatureSpec(
                name=feature_name,
                source_column=column,
                group=group,  # type: ignore[arg-type]
                vocab=vocab,
            )
        )
    features = pd.DataFrame(feature_columns, index=steps.index)
    return features, specs


def _frame_to_tensor(rows: pd.DataFrame, columns: list[str]) -> torch.Tensor:
    if not columns:
        return torch.zeros((len(rows), 0), dtype=torch.float32)
    values = rows[columns].to_numpy(dtype="float32", copy=True)
    return torch.from_numpy(values)


def _frame_to_categorical_tensor(
    rows: pd.DataFrame,
    columns: list[str],
    id_maps: list[dict[str, int]],
) -> torch.Tensor:
    if not columns:
        return torch.zeros((len(rows), 0), dtype=torch.long)

    encoded = []
    for column, id_map in zip(columns, id_maps, strict=True):
        unknown_id = id_map["__unknown__"]
        column_values = rows[column].astype(str).tolist()
        encoded_values = [id_map.get(value, unknown_id) for value in column_values]
        encoded.append(np.asarray(encoded_values, dtype="int64"))
    matrix = torch.from_numpy(pd.DataFrame(encoded).T.to_numpy(dtype="int64", copy=True))
    return matrix
