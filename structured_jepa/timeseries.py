from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .storage import PreparedDataset, finalize_processed_dataset
from .utils import make_split_map, parse_timestamp_series, read_table

logger = logging.getLogger(__name__)


def prepare_timeseries_dataset(
    *,
    input_path: str | Path,
    output_dir: str | Path,
    entity_column: str,
    timestamp_column: str,
    observation_categorical_columns: list[str] | None = None,
    action_numeric_columns: list[str] | None = None,
    action_categorical_columns: list[str] | None = None,
    auxiliary_numeric_target_columns: list[str] | None = None,
    seed: int = 7,
    train_fraction: float = 0.7,
    val_fraction: float = 0.15,
) -> PreparedDataset:
    logger.info(
        "timeseries_preparation_start",
        extra={"input_path": str(input_path), "entity_column": entity_column},
    )
    frame = read_table(input_path)
    if entity_column not in frame.columns:
        raise ValueError(f"entity column not found: {entity_column}")
    if timestamp_column not in frame.columns:
        raise ValueError(f"timestamp column not found: {timestamp_column}")

    observation_categorical = observation_categorical_columns or []
    action_numeric = action_numeric_columns or []
    action_categorical = action_categorical_columns or []
    auxiliary_numeric_targets = auxiliary_numeric_target_columns or []
    reserved = {
        entity_column,
        timestamp_column,
        *observation_categorical,
        *action_numeric,
        *action_categorical,
        *auxiliary_numeric_targets,
    }

    observation_numeric = [
        column
        for column in frame.columns
        if column not in reserved and pd.api.types.is_numeric_dtype(frame[column])
    ]

    steps = frame.copy()
    steps["episode_id"] = steps[entity_column].astype(str)
    parsed_timestamp = parse_timestamp_series(steps[timestamp_column])
    steps["timestamp"] = parsed_timestamp.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    steps = steps.sort_values(["episode_id", "timestamp"]).reset_index(drop=True)
    steps["step_idx"] = steps.groupby("episode_id").cumcount()
    steps["delta_t_s"] = steps.groupby("episode_id")["timestamp"].transform(_iso_delta_seconds)
    steps["done"] = steps.groupby("episode_id")["step_idx"].transform("max") == steps["step_idx"]
    split_map = make_split_map(
        steps["episode_id"].tolist(),
        seed=seed,
        train_fraction=train_fraction,
        val_fraction=val_fraction,
    )
    steps["split"] = steps["episode_id"].map(split_map)
    steps["action_name"] = steps.apply(
        lambda row: _action_name_for_row(row, action_numeric, action_categorical),
        axis=1,
    )

    metadata_columns = [
        column
        for column in [entity_column, timestamp_column]
        if column not in {"episode_id", "timestamp"}
    ]

    logger.info(
        "timeseries_preparation_complete",
        extra={
            "rows": len(steps),
            "episodes": steps["episode_id"].nunique(),
            "observation_numeric_count": len(observation_numeric),
        },
    )
    return finalize_processed_dataset(
        raw_steps=steps,
        output_dir=output_dir,
        dataset_kind="timeseries",
        observation_numeric_columns=observation_numeric + ["delta_t_s"],
        observation_categorical_columns=observation_categorical,
        action_numeric_columns=action_numeric,
        action_categorical_columns=action_categorical,
        auxiliary_numeric_targets=auxiliary_numeric_targets,
        metadata_columns=metadata_columns,
        notes={
            "entity_column": entity_column,
            "timestamp_column": timestamp_column,
        },
    )


def _action_name_for_row(
    row: pd.Series,
    action_numeric_columns: list[str],
    action_categorical_columns: list[str],
) -> str:
    if action_categorical_columns:
        parts = [
            f"{column}={row[column]}"
            for column in action_categorical_columns
            if str(row[column]).strip()
        ]
        if parts:
            return "|".join(parts)
    if action_numeric_columns:
        return "numeric_control"
    return "__none__"


def _iso_delta_seconds(values: pd.Series) -> pd.Series:
    timestamps = pd.to_datetime(values, utc=True, errors="coerce")
    deltas = timestamps.diff().dt.total_seconds().fillna(0.0)
    return deltas.astype(float)
