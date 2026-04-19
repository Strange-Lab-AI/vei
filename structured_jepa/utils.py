from __future__ import annotations

import json
import math
import random
from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pandas as pd
import torch


def ensure_directory(path: str | Path) -> Path:
    resolved = Path(path).expanduser().resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def read_table(path: str | Path) -> pd.DataFrame:
    resolved = Path(path).expanduser().resolve()
    if resolved.suffix == ".csv":
        return pd.read_csv(resolved)
    if resolved.suffix in {".parquet", ".pq"}:
        return pd.read_parquet(resolved)
    raise ValueError(f"unsupported input format: {resolved.suffix}")


def parse_timestamp_series(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, utc=True, errors="coerce")
    if parsed.isna().any():
        raise ValueError("timestamps must parse cleanly into datetimes")
    return parsed


def make_split_map(
    episode_ids: Iterable[str],
    seed: int,
    *,
    train_fraction: float = 0.7,
    val_fraction: float = 0.15,
) -> dict[str, str]:
    if train_fraction <= 0 or val_fraction <= 0:
        raise ValueError("train_fraction and val_fraction must be positive")
    if train_fraction + val_fraction >= 1.0:
        raise ValueError("train_fraction + val_fraction must be less than 1.0")

    unique_ids = sorted({str(value) for value in episode_ids})
    if len(unique_ids) <= 1:
        return {episode_id: "train" for episode_id in unique_ids}

    rng = random.Random(seed)  # nosec B311
    shuffled = list(unique_ids)
    rng.shuffle(shuffled)

    train_cut = max(1, math.floor(len(shuffled) * train_fraction))
    val_cut = (
        max(train_cut + 1, math.floor(len(shuffled) * (train_fraction + val_fraction)))
        if len(shuffled) >= 3
        else len(shuffled)
    )

    mapping: dict[str, str] = {}
    for index, episode_id in enumerate(shuffled):
        if index < train_cut:
            mapping[episode_id] = "train"
        elif index < val_cut:
            mapping[episode_id] = "val"
        else:
            mapping[episode_id] = "test"
    return mapping


def prefix_columns(columns: Iterable[str], prefix: str) -> list[str]:
    return [f"{prefix}{column}" for column in columns]


def normalize_token(value: object, *, default: str = "__missing__") -> str:
    if value is None:
        return default
    if isinstance(value, float) and np.isnan(value):
        return default
    text = str(value).strip()
    return text if text else default


def json_dump(path: str | Path, payload: object) -> Path:
    resolved = Path(path).expanduser().resolve()
    resolved.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return resolved


def set_random_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
