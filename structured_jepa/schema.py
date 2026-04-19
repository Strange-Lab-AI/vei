from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import torch
from pydantic import BaseModel, Field

DatasetKind = Literal["timeseries", "vei_runs", "vei_context"]


class NumericFeatureSpec(BaseModel):
    name: str
    source_column: str
    group: Literal["observation", "action"]
    mean: float
    std: float
    mask_column: str


class CategoricalFeatureSpec(BaseModel):
    name: str
    source_column: str
    group: Literal["observation", "action"]
    vocab: list[str]
    missing_token: str = "__missing__"
    unknown_token: str = "__unknown__"

    def to_id_map(self) -> dict[str, int]:
        return {value: idx for idx, value in enumerate(self.vocab)}


class DatasetSchema(BaseModel):
    version: Literal["1"] = "1"
    dataset_kind: DatasetKind
    context_length: int = 16
    prediction_horizon: int = 1
    row_count: int
    episode_count: int
    metadata_columns: list[str] = Field(default_factory=list)
    observation_numeric: list[NumericFeatureSpec] = Field(default_factory=list)
    observation_categorical: list[CategoricalFeatureSpec] = Field(default_factory=list)
    action_numeric: list[NumericFeatureSpec] = Field(default_factory=list)
    action_categorical: list[CategoricalFeatureSpec] = Field(default_factory=list)
    auxiliary_numeric_targets: list[str] = Field(default_factory=list)
    notes: dict[str, Any] = Field(default_factory=dict)

    @property
    def window_size(self) -> int:
        return self.context_length + self.prediction_horizon

    @property
    def observation_numeric_columns(self) -> list[str]:
        return [feature.name for feature in self.observation_numeric]

    @property
    def observation_mask_columns(self) -> list[str]:
        return [feature.mask_column for feature in self.observation_numeric]

    @property
    def observation_categorical_columns(self) -> list[str]:
        return [feature.name for feature in self.observation_categorical]

    @property
    def action_numeric_columns(self) -> list[str]:
        return [feature.name for feature in self.action_numeric]

    @property
    def action_mask_columns(self) -> list[str]:
        return [feature.mask_column for feature in self.action_numeric]

    @property
    def action_categorical_columns(self) -> list[str]:
        return [feature.name for feature in self.action_categorical]


class ModelConfig(BaseModel):
    encoder_type: Literal["flat", "tokenized"] = "flat"
    d_state: int = 128
    d_action: int = 64
    context_length: int = 16
    depth: int = 4
    heads: int = 8
    dropout: float = 0.1
    feature_token_depth: int = 2
    sigreg_lambda: float = 0.05
    sigreg_knots: int = 17
    sigreg_num_proj: int = 512


class TrainConfig(BaseModel):
    batch_size: int = 16
    epochs: int = 8
    lr: float = 1e-3
    weight_decay: float = 1e-4
    seed: int = 7
    device: str = "cpu"


class TrainHistory(BaseModel):
    train_losses: list[float] = Field(default_factory=list)
    val_losses: list[float] = Field(default_factory=list)


class TrainingArtifacts(BaseModel):
    model_path: str
    summary_path: str
    history: TrainHistory


class EvaluationResult(BaseModel):
    mean_surprise: float
    latent_mse: float
    naive_observation_mse: float


class LinearProbeResult(BaseModel):
    target_column: str
    mse: float
    baseline_mse: float


@dataclass
class StepBatch:
    observation_numeric: torch.Tensor
    observation_masks: torch.Tensor
    observation_categorical: torch.Tensor
    action_numeric: torch.Tensor
    action_masks: torch.Tensor
    action_categorical: torch.Tensor
    auxiliary_numeric_targets: torch.Tensor
    valid_mask: torch.Tensor

    def to(self, device: str | torch.device) -> StepBatch:
        return StepBatch(
            observation_numeric=self.observation_numeric.to(device),
            observation_masks=self.observation_masks.to(device),
            observation_categorical=self.observation_categorical.to(device),
            action_numeric=self.action_numeric.to(device),
            action_masks=self.action_masks.to(device),
            action_categorical=self.action_categorical.to(device),
            auxiliary_numeric_targets=self.auxiliary_numeric_targets.to(device),
            valid_mask=self.valid_mask.to(device),
        )


@dataclass
class ActionCandidateBatch:
    action_numeric: torch.Tensor
    action_masks: torch.Tensor
    action_categorical: torch.Tensor
    target_latent: torch.Tensor | None = None

    def to(self, device: str | torch.device) -> ActionCandidateBatch:
        target_latent = None if self.target_latent is None else self.target_latent.to(device)
        return ActionCandidateBatch(
            action_numeric=self.action_numeric.to(device),
            action_masks=self.action_masks.to(device),
            action_categorical=self.action_categorical.to(device),
            target_latent=target_latent,
        )


def dataset_paths(root: str | Path) -> tuple[Path, Path]:
    dataset_root = Path(root).expanduser().resolve()
    return dataset_root / "steps.parquet", dataset_root / "schema.json"
