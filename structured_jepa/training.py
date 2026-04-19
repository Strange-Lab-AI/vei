from __future__ import annotations

import json
import logging
from collections.abc import Sized
from pathlib import Path
from typing import cast

import torch
from torch.utils.data import DataLoader

from .model import StructuredStateJEPA
from .readouts import apply_linear_readout, fit_least_squares
from .schema import (
    DatasetSchema,
    EvaluationResult,
    LinearProbeResult,
    ModelConfig,
    StepBatch,
    TrainConfig,
    TrainHistory,
    TrainingArtifacts,
)
from .storage import PreparedDataset, WindowDataset, collate_step_batches, load_processed_dataset
from .utils import ensure_directory, set_random_seed

logger = logging.getLogger(__name__)


def build_model(schema: DatasetSchema, config: ModelConfig) -> StructuredStateJEPA:
    return StructuredStateJEPA(
        config=config,
        observation_numeric_dim=len(schema.observation_numeric_columns),
        observation_mask_dim=len(schema.observation_mask_columns),
        observation_cardinalities=[len(spec.vocab) for spec in schema.observation_categorical],
        action_numeric_dim=len(schema.action_numeric_columns),
        action_mask_dim=len(schema.action_mask_columns),
        action_cardinalities=[len(spec.vocab) for spec in schema.action_categorical],
    )


def train_model(
    *,
    dataset_root: str | Path,
    output_dir: str | Path,
    model_config: ModelConfig | None = None,
    train_config: TrainConfig | None = None,
) -> TrainingArtifacts:
    prepared = load_processed_dataset(dataset_root)
    config = model_config or ModelConfig()
    config = config.model_copy(update={"context_length": prepared.schema.context_length})
    train_args = train_config or TrainConfig()
    set_random_seed(train_args.seed)

    logger.info(
        "training_start",
        extra={
            "epochs": train_args.epochs,
            "batch_size": train_args.batch_size,
            "lr": train_args.lr,
            "device": train_args.device,
            "encoder_type": config.encoder_type,
            "d_state": config.d_state,
            "rows": prepared.schema.row_count,
            "episodes": prepared.schema.episode_count,
        },
    )

    model = build_model(prepared.schema, config).to(train_args.device)
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=train_args.lr,
        weight_decay=train_args.weight_decay,
    )

    train_loader = _build_loader(
        prepared, split="train", batch_size=train_args.batch_size, shuffle=True
    )
    val_loader = _build_loader(
        prepared, split="val", batch_size=train_args.batch_size, shuffle=False
    )

    history = TrainHistory()
    for epoch in range(train_args.epochs):
        train_loss = _run_epoch(model, train_loader, optimizer, train_args.device)
        val_loss = _run_epoch(model, val_loader, None, train_args.device)
        history.train_losses.append(train_loss)
        history.val_losses.append(val_loss)
        logger.info(
            "epoch_complete",
            extra={"epoch": epoch, "train_loss": train_loss, "val_loss": val_loss},
        )

    output_root = ensure_directory(output_dir)
    model_path = output_root / "model.pt"
    summary_path = output_root / "training_summary.json"
    torch.save(
        {
            "model_state_dict": model.state_dict(),
            "model_config": config.model_dump(mode="json"),
            "schema": prepared.schema.model_dump(mode="json"),
        },
        model_path,
    )
    summary_path.write_text(
        json.dumps({"history": history.model_dump(mode="json")}, indent=2),
        encoding="utf-8",
    )
    logger.info(
        "training_complete",
        extra={
            "final_train_loss": history.train_losses[-1],
            "final_val_loss": history.val_losses[-1],
            "model_path": str(model_path),
        },
    )
    return TrainingArtifacts(
        model_path=str(model_path),
        summary_path=str(summary_path),
        history=history,
    )


def load_trained_model(checkpoint_path: str | Path) -> tuple[StructuredStateJEPA, DatasetSchema]:
    payload = torch.load(
        Path(checkpoint_path).expanduser().resolve(),
        map_location="cpu",
        weights_only=True,
    )
    schema = DatasetSchema.model_validate(payload["schema"])
    config = ModelConfig.model_validate(payload["model_config"])
    model = build_model(schema, config)
    model.load_state_dict(payload["model_state_dict"])
    model.eval()
    return model, schema


def evaluate_model(
    *,
    dataset_root: str | Path,
    checkpoint_path: str | Path,
    device: str = "cpu",
) -> EvaluationResult:
    prepared = load_processed_dataset(dataset_root)
    model, _ = load_trained_model(checkpoint_path)
    model = model.to(device)
    test_loader = _build_loader(prepared, split="test", batch_size=16, shuffle=False)
    if len(cast(Sized, test_loader.dataset)) == 0:
        test_loader = _build_loader(prepared, split="train", batch_size=16, shuffle=False)

    surprises: list[torch.Tensor] = []
    latent_errors: list[torch.Tensor] = []
    naive_errors: list[torch.Tensor] = []
    with torch.no_grad():
        for batch in test_loader:
            moved = batch.to(device)
            forward_pass = model.forward(moved)
            surprises.append(model.surprise_score(moved, forward_pass=forward_pass).cpu())
            latent_errors.append(
                (forward_pass.predicted_latents - forward_pass.target_latents)
                .pow(2)
                .mean(dim=-1)
                .cpu()
            )
            naive_errors.append(
                (moved.observation_numeric[:, :-1] - moved.observation_numeric[:, 1:])
                .pow(2)
                .mean(dim=-1)
                .cpu()
            )
    result = EvaluationResult(
        mean_surprise=float(torch.cat(surprises).mean().item()),
        latent_mse=float(torch.cat(latent_errors).mean().item()),
        naive_observation_mse=float(torch.cat(naive_errors).mean().item()),
    )
    logger.info(
        "evaluation_complete",
        extra={
            "mean_surprise": result.mean_surprise,
            "latent_mse": result.latent_mse,
            "naive_observation_mse": result.naive_observation_mse,
        },
    )
    return result


def fit_linear_probe(
    *,
    dataset_root: str | Path,
    checkpoint_path: str | Path,
    target_column: str,
    device: str = "cpu",
) -> LinearProbeResult:
    prepared = load_processed_dataset(dataset_root)
    model, _ = load_trained_model(checkpoint_path)
    model = model.to(device)
    train_loader = _build_loader(prepared, split="train", batch_size=32, shuffle=False)
    eval_loader = _build_loader(prepared, split="test", batch_size=32, shuffle=False)
    if len(cast(Sized, eval_loader.dataset)) == 0:
        eval_loader = _build_loader(prepared, split="val", batch_size=32, shuffle=False)

    try:
        target_index = prepared.schema.auxiliary_numeric_targets.index(target_column)
    except ValueError as exc:
        raise ValueError(f"probe target not found in schema: {target_column}") from exc

    train_latents, train_targets = _collect_probe_data(model, train_loader, target_index, device)
    eval_latents, eval_targets = _collect_probe_data(model, eval_loader, target_index, device)

    weights = fit_least_squares(train_latents, train_targets)
    predictions = apply_linear_readout(eval_latents, weights)
    baseline = torch.full_like(eval_targets, float(train_targets.mean().item()))
    mse = float(torch.mean((predictions - eval_targets) ** 2).item())
    baseline_mse = float(torch.mean((baseline - eval_targets) ** 2).item())
    logger.info(
        "probe_complete",
        extra={"target": target_column, "mse": mse, "baseline_mse": baseline_mse},
    )
    return LinearProbeResult(
        target_column=target_column,
        mse=mse,
        baseline_mse=baseline_mse,
    )


def fit_summary_decoder(
    *,
    dataset_root: str | Path,
    checkpoint_path: str | Path,
    columns: list[str] | None = None,
    device: str = "cpu",
) -> dict[str, object]:
    prepared = load_processed_dataset(dataset_root)
    model, _ = load_trained_model(checkpoint_path)
    model = model.to(device)
    selected = (
        columns
        or prepared.schema.observation_numeric_columns[
            : min(4, len(prepared.schema.observation_numeric_columns))
        ]
    )
    if not selected:
        return {"columns": [], "weights": []}
    column_indexes = [
        prepared.schema.observation_numeric_columns.index(column)
        for column in selected
        if column in prepared.schema.observation_numeric_columns
    ]
    if not column_indexes:
        return {"columns": [], "weights": []}

    train_loader = _build_loader(prepared, split="train", batch_size=32, shuffle=False)
    latents: list[torch.Tensor] = []
    targets: list[torch.Tensor] = []
    with torch.no_grad():
        for batch in train_loader:
            moved = batch.to(device)
            encoded = model.encode_steps(moved)[:, -1]
            latents.append(encoded.cpu())
            targets.append(moved.observation_numeric[:, -1, column_indexes].cpu())
    weights = fit_least_squares(torch.cat(latents), torch.cat(targets))
    logger.info(
        "decoder_fit_complete",
        extra={"columns": selected, "sample_count": torch.cat(latents).size(0)},
    )
    return {
        "columns": selected,
        "weights": weights.tolist(),
    }


def _build_loader(
    prepared: PreparedDataset,
    *,
    split: str,
    batch_size: int,
    shuffle: bool,
) -> DataLoader[StepBatch]:
    dataset = WindowDataset(prepared, split=split)
    return DataLoader(
        dataset, batch_size=batch_size, shuffle=shuffle, collate_fn=collate_step_batches
    )


def _run_epoch(
    model: StructuredStateJEPA,
    loader: DataLoader[StepBatch],
    optimizer: torch.optim.Optimizer | None,
    device: str,
) -> float:
    training = optimizer is not None
    model.train(training)
    losses: list[float] = []
    for batch in loader:
        moved = batch.to(device)
        if optimizer is not None:
            optimizer.zero_grad(set_to_none=True)
        forward_pass = model.forward(moved)
        if optimizer is not None:
            forward_pass.total_loss.backward()
            optimizer.step()
        losses.append(float(forward_pass.total_loss.detach().cpu().item()))
    return float(sum(losses) / max(1, len(losses)))


def _collect_probe_data(
    model: StructuredStateJEPA,
    loader: DataLoader[StepBatch],
    target_index: int,
    device: str,
) -> tuple[torch.Tensor, torch.Tensor]:
    latents: list[torch.Tensor] = []
    targets: list[torch.Tensor] = []
    with torch.no_grad():
        for batch in loader:
            moved = batch.to(device)
            latents.append(model.encode_steps(moved)[:, -1].cpu())
            targets.append(moved.auxiliary_numeric_targets[:, -1, target_index].cpu())
    return torch.cat(latents), torch.cat(targets)
