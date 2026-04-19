from __future__ import annotations

import glob
import json
from typing import Literal, cast

import typer

from .api import (
    ablation_timeseries,
    benchmark_timeseries,
    benchmark_vei_demo,
    evaluate_model,
    fit_linear_probe,
    fit_summary_decoder,
    prepare_timeseries_dataset,
    prepare_vei_context_dataset,
    prepare_vei_runs_dataset,
    publish_bundle,
    train_model,
    write_brief,
)
from .schema import ModelConfig, TrainConfig

app = typer.Typer(add_completion=False)
RUN_ID_OPTION = typer.Option(None, "--run-id", help="Specific run id(s) to include")


@app.command("prepare-timeseries")
def prepare_timeseries_command(
    input_path: str = typer.Option(..., "--input", help="CSV or Parquet file"),
    output_dir: str = typer.Option(..., "--output", help="Prepared dataset directory"),
    entity_column: str = typer.Option(..., "--entity-column", help="Entity identifier column"),
    timestamp_column: str = typer.Option(..., "--timestamp-column", help="Timestamp column"),
    observation_categorical_columns: str = typer.Option(
        "",
        "--observation-categorical-columns",
        help="Comma-separated categorical observation columns",
    ),
    action_numeric_columns: str = typer.Option(
        "", "--action-numeric-columns", help="Comma-separated numeric action columns"
    ),
    action_categorical_columns: str = typer.Option(
        "", "--action-categorical-columns", help="Comma-separated categorical action columns"
    ),
    auxiliary_numeric_targets: str = typer.Option(
        "", "--auxiliary-numeric-targets", help="Comma-separated auxiliary numeric target columns"
    ),
    seed: int = typer.Option(7, "--seed", help="Split seed"),
    train_fraction: float = typer.Option(0.7, "--train-fraction", help="Train split fraction"),
    val_fraction: float = typer.Option(0.15, "--val-fraction", help="Validation split fraction"),
) -> None:
    prepared = prepare_timeseries_dataset(
        input_path=input_path,
        output_dir=output_dir,
        entity_column=entity_column,
        timestamp_column=timestamp_column,
        observation_categorical_columns=_split_csv_values(observation_categorical_columns),
        action_numeric_columns=_split_csv_values(action_numeric_columns),
        action_categorical_columns=_split_csv_values(action_categorical_columns),
        auxiliary_numeric_target_columns=_split_csv_values(auxiliary_numeric_targets),
        seed=seed,
        train_fraction=train_fraction,
        val_fraction=val_fraction,
    )
    typer.echo(f"Wrote {prepared.schema.row_count} rows to {prepared.root}")


@app.command("prepare-vei-runs")
def prepare_vei_runs_command(
    workspace_root: str = typer.Option(..., "--workspace-root", help="VEI workspace root"),
    output_dir: str = typer.Option(..., "--output", help="Prepared dataset directory"),
    run_id: list[str] = RUN_ID_OPTION,
    seed: int = typer.Option(7, "--seed", help="Split seed"),
    train_fraction: float = typer.Option(0.7, "--train-fraction", help="Train split fraction"),
    val_fraction: float = typer.Option(0.15, "--val-fraction", help="Validation split fraction"),
) -> None:
    prepared = prepare_vei_runs_dataset(
        workspace_root=workspace_root,
        output_dir=output_dir,
        run_ids=run_id or None,
        seed=seed,
        train_fraction=train_fraction,
        val_fraction=val_fraction,
    )
    typer.echo(f"Wrote {prepared.schema.row_count} VEI run rows to {prepared.root}")


@app.command("prepare-vei-context")
def prepare_vei_context_command(
    snapshot_glob: str = typer.Option(
        ..., "--snapshot-glob", help="Glob for context snapshot files"
    ),
    output_dir: str = typer.Option(..., "--output", help="Prepared dataset directory"),
    diff_glob: str = typer.Option("", "--diff-glob", help="Optional glob for context diff files"),
    seed: int = typer.Option(7, "--seed", help="Split seed"),
    train_fraction: float = typer.Option(0.7, "--train-fraction", help="Train split fraction"),
    val_fraction: float = typer.Option(0.15, "--val-fraction", help="Validation split fraction"),
) -> None:
    snapshot_paths = sorted(glob.glob(snapshot_glob))
    if not snapshot_paths:
        raise typer.BadParameter(f"no snapshots matched: {snapshot_glob}")
    diff_paths = sorted(glob.glob(diff_glob)) if diff_glob else []
    prepared = prepare_vei_context_dataset(
        snapshot_paths=snapshot_paths,
        diff_paths=diff_paths,
        output_dir=output_dir,
        seed=seed,
        train_fraction=train_fraction,
        val_fraction=val_fraction,
    )
    typer.echo(f"Wrote {prepared.schema.row_count} VEI context rows to {prepared.root}")


@app.command("train")
def train_command(
    dataset_root: str = typer.Option(..., "--dataset", help="Prepared dataset directory"),
    output_dir: str = typer.Option(..., "--output", help="Training output directory"),
    epochs: int = typer.Option(8, "--epochs", help="Epoch count"),
    batch_size: int = typer.Option(16, "--batch-size", help="Batch size"),
    lr: float = typer.Option(1e-3, "--lr", help="Learning rate"),
    device: str = typer.Option("cpu", "--device", help="Training device"),
    encoder: str = typer.Option(
        "flat",
        "--encoder",
        help="Step encoder: flat or tokenized",
    ),
) -> None:
    normalized_encoder = encoder.strip().lower()
    if normalized_encoder not in {"flat", "tokenized"}:
        raise typer.BadParameter("encoder must be one of: flat, tokenized")
    artifacts = train_model(
        dataset_root=dataset_root,
        output_dir=output_dir,
        model_config=ModelConfig(
            encoder_type=cast(Literal["flat", "tokenized"], normalized_encoder)
        ),
        train_config=TrainConfig(
            epochs=epochs,
            batch_size=batch_size,
            lr=lr,
            device=device,
        ),
    )
    typer.echo(f"Model saved to {artifacts.model_path}")


@app.command("evaluate")
def evaluate_command(
    dataset_root: str = typer.Option(..., "--dataset", help="Prepared dataset directory"),
    checkpoint_path: str = typer.Option(..., "--checkpoint", help="Saved model checkpoint"),
    device: str = typer.Option("cpu", "--device", help="Evaluation device"),
) -> None:
    result = evaluate_model(
        dataset_root=dataset_root,
        checkpoint_path=checkpoint_path,
        device=device,
    )
    typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))


@app.command("probe")
def probe_command(
    dataset_root: str = typer.Option(..., "--dataset", help="Prepared dataset directory"),
    checkpoint_path: str = typer.Option(..., "--checkpoint", help="Saved model checkpoint"),
    target_column: str = typer.Option(..., "--target-column", help="Auxiliary target column"),
    device: str = typer.Option("cpu", "--device", help="Probe device"),
) -> None:
    result = fit_linear_probe(
        dataset_root=dataset_root,
        checkpoint_path=checkpoint_path,
        target_column=target_column,
        device=device,
    )
    typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))


@app.command("decode-summary")
def decode_summary_command(
    dataset_root: str = typer.Option(..., "--dataset", help="Prepared dataset directory"),
    checkpoint_path: str = typer.Option(..., "--checkpoint", help="Saved model checkpoint"),
    columns: str = typer.Option("", "--columns", help="Comma-separated numeric columns to decode"),
    device: str = typer.Option("cpu", "--device", help="Decoder device"),
) -> None:
    result = fit_summary_decoder(
        dataset_root=dataset_root,
        checkpoint_path=checkpoint_path,
        columns=_split_csv_values(columns),
        device=device,
    )
    typer.echo(json.dumps(result, indent=2))


@app.command("benchmark-timeseries")
def benchmark_timeseries_command(
    dataset_root: str = typer.Option(..., "--dataset", help="Prepared timeseries dataset"),
    output_dir: str = typer.Option(..., "--output", help="Benchmark report directory"),
    epochs: int = typer.Option(8, "--epochs", help="Epoch count"),
    batch_size: int = typer.Option(16, "--batch-size", help="Batch size"),
    lr: float = typer.Option(1e-3, "--lr", help="Learning rate"),
    device: str = typer.Option("cpu", "--device", help="Training device"),
) -> None:
    output_root = benchmark_timeseries(
        dataset_root=dataset_root,
        output_dir=output_dir,
        train_config=TrainConfig(
            epochs=epochs,
            batch_size=batch_size,
            lr=lr,
            device=device,
        ),
    )
    typer.echo(f"Benchmark report written to {output_root}")


@app.command("benchmark-vei-demo")
def benchmark_vei_demo_command(
    dataset_root: str = typer.Option(..., "--dataset", help="Prepared VEI dataset"),
    checkpoint_path: str = typer.Option(..., "--checkpoint", help="Saved model checkpoint"),
    output_dir: str = typer.Option(..., "--output", help="Demo report directory"),
    episode_id: str = typer.Option("", "--episode-id", help="Optional VEI episode id"),
    max_steps: int = typer.Option(5, "--max-steps", help="Number of steps to summarize"),
    device: str = typer.Option("cpu", "--device", help="Inference device"),
) -> None:
    output_root = benchmark_vei_demo(
        dataset_root=dataset_root,
        checkpoint_path=checkpoint_path,
        output_dir=output_dir,
        episode_id=episode_id or None,
        max_steps=max_steps,
        device=device,
    )
    typer.echo(f"VEI demo written to {output_root}")


@app.command("ablation-timeseries")
def ablation_timeseries_command(
    dataset_root: str = typer.Option(..., "--dataset", help="Prepared timeseries dataset"),
    output_dir: str = typer.Option(..., "--output", help="Ablation report directory"),
    preset: str = typer.Option("full", "--preset", help="Ablation preset: quick or full"),
    epochs: int = typer.Option(8, "--epochs", help="Epoch count"),
    batch_size: int = typer.Option(16, "--batch-size", help="Batch size"),
    lr: float = typer.Option(1e-3, "--lr", help="Learning rate"),
    device: str = typer.Option("cpu", "--device", help="Training device"),
) -> None:
    normalized_preset = preset.strip().lower()
    if normalized_preset not in {"quick", "full"}:
        raise typer.BadParameter("preset must be one of: quick, full")
    output_root = ablation_timeseries(
        dataset_root=dataset_root,
        output_dir=output_dir,
        preset=normalized_preset,
        train_config=TrainConfig(
            epochs=epochs,
            batch_size=batch_size,
            lr=lr,
            device=device,
        ),
    )
    typer.echo(f"Ablation report written to {output_root}")


@app.command("write-brief")
def write_brief_command(
    benchmark_dir: str = typer.Option(..., "--benchmark-dir", help="Benchmark report directory"),
    output_path: str = typer.Option(..., "--output", help="Markdown brief output path"),
    vei_demo_dir: str = typer.Option(
        "",
        "--vei-demo-dir",
        help="Optional VEI demo report directory",
    ),
) -> None:
    output = write_brief(
        benchmark_dir=benchmark_dir,
        output_path=output_path,
        vei_demo_dir=vei_demo_dir or None,
    )
    typer.echo(f"Brief written to {output}")


@app.command("publish-bundle")
def publish_bundle_command(
    benchmark_dataset_root: str = typer.Option(
        ..., "--benchmark-dataset", help="Prepared timeseries dataset for publish artifacts"
    ),
    output_dir: str = typer.Option(..., "--output", help="Publish bundle directory"),
    preset: str = typer.Option("full", "--preset", help="Ablation preset: quick or full"),
    epochs: int = typer.Option(8, "--epochs", help="Epoch count for business benchmark runs"),
    batch_size: int = typer.Option(16, "--batch-size", help="Batch size"),
    lr: float = typer.Option(1e-3, "--lr", help="Learning rate"),
    device: str = typer.Option("cpu", "--device", help="Training device"),
    vei_dataset_root: str = typer.Option(
        "",
        "--vei-dataset",
        help="Optional prepared VEI dataset for a demo bundle",
    ),
    vei_epochs: int = typer.Option(6, "--vei-epochs", help="Epoch count for VEI demo training"),
    vei_batch_size: int = typer.Option(16, "--vei-batch-size", help="VEI batch size"),
    vei_max_steps: int = typer.Option(5, "--vei-max-steps", help="VEI steps to summarize"),
) -> None:
    normalized_preset = preset.strip().lower()
    if normalized_preset not in {"quick", "full"}:
        raise typer.BadParameter("preset must be one of: quick, full")
    output_root = publish_bundle(
        benchmark_dataset_root=benchmark_dataset_root,
        output_dir=output_dir,
        ablation_preset=normalized_preset,
        benchmark_train_config=TrainConfig(
            epochs=epochs,
            batch_size=batch_size,
            lr=lr,
            device=device,
        ),
        vei_dataset_root=vei_dataset_root or None,
        vei_train_config=TrainConfig(
            epochs=vei_epochs,
            batch_size=vei_batch_size,
            lr=lr,
            device=device,
        )
        if vei_dataset_root
        else None,
        vei_max_steps=vei_max_steps,
    )
    typer.echo(f"Publish bundle written to {output_root}")


def _split_csv_values(raw: str) -> list[str]:
    if not raw.strip():
        return []
    return [value.strip() for value in raw.split(",") if value.strip()]
