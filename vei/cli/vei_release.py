from __future__ import annotations

import json
from pathlib import Path

import typer

from vei.release.api import (
    build_release_version,
    export_dataset_release,
    run_nightly_release,
    snapshot_benchmark_release,
)

app = typer.Typer(
    add_completion=False,
    help="Versioned release tooling for VEI datasets and benchmarks.",
)


@app.command("dataset")
def dataset_release(
    input_path: Path = typer.Option(
        ..., exists=True, readable=True, help="Dataset or corpus JSON file"
    ),
    release_root: Path = typer.Option(
        Path("_vei_out/releases"), help="Root directory for release bundles"
    ),
    version: str | None = typer.Option(None, help="Release version identifier"),
    label: str = typer.Option("dataset", help="Human label for this release"),
    dataset_kind: str = typer.Option(
        "auto", help="auto|vei_dataset|corpus|quality_report"
    ),
) -> None:
    resolved_version = version or build_release_version(prefix="dataset")
    result = export_dataset_release(
        input_path=input_path,
        release_root=release_root,
        version=resolved_version,
        label=label,
        dataset_kind=dataset_kind,  # type: ignore[arg-type]
    )
    typer.echo(json.dumps(result.manifest.model_dump(mode="json"), indent=2))


@app.command("benchmark")
def benchmark_release(
    benchmark_dir: Path = typer.Option(
        ..., exists=True, readable=True, help="Benchmark run directory"
    ),
    release_root: Path = typer.Option(
        Path("_vei_out/releases"), help="Root directory for release bundles"
    ),
    version: str | None = typer.Option(None, help="Release version identifier"),
    label: str = typer.Option("benchmark", help="Human label for this release"),
) -> None:
    resolved_version = version or build_release_version(prefix="benchmark")
    result = snapshot_benchmark_release(
        benchmark_dir=benchmark_dir,
        release_root=release_root,
        version=resolved_version,
        label=label,
    )
    typer.echo(json.dumps(result.manifest.model_dump(mode="json"), indent=2))


@app.command("nightly")
def nightly_release(
    release_root: Path = typer.Option(
        Path("_vei_out/releases"), help="Root directory for versioned release bundles"
    ),
    workspace_root: Path = typer.Option(
        Path("_vei_out/nightly"), help="Workspace for intermediate nightly artifacts"
    ),
    version: str | None = typer.Option(None, help="Version identifier to stamp"),
    seed: int = typer.Option(42042, help="Seed for deterministic generation"),
    environments: int = typer.Option(25, help="Enterprise environments to generate"),
    scenarios_per_environment: int = typer.Option(
        20, help="Workflow scenarios per generated environment"
    ),
    realism_threshold: float = typer.Option(
        0.55, help="Corpus realism threshold for acceptance"
    ),
    rollout_episodes: int = typer.Option(3, help="Scripted rollout episodes"),
    rollout_scenario: str = typer.Option(
        "multi_channel", help="Scenario used for rollout generation"
    ),
    benchmark_scenario: list[str] = typer.Option(
        ["multi_channel"],
        "--benchmark-scenario",
        help="Scenario(s) used for scripted benchmark snapshots",
    ),
    llm_model: str | None = typer.Option(
        None, help="Optional model to include an LLM benchmark snapshot"
    ),
    llm_provider: str = typer.Option(
        "auto", help="Provider for optional LLM benchmark"
    ),
) -> None:
    resolved_version = version or build_release_version(prefix="nightly")
    result = run_nightly_release(
        release_root=release_root,
        workspace_root=workspace_root,
        version=resolved_version,
        seed=seed,
        environment_count=environments,
        scenarios_per_environment=scenarios_per_environment,
        realism_threshold=realism_threshold,
        rollout_episodes=rollout_episodes,
        rollout_scenario=rollout_scenario,
        benchmark_scenarios=benchmark_scenario,
        llm_model=llm_model,
        llm_provider=llm_provider,
    )
    typer.echo(json.dumps(result.manifest.model_dump(mode="json"), indent=2))


if __name__ == "__main__":
    app()
