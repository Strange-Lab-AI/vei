from __future__ import annotations

import json
from pathlib import Path

import typer

from vei.playable import run_playable_showcase
from vei.verticals import (
    get_vertical_pack_manifest,
)
from vei.verticals.demo import (
    VerticalShowcaseSpec,
    VerticalStoryShowcaseSpec,
    VerticalVariantMatrixSpec,
    resolve_vertical_names,
    run_vertical_story_showcase,
    run_vertical_showcase,
    run_vertical_variant_matrix,
)

app = typer.Typer(
    add_completion=False,
    help="Run polished showcase bundles for VEI product demos.",
)


def _resolve_compare_runner(
    compare_runner: str,
    *,
    compare_model: str | None,
    compare_bc_model: Path | None,
) -> str:
    normalized_runner = compare_runner.strip().lower()
    if normalized_runner not in {"scripted", "bc", "llm"}:
        raise typer.BadParameter("compare-runner must be one of scripted|bc|llm")
    if normalized_runner == "llm" and not compare_model:
        raise typer.BadParameter("llm showcase requires --compare-model")
    if normalized_runner == "bc" and compare_bc_model is None:
        raise typer.BadParameter("bc showcase requires --compare-bc-model")
    return normalized_runner


def _resolve_showcase_verticals(vertical: list[str]) -> list[str]:
    selected_verticals = resolve_vertical_names(vertical)
    for name in selected_verticals:
        try:
            get_vertical_pack_manifest(name)
        except KeyError as exc:
            raise typer.BadParameter(str(exc)) from exc
    return selected_verticals


@app.command("verticals")
def verticals_command(
    root: Path = typer.Option(
        Path("_vei_out/vertical_showcase"),
        help="Root directory for generated showcase workspaces and bundles",
    ),
    vertical: list[str] = typer.Option(
        [],
        "--vertical",
        "-v",
        help="Optional subset of vertical packs to include",
    ),
    compare_runner: str = typer.Option(
        "scripted", help="Comparison runner for each vertical: scripted|bc|llm"
    ),
    run_id: str = typer.Option("vertical_showcase", help="Showcase bundle identifier"),
    overwrite: bool = typer.Option(
        True, help="Recreate each vertical workspace before running"
    ),
    seed: int = typer.Option(42042, help="Seed for reproducibility"),
    max_steps: int = typer.Option(18, help="Max steps for comparison runs"),
    compare_model: str | None = typer.Option(
        None, help="Model name when compare-runner=llm"
    ),
    compare_provider: str | None = typer.Option(
        None, help="Provider name when compare-runner=llm"
    ),
    compare_bc_model: Path | None = typer.Option(
        None,
        exists=True,
        readable=True,
        help="BC policy file when compare-runner=bc",
    ),
) -> None:
    normalized_runner = _resolve_compare_runner(
        compare_runner,
        compare_model=compare_model,
        compare_bc_model=compare_bc_model,
    )
    selected_verticals = _resolve_showcase_verticals(vertical)

    result = run_vertical_showcase(
        VerticalShowcaseSpec(
            vertical_names=selected_verticals,
            root=root,
            compare_runner=normalized_runner,  # type: ignore[arg-type]
            overwrite=overwrite,
            seed=seed,
            max_steps=max_steps,
            compare_model=compare_model,
            compare_provider=compare_provider,
            compare_bc_model_path=compare_bc_model,
            run_id=run_id,
        )
    )
    typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))


@app.command("variant-matrix")
def variant_matrix_command(
    root: Path = typer.Option(
        Path("_vei_out/vertical_showcase"),
        help="Root directory for generated variant-matrix workspaces and bundles",
    ),
    vertical: list[str] = typer.Option(
        [],
        "--vertical",
        "-v",
        help="Optional subset of vertical packs to include",
    ),
    compare_runner: str = typer.Option(
        "scripted",
        help="Comparison runner for each matrix combination: scripted|bc|llm",
    ),
    run_id: str = typer.Option(
        "variant_matrix", help="Variant matrix bundle identifier"
    ),
    overwrite: bool = typer.Option(
        True, help="Recreate each vertical workspace before running"
    ),
    seed: int = typer.Option(42042, help="Seed for reproducibility"),
    max_steps: int = typer.Option(18, help="Max steps for comparison runs"),
    compare_model: str | None = typer.Option(
        None, help="Model name when compare-runner=llm"
    ),
    compare_provider: str | None = typer.Option(
        None, help="Provider name when compare-runner=llm"
    ),
    compare_bc_model: Path | None = typer.Option(
        None,
        exists=True,
        readable=True,
        help="BC policy file when compare-runner=bc",
    ),
) -> None:
    normalized_runner = _resolve_compare_runner(
        compare_runner,
        compare_model=compare_model,
        compare_bc_model=compare_bc_model,
    )
    selected_verticals = _resolve_showcase_verticals(vertical)

    result = run_vertical_variant_matrix(
        VerticalVariantMatrixSpec(
            vertical_names=selected_verticals,
            root=root,
            compare_runner=normalized_runner,  # type: ignore[arg-type]
            overwrite=overwrite,
            seed=seed,
            max_steps=max_steps,
            compare_model=compare_model,
            compare_provider=compare_provider,
            compare_bc_model_path=compare_bc_model,
            run_id=run_id,
        )
    )
    typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))


@app.command("story")
def story_command(
    root: Path = typer.Option(
        Path("_vei_out/vertical_showcase"),
        help="Root directory for generated story workspaces and bundles",
    ),
    vertical: list[str] = typer.Option(
        [],
        "--vertical",
        "-v",
        help="Optional subset of vertical packs to include",
    ),
    scenario_variant: str | None = typer.Option(
        None,
        help="Override the scenario variant when exactly one vertical is selected",
    ),
    contract_variant: str | None = typer.Option(
        None,
        help="Override the contract variant when exactly one vertical is selected",
    ),
    compare_runner: str = typer.Option(
        "scripted",
        help="Comparison runner for the story path: scripted|bc|llm",
    ),
    run_id: str = typer.Option("story_showcase", help="Story bundle identifier"),
    overwrite: bool = typer.Option(
        True, help="Recreate each story workspace before running"
    ),
    seed: int = typer.Option(42042, help="Seed for reproducibility"),
    max_steps: int = typer.Option(18, help="Max steps for comparison runs"),
    compare_model: str | None = typer.Option(
        None, help="Model name when compare-runner=llm"
    ),
    compare_provider: str | None = typer.Option(
        None, help="Provider name when compare-runner=llm"
    ),
    compare_bc_model: Path | None = typer.Option(
        None,
        exists=True,
        readable=True,
        help="BC policy file when compare-runner=bc",
    ),
) -> None:
    normalized_runner = _resolve_compare_runner(
        compare_runner,
        compare_model=compare_model,
        compare_bc_model=compare_bc_model,
    )
    selected_verticals = _resolve_showcase_verticals(vertical)

    try:
        result = run_vertical_story_showcase(
            VerticalStoryShowcaseSpec(
                vertical_names=selected_verticals,
                root=root,
                compare_runner=normalized_runner,  # type: ignore[arg-type]
                overwrite=overwrite,
                seed=seed,
                max_steps=max_steps,
                compare_model=compare_model,
                compare_provider=compare_provider,
                compare_bc_model_path=compare_bc_model,
                run_id=run_id,
                scenario_variant=scenario_variant,
                contract_variant=contract_variant,
            )
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))


@app.command("playable")
def playable_command(
    root: Path = typer.Option(
        Path("_vei_out/playable_showcase"),
        help="Root directory for generated playable workspaces and bundles",
    ),
    vertical: list[str] = typer.Option(
        [],
        "--vertical",
        "-v",
        help="Optional subset of worlds to include",
    ),
    mission: str | None = typer.Option(
        None,
        help="Override the mission when exactly one world is selected",
    ),
    objective: str | None = typer.Option(
        None,
        help="Override the objective when exactly one world is selected",
    ),
    compare_runner: str = typer.Option(
        "scripted",
        help="Comparison runner for the playable bundle: scripted|bc|llm",
    ),
    run_id: str = typer.Option("playable_release", help="Playable bundle identifier"),
    overwrite: bool = typer.Option(
        True, help="Recreate each playable workspace before running"
    ),
    seed: int = typer.Option(42042, help="Seed for reproducibility"),
    max_steps: int = typer.Option(18, help="Max steps for comparison runs"),
    compare_model: str | None = typer.Option(
        None, help="Model name when compare-runner=llm"
    ),
    compare_bc_model: Path | None = typer.Option(
        None,
        exists=True,
        readable=True,
        help="BC policy file when compare-runner=bc",
    ),
) -> None:
    normalized_runner = _resolve_compare_runner(
        compare_runner,
        compare_model=compare_model,
        compare_bc_model=compare_bc_model,
    )
    selected_verticals = _resolve_showcase_verticals(vertical)
    if len(selected_verticals) != 1 and (mission or objective):
        raise typer.BadParameter(
            "--mission and --objective require exactly one --vertical"
        )
    result = run_playable_showcase(
        root=root,
        world_names=selected_verticals,
        mission_name=mission,
        objective_variant=objective,
        compare_runner=normalized_runner,
        run_id=run_id,
        overwrite=overwrite,
        seed=seed,
        max_steps=max_steps,
    )
    typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))


if __name__ == "__main__":
    app()
