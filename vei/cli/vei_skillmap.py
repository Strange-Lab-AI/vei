from __future__ import annotations

import json
from pathlib import Path

import typer

from vei.skillmap.api import (
    CompanySkillMap,
    build_company_skill_map_from_context_path,
    validate_company_skill_map,
    write_company_skill_map_outputs,
)

app = typer.Typer(add_completion=False)


@app.command("build")
def build(
    source_dir: str = typer.Option(
        ..., "--source-dir", help="Path to a context snapshot or bundle directory."
    ),
    output: str = typer.Option(
        "company_skill_map",
        "--output",
        "-o",
        help="Directory for company_skill_map.json and Markdown reports.",
    ),
    limit: int = typer.Option(
        12,
        "--limit",
        help="Maximum number of candidate skills to emit.",
        min=1,
    ),
    replay: bool = typer.Option(
        True,
        "--replay/--no-replay",
        help="Attach deterministic historical replay scores when a context bundle can be loaded as a what-if world.",
    ),
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="LLM provider for skill synthesis. Defaults to .agents.yml.",
    ),
    model: str | None = typer.Option(
        None,
        "--model",
        help="LLM model for skill synthesis. Defaults to .agents.yml/provider fallback.",
    ),
    previous_map: str | None = typer.Option(
        None,
        "--previous-map",
        help="Previous company_skill_map.json or output directory to preserve review state and retire missing skills.",
    ),
    timeout_s: int = typer.Option(
        240,
        "--timeout-s",
        help="LLM request timeout in seconds.",
        min=1,
    ),
    catalog_shard_size: int = typer.Option(
        80,
        "--catalog-shard-size",
        help="Evidence items per LLM call. All shards are processed; use 0 to send one full catalog.",
        min=0,
    ),
) -> None:
    """Build an evidence-backed company skill map from a context bundle."""
    skill_map = build_company_skill_map_from_context_path(
        source_dir,
        limit=limit,
        include_replay=replay,
        provider=provider,
        model=model,
        previous_map_path=previous_map,
        timeout_s=timeout_s,
        catalog_shard_size=catalog_shard_size,
    )
    paths = write_company_skill_map_outputs(skill_map, output)
    typer.echo(
        "Wrote "
        f"{skill_map.skill_count} skills "
        f"({skill_map.validation.error_count} errors, "
        f"{skill_map.validation.warning_count} warnings) "
        f"-> {paths['json'].parent}"
    )


@app.command("validate")
def validate(
    map_path: str = typer.Option(..., "--map", help="Path to company_skill_map.json."),
    output: str = typer.Option(
        "-", "--output", "-o", help="Output validation JSON path or stdout."
    ),
) -> None:
    """Validate a company skill map before activation."""
    path = Path(map_path).expanduser().resolve()
    if not path.exists():
        raise typer.BadParameter(f"skill map not found: {map_path}")
    skill_map = CompanySkillMap.model_validate_json(path.read_text(encoding="utf-8"))
    validation = validate_company_skill_map(skill_map)
    text = json.dumps(validation.model_dump(mode="json"), indent=2) + "\n"
    if output == "-":
        typer.echo(text, nl=False)
        if not validation.ok:
            raise typer.Exit(1)
        return
    output_path = Path(output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    typer.echo(
        "Validated "
        f"{validation.active_skill_count} active and "
        f"{validation.draft_skill_count} draft skills "
        f"({validation.error_count} errors, {validation.warning_count} warnings) "
        f"-> {output_path}"
    )
    if not validation.ok:
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
