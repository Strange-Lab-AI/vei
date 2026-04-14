from __future__ import annotations

import typer

from vei.whatif.benchmark import (
    build_branch_point_benchmark,
    evaluate_branch_point_benchmark_model,
    judge_branch_point_benchmark,
    load_branch_point_benchmark_build_result,
    load_branch_point_benchmark_eval_result,
    load_branch_point_benchmark_judge_result,
    load_branch_point_benchmark_study_result,
    load_branch_point_benchmark_train_result,
    run_branch_point_benchmark_study,
    train_branch_point_benchmark_model,
)
from vei.whatif.research import get_research_pack, run_research_pack

from .whatif_benchmark import register_benchmark_commands
from .whatif_catalog import register_catalog_commands
from .whatif_episode import register_episode_commands
from .whatif_experiment import register_experiment_commands
from .whatif_research import register_pack_commands

app = typer.Typer(
    add_completion=False,
    help="Explore counterfactuals and materialize replayable what-if episodes.",
)
pack_app = typer.Typer(
    add_completion=False,
    help="Run Enron research packs and compare multiple outcome backends.",
)
benchmark_app = typer.Typer(
    add_completion=False,
    help="Build, train, and evaluate pre-branch Enron benchmark models.",
)

register_catalog_commands(app)
register_episode_commands(app)
register_experiment_commands(app)
register_pack_commands(pack_app)
register_benchmark_commands(benchmark_app)

app.add_typer(pack_app, name="pack")
app.add_typer(benchmark_app, name="benchmark")

__all__ = [
    "app",
    "benchmark_app",
    "build_branch_point_benchmark",
    "evaluate_branch_point_benchmark_model",
    "get_research_pack",
    "judge_branch_point_benchmark",
    "load_branch_point_benchmark_build_result",
    "load_branch_point_benchmark_eval_result",
    "load_branch_point_benchmark_judge_result",
    "load_branch_point_benchmark_study_result",
    "load_branch_point_benchmark_train_result",
    "pack_app",
    "run_branch_point_benchmark_study",
    "run_research_pack",
    "train_branch_point_benchmark_model",
]
