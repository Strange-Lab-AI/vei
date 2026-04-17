from __future__ import annotations

from pathlib import Path
from typing import List

from .models import AgentConfig, Runbook, TrainingFormat, TrainingSet


def synthesize_runbook(
    root: str | Path,
    run_id: str,
) -> Runbook:
    from .runbook import build_runbook

    return build_runbook(Path(root).expanduser().resolve(), run_id)


def synthesize_training_set(
    root: str | Path,
    run_ids: List[str],
    *,
    format: TrainingFormat = "conversations",
) -> TrainingSet:
    from .training import build_training_set

    return build_training_set(Path(root).expanduser().resolve(), run_ids, format=format)


def synthesize_authoring_finetune_set(
    root: str | Path,
    run_ids: List[str],
) -> TrainingSet:
    from .training import build_training_set

    return build_training_set(
        Path(root).expanduser().resolve(),
        run_ids,
        format="authoring",
    )


def synthesize_agent_config(
    root: str | Path,
    run_id: str,
) -> AgentConfig:
    from .agent_config import build_agent_config

    return build_agent_config(Path(root).expanduser().resolve(), run_id)
