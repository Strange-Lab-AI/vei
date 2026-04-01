from __future__ import annotations

from pathlib import Path
from typing import List

import typer

from vei.rl.train import BehaviorCloningTrainer

app = typer.Typer(add_completion=False)


@app.command()
def bc(
    dataset: List[str] = typer.Option(
        ..., "--dataset", "-d", help="Rollout dataset path(s)"
    ),
    output: Path = typer.Option(
        Path("bc_policy.json"), help="Path to save trained policy"
    ),
) -> None:
    trainer = BehaviorCloningTrainer([Path(p) for p in dataset])
    policy = trainer.train()
    output.parent.mkdir(parents=True, exist_ok=True)
    policy.save(output)
    typer.echo(
        f"trained bc policy covering {len(policy.tool_counts)} tools; saved to {output}"  # noqa: E501
    )


if __name__ == "__main__":
    app()
