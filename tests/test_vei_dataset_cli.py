from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from vei.cli import vei_dataset
from vei.cli.vei_dataset import app
from vei.dataset.models import (
    DatasetBuildSpec,
    DatasetBundle,
    DatasetExampleManifest,
    DatasetRunRecord,
    DatasetSplitManifest,
)


def test_dataset_cli_commands_are_wired_into_root_app(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runner = CliRunner()
    root = tmp_path / "dataset"

    monkeypatch.setattr(
        vei_dataset,
        "build_dataset_bundle",
        lambda *args, **kwargs: _sample_bundle(root),
    )
    monkeypatch.setattr(
        vei_dataset,
        "load_dataset_bundle",
        lambda *args, **kwargs: _sample_bundle(root),
    )

    build_result = runner.invoke(app, ["build", "--output-root", str(root)])
    assert build_result.exit_code == 0, build_result.output
    build_payload = json.loads(build_result.output)
    assert build_payload["environment_count"] == 2

    status_result = runner.invoke(app, ["status", "--root", str(root)])
    assert status_result.exit_code == 0, status_result.output
    status_payload = json.loads(status_result.output)
    assert status_payload["run_count"] == 6


def _sample_bundle(root: Path) -> DatasetBundle:
    return DatasetBundle(
        spec=DatasetBuildSpec(output_root=root),
        environment_count=2,
        run_count=6,
        runs=[
            DatasetRunRecord(
                workspace_root=root / "workspace-a",
                variant_id="variant-a",
                archetype="b2b_saas",
                run_id="run_a",
                runner="workflow",
                split="train",
                status="ok",
            )
        ],
        exports=[
            DatasetExampleManifest(
                format="conversations",
                split="train",
                example_count=12,
                path="exports/train_conversations.json",
            )
        ],
        splits=[
            DatasetSplitManifest(
                split="train",
                run_count=4,
                example_count=12,
                run_ids=["run_a"],
            )
        ],
        reward_summary={"success_rate": 0.75},
        generated_at="2026-03-25T18:00:00+00:00",
    )
