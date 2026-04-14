from __future__ import annotations

import json
from pathlib import Path

import pytest
import typer.testing

from vei.cli.vei_det_pipeline import app

pytestmark = pytest.mark.integration


def test_det_pipeline_generate_and_filter(tmp_path: Path) -> None:
    runner = typer.testing.CliRunner()
    corpus_path = tmp_path / "corpus.json"
    report_path = tmp_path / "report.json"

    result_gen = runner.invoke(
        app,
        [
            "generate-corpus",
            "--seed",
            "77",
            "--environments",
            "2",
            "--scenarios-per-environment",
            "2",
            "--output",
            str(corpus_path),
        ],
    )
    assert result_gen.exit_code == 0, result_gen.output
    assert corpus_path.exists()

    result_filter = runner.invoke(
        app,
        [
            "filter-corpus",
            "--corpus",
            str(corpus_path),
            "--output",
            str(report_path),
        ],
    )
    assert result_filter.exit_code == 0, result_filter.output
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert "summary" in payload
    assert payload["summary"]["accepted"] + payload["summary"]["rejected"] == 4
