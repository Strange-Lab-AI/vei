from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from vei.cli import whatif_experiment
from vei.cli.vei import app


def test_whatif_experiment_cli_keeps_heuristic_mode_when_backend_is_auto(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runner = CliRunner()
    captured: dict[str, object] = {}

    def fake_run_counterfactual_experiment(*args, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            artifacts=SimpleNamespace(root=tmp_path / "artifacts"),
            model_dump=lambda mode="json": {"label": kwargs["label"]},
        )

    fake_api = SimpleNamespace(
        load_world=lambda **kwargs: SimpleNamespace(**kwargs),
        run_counterfactual_experiment=fake_run_counterfactual_experiment,
    )
    fake_render = SimpleNamespace(render_experiment=lambda result: {"label": "unused"})
    fake_validation = SimpleNamespace(validate_artifact_tree=lambda root: [])

    monkeypatch.setattr(whatif_experiment, "_whatif_api", lambda: fake_api)
    monkeypatch.setattr(whatif_experiment, "_whatif_render", lambda: fake_render)
    monkeypatch.setattr(
        whatif_experiment,
        "_whatif_validation",
        lambda: fake_validation,
    )

    result = runner.invoke(
        app,
        [
            "whatif",
            "experiment",
            "--source",
            "mail_archive",
            "--source-dir",
            str(tmp_path / "context_snapshot.json"),
            "--label",
            "heuristic_hold",
            "--event-id",
            "evt-001",
            "--counterfactual-prompt",
            "Keep this internal.",
            "--mode",
            "heuristic_baseline",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["mode"] == "heuristic_baseline"
    assert captured["forecast_backend"] == "heuristic_baseline"
