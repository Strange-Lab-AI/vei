from __future__ import annotations

from pathlib import Path

import pytest

from vei.whatif.benchmark_runtime import _run_bridge_command
from vei.whatif.ejepa_bridge import _clamp_count, _delta_as_count
from vei.whatif.ejepa import default_forecast_backend, resolve_ejepa_runtime


def test_default_forecast_backend_falls_back_without_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("vei.whatif.ejepa.resolve_ejepa_runtime", lambda *_args: None)

    assert default_forecast_backend() == "heuristic_baseline"


def test_resolve_ejepa_runtime_requires_python_and_source(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    assert resolve_ejepa_runtime(runtime_root) is None

    python_path = runtime_root / ".venv" / "bin" / "python"
    python_path.parent.mkdir(parents=True, exist_ok=True)
    python_path.write_text("#!/usr/bin/env python\n", encoding="utf-8")
    assert resolve_ejepa_runtime(runtime_root) is None

    source_root = runtime_root / "src"
    source_root.mkdir(parents=True, exist_ok=True)

    resolved = resolve_ejepa_runtime(runtime_root)
    assert resolved is not None
    assert resolved[0] == runtime_root.resolve()
    assert resolved[1] == python_path.resolve()


def test_benchmark_runtime_bridge_requires_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "vei.whatif.benchmark_runtime.resolve_ejepa_runtime",
        lambda *_args: None,
    )

    with pytest.raises(RuntimeError, match="No torch runtime was found"):
        _run_bridge_command(
            command_name="train",
            request={"build_root": str(tmp_path / "build")},
            output_root=tmp_path / "out",
            runtime_root=tmp_path / "missing_runtime",
        )


def test_ejepa_count_helpers_treat_nan_as_zero() -> None:
    assert _delta_as_count(float("nan"), 2.0) == 0
    assert _delta_as_count(5.0, float("nan")) == 0
    assert _clamp_count(float("nan"), 2.0, ceiling=10) == 0
    assert _clamp_count(5.0, float("nan"), ceiling=10) == 0
