from __future__ import annotations

import sys
from pathlib import Path

import pytest

from vei.whatif.benchmark_runtime import (
    _default_prediction_runtime_root,
    _run_bridge_command,
)
from vei.whatif.ejepa_bridge import _clamp_count, _delta_as_count
from vei.whatif.ejepa import (
    default_forecast_backend,
    resolve_ejepa_runtime,
    resolve_reference_backend_checkpoint,
)


def test_default_forecast_backend_falls_back_without_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "vei.whatif.ejepa.resolve_reference_backend_checkpoint",
        lambda *_args: None,
    )
    monkeypatch.setattr("vei.whatif.ejepa.resolve_ejepa_runtime", lambda *_args: None)

    assert default_forecast_backend() == "heuristic_baseline"


def test_default_forecast_backend_prefers_reference_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkpoint = tmp_path / "model.pt"
    checkpoint.write_bytes(b"checkpoint")
    monkeypatch.setenv("VEI_REFERENCE_BACKEND_CHECKPOINT", str(checkpoint))
    monkeypatch.setattr("vei.whatif.ejepa.resolve_ejepa_runtime", lambda *_args: None)

    assert default_forecast_backend() == "reference"
    assert resolve_reference_backend_checkpoint() == checkpoint.resolve()


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


def test_resolve_ejepa_runtime_prefers_vendored_package(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VEI_EJEPA_ROOT", raising=False)

    resolved = resolve_ejepa_runtime()

    assert resolved is not None
    assert resolved[0].name == "digital-enterprise-twin"
    assert resolved[1] == Path(sys.executable).resolve()


def test_resolve_ejepa_runtime_prefers_vendored_root_without_importable_package(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VEI_EJEPA_ROOT", raising=False)
    monkeypatch.setattr("importlib.util.find_spec", lambda *_args, **_kwargs: None)

    resolved = resolve_ejepa_runtime()

    assert resolved is not None
    assert resolved[0].name == "digital-enterprise-twin"
    assert resolved[1] == Path(sys.executable).resolve()


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


def test_reference_runtime_scratch_defaults_to_vei_out(tmp_path: Path) -> None:
    checkpoint = tmp_path / "reference_backend" / "model.pt"
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_bytes(b"checkpoint")

    runtime_root = _default_prediction_runtime_root(checkpoint)

    assert runtime_root.parts[-3:] == (
        "_vei_out",
        "reference_backend_runtime",
        "reference_backend",
    )
    assert runtime_root.parent.parent.name == "_vei_out"
    assert runtime_root != checkpoint.parent
