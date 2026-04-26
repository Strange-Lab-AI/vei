from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "clean_workspace.py"


def _run_clean(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_SCRIPT), "--root", str(root), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def test_clean_workspace_dry_run_keeps_files_in_place(tmp_path: Path) -> None:
    (tmp_path / ".artifacts").mkdir()
    (tmp_path / "_vei_out" / "quickstart").mkdir(parents=True)
    (tmp_path / "_vei_out" / "llm_live" / "latest").mkdir(parents=True)
    (tmp_path / "_vei_out" / "datasets").mkdir(parents=True)
    (tmp_path / "_vei_out" / "world_model_current").mkdir(parents=True)

    result = _run_clean(tmp_path, "--dry-run")

    assert result.returncode == 0, result.stderr
    assert "Would remove" in result.stdout
    assert (tmp_path / ".artifacts").exists()
    assert (tmp_path / "_vei_out" / "quickstart").exists()
    assert (tmp_path / "_vei_out" / "llm_live" / "latest").exists()
    assert (tmp_path / "_vei_out" / "datasets").exists()
    assert (tmp_path / "_vei_out" / "world_model_current").exists()


def test_clean_workspace_removes_local_generated_outputs(tmp_path: Path) -> None:
    (tmp_path / ".artifacts").mkdir()
    (tmp_path / "build").mkdir()
    (tmp_path / "output").mkdir()
    (tmp_path / ".coverage").write_text("coverage", encoding="utf-8")
    (tmp_path / ".coverage.integration").write_text("coverage", encoding="utf-8")
    (tmp_path / "pkg" / "__pycache__").mkdir(parents=True)
    (tmp_path / "pkg" / "__pycache__" / "module.pyc").write_bytes(b"pyc")
    (tmp_path / "pkg" / ".DS_Store").write_text("ds-store", encoding="utf-8")
    (tmp_path / "_vei_out" / "quickstart").mkdir(parents=True)
    (tmp_path / "_vei_out" / "dispatch_whatif").mkdir(parents=True)
    (tmp_path / "_vei_out" / "llm_live" / "latest").mkdir(parents=True)
    (tmp_path / "_vei_out" / "llm_live" / "debug_run").mkdir(parents=True)
    (tmp_path / "_vei_out" / "datasets" / "latest").mkdir(parents=True)
    (tmp_path / "_vei_out" / "world_model_current").mkdir(parents=True)
    (tmp_path / "_vei_out" / "world_model_current" / "latest.csv").write_text(
        "score\n",
        encoding="utf-8",
    )
    (tmp_path / "docs" / "examples" / "workspace" / ".artifacts").mkdir(parents=True)
    (tmp_path / ".venv" / "lib" / "__pycache__").mkdir(parents=True)
    (tmp_path / ".venv" / "lib" / "__pycache__" / "keep.pyc").write_bytes(b"pyc")

    result = _run_clean(tmp_path)

    assert result.returncode == 0, result.stderr
    assert not (tmp_path / ".artifacts").exists()
    assert not (tmp_path / "build").exists()
    assert not (tmp_path / "output").exists()
    assert not (tmp_path / ".coverage").exists()
    assert not (tmp_path / ".coverage.integration").exists()
    assert not (tmp_path / "pkg" / "__pycache__").exists()
    assert not (tmp_path / "pkg" / ".DS_Store").exists()
    assert (tmp_path / "_vei_out" / "quickstart").exists()
    assert (tmp_path / "_vei_out" / "dispatch_whatif").exists()
    assert (tmp_path / "_vei_out" / "llm_live" / "debug_run").exists()
    assert (tmp_path / "_vei_out" / "llm_live" / "latest").exists()
    assert (tmp_path / "_vei_out" / "datasets").exists()
    assert (tmp_path / "_vei_out" / "world_model_current" / "latest.csv").exists()
    assert (tmp_path / "docs" / "examples" / "workspace" / ".artifacts").exists()
    assert (tmp_path / ".venv" / "lib" / "__pycache__").exists()
    assert "Kept useful local outputs:" not in result.stdout


def test_clean_workspace_hard_prunes_old_vei_runs(tmp_path: Path) -> None:
    (tmp_path / "_vei_out" / "quickstart").mkdir(parents=True)
    (tmp_path / "_vei_out" / "dispatch_whatif").mkdir(parents=True)
    (tmp_path / "_vei_out" / "llm_live" / "latest").mkdir(parents=True)
    (tmp_path / "_vei_out" / "llm_live" / "debug_run").mkdir(parents=True)
    (tmp_path / "_vei_out" / "datasets" / "latest").mkdir(parents=True)
    (tmp_path / "_vei_out" / "world_model_current").mkdir(parents=True)
    (tmp_path / "_vei_out" / "world_model_current" / "latest.csv").write_text(
        "score\n",
        encoding="utf-8",
    )

    result = _run_clean(tmp_path, "--hard")

    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "_vei_out" / "quickstart").exists()
    assert not (tmp_path / "_vei_out" / "dispatch_whatif").exists()
    assert not (tmp_path / "_vei_out" / "llm_live" / "debug_run").exists()
    assert (tmp_path / "_vei_out" / "llm_live" / "latest").exists()
    assert (tmp_path / "_vei_out" / "datasets").exists()
    assert (tmp_path / "_vei_out" / "world_model_current" / "latest.csv").exists()
    assert "Kept useful local outputs:" in result.stdout
