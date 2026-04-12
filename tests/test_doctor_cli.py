from __future__ import annotations

import json
import socket
from pathlib import Path

from typer.testing import CliRunner

import vei.cli.vei_doctor as doctor
from vei.cli.vei import app


def test_doctor_reports_busy_ports_missing_env_and_missing_workspace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / "pyproject.toml").write_text(
        "[project]\nname='vei'\n", encoding="utf-8"
    )
    (repo_root / "Makefile").write_text("setup:\n\t@true\n", encoding="utf-8")
    (repo_root / "vei").mkdir()
    broken_workspace = repo_root / "broken_workspace"
    broken_workspace.mkdir()

    monkeypatch.chdir(repo_root)
    monkeypatch.setattr(doctor.sys, "prefix", str(repo_root / ".venv"))
    monkeypatch.setattr(doctor.sys, "base_prefix", str(repo_root / "/usr/bin/python"))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as busy:
        busy.bind(("127.0.0.1", 0))
        busy.listen(1)
        busy_port = busy.getsockname()[1]

        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                "doctor",
                "--root",
                str(broken_workspace),
                "--studio-port",
                str(busy_port),
                "--gateway-port",
                str(_free_port()),
                "--json",
            ],
        )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    checks = {item["name"]: item for item in payload["checks"]}
    assert checks["studio_port"]["status"] == "error"
    assert checks["env_file"]["status"] == "warning"
    assert checks["openai_api_key"]["status"] == "warning"
    assert checks["workspace"]["status"] == "error"


def test_doctor_text_output_uses_real_command_names(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / "pyproject.toml").write_text(
        "[project]\nname='vei'\n", encoding="utf-8"
    )
    (repo_root / "Makefile").write_text("setup:\n\t@true\n", encoding="utf-8")
    (repo_root / "vei").mkdir()

    monkeypatch.chdir(repo_root)
    monkeypatch.setattr(doctor.sys, "prefix", str(repo_root / ".venv"))
    monkeypatch.setattr(doctor.sys, "base_prefix", str(repo_root / "/usr/bin/python"))

    runner = CliRunner()
    result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 0
    assert "vei doctor -> vei quickstart run" in result.output
    assert "vei doctor run" not in result.output


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
