from __future__ import annotations

import json
import socket
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import typer

from vei.workspace.api import WORKSPACE_MANIFEST

app = typer.Typer(
    add_completion=False,
    invoke_without_command=True,
    help="Preflight checks for local VEI development and quickstart runs.",
)


@dataclass(frozen=True)
class DoctorCheck:
    name: str
    status: str
    message: str
    fix: str


@app.callback()
def doctor_command(
    root: Path = typer.Option(
        Path("_vei_out/quickstart"),
        help="Workspace root that quickstart should use or reuse.",
    ),
    studio_port: int = typer.Option(3011, help="Studio port to validate."),
    gateway_port: int = typer.Option(3012, help="Twin Gateway port to validate."),
    json_output: bool = typer.Option(
        False, "--json", help="Emit machine-readable output."
    ),
) -> None:
    checks = run_doctor_checks(
        repo_root=Path.cwd(),
        workspace_root=root,
        studio_port=studio_port,
        gateway_port=gateway_port,
    )
    payload = {
        "ok": not any(item.status == "error" for item in checks),
        "checks": [asdict(item) for item in checks],
    }
    if json_output:
        typer.echo(json.dumps(payload, indent=2))
    else:
        for item in checks:
            typer.echo(f"[{item.status.upper()}] {item.name}: {item.message}")
            typer.echo(f"  Fix: {item.fix}")
        typer.echo("")
        typer.echo(
            "Next path: vei doctor -> vei quickstart run -> vei twin status -> vei eval benchmark"
        )
    raise typer.Exit(0 if payload["ok"] else 1)


def run_doctor_checks(
    *,
    repo_root: Path,
    workspace_root: Path,
    studio_port: int,
    gateway_port: int,
) -> list[DoctorCheck]:
    root = repo_root.expanduser().resolve()
    workspace = workspace_root.expanduser().resolve()
    env_values = _read_env_file(root / ".env")
    checks = [
        _python_check(),
        _venv_check(),
        _repo_check(root),
        _port_check("studio_port", studio_port),
        _port_check("gateway_port", gateway_port),
        _env_file_check(root / ".env"),
        _env_key_check(env_values, "OPENAI_API_KEY"),
        _workspace_check(workspace),
    ]
    return checks


def _python_check() -> DoctorCheck:
    version = sys.version_info
    if (version.major, version.minor) >= (3, 11):
        return DoctorCheck(
            name="python",
            status="ok",
            message=f"Python {version.major}.{version.minor} is available.",
            fix="Use this interpreter.",
        )
    return DoctorCheck(
        name="python",
        status="error",
        message=f"Python {version.major}.{version.minor} is active; VEI expects 3.11 or newer.",
        fix="Create a Python 3.11 virtual environment, then rerun `vei doctor`.",
    )


def _venv_check() -> DoctorCheck:
    if sys.prefix != getattr(sys, "base_prefix", sys.prefix):
        return DoctorCheck(
            name="virtualenv",
            status="ok",
            message=f"Using virtual environment at {sys.prefix}.",
            fix="Keep this environment active.",
        )
    return DoctorCheck(
        name="virtualenv",
        status="error",
        message="No virtual environment is active.",
        fix="Run `python3.11 -m venv .venv && . .venv/bin/activate`.",
    )


def _repo_check(repo_root: Path) -> DoctorCheck:
    required = [repo_root / "pyproject.toml", repo_root / "Makefile", repo_root / "vei"]
    missing = [path.name for path in required if not path.exists()]
    if not missing:
        return DoctorCheck(
            name="repo",
            status="ok",
            message=f"Repo files are present in {repo_root}.",
            fix="Work from this checkout.",
        )
    return DoctorCheck(
        name="repo",
        status="error",
        message=f"Missing repo files: {', '.join(missing)}.",
        fix="Run the command from the VEI repository root.",
    )


def _port_check(name: str, port: int) -> DoctorCheck:
    if _port_is_free(port):
        return DoctorCheck(
            name=name,
            status="ok",
            message=f"Port {port} is free.",
            fix=f"Use port {port}.",
        )
    return DoctorCheck(
        name=name,
        status="error",
        message=f"Port {port} is already in use.",
        fix=f"Stop the process on {port} or choose a different port with `--{name.replace('_', '-')}`.",
    )


def _env_file_check(env_path: Path) -> DoctorCheck:
    if env_path.exists():
        return DoctorCheck(
            name="env_file",
            status="ok",
            message=f"Found {env_path.name}.",
            fix="Keep environment values in this file.",
        )
    return DoctorCheck(
        name="env_file",
        status="warning",
        message=".env is missing.",
        fix="Create `.env` if you plan to run LLM-backed commands.",
    )


def _env_key_check(env_values: dict[str, str], key: str) -> DoctorCheck:
    if key in env_values and env_values[key].strip():
        return DoctorCheck(
            name=key.lower(),
            status="ok",
            message=f"{key} is set.",
            fix="Reuse this key for live model checks.",
        )
    return DoctorCheck(
        name=key.lower(),
        status="warning",
        message=f"{key} is missing.",
        fix=f"Add `{key}=...` to `.env` before running `make llm-live` or `vei llm-test run`.",
    )


def _workspace_check(workspace_root: Path) -> DoctorCheck:
    if not workspace_root.exists():
        return DoctorCheck(
            name="workspace",
            status="ok",
            message=f"{workspace_root} does not exist yet; quickstart can create it.",
            fix=f"Run `vei quickstart run --root {workspace_root}`.",
        )
    manifest_path = workspace_root / WORKSPACE_MANIFEST
    if manifest_path.exists():
        return DoctorCheck(
            name="workspace",
            status="ok",
            message=f"Found workspace state at {manifest_path}.",
            fix="Reuse this workspace or replace it with quickstart.",
        )
    return DoctorCheck(
        name="workspace",
        status="error",
        message=f"{workspace_root} exists but is missing {WORKSPACE_MANIFEST}.",
        fix=f"Remove {workspace_root} or recreate it with `vei quickstart run --root {workspace_root}`.",
    )


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def _port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


__all__ = ["DoctorCheck", "app", "run_doctor_checks"]
