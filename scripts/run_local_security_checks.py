from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_TEXTUAL_SUFFIXES = {
    ".cfg",
    ".conf",
    ".css",
    ".env",
    ".html",
    ".ini",
    ".js",
    ".json",
    ".jsx",
    ".md",
    ".py",
    ".rst",
    ".sh",
    ".svg",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".xml",
    ".yaml",
    ".yml",
}
_TEXTUAL_FILENAMES = {
    "Dockerfile",
    "Makefile",
    ".env",
    ".env.example",
}
_SECRET_SCAN_EXCLUDED_PREFIXES = (
    ".artifacts/",
    "_vei_out/",
    "data/enron/reference_backend/",
    "docs/assets/",
    "docs/examples/",
)


def _git_output(root: Path, *args: str) -> list[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _head_exists(root: Path) -> bool:
    result = subprocess.run(
        ["git", "rev-parse", "--verify", "HEAD"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def collect_changed_files(root: Path) -> list[Path]:
    candidates: set[Path] = set()

    diff_args = ["diff", "--name-only", "--diff-filter=ACMR"]
    if _head_exists(root):
        for path in _git_output(root, *diff_args, "HEAD", "--"):
            candidates.add(root / path)
    else:
        for path in _git_output(root, "ls-files"):
            candidates.add(root / path)

    for path in _git_output(root, *diff_args, "--"):
        candidates.add(root / path)

    for path in _git_output(root, "ls-files", "--others", "--exclude-standard"):
        candidates.add(root / path)

    return sorted(path for path in candidates if path.is_file())


def _run(command: list[str], *, cwd: Path) -> None:
    subprocess.run(command, cwd=cwd, check=True)


def _bandit_executable() -> list[str]:
    return [sys.executable, "-m", "bandit"]


def _console_script(name: str) -> Path:
    return Path(sys.executable).parent / name


def _looks_textual(path: Path) -> bool:
    if path.suffix.lower() in _TEXTUAL_SUFFIXES:
        return True
    if path.name in _TEXTUAL_FILENAMES:
        return True
    try:
        chunk = path.read_bytes()[:4096]
    except OSError:
        return False
    if not chunk:
        return True
    if b"\x00" in chunk:
        return False
    try:
        chunk.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


def _include_in_secret_scan(root: Path, path: Path) -> bool:
    relative = path.relative_to(root).as_posix()
    for prefix in _SECRET_SCAN_EXCLUDED_PREFIXES:
        if relative.startswith(prefix):
            return False
    return _looks_textual(path)


def run_local_security_checks(root: Path) -> None:
    changed_files = collect_changed_files(root)
    changed_python = [
        path
        for path in changed_files
        if path.suffix == ".py"
        and path.relative_to(root).parts[0] in {"vei", "scripts"}
    ]

    if changed_python:
        print("Running Bandit on changed Python files:")
        for path in changed_python:
            print(f"  - {path.relative_to(root)}")
        _run(
            _bandit_executable()
            + ["-q", "-ll", *[str(path.relative_to(root)) for path in changed_python]],
            cwd=root,
        )
    else:
        print("No changed Python files for local Bandit.")

    if not changed_files:
        print("No changed files for local detect-secrets.")
        return

    artifacts_dir = root / ".artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    relative_paths = [
        str(path.relative_to(root))
        for path in changed_files
        if _include_in_secret_scan(root, path)
        if path.relative_to(root) != Path(".secrets.baseline")
    ]
    if not relative_paths:
        print("No textual changed files for local detect-secrets after filtering.")
        return
    baseline = root / ".secrets.baseline"
    if baseline.exists():
        print("Running detect-secrets-hook on changed files.")
        _run(
            [
                str(_console_script("detect-secrets-hook")),
                "--baseline",
                str(baseline),
                *relative_paths,
            ],
            cwd=root,
        )
        return

    output_path = artifacts_dir / "detect-secrets-local.json"
    print(
        "No .secrets.baseline found; running advisory detect-secrets on changed files."
    )
    result = subprocess.run(
        [str(_console_script("detect-secrets")), "scan", *relative_paths],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    output_path.write_text(result.stdout, encoding="utf-8")


def main() -> None:
    run_local_security_checks(Path.cwd())


if __name__ == "__main__":
    main()
