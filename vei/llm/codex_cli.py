from __future__ import annotations

import json
import os
import subprocess
import tempfile
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CodexExecOutput:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str
    raw_text: str


def _normalize_output_schema(node: Any) -> Any:
    if isinstance(node, list):
        return [_normalize_output_schema(item) for item in node]
    if not isinstance(node, dict):
        return node

    normalized = {key: _normalize_output_schema(value) for key, value in node.items()}
    properties = normalized.get("properties")
    if isinstance(properties, dict):
        normalized["properties"] = {
            key: _normalize_output_schema(value) for key, value in properties.items()
        }
        normalized["additionalProperties"] = False
        normalized["required"] = list(normalized["properties"].keys())
    elif normalized.get("type") == "object":
        normalized["additionalProperties"] = False
        normalized.setdefault("required", [])
    return normalized


@lru_cache(maxsize=1)
def _codex_exec_help_text() -> str:
    completed = subprocess.run(
        ["codex", "exec", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        return ""
    return "\n".join(
        part.strip()
        for part in (completed.stdout, completed.stderr)
        if part and part.strip()
    ).strip()


def _codex_exec_supports(flag: str) -> bool:
    return flag in _codex_exec_help_text()


def _codex_reasoning_effort() -> str:
    value = os.environ.get("VEI_CODEX_MODEL_REASONING_EFFORT", "low").strip().lower()
    if value in {"minimal", "low", "medium", "high", "xhigh"}:
        return value
    return "low"


def run_codex_json(
    *,
    model: str,
    prompt: str,
    output_schema: dict[str, Any],
    cwd: str | Path | None = None,
    timeout_s: int = 240,
) -> dict[str, Any]:
    result = run_codex_exec(
        model=model,
        prompt=prompt,
        output_schema=output_schema,
        cwd=cwd,
        timeout_s=timeout_s,
    )
    try:
        payload = json.loads(result.raw_text)
    except json.JSONDecodeError as exc:  # pragma: no cover
        snippet = result.raw_text.strip()
        if len(snippet) > 200:
            snippet = snippet[:200] + "..."
        raise RuntimeError(f"Codex returned non-JSON output: {snippet}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Codex returned JSON that was not an object.")
    return payload


def run_codex_exec(
    *,
    model: str,
    prompt: str,
    output_schema: dict[str, Any],
    cwd: str | Path | None = None,
    timeout_s: int = 240,
) -> CodexExecOutput:
    resolved_cwd = None if cwd is None else str(Path(cwd).expanduser().resolve())
    with tempfile.NamedTemporaryFile(
        prefix="codex_output_schema_",
        suffix=".json",
        delete=False,
    ) as schema_handle:
        schema_path = Path(schema_handle.name)
    with tempfile.NamedTemporaryFile(
        prefix="codex_last_message_",
        suffix=".json",
        delete=False,
    ) as output_handle:
        output_path = Path(output_handle.name)
    try:
        schema_path.write_text(
            json.dumps(
                _normalize_output_schema(output_schema),
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        command = [
            "codex",
            "exec",
            "--skip-git-repo-check",
            "-c",
            f'model_reasoning_effort="{_codex_reasoning_effort()}"',
            "--ephemeral",
            "--sandbox",
            "read-only",
            "-m",
            model,
            "--output-schema",
            str(schema_path),
            "-o",
            str(output_path),
            "-",
        ]
        if _codex_exec_supports("--ignore-rules"):
            command.insert(3, "--ignore-rules")
        if resolved_cwd is not None:
            command[2:2] = ["-C", resolved_cwd]

        completed = subprocess.run(
            command,
            input=prompt,
            text=True,
            capture_output=True,
            timeout=max(1, int(timeout_s)),
            cwd=resolved_cwd,
            check=False,
        )
        raw_text = ""
        if output_path.exists():
            raw_text = output_path.read_text(encoding="utf-8").strip()
        result = CodexExecOutput(
            command=command,
            returncode=int(completed.returncode),
            stdout=completed.stdout,
            stderr=completed.stderr,
            raw_text=raw_text,
        )
        if result.returncode != 0:
            details = "\n".join(
                item.strip()
                for item in (result.stdout, result.stderr)
                if item and item.strip()
            ).strip()
            raise RuntimeError(details or "Codex CLI invocation failed.")
        if not result.raw_text:
            raise RuntimeError("Codex CLI completed without writing a final message.")
        return result
    finally:
        schema_path.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)
