from __future__ import annotations

import json
import os
import subprocess
from hashlib import sha256
from pathlib import Path
from typing import Sequence

from .models import (
    WhatIfForecastBackend,
    WhatIfCounterfactualEstimateResult,
    WhatIfLLMGeneratedMessage,
)

_DEFAULT_SIBLING_ROOT = Path(__file__).resolve().parents[3] / "ARP_Jepa_exp"


def default_forecast_backend() -> WhatIfForecastBackend:
    runtime = resolve_ejepa_runtime()
    if runtime is not None:
        return "e_jepa"
    return "e_jepa_proxy"


def resolve_ejepa_runtime(
    runtime_root: str | Path | None = None,
) -> tuple[Path, Path] | None:
    root = runtime_root
    if root is None:
        root = os.environ.get("VEI_EJEPA_ROOT")
    candidate_root = (
        Path(root).expanduser().resolve()
        if root is not None
        else _DEFAULT_SIBLING_ROOT.expanduser().resolve()
    )
    python_path = candidate_root / ".venv" / "bin" / "python"
    source_root = candidate_root / "src"
    if not python_path.exists() or not source_root.exists():
        return None
    return candidate_root, python_path


def run_ejepa_counterfactual(
    root: str | Path,
    *,
    prompt: str,
    source: str,
    source_dir: str | Path,
    thread_id: str,
    branch_event_id: str,
    llm_messages: Sequence[WhatIfLLMGeneratedMessage] | None = None,
    cache_root: str | Path | None = None,
    runtime_root: str | Path | None = None,
    device: str | None = None,
    epochs: int = 4,
    batch_size: int = 64,
    force_retrain: bool = False,
) -> WhatIfCounterfactualEstimateResult:
    runtime = resolve_ejepa_runtime(runtime_root)
    if runtime is None:
        return WhatIfCounterfactualEstimateResult(
            status="error",
            backend="e_jepa",
            prompt=prompt,
            summary="No local E-JEPA runtime was found.",
            notes=[
                "Set VEI_EJEPA_ROOT or place the ARP_Jepa_exp repo next to digital-enterprise-twin."
            ],
            error="E-JEPA runtime unavailable",
        )
    runtime_dir, python_path = runtime
    workspace_root = Path(root).expanduser().resolve()
    resolved_source_dir = Path(source_dir).expanduser().resolve()
    resolved_cache_root = (
        Path(cache_root).expanduser().resolve()
        if cache_root is not None
        else _default_cache_root(
            resolved_source_dir,
            thread_id=thread_id,
            branch_event_id=branch_event_id,
        )
    )
    request_root = workspace_root / ".whatif_ejepa"
    request_root.mkdir(parents=True, exist_ok=True)
    request_path = request_root / "forecast_request.json"
    response_path = request_root / "forecast_response.json"
    request = {
        "source": str(source).strip().lower() or "mail_archive",
        "source_dir": str(resolved_source_dir),
        "cache_root": str(resolved_cache_root),
        "thread_id": thread_id,
        "branch_event_id": branch_event_id,
        "prompt": prompt,
        "device": device or os.environ.get("VEI_EJEPA_DEVICE", ""),
        "epochs": epochs,
        "batch_size": batch_size,
        "force_retrain": force_retrain,
        "llm_messages": [
            message.model_dump(mode="json") for message in (llm_messages or [])
        ],
    }
    request_path.write_text(json.dumps(request, indent=2), encoding="utf-8")
    env = os.environ.copy()
    pythonpath_entries = [
        str(Path(__file__).resolve().parents[2]),
        str(runtime_dir / "src"),
    ]
    existing_pythonpath = env.get("PYTHONPATH", "")
    if existing_pythonpath:
        pythonpath_entries.append(existing_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)

    command = [
        str(python_path),
        "-m",
        "vei.whatif.ejepa_bridge",
        "forecast",
        "--request",
        str(request_path),
        "--output",
        str(response_path),
    ]
    completed = subprocess.run(
        command,
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        error_text = completed.stderr.strip() or completed.stdout.strip()
        return WhatIfCounterfactualEstimateResult(
            status="error",
            backend="e_jepa",
            prompt=prompt,
            summary="The E-JEPA forecast run failed.",
            notes=[line for line in error_text.splitlines() if line.strip()][:5],
            error=error_text or "E-JEPA forecast subprocess failed",
        )
    if not response_path.exists():
        return WhatIfCounterfactualEstimateResult(
            status="error",
            backend="e_jepa",
            prompt=prompt,
            summary="The E-JEPA forecast run did not write a response.",
            error="missing E-JEPA response file",
        )
    return WhatIfCounterfactualEstimateResult.model_validate_json(
        response_path.read_text(encoding="utf-8")
    )


def _default_cache_root(
    source_dir: Path,
    *,
    thread_id: str,
    branch_event_id: str,
) -> Path:
    digest = sha256(
        f"{source_dir}|{thread_id}|{branch_event_id}".encode("utf-8")
    ).hexdigest()[:12]
    return Path("_vei_out/whatif_ejepa") / f"historical_{digest}"
