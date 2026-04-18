from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from vei.project_settings import find_agents_file, load_agents_settings

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_CONFIG_MISSING = 4


@dataclass(frozen=True)
class DynamicsValidationResult:
    exit_code: int
    message: str


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_dynamics_metrics(
    metrics_path: str | Path,
    *,
    agents_file: str | Path | None = None,
) -> DynamicsValidationResult:
    resolved_agents = find_agents_file(agents_file)
    if not resolved_agents.exists():
        return DynamicsValidationResult(
            exit_code=EXIT_CONFIG_MISSING,
            message=f"agents config missing: {resolved_agents}",
        )

    resolved_metrics = Path(metrics_path).expanduser().resolve()
    if not resolved_metrics.exists():
        return DynamicsValidationResult(
            exit_code=EXIT_CONFIG_MISSING,
            message=f"dynamics metrics missing: {resolved_metrics}",
        )

    settings = load_agents_settings(resolved_agents)
    dynamics = settings.get("dynamics")
    if not isinstance(dynamics, dict):
        return DynamicsValidationResult(
            exit_code=EXIT_CONFIG_MISSING,
            message=f"dynamics settings missing in {resolved_agents}",
        )
    thresholds = dynamics.get("evaluation_thresholds")
    if not isinstance(thresholds, dict):
        return DynamicsValidationResult(
            exit_code=EXIT_CONFIG_MISSING,
            message=f"dynamics evaluation thresholds missing in {resolved_agents}",
        )

    metrics = _load_json(resolved_metrics)
    factual_auroc = float(metrics.get("factual_next_event_auroc") or 0.0)
    rank_pct = float(metrics.get("counterfactual_rank_pct") or 0.0)
    calibration_ece = float(metrics.get("calibration_ece") or 0.0)

    factual_threshold = float(thresholds.get("factual_next_event_auroc") or 0.0)
    rank_threshold = float(thresholds.get("counterfactual_rank_pct") or 0.0)
    ece_threshold = float(thresholds.get("calibration_ece_max") or 0.0)

    failures: list[str] = []
    if factual_auroc < factual_threshold:
        failures.append(
            f"factual_next_event_auroc={factual_auroc:.3f}<{factual_threshold:.3f}"
        )
    if rank_pct < rank_threshold:
        failures.append(
            f"counterfactual_rank_pct={rank_pct:.3f}<{rank_threshold:.3f}"
        )
    if calibration_ece > ece_threshold:
        failures.append(f"calibration_ece={calibration_ece:.3f}>{ece_threshold:.3f}")

    if failures:
        return DynamicsValidationResult(
            exit_code=EXIT_FAILURE,
            message=(
                "dynamics metrics failed thresholds: "
                + "; ".join(failures)
                + f" (metrics={resolved_metrics})"
            ),
        )

    return DynamicsValidationResult(
        exit_code=EXIT_OK,
        message=(
            "dynamics metrics: "
            f"factual_next_event_auroc={factual_auroc:.3f} "
            f"counterfactual_rank_pct={rank_pct:.3f} "
            f"calibration_ece={calibration_ece:.3f} "
            f"thresholds_ok={resolved_metrics}"
        ),
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate dynamics metrics against .agents.yml thresholds."
    )
    parser.add_argument(
        "--metrics",
        required=True,
        help="Path to the dynamics metrics JSON file.",
    )
    parser.add_argument(
        "--agents-file",
        default=None,
        help="Optional path to .agents.yml.",
    )
    args = parser.parse_args()
    result = validate_dynamics_metrics(
        args.metrics,
        agents_file=args.agents_file,
    )
    print(result.message)
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
