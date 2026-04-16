from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._constants import (
    BUSINESS_STATE_COMPARISON_FILE,
    BUSINESS_STATE_COMPARISON_OVERVIEW_FILE,
    CONTEXT_SNAPSHOT_FILE,
    EXPERIMENT_RESULT_FILE,
    WORKSPACE_DIRECTORY,
)
from .ranking import get_objective_pack


@dataclass(frozen=True)
class SavedWhatIfBundle:
    workspace_root: Path
    bundle_root: Path

    def load_json(self, filename: str) -> dict[str, Any] | None:
        path = self.bundle_root / filename
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def source_dir_text(self) -> str:
        saved_snapshot = self.workspace_root / CONTEXT_SNAPSHOT_FILE
        if saved_snapshot.exists():
            return str(saved_snapshot.resolve())
        return str(self.workspace_root)


def resolve_saved_whatif_bundle(root: str | Path) -> SavedWhatIfBundle | None:
    workspace_root = Path(root).expanduser().resolve()
    if workspace_root.name != WORKSPACE_DIRECTORY:
        return None
    bundle_root = workspace_root.parent
    if not (bundle_root / EXPERIMENT_RESULT_FILE).exists():
        return None
    return SavedWhatIfBundle(workspace_root=workspace_root, bundle_root=bundle_root)


def build_saved_ranked_result_payload(
    bundle: SavedWhatIfBundle,
    *,
    objective_pack_id: str = "contain_exposure",
) -> dict[str, Any] | None:
    comparison_payload = bundle.load_json(BUSINESS_STATE_COMPARISON_FILE)
    experiment_payload = bundle.load_json(EXPERIMENT_RESULT_FILE)
    if comparison_payload is None:
        return None

    requested_objective_pack_id = str(objective_pack_id).strip()
    saved_objective_pack_id = _saved_objective_pack_id(
        comparison_payload,
        default_objective_pack_id="",
    )
    resolved_objective_pack_id = (
        saved_objective_pack_id or requested_objective_pack_id or "contain_exposure"
    )
    try:
        objective_pack = get_objective_pack(resolved_objective_pack_id).model_dump(
            mode="json"
        )
    except KeyError:
        objective_pack = {
            "pack_id": resolved_objective_pack_id,
            "title": resolved_objective_pack_id.replace("_", " ").title(),
            "summary": "Saved business-state comparison",
            "weights": {},
            "evidence_labels": [],
        }

    transformed_candidates: list[dict[str, Any]] = []
    for index, candidate in enumerate(
        comparison_payload.get("candidates", []), start=1
    ):
        if not isinstance(candidate, dict):
            continue
        business_state_change = candidate.get("business_state_change")
        if not isinstance(business_state_change, dict):
            business_state_change = {}
        net_effect_score = business_state_change.get("net_effect_score", 0.0)
        try:
            overall_score = float(net_effect_score)
        except (TypeError, ValueError):
            overall_score = 0.0
        rank = _candidate_rank_value(candidate.get("rank"), default=index)
        transformed_candidates.append(
            {
                "intervention": {
                    "label": candidate.get("label") or f"Candidate {index}",
                    "prompt": candidate.get("prompt") or "",
                },
                "rank": rank,
                "rollout_count": 0,
                "saved_result": True,
                "average_outcome_signals": {},
                "outcome_score": {
                    "objective_pack_id": objective_pack["pack_id"],
                    "overall_score": overall_score,
                    "components": {},
                    "evidence": [],
                },
                "reason": (
                    ((candidate.get("forecast") or {}).get("summary"))
                    or business_state_change.get("summary")
                    or ""
                ),
                "rollouts": [],
                "business_state_change": business_state_change,
            }
        )
    transformed_candidates.sort(key=lambda candidate: int(candidate.get("rank", 0)))

    return {
        "version": "1",
        "label": comparison_payload.get("label") or "saved_business_state_comparison",
        "objective_pack": objective_pack,
        "selection": (
            experiment_payload.get("selection", {})
            if isinstance(experiment_payload, dict)
            else {}
        ),
        "materialization": (
            experiment_payload.get("materialization", {})
            if isinstance(experiment_payload, dict)
            else {}
        ),
        "baseline": (
            experiment_payload.get("baseline", {})
            if isinstance(experiment_payload, dict)
            else {}
        ),
        "recommended_candidate_label": (
            transformed_candidates[0]["intervention"]["label"]
            if transformed_candidates
            else ""
        ),
        "candidates": transformed_candidates,
        "artifacts": {
            "root": ".",
            "result_json_path": BUSINESS_STATE_COMPARISON_FILE,
            "overview_markdown_path": BUSINESS_STATE_COMPARISON_OVERVIEW_FILE,
        },
    }


def _saved_objective_pack_id(
    comparison_payload: dict[str, Any],
    *,
    default_objective_pack_id: str,
) -> str:
    objective_pack = comparison_payload.get("objective_pack")
    if isinstance(objective_pack, dict):
        pack_id = str(objective_pack.get("pack_id", "")).strip()
        if pack_id:
            return pack_id
    pack_id = str(comparison_payload.get("objective_pack_id", "")).strip()
    if pack_id:
        return pack_id
    return default_objective_pack_id


def _candidate_rank_value(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


__all__ = [
    "SavedWhatIfBundle",
    "build_saved_ranked_result_payload",
    "resolve_saved_whatif_bundle",
]
