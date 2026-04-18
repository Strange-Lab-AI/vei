"""Heuristic baseline dynamics backend.

It shifts event counts, escalations, approvals, external sends, and risk up or
down from intervention tags. It is a reasonable demo baseline, not a learned
model.
"""

from __future__ import annotations

from vei.dynamics.models import (
    BackendInfo,
    BusinessHeads,
    DeterminismManifest,
    DynamicsRequest,
    DynamicsResponse,
    PointInterval,
)
from vei.whatif.api import macro_delta_from_prompt

_INTERVENTION_KEYWORDS: dict[str, set[str]] = {
    "legal": {"legal", "compliance"},
    "hold": {"hold", "pause_forward"},
    "clarify": {"reply_immediately", "clarify_owner"},
    "status_only": {"status_only"},
    "executive_gate": {"executive_gate"},
    "attachment_removed": {"attachment_removed"},
    "external_removed": {"external_removed"},
    "send_now": {"send_now", "widen_loop"},
}


def _extract_tags(text: str) -> set[str]:
    lower = text.lower()
    tags: set[str] = set()
    for tag, keywords in _INTERVENTION_KEYWORDS.items():
        for kw in keywords:
            if kw in lower:
                tags.add(kw)
    return tags


class HeuristicBaseline:
    """Tag-driven heuristic forecast backend."""

    def forecast(self, request: DynamicsRequest) -> DynamicsResponse:
        action_text = ""
        if request.candidate_action is not None:
            action_text = (
                f"{request.candidate_action.label} "
                f"{request.candidate_action.description}"
            )
        tags = _extract_tags(action_text)

        risk_shift = 0.0
        spread_shift = 0.0
        escalation_shift = 0.0
        approval_shift = 0.0
        load_shift = 0.0
        drag_shift = 0.0

        if {"legal", "compliance"} & tags:
            escalation_shift -= 0.15
            approval_shift += 0.10
            risk_shift -= 0.18
        if {"hold", "pause_forward"} & tags:
            spread_shift -= 0.25
            risk_shift -= 0.20
            load_shift -= 0.10
        if {"reply_immediately", "clarify_owner"} & tags:
            drag_shift -= 0.12
            load_shift -= 0.08
            risk_shift -= 0.12
        if "status_only" in tags:
            spread_shift -= 0.08
            risk_shift -= 0.08
        if "executive_gate" in tags:
            escalation_shift -= 0.14
            approval_shift += 0.10
            risk_shift -= 0.14
        if "external_removed" in tags:
            spread_shift -= 0.30
            risk_shift -= 0.24
        if {"send_now", "widen_loop"} & tags:
            spread_shift += 0.12
            load_shift += 0.08
            risk_shift += 0.12
        macro_delta = macro_delta_from_prompt(action_text)

        return DynamicsResponse(
            backend_id="heuristic_baseline",
            backend_version="1.0.0",
            business_heads=BusinessHeads(
                risk=PointInterval(point=max(-1.0, min(1.0, risk_shift))),
                spread=PointInterval(point=max(-1.0, min(1.0, spread_shift))),
                escalation=PointInterval(
                    point=max(-1.0, min(1.0, escalation_shift)),
                ),
                approval=PointInterval(
                    point=max(-1.0, min(1.0, approval_shift)),
                ),
                load=PointInterval(point=max(-1.0, min(1.0, load_shift))),
                drag=PointInterval(point=max(-1.0, min(1.0, drag_shift))),
                stock_return_5d=PointInterval(
                    point=macro_delta["stock_return_5d_delta"]
                ),
                credit_action_30d=PointInterval(
                    point=macro_delta["credit_action_30d_delta"]
                ),
                ferc_action_180d=PointInterval(
                    point=macro_delta["ferc_action_180d_delta"]
                ),
            ),
        )

    def describe(self) -> BackendInfo:
        return BackendInfo(
            name="heuristic_baseline",
            version="1.0.0",
            backend_type="heuristic",
            deterministic=True,
            metadata={
                "note": "Tag-driven heuristic forecast. Not a learned model.",
            },
        )

    def determinism_manifest(self) -> DeterminismManifest:
        return DeterminismManifest(
            backend_id="heuristic_baseline",
            backend_version="1.0.0",
            notes=["Deterministic tag-based heuristic; no model weights."],
        )
