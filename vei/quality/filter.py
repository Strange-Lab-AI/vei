from __future__ import annotations

import hashlib
import json
from typing import Dict, Iterable, List, Set

from vei.corpus.models import GeneratedWorkflowSpec
from vei.scenario_engine.api import compile_workflow_spec
from vei.scenario_runner import validate_workflow as static_validate_workflow

from .models import QualityFilterReport, WorkflowQualityScore


DEFAULT_REALISM_THRESHOLD = 0.55


def filter_workflow_corpus(
    workflows: Iterable[GeneratedWorkflowSpec],
    *,
    realism_threshold: float = DEFAULT_REALISM_THRESHOLD,
) -> QualityFilterReport:
    seen_fingerprints: Set[str] = set()
    accepted: List[WorkflowQualityScore] = []
    rejected: List[WorkflowQualityScore] = []
    structure_counter: Dict[str, int] = {}

    for workflow in workflows:
        fp = workflow_fingerprint(workflow.spec)
        structure_key = _structure_key(workflow.spec)
        structure_counter[structure_key] = structure_counter.get(structure_key, 0) + 1
        novelty = 1.0 / float(structure_counter[structure_key])
        realism = realism_score(workflow.spec)
        runnability = runnability_score(workflow.spec)
        reasons: List[str] = []
        accepted_flag = True

        if fp in seen_fingerprints:
            reasons.append("duplicate_fingerprint")
            accepted_flag = False
        if realism < realism_threshold:
            reasons.append(f"realism_below_threshold:{realism:.3f}")
            accepted_flag = False
        if runnability < 1.0:
            reasons.append("static_runnability_failed")
            accepted_flag = False
        if novelty < 0.2:
            reasons.append(f"low_structural_novelty:{novelty:.3f}")
            accepted_flag = False

        score = WorkflowQualityScore(
            scenario_id=workflow.scenario_id,
            fingerprint=fp,
            realism_score=realism,
            novelty_score=novelty,
            runnability_score=runnability,
            accepted=accepted_flag,
            reasons=reasons,
        )

        if accepted_flag:
            seen_fingerprints.add(fp)
            accepted.append(score)
        else:
            rejected.append(score)

    return QualityFilterReport(accepted=accepted, rejected=rejected)


def workflow_fingerprint(spec: Dict[str, object]) -> str:
    normalized = _normalized_spec(spec)
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def realism_score(spec: Dict[str, object]) -> float:
    score = 0.0
    objective = spec.get("objective", {})
    if isinstance(objective, dict) and objective.get("statement"):
        score += 0.2

    steps = spec.get("steps", [])
    if isinstance(steps, list):
        count = len(steps)
        if 4 <= count <= 12:
            score += 0.2
        elif count >= 3:
            score += 0.1
        services = {_tool_service(step) for step in steps if isinstance(step, dict)}
        services.discard("")
        score += min(0.3, 0.1 * len(services))
        if {"browser", "mail", "slack"} <= services:
            score += 0.15
        if "tickets" in services or "docs" in services:
            score += 0.1
        if "db" in services:
            score += 0.05
        if "crm" in services:
            score += 0.05
        if "erp" in services:
            score += 0.05
        if "okta" in services:
            score += 0.05
        if "servicedesk" in services:
            score += 0.05
        if {"okta", "servicedesk"} <= services:
            score += 0.05

    approvals = spec.get("approvals", [])
    if isinstance(approvals, list) and approvals:
        score += 0.05

    constraints = spec.get("constraints", [])
    if isinstance(constraints, list) and constraints:
        score += 0.05

    return max(0.0, min(1.0, score))


def runnability_score(spec: Dict[str, object]) -> float:
    try:
        workflow = compile_workflow_spec(spec)
        report = static_validate_workflow(workflow)
        return 1.0 if report.ok else 0.0
    except Exception:  # noqa: BLE001
        return 0.0


def _tool_service(step: Dict[str, object]) -> str:
    tool = str(step.get("tool", ""))
    if "." not in tool:
        return ""
    service = tool.split(".", 1)[0]
    if service in {"salesforce", "hubspot"}:
        return "crm"
    if service in {"xero", "netsuite", "dynamics", "quickbooks"}:
        return "erp"
    return service


def _structure_key(spec: Dict[str, object]) -> str:
    steps = spec.get("steps", [])
    if not isinstance(steps, list):
        return "none"
    services = [_tool_service(step) for step in steps if isinstance(step, dict)]
    return "|".join(services)


def _normalized_spec(spec: Dict[str, object]) -> Dict[str, object]:
    normalized = dict(spec)
    metadata = normalized.get("metadata")
    if isinstance(metadata, dict):
        metadata = {k: v for k, v in metadata.items() if k != "scenario_seed"}
        normalized["metadata"] = metadata
    return normalized
