from __future__ import annotations

import csv
import json
import shutil
from collections import Counter, defaultdict
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Sequence, cast

from pydantic import BaseModel, Field

from ..score_frontier import run_llm_judge_prompt
from ._benchmark_dossiers import build_dossier_files as _write_case_dossiers
from ._benchmark_utils import slug as _slug
from ._benchmark_utils import write_jsonl as _write_jsonl
from .benchmark import (
    _action_schema_from_prompt,
    _audit_template_rows,
    _judge_template_rows,
    evaluate_branch_point_benchmark_model,
    load_branch_point_benchmark_build_result,
)
from .corpus import event_reference
from .models import (
    WhatIfActionSchema,
    WhatIfBenchmarkBuildArtifacts,
    WhatIfBenchmarkBuildResult,
    WhatIfBenchmarkCandidate,
    WhatIfBenchmarkCase,
    WhatIfBenchmarkDatasetManifest,
    WhatIfBenchmarkDatasetRow,
    WhatIfBenchmarkEvalResult,
    WhatIfBenchmarkModelId,
    WhatIfBusinessObjectivePackId,
    WhatIfEvent,
    WhatIfEventReference,
    WhatIfResearchHypothesisLabel,
)
from .multitenant_benchmark import (
    MultiTenantBenchmarkSource,
    _RowCandidate,
    _build_tenant_rows,
)

CriticalCandidateGenerationMode = Literal["llm", "template"]

_CRITICAL_DECISION_PACK_ID = "critical_decision_counterfactuals_v1"
_BALANCED_OBJECTIVE_WEIGHTS: dict[str, float] = {
    "minimize_enterprise_risk": 0.30,
    "protect_commercial_position": 0.20,
    "reduce_org_strain": 0.15,
    "preserve_stakeholder_trust": 0.20,
    "maintain_execution_velocity": 0.15,
}
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "for",
    "from",
    "in",
    "into",
    "of",
    "on",
    "or",
    "the",
    "this",
    "to",
    "with",
}
_RISK_TERMS = (
    "privacy",
    "consent",
    "gdpr",
    "legal",
    "compliance",
    "permission",
    "security",
    "confidential",
    "contract",
    "accounting",
    "audit",
    "regulatory",
)
_CUSTOMER_TERMS = (
    "client",
    "customer",
    "partner",
    "invoice",
    "deal",
    "proposal",
    "renewal",
    "sales",
    "commercial",
    "payment",
)
_PRODUCT_TERMS = (
    "bug",
    "issue",
    "ticket",
    "clickup",
    "app",
    "dashboard",
    "page",
    "ui",
    "api",
    "data",
    "feature",
    "release",
)
_PEOPLE_TERMS = (
    "hiring",
    "candidate",
    "recruit",
    "interview",
    "offer",
    "employee",
    "team",
)
_URGENCY_TERMS = ("urgent", "asap", "today", "tomorrow", "now", "blocked", "critical")
_CONFLICT_TERMS = (
    "concern",
    "confused",
    "blocked",
    "delay",
    "risk",
    "problem",
    "fail",
    "missed",
)


class CriticalDecisionRunArtifacts(BaseModel):
    root: Path
    build_manifest_path: Path
    heldout_cases_path: Path
    candidate_manifest_path: Path
    selection_manifest_path: Path
    leakage_report_path: Path
    eval_result_path: Path | None = None
    prediction_jsonl_path: Path | None = None
    csv_path: Path | None = None
    markdown_path: Path | None = None


class CriticalDecisionRunResult(BaseModel):
    version: Literal["1"] = "1"
    label: str
    selected_decision_count: int = 0
    candidate_count: int = 0
    tenants: dict[str, int] = Field(default_factory=dict)
    candidate_generation_mode: CriticalCandidateGenerationMode = "template"
    candidate_model: str = ""
    model_id: WhatIfBenchmarkModelId | str = ""
    checkpoint_path: Path | None = None
    source_build_root: Path | None = None
    notes: list[str] = Field(default_factory=list)
    artifacts: CriticalDecisionRunArtifacts


@dataclass(frozen=True)
class _CandidateTypeSpec:
    candidate_type: str
    label: str
    instruction: str


@dataclass(frozen=True)
class _CriticalDecisionItem:
    row_candidate: _RowCandidate
    category: str
    criticality_score: float
    score_components: dict[str, float]
    selection_reason: str
    selection_rank: int


@dataclass(frozen=True)
class _GeneratedCandidateSet:
    prompt: str
    raw_response: str
    candidates: list[WhatIfBenchmarkCandidate]
    manifest: dict[str, Any]


_CANDIDATE_TYPE_SPECS: tuple[_CandidateTypeSpec, ...] = (
    _CandidateTypeSpec(
        "assign_owner_fix_path",
        "Assign owner and fix path",
        "Name one accountable owner, define the concrete reproduction or work path, add review/QA, and close the loop.",
    ),
    _CandidateTypeSpec(
        "customer_status_note",
        "Send controlled stakeholder status",
        "Send one short stakeholder-facing status note with scope, impact, owner, and next update time.",
    ),
    _CandidateTypeSpec(
        "product_triage_queue",
        "Move to product triage",
        "Move the issue into a scheduled triage path with severity, priority, and a named review owner.",
    ),
    _CandidateTypeSpec(
        "fast_ship_low_risk",
        "Fast ship low-risk fix",
        "Ship the smallest low-risk change quickly, with a narrow rollback or verification step.",
    ),
    _CandidateTypeSpec(
        "expert_review_gate",
        "Route to expert review",
        "Route to the relevant expert group before acting because the decision may affect policy, data, legal, or trust boundaries.",
    ),
    _CandidateTypeSpec(
        "hold_compliance_review",
        "Hold for compliance review",
        "Do not change or send until compliance, privacy, legal, or accountable leadership explicitly approves the language or path.",
    ),
    _CandidateTypeSpec(
        "executive_escalation",
        "Escalate to executive owner",
        "Escalate to an executive owner with a decision memo, options, risks, and a time-boxed call.",
    ),
    _CandidateTypeSpec(
        "narrow_pilot",
        "Run narrow pilot",
        "Test the action with a narrow subset or staging path before broader rollout or customer communication.",
    ),
    _CandidateTypeSpec(
        "commercial_reset",
        "Reset commercial expectations",
        "Reset the commercial or customer expectation with a clear tradeoff, deadline, and commitment boundary.",
    ),
    _CandidateTypeSpec(
        "decision_log_evidence",
        "Create decision log",
        "Capture the decision, evidence, owner, and approval trail before further distribution or execution.",
    ),
    _CandidateTypeSpec(
        "data_privacy_red_team",
        "Run data/privacy red-team",
        "Stress-test the decision for data, privacy, consent, and external trust failure modes before acting.",
    ),
    _CandidateTypeSpec(
        "cross_function_war_room",
        "Open cross-functional war room",
        "Create a time-boxed cross-functional coordination room with daily decisions, owner handoffs, and stakeholder updates.",
    ),
)


def run_critical_decision_benchmark(
    sources: Sequence[MultiTenantBenchmarkSource],
    *,
    checkpoint_path: str | Path,
    artifacts_root: str | Path,
    label: str,
    source_build_root: str | Path | None = None,
    cases_per_tenant: int = 4,
    candidates_per_decision: int = 10,
    candidate_generation_mode: CriticalCandidateGenerationMode = "template",
    candidate_model: str = "gpt-5-mini",
    model_id: WhatIfBenchmarkModelId | str | None = None,
    future_horizon_events: int = 8,
    max_branch_rows_per_thread: int = 32,
    device: str | None = None,
    runtime_root: str | Path | None = None,
) -> CriticalDecisionRunResult:
    checkpoint = Path(checkpoint_path).expanduser().resolve()
    if not checkpoint.is_file():
        raise ValueError(f"checkpoint not found: {checkpoint}")
    resolved_model_id = _resolve_model_id(model_id, checkpoint)
    build, base_result = build_critical_decision_benchmark(
        sources,
        artifacts_root=artifacts_root,
        label=label,
        source_build_root=source_build_root,
        cases_per_tenant=cases_per_tenant,
        candidates_per_decision=candidates_per_decision,
        candidate_generation_mode=candidate_generation_mode,
        candidate_model=candidate_model,
        future_horizon_events=future_horizon_events,
        max_branch_rows_per_thread=max_branch_rows_per_thread,
    )
    model_root = build.artifacts.root / "model_runs" / str(resolved_model_id)
    model_root.mkdir(parents=True, exist_ok=True)
    model_path = model_root / "model.pt"
    if checkpoint != model_path:
        shutil.copy2(checkpoint, model_path)

    eval_result = evaluate_branch_point_benchmark_model(
        build.artifacts.root,
        model_id=cast(WhatIfBenchmarkModelId, resolved_model_id),
        device=device,
        runtime_root=runtime_root,
        output_root=model_root,
    )
    csv_path = build.artifacts.root / "critical_decision_scores.csv"
    markdown_path = build.artifacts.root / "critical_decision_scores.md"
    rows = _export_scored_rows(
        eval_result=eval_result,
        selection_manifest_path=base_result.artifacts.selection_manifest_path,
        csv_path=csv_path,
        markdown_path=markdown_path,
    )
    notes = list(base_result.notes)
    notes.append(
        "This is an application/scoring run over selected decision points, not a new training run."
    )
    result = base_result.model_copy(
        update={
            "model_id": str(resolved_model_id),
            "checkpoint_path": checkpoint,
            "notes": notes,
            "artifacts": base_result.artifacts.model_copy(
                update={
                    "eval_result_path": eval_result.artifacts.eval_result_path,
                    "prediction_jsonl_path": eval_result.artifacts.prediction_jsonl_path,
                    "csv_path": csv_path,
                    "markdown_path": markdown_path,
                }
            ),
        }
    )
    result_path = build.artifacts.root / "critical_decision_run_result.json"
    result_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    _write_run_summary(
        result=result,
        row_count=len(rows),
        path=build.artifacts.root / "critical_decision_run_summary.md",
    )
    return result


def build_critical_decision_benchmark(
    sources: Sequence[MultiTenantBenchmarkSource],
    *,
    artifacts_root: str | Path,
    label: str,
    source_build_root: str | Path | None = None,
    cases_per_tenant: int = 4,
    candidates_per_decision: int = 10,
    candidate_generation_mode: CriticalCandidateGenerationMode = "template",
    candidate_model: str = "gpt-5-mini",
    future_horizon_events: int = 8,
    max_branch_rows_per_thread: int = 32,
) -> tuple[WhatIfBenchmarkBuildResult, CriticalDecisionRunResult]:
    if not sources:
        raise ValueError("at least one source is required")
    if cases_per_tenant < 1:
        raise ValueError("cases_per_tenant must be at least 1")
    _validate_candidate_count(candidates_per_decision)

    root = Path(artifacts_root).expanduser().resolve() / _slug(label)
    root.mkdir(parents=True, exist_ok=True)
    build_path = root / "branch_point_benchmark_build.json"
    heldout_cases_path = root / "heldout_cases.json"
    judge_template_path = root / "judged_ranking_template.json"
    audit_template_path = root / "audit_record_template.json"
    candidate_manifest_path = root / "candidate_generation_manifest.json"
    selection_manifest_path = root / "critical_decision_selection_manifest.json"
    leakage_report_path = root / "leakage_report.json"
    dataset_root = root / "dataset"
    dossier_root = root / "dossiers"
    dataset_root.mkdir(parents=True, exist_ok=True)
    dossier_root.mkdir(parents=True, exist_ok=True)

    allowed_branch_event_ids = _source_test_branch_event_ids(source_build_root)
    selected_items: list[_CriticalDecisionItem] = []
    tenant_counts: dict[str, int] = {}
    tenant_pool_counts: dict[str, int] = {}
    notes: list[str] = [
        "Critical decision selection uses branch and pre-branch context only.",
        "Candidate generation prompts exclude recorded future tails.",
    ]
    if source_build_root is not None:
        notes.append(
            "Candidate pool restricted to source build test/heldout branch event ids."
        )

    for source in sources:
        rows = _build_tenant_rows(
            source,
            future_horizon_events=future_horizon_events,
            max_branch_rows_per_thread=max_branch_rows_per_thread,
        )
        if allowed_branch_event_ids:
            rows = [
                row
                for row in rows
                if row.row.branch_event_id in allowed_branch_event_ids
            ]
        tenant_pool_counts[source.tenant_id] = len(rows)
        if not rows:
            raise ValueError(
                f"no eligible critical decision rows for tenant {source.tenant_id!r}"
            )
        selected = _select_critical_decisions(rows, limit=cases_per_tenant)
        tenant_counts[source.tenant_id] = len(selected)
        selected_items.extend(selected)

    benchmark_cases: list[WhatIfBenchmarkCase] = []
    selected_rows: list[WhatIfBenchmarkDatasetRow] = []
    candidate_manifest: list[dict[str, Any]] = []
    selection_manifest: list[dict[str, Any]] = []
    leakage_cases: list[dict[str, Any]] = []
    for item in selected_items:
        row_item = item.row_candidate
        generated = _generate_candidates_for_critical_decision(
            item,
            mode=candidate_generation_mode,
            model=candidate_model,
            root=root,
            candidates_per_decision=candidates_per_decision,
        )
        case = WhatIfBenchmarkCase(
            case_id=row_item.row.contract.case_id,
            title=f"{row_item.display_name}: {row_item.subject or row_item.branch_event.subject}",
            event_id=row_item.row.branch_event_id,
            thread_id=row_item.row.thread_id,
            summary=(
                "Replicable critical-decision case selected from branch and "
                "pre-branch signals only."
            ),
            case_family="critical_decision_counterfactuals",
            branch_event=row_item.row.contract.branch_event,
            history_preview=[
                event_reference(event) for event in row_item.history_events[-8:]
            ],
            candidates=generated.candidates,
        )
        case_dossier_root = dossier_root / case.case_id
        case_dossier_root.mkdir(parents=True, exist_ok=True)
        dossier_paths = _write_case_dossiers(
            case=case,
            dossier_root=case_dossier_root,
        )
        case = case.model_copy(update={"objective_dossier_paths": dossier_paths})
        benchmark_cases.append(case)
        selected_rows.append(row_item.row.model_copy(update={"split": "heldout"}))
        candidate_manifest.append(generated.manifest)
        selection_manifest.append(_selection_manifest_row(item))
        leakage_cases.append(
            _critical_case_leakage_payload(
                item=item,
                generation_prompt=generated.prompt,
                raw_response=generated.raw_response,
                candidates=generated.candidates,
                dossier_paths=dossier_paths,
            )
        )

    split_rows: dict[str, list[WhatIfBenchmarkDatasetRow]] = {
        "train": [],
        "validation": [],
        "test": [row.model_copy(update={"split": "test"}) for row in selected_rows],
        "heldout": selected_rows,
    }
    split_paths: dict[str, str] = {}
    split_counts: dict[str, int] = {}
    for split_name, rows in split_rows.items():
        split_path = dataset_root / f"{split_name}_rows.jsonl"
        _write_jsonl(split_path, rows)
        split_paths[split_name] = str(split_path)
        split_counts[split_name] = len(rows)

    heldout_cases_path.write_text(
        json.dumps(
            [case.model_dump(mode="json") for case in benchmark_cases], indent=2
        ),
        encoding="utf-8",
    )
    judged_template = _judge_template_rows(benchmark_cases)
    judge_template_path.write_text(
        json.dumps([row.model_dump(mode="json") for row in judged_template], indent=2),
        encoding="utf-8",
    )
    audit_template = _audit_template_rows(benchmark_cases)
    audit_template_path.write_text(
        json.dumps([row.model_dump(mode="json") for row in audit_template], indent=2),
        encoding="utf-8",
    )
    candidate_manifest_path.write_text(
        json.dumps(candidate_manifest, indent=2), encoding="utf-8"
    )
    selection_manifest_path.write_text(
        json.dumps(selection_manifest, indent=2), encoding="utf-8"
    )
    leakage_report = {
        "checks": {
            "selection_uses_pre_branch_only": True,
            "candidate_prompts_exclude_future_markers": all(
                not item["candidate_prompt_future_marker_hits"]
                for item in leakage_cases
            ),
            "candidate_outputs_exclude_future_markers": all(
                not item["candidate_output_future_marker_hits"]
                for item in leakage_cases
            ),
            "judge_dossiers_exclude_future_markers": all(
                not item["judge_dossier_future_marker_hits"] for item in leakage_cases
            ),
        },
        "candidate_cases": leakage_cases,
    }
    leakage_report_path.write_text(
        json.dumps(leakage_report, indent=2), encoding="utf-8"
    )

    dataset_manifest = WhatIfBenchmarkDatasetManifest(
        root=dataset_root,
        split_row_counts=split_counts,
        split_paths=split_paths,
        heldout_cases_path=str(heldout_cases_path),
        judge_template_path=str(judge_template_path),
        audit_template_path=str(audit_template_path),
        dossier_root=str(dossier_root),
        heldout_thread_ids=sorted({row.thread_id for row in selected_rows}),
        metadata={
            "benchmark_kind": "critical_decision_counterfactuals",
            "candidate_generation_mode": candidate_generation_mode,
            "candidate_model": candidate_model,
            "cases_per_tenant": cases_per_tenant,
            "candidates_per_decision": candidates_per_decision,
            "future_horizon_events": future_horizon_events,
            "max_branch_rows_per_thread": max_branch_rows_per_thread,
            "source_build_root": (
                str(Path(source_build_root).expanduser().resolve())
                if source_build_root is not None
                else ""
            ),
            "candidate_pool_policy": (
                "source_test_and_heldout_splits_only"
                if source_build_root is not None
                else "all_canonical_branch_rows"
            ),
            "candidate_generation_manifest_path": str(candidate_manifest_path),
            "critical_decision_selection_manifest_path": str(selection_manifest_path),
            "leakage_report_path": str(leakage_report_path),
            "tenant_pool_counts": tenant_pool_counts,
        },
    )
    (dataset_root / "dataset_manifest.json").write_text(
        dataset_manifest.model_dump_json(indent=2), encoding="utf-8"
    )
    build = WhatIfBenchmarkBuildResult(
        label=label,
        heldout_pack_id=_CRITICAL_DECISION_PACK_ID,
        dataset=dataset_manifest,
        cases=benchmark_cases,
        artifacts=WhatIfBenchmarkBuildArtifacts(
            root=root,
            manifest_path=build_path,
            heldout_cases_path=heldout_cases_path,
            judge_template_path=judge_template_path,
            audit_template_path=audit_template_path,
            dossier_root=dossier_root,
        ),
        metadata={
            "benchmark_kind": "critical_decision_counterfactuals",
            "candidate_generation_manifest_path": str(candidate_manifest_path),
            "critical_decision_selection_manifest_path": str(selection_manifest_path),
            "leakage_report_path": str(leakage_report_path),
            "balanced_objective_weights": _BALANCED_OBJECTIVE_WEIGHTS,
        },
    )
    build_path.write_text(build.model_dump_json(indent=2), encoding="utf-8")
    result = CriticalDecisionRunResult(
        label=label,
        selected_decision_count=len(selected_rows),
        candidate_count=len(selected_rows) * candidates_per_decision,
        tenants=tenant_counts,
        candidate_generation_mode=candidate_generation_mode,
        candidate_model=candidate_model,
        source_build_root=(
            Path(source_build_root).expanduser().resolve()
            if source_build_root is not None
            else None
        ),
        notes=notes,
        artifacts=CriticalDecisionRunArtifacts(
            root=root,
            build_manifest_path=build_path,
            heldout_cases_path=heldout_cases_path,
            candidate_manifest_path=candidate_manifest_path,
            selection_manifest_path=selection_manifest_path,
            leakage_report_path=leakage_report_path,
        ),
    )
    return build, result


def build_critical_candidate_generation_prompt(
    item: _CriticalDecisionItem,
    *,
    candidates_per_decision: int = 10,
) -> str:
    _validate_candidate_count(candidates_per_decision)
    row_item = item.row_candidate
    branch = row_item.row.contract.branch_event
    evidence_hash = _pre_branch_evidence_hash(
        branch_event=branch,
        history=[event_reference(event) for event in row_item.history_events[-8:]],
    )
    history_lines = [
        (
            f"- {event.timestamp} {event.event_type} from {event.actor_id}: "
            f"{event.subject or event.snippet or event.event_id}"
        )
        for event in row_item.history_events[-8:]
    ]
    candidate_lines = [
        f"- {spec.candidate_type}: {spec.instruction}"
        for spec in _candidate_type_specs(candidates_per_decision)
    ]
    return "\n".join(
        [
            "You are generating concrete CEO decision options for a historical business branch point.",
            "Use only the pre-branch evidence below. Do not infer from, mention, or rely on any recorded future outcome.",
            "Make each option operationally specific enough that a manager could choose it.",
            "",
            f"Tenant: {row_item.display_name}",
            f"Decision category: {item.category}",
            f"Why this decision was selected: {item.selection_reason}",
            f"Evidence hash: {evidence_hash}",
            "",
            "Branch event:",
            f"- Event id: {branch.event_id}",
            f"- Timestamp: {branch.timestamp}",
            f"- Surface: {branch.surface or 'unknown'}",
            f"- Actor: {branch.actor_id}",
            f"- Target: {branch.target_id}",
            f"- Subject: {branch.subject}",
            f"- Excerpt: {branch.snippet}",
            "",
            "Pre-branch history:",
            *(history_lines or ["- No earlier events in this branch context."]),
            "",
            f"Return JSON with exactly {candidates_per_decision} candidates, one for each candidate_type below.",
            *candidate_lines,
            "",
            "Each candidate must include candidate_type, label, and action.",
            "The action must be a materially different action, not a minor wording variant.",
            "Return this JSON shape only:",
            '{"candidates":[{"candidate_type":"assign_owner_fix_path","label":"...","action":"..."}]}',
        ]
    )


def validate_critical_candidate_diversity(
    candidates: Sequence[WhatIfBenchmarkCandidate],
    *,
    candidates_per_decision: int = 10,
) -> None:
    _validate_candidate_count(candidates_per_decision)
    if len(candidates) != candidates_per_decision:
        raise ValueError(
            f"candidate generation must produce exactly {candidates_per_decision} candidates"
        )
    required_types = {
        spec.candidate_type for spec in _candidate_type_specs(candidates_per_decision)
    }
    candidate_types = {
        str(candidate.metadata.get("candidate_type") or "").strip()
        for candidate in candidates
    }
    if candidate_types != required_types:
        missing = sorted(required_types - candidate_types)
        extra = sorted(candidate_types - required_types)
        raise ValueError(
            f"candidate types must match the configured set; missing={missing}, extra={extra}"
        )
    labels = {candidate.label.strip().lower() for candidate in candidates}
    if len(labels) != len(candidates):
        raise ValueError("candidate labels must be distinct")
    signatures = {
        (
            candidate.action_schema.decision_posture,
            candidate.action_schema.review_path,
            candidate.action_schema.coordination_breadth,
            candidate.action_schema.outside_sharing_posture,
            candidate.action_schema.escalation_level,
            candidate.action_schema.legal_review_required,
            candidate.action_schema.hold_required,
        )
        for candidate in candidates
    }
    if len(signatures) < min(8, candidates_per_decision):
        raise ValueError("candidate action schemas are not broad enough")
    for left_index, left in enumerate(candidates):
        for right in candidates[left_index + 1 :]:
            similarity = _token_similarity(left.prompt, right.prompt)
            if similarity >= 0.82:
                raise ValueError(
                    f"candidate prompts are too similar: {left.candidate_id} and {right.candidate_id}"
                )


def _select_critical_decisions(
    rows: Sequence[_RowCandidate],
    *,
    limit: int,
) -> list[_CriticalDecisionItem]:
    scored = [_score_row_candidate(row) for row in rows]
    scored.sort(
        key=lambda item: (
            -item.criticality_score,
            item.row_candidate.branch_timestamp_ms,
            item.row_candidate.row.row_id,
        )
    )
    category_counts: Counter[str] = Counter()
    selected: list[_CriticalDecisionItem] = []
    seen_threads: set[str] = set()
    category_cap = max(1, (limit + 1) // 2)
    for item in scored:
        if item.row_candidate.row.thread_id in seen_threads:
            continue
        if category_counts[item.category] >= category_cap:
            continue
        selected.append(item)
        seen_threads.add(item.row_candidate.row.thread_id)
        category_counts[item.category] += 1
        if len(selected) >= limit:
            break
    if len(selected) < limit:
        for item in scored:
            if item.row_candidate.row.thread_id in seen_threads:
                continue
            selected.append(item)
            seen_threads.add(item.row_candidate.row.thread_id)
            if len(selected) >= limit:
                break
    return [
        _CriticalDecisionItem(
            row_candidate=item.row_candidate,
            category=item.category,
            criticality_score=item.criticality_score,
            score_components=item.score_components,
            selection_reason=item.selection_reason,
            selection_rank=index,
        )
        for index, item in enumerate(selected, start=1)
    ]


def _score_row_candidate(row_candidate: _RowCandidate) -> _CriticalDecisionItem:
    text = _decision_text(row_candidate)
    organization_domain = row_candidate.world.summary.organization_domain
    branch = row_candidate.branch_event
    history = row_candidate.history_events
    external_count = _external_count(branch, organization_domain=organization_domain)
    history_external_count = sum(
        _external_count(event, organization_domain=organization_domain)
        for event in history
    )
    participants = {
        value
        for event in [*history, branch]
        for value in (event.actor_id, event.target_id, *event.flags.to_recipients)
        if value
    }
    score_components = {
        "external_scope": min(5.0, external_count * 2.0 + history_external_count * 0.5),
        "risk_or_governance": min(5.0, _keyword_hits(text, _RISK_TERMS) * 1.2),
        "customer_or_commercial": min(4.0, _keyword_hits(text, _CUSTOMER_TERMS)),
        "product_or_delivery": min(4.0, _keyword_hits(text, _PRODUCT_TERMS)),
        "people_or_org": min(3.0, _keyword_hits(text, _PEOPLE_TERMS)),
        "coordination_complexity": min(
            5.0,
            max(0, len(participants) - 2) * 0.5
            + branch.flags.cc_count * 0.5
            + len(branch.flags.cc_recipients) * 0.25,
        ),
        "urgency_or_escalation": min(
            4.0,
            _keyword_hits(text, _URGENCY_TERMS)
            + (2.0 if branch.flags.is_escalation else 0.0),
        ),
        "conflict_or_delay": min(4.0, _keyword_hits(text, _CONFLICT_TERMS)),
        "evidence_pressure": min(
            3.0,
            (1.5 if branch.flags.has_attachment_reference else 0.0)
            + (1.0 if branch.flags.is_forward else 0.0),
        ),
    }
    weights = {
        "external_scope": 1.15,
        "risk_or_governance": 1.25,
        "customer_or_commercial": 1.05,
        "product_or_delivery": 0.90,
        "people_or_org": 0.65,
        "coordination_complexity": 0.85,
        "urgency_or_escalation": 0.95,
        "conflict_or_delay": 0.80,
        "evidence_pressure": 0.75,
    }
    score = round(
        sum(score_components[name] * weights[name] for name in score_components), 3
    )
    category = _infer_category(score_components)
    reason = _selection_reason(score_components)
    return _CriticalDecisionItem(
        row_candidate=row_candidate,
        category=category,
        criticality_score=score,
        score_components=score_components,
        selection_reason=reason,
        selection_rank=0,
    )


def _generate_candidates_for_critical_decision(
    item: _CriticalDecisionItem,
    *,
    mode: CriticalCandidateGenerationMode,
    model: str,
    root: Path,
    candidates_per_decision: int,
) -> _GeneratedCandidateSet:
    prompt = build_critical_candidate_generation_prompt(
        item,
        candidates_per_decision=candidates_per_decision,
    )
    prompt_hash = sha256(prompt.encode("utf-8")).hexdigest()
    evidence_hash = _pre_branch_evidence_hash(
        branch_event=item.row_candidate.row.contract.branch_event,
        history=[
            event_reference(event) for event in item.row_candidate.history_events[-8:]
        ],
    )
    raw_response = ""
    source = "template"
    llm_error = ""
    payloads: list[dict[str, Any]]
    if mode == "llm":
        try:
            raw_response = run_llm_judge_prompt(
                prompt,
                model=model,
                max_tokens=3600,
                temperature=None if model.startswith("gpt-5") else 0.0,
                json_mode=True,
            )
            payload = json.loads(raw_response)
            payloads = list(payload.get("candidates") or [])
            source = "llm"
        except Exception as exc:  # pragma: no cover - exercised in live runs.
            llm_error = str(exc)
            payloads = []
            source = "template_fallback"
    else:
        payloads = []
    if not payloads:
        payloads = _template_candidate_payloads(
            item, candidates_per_decision=candidates_per_decision
        )
        if not raw_response:
            raw_response = json.dumps({"candidates": payloads}, indent=2)
    candidates = _candidates_from_payloads(
        payloads,
        item=item,
        model=model,
        source=source,
        prompt_hash=prompt_hash,
        evidence_hash=evidence_hash,
        candidates_per_decision=candidates_per_decision,
    )
    validate_critical_candidate_diversity(
        candidates,
        candidates_per_decision=candidates_per_decision,
    )
    prompt_path = (
        root / "candidate_prompts" / f"{item.row_candidate.row.contract.case_id}.txt"
    )
    response_path = (
        root / "candidate_responses" / f"{item.row_candidate.row.contract.case_id}.json"
    )
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    response_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text(prompt, encoding="utf-8")
    response_path.write_text(raw_response, encoding="utf-8")
    return _GeneratedCandidateSet(
        prompt=prompt,
        raw_response=raw_response,
        candidates=candidates,
        manifest={
            "case_id": item.row_candidate.row.contract.case_id,
            "tenant_id": item.row_candidate.tenant_id,
            "source": source,
            "model": model,
            "llm_error": llm_error,
            "prompt_path": str(prompt_path),
            "response_path": str(response_path),
            "generation_prompt_sha256": prompt_hash,
            "pre_branch_evidence_sha256": evidence_hash,
            "no_future_context": True,
            "candidate_ids": [candidate.candidate_id for candidate in candidates],
            "candidate_types": [
                str(candidate.metadata.get("candidate_type") or "")
                for candidate in candidates
            ],
        },
    )


def _candidates_from_payloads(
    payloads: Sequence[dict[str, Any]],
    *,
    item: _CriticalDecisionItem,
    model: str,
    source: str,
    prompt_hash: str,
    evidence_hash: str,
    candidates_per_decision: int,
) -> list[WhatIfBenchmarkCandidate]:
    required_specs = _candidate_type_specs(candidates_per_decision)
    payload_by_type: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        candidate_type = _normalize_candidate_type(payload)
        if candidate_type and candidate_type not in payload_by_type:
            payload_by_type[candidate_type] = dict(payload)
    template_payloads = {
        payload["candidate_type"]: payload
        for payload in _template_candidate_payloads(
            item, candidates_per_decision=candidates_per_decision
        )
    }
    candidates: list[WhatIfBenchmarkCandidate] = []
    for spec in required_specs:
        payload = (
            payload_by_type.get(spec.candidate_type)
            or template_payloads[spec.candidate_type]
        )
        action = str(
            payload.get("action")
            or payload.get("prompt")
            or payload.get("candidate_action")
            or ""
        ).strip()
        label = str(payload.get("label") or spec.label).strip()
        if not action:
            action = str(template_payloads[spec.candidate_type]["action"])
        action_schema = _action_schema_for_candidate_type(
            candidate_type=spec.candidate_type,
            action=action,
            item=item,
        )
        candidates.append(
            WhatIfBenchmarkCandidate(
                candidate_id=spec.candidate_type,
                label=label,
                prompt=action,
                action_schema=action_schema,
                expected_hypotheses=_expected_hypotheses_for_candidate_type(
                    spec.candidate_type
                ),
                metadata={
                    "candidate_type": spec.candidate_type,
                    "generation_source": source,
                    "generation_model": model,
                    "generation_prompt_sha256": prompt_hash,
                    "pre_branch_evidence_sha256": evidence_hash,
                    "no_future_context": True,
                },
            )
        )
    return candidates


def _template_candidate_payloads(
    item: _CriticalDecisionItem,
    *,
    candidates_per_decision: int,
) -> list[dict[str, str]]:
    row_item = item.row_candidate
    branch = row_item.branch_event
    subject = _clean_one_line(row_item.subject or branch.subject or "this decision")
    owner = _friendly_owner(branch.actor_id)
    stakeholder = _friendly_stakeholder(branch.target_id)
    issue_kind = _issue_kind(item.category)
    payloads = {
        "assign_owner_fix_path": (
            "Assign owner and fix path",
            f"Assign {owner} as the accountable owner for {issue_kind} '{subject}', reproduce or restate the issue today, define the exact fix path, add one review/QA step, and close the task only after evidence is attached.",
        ),
        "customer_status_note": (
            "Send controlled stakeholder status",
            f"Send {stakeholder} one short status note on '{subject}' saying what is known, what is not changing, who owns it, and when the next update will arrive; do not attach raw internal material.",
        ),
        "product_triage_queue": (
            "Move to product triage",
            f"Move '{subject}' into a product triage bucket with severity, customer impact, owner, and next-review date; keep it non-blocking unless the owner confirms user or revenue impact.",
        ),
        "fast_ship_low_risk": (
            "Fast ship low-risk fix",
            f"Ship the smallest low-risk change for '{subject}' immediately, keep the change narrow, verify in staging or with one reviewer, and publish the follow-up without widening the loop.",
        ),
        "expert_review_gate": (
            "Route to expert review",
            f"Route '{subject}' to the relevant product, data, privacy, legal, or commercial expert before acting; ask for one accountable recommendation and a written risk note.",
        ),
        "hold_compliance_review": (
            "Hold for compliance review",
            f"Hold all changes and outside messages on '{subject}' until compliance/privacy/legal and the accountable business owner approve the exact language or path.",
        ),
        "executive_escalation": (
            "Escalate to executive owner",
            f"Escalate '{subject}' to an executive owner with a one-page decision memo covering context, options, risk, customer impact, and the decision needed within 24 hours.",
        ),
        "narrow_pilot": (
            "Run narrow pilot",
            f"Run a narrow pilot for '{subject}' with one internal group or staging path, define success/failure criteria, and only then decide whether to expand or notify more stakeholders.",
        ),
        "commercial_reset": (
            "Reset commercial expectations",
            f"Reset expectations with {stakeholder} on '{subject}': state the tradeoff, commit to the next deliverable, and avoid promising scope or dates not owned by the team.",
        ),
        "decision_log_evidence": (
            "Create decision log",
            f"Create a decision log for '{subject}' with the evidence, owner, approvals, customer-facing language, and follow-up trigger before any broader distribution.",
        ),
        "data_privacy_red_team": (
            "Run data/privacy red-team",
            f"Run a focused data/privacy red-team on '{subject}' before acting: list consent, data access, customer trust, and external communication failure modes, then approve only the safe path.",
        ),
        "cross_function_war_room": (
            "Open cross-functional war room",
            f"Open a time-boxed cross-functional room for '{subject}' with product, commercial, privacy/legal, and operations owners; make daily decisions and send one coordinated stakeholder update.",
        ),
    }
    return [
        {
            "candidate_type": spec.candidate_type,
            "label": payloads[spec.candidate_type][0],
            "action": payloads[spec.candidate_type][1],
        }
        for spec in _candidate_type_specs(candidates_per_decision)
    ]


def _action_schema_for_candidate_type(
    *,
    candidate_type: str,
    action: str,
    item: _CriticalDecisionItem,
) -> WhatIfActionSchema:
    row_item = item.row_candidate
    base = _action_schema_from_prompt(
        action,
        branch_event=row_item.branch_event,
        historical_action=row_item.row.contract.action_schema,
    )
    updates: dict[str, Any] = {
        "action_tags": sorted(set(base.action_tags) | {candidate_type}),
    }
    if candidate_type == "assign_owner_fix_path":
        updates.update(
            {
                "owner_clarity": "single_owner",
                "review_path": "business_owner",
                "coordination_breadth": "single_owner",
                "outside_sharing_posture": "internal_only",
                "decision_posture": "resolve",
            }
        )
    elif candidate_type == "customer_status_note":
        updates.update(
            {
                "recipient_scope": "external",
                "external_recipient_count": 1,
                "attachment_policy": "sanitized",
                "owner_clarity": "single_owner",
                "review_path": "business_owner",
                "coordination_breadth": "single_owner",
                "outside_sharing_posture": "status_only",
                "decision_posture": "resolve",
            }
        )
    elif candidate_type == "product_triage_queue":
        updates.update(
            {
                "recipient_scope": "internal",
                "owner_clarity": "single_owner",
                "review_path": "business_owner",
                "coordination_breadth": "narrow",
                "outside_sharing_posture": "internal_only",
                "decision_posture": "review",
            }
        )
    elif candidate_type == "fast_ship_low_risk":
        updates.update(
            {
                "recipient_scope": "mixed",
                "external_recipient_count": max(1, base.external_recipient_count),
                "owner_clarity": "single_owner",
                "review_path": "none",
                "coordination_breadth": "targeted",
                "outside_sharing_posture": "limited_external",
                "decision_posture": "resolve",
            }
        )
    elif candidate_type == "expert_review_gate":
        updates.update(
            {
                "legal_review_required": True,
                "owner_clarity": "single_owner",
                "review_path": "cross_functional",
                "coordination_breadth": "targeted",
                "outside_sharing_posture": "limited_external",
                "decision_posture": "escalate",
                "escalation_level": "manager",
            }
        )
    elif candidate_type == "hold_compliance_review":
        updates.update(
            {
                "recipient_scope": "internal",
                "external_recipient_count": 0,
                "hold_required": True,
                "legal_review_required": True,
                "owner_clarity": "single_owner",
                "review_path": "internal_legal",
                "coordination_breadth": "narrow",
                "outside_sharing_posture": "internal_only",
                "decision_posture": "hold",
            }
        )
    elif candidate_type == "executive_escalation":
        updates.update(
            {
                "owner_clarity": "single_owner",
                "review_path": "executive",
                "coordination_breadth": "targeted",
                "outside_sharing_posture": "internal_only",
                "decision_posture": "escalate",
                "escalation_level": "executive",
            }
        )
    elif candidate_type == "narrow_pilot":
        updates.update(
            {
                "owner_clarity": "single_owner",
                "review_path": "business_owner",
                "coordination_breadth": "narrow",
                "outside_sharing_posture": "limited_external",
                "decision_posture": "review",
            }
        )
    elif candidate_type == "commercial_reset":
        updates.update(
            {
                "recipient_scope": "external",
                "external_recipient_count": 1,
                "attachment_policy": "sanitized",
                "owner_clarity": "single_owner",
                "review_path": "business_owner",
                "coordination_breadth": "targeted",
                "outside_sharing_posture": "status_only",
                "decision_posture": "resolve",
            }
        )
    elif candidate_type == "data_privacy_red_team":
        updates.update(
            {
                "recipient_scope": "internal",
                "external_recipient_count": 0,
                "legal_review_required": True,
                "owner_clarity": "single_owner",
                "review_path": "internal_legal",
                "coordination_breadth": "targeted",
                "outside_sharing_posture": "internal_only",
                "decision_posture": "review",
            }
        )
    elif candidate_type == "cross_function_war_room":
        updates.update(
            {
                "recipient_scope": "mixed",
                "external_recipient_count": max(1, base.external_recipient_count),
                "owner_clarity": "multi_owner",
                "review_path": "cross_functional",
                "coordination_breadth": "broad",
                "outside_sharing_posture": "limited_external",
                "decision_posture": "escalate",
                "escalation_level": "manager",
            }
        )
    else:
        updates.update(
            {
                "recipient_scope": "internal",
                "owner_clarity": "single_owner",
                "review_path": "cross_functional",
                "coordination_breadth": "targeted",
                "outside_sharing_posture": "internal_only",
                "decision_posture": "review",
            }
        )
    return base.model_copy(update=updates)


def _expected_hypotheses_for_candidate_type(
    candidate_type: str,
) -> dict[WhatIfBusinessObjectivePackId, WhatIfResearchHypothesisLabel]:
    if candidate_type in {
        "hold_compliance_review",
        "expert_review_gate",
        "data_privacy_red_team",
    }:
        return {
            "minimize_enterprise_risk": "best_expected",
            "protect_commercial_position": "middle_expected",
            "reduce_org_strain": "middle_expected",
            "preserve_stakeholder_trust": "middle_expected",
            "maintain_execution_velocity": "worst_expected",
        }
    if candidate_type in {"fast_ship_low_risk", "assign_owner_fix_path"}:
        return {
            "minimize_enterprise_risk": "middle_expected",
            "protect_commercial_position": "middle_expected",
            "reduce_org_strain": "best_expected",
            "preserve_stakeholder_trust": "middle_expected",
            "maintain_execution_velocity": "best_expected",
        }
    if candidate_type in {"customer_status_note", "commercial_reset"}:
        return {
            "minimize_enterprise_risk": "middle_expected",
            "protect_commercial_position": "best_expected",
            "reduce_org_strain": "middle_expected",
            "preserve_stakeholder_trust": "best_expected",
            "maintain_execution_velocity": "middle_expected",
        }
    if candidate_type == "executive_escalation":
        return {
            "minimize_enterprise_risk": "middle_expected",
            "protect_commercial_position": "middle_expected",
            "reduce_org_strain": "worst_expected",
            "preserve_stakeholder_trust": "middle_expected",
            "maintain_execution_velocity": "middle_expected",
        }
    if candidate_type == "cross_function_war_room":
        return {
            "minimize_enterprise_risk": "middle_expected",
            "protect_commercial_position": "middle_expected",
            "reduce_org_strain": "worst_expected",
            "preserve_stakeholder_trust": "middle_expected",
            "maintain_execution_velocity": "middle_expected",
        }
    return {
        "minimize_enterprise_risk": "middle_expected",
        "protect_commercial_position": "middle_expected",
        "reduce_org_strain": "middle_expected",
        "preserve_stakeholder_trust": "middle_expected",
        "maintain_execution_velocity": "middle_expected",
    }


def _export_scored_rows(
    *,
    eval_result: WhatIfBenchmarkEvalResult,
    selection_manifest_path: Path,
    csv_path: Path,
    markdown_path: Path,
) -> list[dict[str, Any]]:
    selection_rows = json.loads(selection_manifest_path.read_text(encoding="utf-8"))
    selection_by_case = {row["case_id"]: row for row in selection_rows}
    rows: list[dict[str, Any]] = []
    for case_eval in eval_result.cases:
        selection = selection_by_case.get(case_eval.case.case_id, {})
        candidate_map: dict[str, dict[str, Any]] = {}
        for objective in case_eval.objectives:
            pack_id = objective.objective_pack.pack_id
            for candidate_prediction in objective.candidates:
                candidate = candidate_prediction.candidate
                entry = candidate_map.setdefault(
                    candidate.candidate_id,
                    {
                        "candidate": candidate,
                        "business": candidate_prediction.predicted_business_outcomes,
                        "evidence": candidate_prediction.predicted_evidence_heads,
                        "objective_scores": {},
                    },
                )
                entry["objective_scores"][
                    pack_id
                ] = candidate_prediction.predicted_objective_score.overall_score
        ranked_entries = sorted(
            candidate_map.values(),
            key=lambda entry: (
                -_balanced_score(entry["objective_scores"]),
                entry["business"].enterprise_risk,
                entry["candidate"].label.lower(),
            ),
        )
        for rank, entry in enumerate(ranked_entries, start=1):
            candidate = entry["candidate"]
            business = entry["business"]
            evidence = entry["evidence"]
            objective_scores = entry["objective_scores"]
            row = {
                "tenant_id": selection.get("tenant_id", ""),
                "tenant": selection.get("display_name", ""),
                "decision_rank": selection.get("selection_rank", 0),
                "case_id": case_eval.case.case_id,
                "thread_id": case_eval.case.thread_id,
                "branch_event_id": case_eval.case.event_id,
                "decision_title": case_eval.case.title,
                "decision_category": selection.get("category", ""),
                "branch_timestamp": selection.get("branch_timestamp", ""),
                "branch_actor": selection.get("branch_actor", ""),
                "branch_target": selection.get("branch_target", ""),
                "branch_subject": selection.get("branch_subject", ""),
                "criticality_score": selection.get("criticality_score", 0.0),
                "selection_reason": selection.get("selection_reason", ""),
                "candidate_rank": rank,
                "candidate_id": candidate.candidate_id,
                "candidate_label": candidate.label,
                "candidate_type": candidate.metadata.get("candidate_type", ""),
                "candidate_action": candidate.prompt,
                "balanced_ceo_score": _balanced_score(objective_scores),
                "minimize_enterprise_risk": objective_scores.get(
                    "minimize_enterprise_risk", 0.0
                ),
                "protect_commercial_position": objective_scores.get(
                    "protect_commercial_position", 0.0
                ),
                "reduce_org_strain": objective_scores.get("reduce_org_strain", 0.0),
                "preserve_stakeholder_trust": objective_scores.get(
                    "preserve_stakeholder_trust", 0.0
                ),
                "maintain_execution_velocity": objective_scores.get(
                    "maintain_execution_velocity", 0.0
                ),
                "enterprise_risk": business.enterprise_risk,
                "commercial_position_proxy": business.commercial_position_proxy,
                "org_strain_proxy": business.org_strain_proxy,
                "stakeholder_trust": business.stakeholder_trust,
                "execution_drag": business.execution_drag,
                "any_external_spread": evidence.any_external_spread,
                "outside_recipient_count": evidence.outside_recipient_count,
                "legal_follow_up_count": evidence.legal_follow_up_count,
                "participant_fanout": evidence.participant_fanout,
                "review_loop_count": evidence.review_loop_count,
                "executive_escalation_count": evidence.executive_escalation_count,
                "generation_source": candidate.metadata.get("generation_source", ""),
                "generation_model": candidate.metadata.get("generation_model", ""),
                "pre_branch_evidence_sha256": candidate.metadata.get(
                    "pre_branch_evidence_sha256", ""
                ),
                "generation_prompt_sha256": candidate.metadata.get(
                    "generation_prompt_sha256", ""
                ),
                "no_future_context": candidate.metadata.get("no_future_context", True),
            }
            rows.append(row)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()) if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(rows)
    _write_markdown_scores(rows, markdown_path)
    return rows


def _write_markdown_scores(rows: Sequence[dict[str, Any]], path: Path) -> None:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["case_id"])].append(dict(row))
    lines = [
        "# Critical Decision Counterfactual Scores",
        "",
        "Selection is deterministic and uses only branch plus pre-branch context. Candidate prompts and LLM/template outputs are saved beside this file.",
        "",
        "## Top JEPA-Ranked Action Per Decision",
        "",
        "| Tenant | Decision | Why Selected | Top Action | Balanced Score | Risk | Trust | Drag |",
        "|---|---|---|---|---:|---:|---:|---:|",
    ]
    for case_rows in grouped.values():
        top = sorted(case_rows, key=lambda row: int(row["candidate_rank"]))[0]
        lines.append(
            "| {tenant} | {decision} | {reason} | {action} | {score:.3f} | {risk:.3f} | {trust:.3f} | {drag:.3f} |".format(
                tenant=_md(top["tenant"]),
                decision=_md(top["branch_subject"] or top["decision_title"]),
                reason=_md(top["selection_reason"]),
                action=_md(top["candidate_label"]),
                score=float(top["balanced_ceo_score"]),
                risk=float(top["enterprise_risk"]),
                trust=float(top["stakeholder_trust"]),
                drag=float(top["execution_drag"]),
            )
        )
    lines.extend(["", "## Candidate Grid", ""])
    for case_id, case_rows in grouped.items():
        ordered = sorted(case_rows, key=lambda row: int(row["candidate_rank"]))
        first = ordered[0]
        lines.extend(
            [
                f"### {_md(first['tenant'])}: {_md(first['branch_subject'] or first['decision_title'])}",
                "",
                f"- Case: `{case_id}`",
                f"- Category: `{first['decision_category']}`",
                f"- Selected because: {first['selection_reason']}",
                "",
                "| Rank | Candidate | Action | Balanced | Risk Obj | Commercial | Trust | Velocity |",
                "|---:|---|---|---:|---:|---:|---:|---:|",
            ]
        )
        for row in ordered:
            lines.append(
                "| {rank} | {label} | {action} | {balanced:.3f} | {risk_obj:.3f} | {commercial:.3f} | {trust:.3f} | {velocity:.3f} |".format(
                    rank=row["candidate_rank"],
                    label=_md(row["candidate_label"]),
                    action=_md(row["candidate_action"]),
                    balanced=float(row["balanced_ceo_score"]),
                    risk_obj=float(row["minimize_enterprise_risk"]),
                    commercial=float(row["protect_commercial_position"]),
                    trust=float(row["preserve_stakeholder_trust"]),
                    velocity=float(row["maintain_execution_velocity"]),
                )
            )
        lines.append("")
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _write_run_summary(
    *,
    result: CriticalDecisionRunResult,
    row_count: int,
    path: Path,
) -> None:
    lines = [
        "# Critical Decision Run Summary",
        "",
        f"- Label: `{result.label}`",
        f"- Selected decisions: `{result.selected_decision_count}`",
        f"- Scored candidate rows: `{row_count}`",
        f"- Candidate mode: `{result.candidate_generation_mode}`",
        f"- Candidate model: `{result.candidate_model}`",
        f"- Model id: `{result.model_id}`",
        f"- CSV: `{result.artifacts.csv_path}`",
        f"- Markdown: `{result.artifacts.markdown_path}`",
        f"- Leakage report: `{result.artifacts.leakage_report_path}`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _critical_case_leakage_payload(
    *,
    item: _CriticalDecisionItem,
    generation_prompt: str,
    raw_response: str,
    candidates: Sequence[WhatIfBenchmarkCandidate],
    dossier_paths: dict[str, str],
) -> dict[str, Any]:
    future_markers = _future_markers(item.row_candidate)
    candidate_text = "\n".join(candidate.prompt for candidate in candidates)
    dossier_text = "\n".join(
        Path(path).read_text(encoding="utf-8") for path in dossier_paths.values()
    )
    prompt_hits = sorted(
        {marker for marker in future_markers if marker in generation_prompt}
    )
    output_hits = sorted(
        {
            marker
            for marker in future_markers
            if marker in raw_response or marker in candidate_text
        }
    )
    dossier_hits = sorted(
        {marker for marker in future_markers if marker in dossier_text}
    )
    return {
        "case_id": item.row_candidate.row.contract.case_id,
        "tenant_id": item.row_candidate.tenant_id,
        "branch_event_id": item.row_candidate.row.branch_event_id,
        "future_marker_count": len(future_markers),
        "candidate_prompt_future_marker_hits": prompt_hits,
        "candidate_output_future_marker_hits": output_hits,
        "judge_dossier_future_marker_hits": dossier_hits,
    }


def _future_markers(item: _RowCandidate) -> list[str]:
    allowed = _allowed_pre_branch_marker_text(item)
    markers: list[str] = []
    for event in item.future_events:
        for marker in _future_unique_markers(event, allowed_text=allowed):
            if marker:
                markers.append(marker)
    return markers


def _allowed_pre_branch_marker_text(item: _RowCandidate) -> str:
    branch = item.row.contract.branch_event
    values = [branch.event_id, branch.timestamp, branch.subject, branch.snippet]
    for event in item.history_events:
        values.extend([event.event_id, event.timestamp, event.subject, event.snippet])
    return "\n".join(value for value in values if value)


def _future_unique_markers(event: WhatIfEvent, *, allowed_text: str) -> list[str]:
    markers = [event.event_id]
    timestamp = event.timestamp.strip()
    if timestamp and timestamp not in allowed_text:
        markers.append(timestamp)
    snippet = event.snippet.strip()
    if len(snippet) >= 40 and snippet not in allowed_text:
        markers.append(snippet)
    return markers


def _selection_manifest_row(item: _CriticalDecisionItem) -> dict[str, Any]:
    row_item = item.row_candidate
    branch = row_item.row.contract.branch_event
    return {
        "tenant_id": row_item.tenant_id,
        "display_name": row_item.display_name,
        "case_id": row_item.row.contract.case_id,
        "thread_id": row_item.row.thread_id,
        "branch_event_id": row_item.row.branch_event_id,
        "branch_timestamp": branch.timestamp,
        "branch_actor": branch.actor_id,
        "branch_target": branch.target_id,
        "branch_subject": branch.subject,
        "category": item.category,
        "criticality_score": item.criticality_score,
        "score_components": item.score_components,
        "selection_reason": item.selection_reason,
        "selection_rank": item.selection_rank,
        "no_future_context_for_selection": True,
    }


def _source_test_branch_event_ids(source_build_root: str | Path | None) -> set[str]:
    if source_build_root is None:
        return set()
    build = load_branch_point_benchmark_build_result(source_build_root)
    event_ids: set[str] = set()
    for split_name in ("test", "heldout"):
        split_path = Path(build.dataset.split_paths[split_name]).expanduser().resolve()
        if not split_path.exists():
            continue
        for line in split_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = WhatIfBenchmarkDatasetRow.model_validate_json(line)
            event_ids.add(row.branch_event_id)
    return event_ids


def _resolve_model_id(
    model_id: WhatIfBenchmarkModelId | str | None,
    checkpoint: Path,
) -> WhatIfBenchmarkModelId | str:
    if model_id:
        return model_id
    parent_name = checkpoint.parent.name
    if parent_name:
        return parent_name
    return "jepa_latent"


def _candidate_type_specs(count: int) -> tuple[_CandidateTypeSpec, ...]:
    _validate_candidate_count(count)
    return _CANDIDATE_TYPE_SPECS[:count]


def _validate_candidate_count(count: int) -> None:
    if count < 8 or count > len(_CANDIDATE_TYPE_SPECS):
        raise ValueError(
            f"candidates_per_decision must be between 8 and {len(_CANDIDATE_TYPE_SPECS)}"
        )


def _normalize_candidate_type(payload: dict[str, Any]) -> str:
    raw = str(
        payload.get("candidate_type")
        or payload.get("type")
        or payload.get("posture")
        or ""
    )
    return _slug(raw).replace("-", "_")


def _balanced_score(objective_scores: dict[str, float]) -> float:
    numerator = 0.0
    denominator = 0.0
    for pack_id, weight in _BALANCED_OBJECTIVE_WEIGHTS.items():
        if pack_id not in objective_scores:
            continue
        numerator += float(objective_scores[pack_id]) * weight
        denominator += weight
    return round(numerator / max(denominator, 1e-9), 6)


def _pre_branch_evidence_hash(
    *,
    branch_event: WhatIfEventReference,
    history: Sequence[WhatIfEventReference],
) -> str:
    payload = {
        "branch_event": branch_event.model_dump(mode="json"),
        "history": [event.model_dump(mode="json") for event in history],
    }
    return sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _decision_text(row_candidate: _RowCandidate) -> str:
    events = [*row_candidate.history_events, row_candidate.branch_event]
    values: list[str] = [row_candidate.subject]
    for event in events:
        values.extend(
            [
                event.event_type,
                event.surface,
                event.subject,
                event.snippet,
                event.actor_id,
                event.target_id,
                " ".join(event.flags.to_recipients),
                " ".join(event.flags.cc_recipients),
            ]
        )
    return " ".join(value for value in values if value).lower()


def _external_count(event: WhatIfEvent, *, organization_domain: str) -> int:
    identifiers = [
        event.target_id,
        *event.flags.to_recipients,
        *event.flags.cc_recipients,
    ]
    return sum(
        1
        for identifier in identifiers
        if _is_external_identifier(identifier, organization_domain)
    )


def _is_external_identifier(identifier: str, organization_domain: str) -> bool:
    value = identifier.strip().lower()
    if not value or "@" not in value:
        return False
    domain = organization_domain.strip().lower()
    if not domain:
        return True
    return not value.endswith(f"@{domain}") and not value.endswith(f".{domain}")


def _keyword_hits(text: str, terms: Sequence[str]) -> int:
    return sum(1 for term in terms if term in text)


def _infer_category(components: dict[str, float]) -> str:
    ranked = sorted(components.items(), key=lambda item: (-item[1], item[0]))
    names = {name for name, value in ranked[:3] if value > 0}
    if "risk_or_governance" in names or "evidence_pressure" in names:
        return "privacy_legal_governance"
    if "customer_or_commercial" in names:
        return "customer_commercial"
    if "product_or_delivery" in names:
        return "product_delivery"
    if "people_or_org" in names:
        return "people_org"
    return "coordination_execution"


def _selection_reason(components: dict[str, float]) -> str:
    ranked = [
        (name, value)
        for name, value in sorted(
            components.items(), key=lambda item: (-item[1], item[0])
        )
        if value > 0
    ][:3]
    if not ranked:
        return "selected as a representative late-branch decision"
    return ", ".join(f"{name}={value:.1f}" for name, value in ranked)


def _issue_kind(category: str) -> str:
    if category == "product_delivery":
        return "product or delivery issue"
    if category == "customer_commercial":
        return "customer or commercial decision"
    if category == "privacy_legal_governance":
        return "risk, privacy, or governance decision"
    if category == "people_org":
        return "people or operating decision"
    return "coordination decision"


def _friendly_owner(actor_id: str) -> str:
    if not actor_id:
        return "the accountable owner"
    return actor_id.split("@", 1)[0].replace(".", " ").replace("_", " ").title()


def _friendly_stakeholder(target_id: str) -> str:
    if not target_id:
        return "the affected stakeholder"
    if "@" in target_id:
        return "the affected external stakeholder"
    return target_id


def _clean_one_line(value: str) -> str:
    return " ".join(value.split())[:180]


def _token_similarity(left: str, right: str) -> float:
    left_tokens = _tokens(left)
    right_tokens = _tokens(right)
    if not left_tokens or not right_tokens:
        return 1.0
    return len(left_tokens & right_tokens) / max(1, len(left_tokens | right_tokens))


def _tokens(text: str) -> set[str]:
    cleaned = [
        "".join(ch for ch in token.lower() if ch.isalnum()) for token in text.split()
    ]
    return {token for token in cleaned if len(token) > 2 and token not in _STOPWORDS}


def _md(value: object) -> str:
    text = str(value).replace("\n", " ").strip()
    return text.replace("|", "\\|")


__all__ = [
    "CriticalCandidateGenerationMode",
    "CriticalDecisionRunArtifacts",
    "CriticalDecisionRunResult",
    "build_critical_candidate_generation_prompt",
    "build_critical_decision_benchmark",
    "run_critical_decision_benchmark",
    "validate_critical_candidate_diversity",
]
