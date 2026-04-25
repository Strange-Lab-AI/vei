from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Sequence

from pydantic import BaseModel, Field

from ..score_frontier import run_llm_judge_prompt
from ._benchmark_dossiers import build_dossier_files as _write_case_dossiers
from ._benchmark_utils import slug as _slug
from ._benchmark_utils import write_jsonl as _write_jsonl
from .benchmark import (
    _action_schema_from_event,
    _action_schema_from_prompt,
    _audit_template_rows,
    _build_pre_branch_contract,
    _judge_template_rows,
    outcome_targets_to_signals,
    summarize_observed_targets,
)
from .benchmark_business import (
    evidence_to_business_outcomes,
    summarize_future_state_heads,
    summarize_observed_evidence,
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
    WhatIfBenchmarkSplit,
    WhatIfBusinessObjectivePackId,
    WhatIfEvent,
    WhatIfEventReference,
    WhatIfResearchHypothesisLabel,
    WhatIfWorld,
)

CandidateGenerationMode = Literal["llm", "template"]

_MULTITENANT_PACK_ID = "multitenant_world_model_v1"
_REQUIRED_POSTURES = (
    "containment_hold",
    "narrow_controlled_response",
    "escalate_expert_review",
    "speed_broad_coordination",
)
_STOPWORDS = {
    "a",
    "an",
    "and",
    "for",
    "in",
    "of",
    "on",
    "one",
    "the",
    "this",
    "to",
    "with",
}


class MultiTenantBenchmarkSource(BaseModel):
    tenant_id: str
    world: WhatIfWorld
    display_name: str = ""
    readiness_required: bool = False
    readiness: dict[str, Any] = Field(default_factory=dict)


@dataclass(frozen=True)
class _RowCandidate:
    tenant_id: str
    display_name: str
    world: WhatIfWorld
    row: WhatIfBenchmarkDatasetRow
    branch_event: WhatIfEvent
    raw_branch_event_id: str
    history_events: list[WhatIfEvent]
    future_events: list[WhatIfEvent]
    subject: str
    branch_timestamp_ms: int
    target_end_timestamp_ms: int


def build_multitenant_world_model_benchmark(
    sources: Sequence[MultiTenantBenchmarkSource],
    *,
    artifacts_root: str | Path,
    label: str,
    heldout_cases_per_tenant: int = 4,
    candidate_generation_mode: CandidateGenerationMode = "template",
    candidate_model: str = "gpt-5-mini",
    future_horizon_events: int = 6,
    max_branch_rows_per_thread: int = 24,
) -> WhatIfBenchmarkBuildResult:
    if not sources:
        raise ValueError("at least one source is required")
    if max_branch_rows_per_thread < 1:
        raise ValueError("max_branch_rows_per_thread must be at least 1")

    root = Path(artifacts_root).expanduser().resolve() / _slug(label)
    root.mkdir(parents=True, exist_ok=True)
    build_path = root / "branch_point_benchmark_build.json"
    heldout_cases_path = root / "heldout_cases.json"
    judge_template_path = root / "judged_ranking_template.json"
    audit_template_path = root / "audit_record_template.json"
    data_provenance_path = root / "data_provenance_report.json"
    dossier_root = root / "dossiers"
    dataset_root = root / "dataset"
    dataset_root.mkdir(parents=True, exist_ok=True)
    dossier_root.mkdir(parents=True, exist_ok=True)

    split_rows: dict[str, list[WhatIfBenchmarkDatasetRow]] = {
        "train": [],
        "validation": [],
        "test": [],
        "heldout": [],
    }
    heldout_candidates: list[_RowCandidate] = []
    candidate_manifest: list[dict[str, Any]] = []
    leakage_manifest: dict[str, Any] = {
        "checks": {},
        "tenants": {},
        "candidate_cases": [],
    }
    provenance_manifest: dict[str, Any] = {
        "label": label,
        "benchmark_kind": "multitenant_world_model",
        "max_branch_rows_per_thread": max_branch_rows_per_thread,
        "notes": [
            "Canonical event counts are loaded from the already-materialized context snapshot.",
            "Eligible branch rows require at least one pre-branch event and at least one future event in the same thread.",
            "Heldout rows are final-tail test rows duplicated into the heldout split for counterfactual scoring.",
        ],
        "tenants": {},
    }

    for source in sources:
        _validate_source_readiness(source)
        tenant_rows = _build_tenant_rows(
            source,
            future_horizon_events=future_horizon_events,
            max_branch_rows_per_thread=max_branch_rows_per_thread,
        )
        if not tenant_rows:
            raise ValueError(f"no eligible branch rows for tenant {source.tenant_id!r}")
        tenant_splits, tenant_heldout = _split_tenant_rows(
            tenant_rows,
            heldout_cases_per_tenant=heldout_cases_per_tenant,
        )
        tenant_split_counts = {
            split_name: len(rows) for split_name, rows in tenant_splits.items()
        }
        for split_name, rows in tenant_splits.items():
            split_rows[split_name].extend(rows)
        heldout_candidates.extend(tenant_heldout)
        leakage_manifest["tenants"][source.tenant_id] = {
            "display_name": source.display_name
            or source.world.summary.organization_name,
            "row_count": len(tenant_rows),
            "split_counts": tenant_split_counts,
            "heldout_case_count": len(tenant_heldout),
            "future_horizon_events": future_horizon_events,
        }
        provenance_manifest["tenants"][source.tenant_id] = _tenant_provenance_payload(
            source=source,
            eligible_branch_rows=len(tenant_rows),
            split_counts=tenant_split_counts,
            heldout_case_count=len(tenant_heldout),
            future_horizon_events=future_horizon_events,
            max_branch_rows_per_thread=max_branch_rows_per_thread,
        )

    benchmark_cases: list[WhatIfBenchmarkCase] = []
    heldout_rows: list[WhatIfBenchmarkDatasetRow] = []
    for item in heldout_candidates:
        generation = _generate_candidates_for_item(
            item,
            mode=candidate_generation_mode,
            model=candidate_model,
            root=root,
        )
        candidates = generation["candidates"]
        benchmark_case = WhatIfBenchmarkCase(
            case_id=item.row.contract.case_id,
            title=f"{item.display_name}: {item.subject or item.branch_event.event_id}",
            event_id=item.row.branch_event_id,
            thread_id=item.row.thread_id,
            summary=(
                "Multi-company held-out branch generated from pre-branch "
                "context only."
            ),
            case_family="multitenant_llm_generated",
            branch_event=item.row.contract.branch_event,
            history_preview=[
                event_reference(event) for event in item.history_events[-8:]
            ],
            candidates=candidates,
        )
        case_dossier_root = dossier_root / benchmark_case.case_id
        case_dossier_root.mkdir(parents=True, exist_ok=True)
        dossier_paths = _write_case_dossiers(
            case=benchmark_case,
            dossier_root=case_dossier_root,
        )
        benchmark_case = benchmark_case.model_copy(
            update={"objective_dossier_paths": dossier_paths}
        )
        benchmark_cases.append(benchmark_case)
        heldout_rows.append(item.row.model_copy(update={"split": "heldout"}))
        candidate_manifest.append(generation["manifest"])
        leakage_manifest["candidate_cases"].append(
            _case_leakage_payload(
                item=item,
                generation_prompt=generation["prompt"],
                dossier_paths=dossier_paths,
            )
        )

    split_rows["heldout"] = heldout_rows
    split_paths: dict[str, str] = {}
    split_counts: dict[str, int] = {}
    for split_name in ("train", "validation", "test", "heldout"):
        path = dataset_root / f"{split_name}_rows.jsonl"
        rows = split_rows[split_name]
        _write_jsonl(path, rows)
        split_paths[split_name] = str(path)
        split_counts[split_name] = len(rows)

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
    heldout_cases_path.write_text(
        json.dumps(
            [case.model_dump(mode="json") for case in benchmark_cases], indent=2
        ),
        encoding="utf-8",
    )

    fit_event_ids = {
        row.branch_event_id
        for split_name in ("train", "validation")
        for row in split_rows[split_name]
    }
    heldout_thread_ids = {row.thread_id for row in split_rows["heldout"]}
    heldout_event_ids = {row.branch_event_id for row in split_rows["heldout"]}
    leakage_manifest["checks"] = {
        "pre_branch_history_thread_overlap_allowed": True,
        "no_fit_heldout_branch_event_overlap": not (fit_event_ids & heldout_event_ids),
        "candidate_prompts_exclude_future_event_ids": all(
            item["candidate_prompt_excludes_future_event_ids"]
            for item in leakage_manifest["candidate_cases"]
        ),
        "judge_dossiers_exclude_future_event_ids": all(
            item["judge_dossiers_exclude_future_event_ids"]
            for item in leakage_manifest["candidate_cases"]
        ),
    }
    leakage_path = root / "leakage_report.json"
    leakage_path.write_text(json.dumps(leakage_manifest, indent=2), encoding="utf-8")
    candidate_manifest_path = root / "candidate_generation_manifest.json"
    candidate_manifest_path.write_text(
        json.dumps(candidate_manifest, indent=2),
        encoding="utf-8",
    )
    provenance_manifest["dataset_split_counts"] = split_counts
    provenance_manifest["tenant_count"] = len(sources)
    data_provenance_path.write_text(
        json.dumps(provenance_manifest, indent=2),
        encoding="utf-8",
    )

    dataset_manifest = WhatIfBenchmarkDatasetManifest(
        root=dataset_root,
        split_row_counts=split_counts,
        split_paths=split_paths,
        heldout_cases_path=str(heldout_cases_path),
        judge_template_path=str(judge_template_path),
        audit_template_path=str(audit_template_path),
        dossier_root=str(dossier_root),
        heldout_thread_ids=sorted(heldout_thread_ids),
        metadata={
            "benchmark_kind": "multitenant_world_model",
            "candidate_generation_mode": candidate_generation_mode,
            "candidate_model": candidate_model,
            "heldout_cases_per_tenant": heldout_cases_per_tenant,
            "future_horizon_events": future_horizon_events,
            "max_branch_rows_per_thread": max_branch_rows_per_thread,
            "candidate_generation_manifest_path": str(candidate_manifest_path),
            "leakage_report_path": str(leakage_path),
            "data_provenance_report_path": str(data_provenance_path),
        },
    )
    (dataset_root / "dataset_manifest.json").write_text(
        dataset_manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    result = WhatIfBenchmarkBuildResult(
        label=label,
        heldout_pack_id=_MULTITENANT_PACK_ID,
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
            "benchmark_kind": "multitenant_world_model",
            "candidate_generation_manifest_path": str(candidate_manifest_path),
            "leakage_report_path": str(leakage_path),
            "data_provenance_report_path": str(data_provenance_path),
            "max_branch_rows_per_thread": max_branch_rows_per_thread,
        },
    )
    build_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    return result


def build_candidate_generation_prompt(item: _RowCandidate) -> str:
    evidence_hash = _pre_branch_evidence_hash(
        branch_event=item.row.contract.branch_event,
        history=[event_reference(event) for event in item.history_events[-8:]],
    )
    history_lines = [
        (
            f"- {event.timestamp} {event.event_type} from {event.actor_id}: "
            f"{event.subject or event.snippet or event.event_id}"
        )
        for event in item.history_events[-8:]
    ]
    branch = item.row.contract.branch_event
    return "\n".join(
        [
            "You are generating broad, realistic counterfactual candidate actions.",
            "Use only the pre-branch evidence below. Do not infer from, mention, or rely on any recorded future outcome.",
            "",
            f"Tenant: {item.display_name}",
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
            "Return JSON with exactly four candidates. Use exactly these posture ids once each:",
            "- containment_hold",
            "- narrow_controlled_response",
            "- escalate_expert_review",
            "- speed_broad_coordination",
            "",
            "Each candidate must be a meaningfully different strategic action, not a minor wording variant.",
            "Return this JSON shape:",
            '{"candidates":[{"posture":"containment_hold","label":"...","prompt":"..."}]}',
        ]
    )


def _validate_source_readiness(source: MultiTenantBenchmarkSource) -> None:
    if not source.readiness_required:
        return
    if bool(source.readiness.get("ready_for_world_modeling")):
        return
    label = str(source.readiness.get("readiness_label") or "unknown")
    notes = source.readiness.get("notes") or []
    note_text = "; ".join(str(note) for note in notes if str(note).strip())
    detail = f" readiness_label={label}"
    if note_text:
        detail += f"; {note_text}"
    raise ValueError(
        f"tenant {source.tenant_id!r} failed timestamp readiness for world-model training.{detail}"
    )


def _tenant_provenance_payload(
    *,
    source: MultiTenantBenchmarkSource,
    eligible_branch_rows: int,
    split_counts: dict[str, int],
    heldout_case_count: int,
    future_horizon_events: int,
    max_branch_rows_per_thread: int,
) -> dict[str, Any]:
    source_count_total = 0
    source_records = _source_record_counts(source)
    for record in source_records:
        counts = record.get("record_counts") or {}
        if isinstance(counts, dict):
            source_count_total += sum(
                int(value) for value in counts.values() if isinstance(value, int)
            )
    used_split_rows = sum(split_counts.values())
    return {
        "display_name": source.display_name
        or source.world.summary.organization_name
        or source.tenant_id,
        "readiness": source.readiness,
        "source_record_counts": source_records,
        "source_record_count_total": source_count_total,
        "canonical_event_count": source.world.summary.event_count,
        "canonical_thread_count": source.world.summary.thread_count,
        "canonical_actor_count": source.world.summary.actor_count,
        "eligible_branch_rows": eligible_branch_rows,
        "split_counts": split_counts,
        "heldout_case_count": heldout_case_count,
        "dropped_by_temporal_split": max(0, eligible_branch_rows - used_split_rows),
        "future_horizon_events": future_horizon_events,
        "max_branch_rows_per_thread": max_branch_rows_per_thread,
    }


def _source_record_counts(source: MultiTenantBenchmarkSource) -> list[dict[str, Any]]:
    snapshot_path = _context_snapshot_path(source.world.source_dir)
    if snapshot_path is None:
        return []
    try:
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    source_payloads = payload.get("sources")
    if not isinstance(source_payloads, list):
        return []
    records: list[dict[str, Any]] = []
    for item in source_payloads:
        if not isinstance(item, dict):
            continue
        record_counts = item.get("record_counts")
        records.append(
            {
                "provider": item.get("provider") or "",
                "status": item.get("status") or "",
                "record_counts": (
                    record_counts if isinstance(record_counts, dict) else {}
                ),
            }
        )
    return records


def _context_snapshot_path(path: Path) -> Path | None:
    candidate = Path(path).expanduser()
    if candidate.is_file():
        return candidate
    context_path = candidate / "context_snapshot.json"
    if context_path.is_file():
        return context_path
    return None


def validate_candidate_diversity(
    candidates: Sequence[WhatIfBenchmarkCandidate],
) -> None:
    if len(candidates) != len(_REQUIRED_POSTURES):
        raise ValueError("candidate generation must produce exactly four candidates")
    postures = [str(item.metadata.get("posture") or "") for item in candidates]
    if set(postures) != set(_REQUIRED_POSTURES):
        raise ValueError(
            "candidate generation must include each required broad posture exactly once"
        )
    seen_labels = {candidate.label.strip().lower() for candidate in candidates}
    if len(seen_labels) != len(candidates):
        raise ValueError("candidate labels must be distinct")
    signatures = {
        (
            candidate.action_schema.decision_posture,
            candidate.action_schema.review_path,
            candidate.action_schema.coordination_breadth,
            candidate.action_schema.outside_sharing_posture,
        )
        for candidate in candidates
    }
    if len(signatures) < len(candidates):
        raise ValueError("candidate action schemas are not broad enough")
    for left_index, left in enumerate(candidates):
        for right in candidates[left_index + 1 :]:
            similarity = _token_similarity(left.prompt, right.prompt)
            if similarity >= 0.86:
                raise ValueError(
                    f"candidate prompts are too similar: {left.candidate_id} and {right.candidate_id}"
                )


def _build_tenant_rows(
    source: MultiTenantBenchmarkSource,
    *,
    future_horizon_events: int,
    max_branch_rows_per_thread: int,
) -> list[_RowCandidate]:
    events_by_thread = _group_events_by_thread(source.world.events)
    thread_subjects = {
        thread.thread_id: thread.subject for thread in source.world.threads
    }
    rows: list[_RowCandidate] = []
    for raw_thread_id, timeline in events_by_thread.items():
        if len(timeline) < 3:
            continue
        for branch_index in _branch_indices_for_timeline(
            len(timeline),
            max_branch_rows_per_thread=max_branch_rows_per_thread,
        ):
            branch_event = timeline[branch_index]
            history_events = list(timeline[:branch_index])
            if future_horizon_events > 0:
                future_events = list(
                    timeline[
                        branch_index + 1 : branch_index + 1 + future_horizon_events
                    ]
                )
            else:
                future_events = list(timeline[branch_index + 1 :])
            if not history_events or not future_events:
                continue
            rows.append(
                _row_candidate_from_branch(
                    source=source,
                    raw_thread_id=raw_thread_id,
                    branch_event=branch_event,
                    history_events=history_events,
                    future_events=future_events,
                    subject=thread_subjects.get(raw_thread_id) or branch_event.subject,
                )
            )
    return sorted(rows, key=lambda item: (item.branch_timestamp_ms, item.row.row_id))


def _row_candidate_from_branch(
    *,
    source: MultiTenantBenchmarkSource,
    raw_thread_id: str,
    branch_event: WhatIfEvent,
    history_events: Sequence[WhatIfEvent],
    future_events: Sequence[WhatIfEvent],
    subject: str,
) -> _RowCandidate:
    safe_thread_id = _safe_id(source.tenant_id, raw_thread_id)
    safe_event_id = _safe_id(source.tenant_id, branch_event.event_id)
    safe_case_id = _safe_id(source.tenant_id, branch_event.case_id or raw_thread_id)
    normalized_branch = branch_event.model_copy(
        update={
            "event_id": safe_event_id,
            "thread_id": safe_thread_id,
            "case_id": safe_case_id,
        }
    )
    action_schema = _action_schema_from_event(
        normalized_branch,
        organization_domain=source.world.summary.organization_domain,
    )
    contract = _build_pre_branch_contract(
        case_id=safe_case_id,
        thread_id=safe_thread_id,
        branch_event=normalized_branch,
        history_events=history_events,
        organization_domain=source.world.summary.organization_domain,
        action_schema=action_schema,
        notes=[
            "Multi-tenant observed historical branch row.",
            f"tenant_id={source.tenant_id}",
            "no_future_context=true",
        ],
    )
    evidence = summarize_observed_evidence(
        branch_event=branch_event,
        future_events=future_events,
    )
    future_state = summarize_future_state_heads(
        future_events=future_events,
        evidence=evidence,
    )
    targets = summarize_observed_targets(
        branch_event=branch_event,
        future_events=future_events,
        organization_domain=source.world.summary.organization_domain,
    )
    row = WhatIfBenchmarkDatasetRow(
        row_id=f"{safe_thread_id}:{safe_event_id}",
        split="train",
        thread_id=safe_thread_id,
        branch_event_id=safe_event_id,
        contract=contract,
        observed_evidence_heads=evidence,
        observed_business_outcomes=evidence_to_business_outcomes(evidence),
        observed_future_state=future_state,
        observed_targets=targets,
        observed_outcome_signals=outcome_targets_to_signals(targets),
    )
    return _RowCandidate(
        tenant_id=source.tenant_id,
        display_name=source.display_name
        or source.world.summary.organization_name
        or source.tenant_id,
        world=source.world,
        row=row,
        branch_event=normalized_branch,
        raw_branch_event_id=branch_event.event_id,
        history_events=list(history_events[-8:]),
        future_events=list(future_events),
        subject=subject,
        branch_timestamp_ms=branch_event.timestamp_ms,
        target_end_timestamp_ms=max(event.timestamp_ms for event in future_events),
    )


def _branch_indices_for_timeline(
    event_count: int,
    *,
    max_branch_rows_per_thread: int,
) -> list[int]:
    if event_count < 3:
        return []
    branch_indices = list(range(1, event_count - 1))
    if len(branch_indices) <= max_branch_rows_per_thread:
        return branch_indices
    if max_branch_rows_per_thread == 1:
        return [branch_indices[-1]]

    sampled: list[int] = []
    last_index = len(branch_indices) - 1
    for sample_index in range(max_branch_rows_per_thread):
        raw_position = round(
            sample_index * last_index / max(max_branch_rows_per_thread - 1, 1)
        )
        branch_index = branch_indices[raw_position]
        if branch_index not in sampled:
            sampled.append(branch_index)
    if sampled[-1] != branch_indices[-1]:
        sampled[-1] = branch_indices[-1]
    return sampled


def _split_tenant_rows(
    rows: Sequence[_RowCandidate],
    *,
    heldout_cases_per_tenant: int,
) -> tuple[dict[str, list[WhatIfBenchmarkDatasetRow]], list[_RowCandidate]]:
    ordered = sorted(rows, key=lambda item: (item.branch_timestamp_ms, item.row.row_id))
    count = len(ordered)
    if count <= 2:
        train_cutoff = max(1, count - 1)
        validation_cutoff = train_cutoff
    else:
        train_cutoff = max(1, int(count * 0.7))
        validation_cutoff = max(train_cutoff, int(count * 0.85))
        if validation_cutoff >= count:
            validation_cutoff = count - 1
    train_cutoff_ts = ordered[train_cutoff].branch_timestamp_ms
    validation_cutoff_ts = ordered[validation_cutoff].branch_timestamp_ms
    item_buckets: dict[str, list[_RowCandidate]] = defaultdict(list)
    for item in ordered:
        if (
            item.branch_timestamp_ms < train_cutoff_ts
            and item.target_end_timestamp_ms < train_cutoff_ts
        ):
            split: WhatIfBenchmarkSplit = "train"
        elif (
            item.branch_timestamp_ms >= train_cutoff_ts
            and item.branch_timestamp_ms < validation_cutoff_ts
            and item.target_end_timestamp_ms < validation_cutoff_ts
        ):
            split = "validation"
        elif item.branch_timestamp_ms >= validation_cutoff_ts:
            split = "test"
        else:
            continue
        item_buckets[split].append(item)
    test_candidates = item_buckets["test"] or ordered[-1:]
    heldout_count = min(max(1, heldout_cases_per_tenant), len(test_candidates))
    heldout_reversed: list[_RowCandidate] = []
    heldout_threads: set[str] = set()
    for item in reversed(test_candidates):
        if item.row.thread_id in heldout_threads:
            continue
        heldout_threads.add(item.row.thread_id)
        heldout_reversed.append(item)
        if len(heldout_reversed) >= heldout_count:
            break
    if len(heldout_reversed) < heldout_count:
        for item in reversed(test_candidates):
            if item in heldout_reversed:
                continue
            heldout_reversed.append(item)
            if len(heldout_reversed) >= heldout_count:
                break
    heldout = sorted(
        heldout_reversed,
        key=lambda item: (item.branch_timestamp_ms, item.row.row_id),
    )
    buckets: dict[str, list[WhatIfBenchmarkDatasetRow]] = defaultdict(list)
    for split_name in ("train", "validation", "test"):
        for item in item_buckets[split_name]:
            buckets[split_name].append(
                item.row.model_copy(update={"split": split_name})
            )
    return buckets, heldout


def _generate_candidates_for_item(
    item: _RowCandidate,
    *,
    mode: CandidateGenerationMode,
    model: str,
    root: Path,
) -> dict[str, Any]:
    prompt = build_candidate_generation_prompt(item)
    prompt_hash = sha256(prompt.encode("utf-8")).hexdigest()
    evidence_hash = _pre_branch_evidence_hash(
        branch_event=item.row.contract.branch_event,
        history=[event_reference(event) for event in item.history_events[-8:]],
    )
    if mode == "llm":
        raw = run_llm_judge_prompt(
            prompt,
            model=model,
            max_tokens=1600,
            temperature=0.0,
            json_mode=True,
        )
        payload = json.loads(raw)
        proposals = list(payload.get("candidates") or [])
        source = "llm"
    else:
        proposals = _template_candidate_payloads(item)
        source = "template"
    candidates = _candidates_from_payloads(
        proposals,
        item=item,
        model=model,
        source=source,
        prompt_hash=prompt_hash,
        evidence_hash=evidence_hash,
    )
    validate_candidate_diversity(candidates)
    prompt_path = root / "candidate_prompts" / f"{item.row.contract.case_id}.txt"
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text(prompt, encoding="utf-8")
    return {
        "prompt": prompt,
        "candidates": candidates,
        "manifest": {
            "case_id": item.row.contract.case_id,
            "tenant_id": item.tenant_id,
            "source": source,
            "model": model,
            "prompt_path": str(prompt_path),
            "generation_prompt_sha256": prompt_hash,
            "pre_branch_evidence_sha256": evidence_hash,
            "no_future_context": True,
            "candidate_ids": [candidate.candidate_id for candidate in candidates],
        },
    }


def _candidates_from_payloads(
    payloads: Sequence[dict[str, Any]],
    *,
    item: _RowCandidate,
    model: str,
    source: str,
    prompt_hash: str,
    evidence_hash: str,
) -> list[WhatIfBenchmarkCandidate]:
    by_posture: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        posture = str(payload.get("posture") or "").strip().lower()
        if posture in _REQUIRED_POSTURES and posture not in by_posture:
            by_posture[posture] = dict(payload)
    missing = [posture for posture in _REQUIRED_POSTURES if posture not in by_posture]
    if missing:
        raise ValueError(f"candidate generation missing postures: {', '.join(missing)}")

    historical_action = item.row.contract.action_schema
    candidates: list[WhatIfBenchmarkCandidate] = []
    for posture in _REQUIRED_POSTURES:
        payload = by_posture[posture]
        prompt = str(payload.get("prompt") or "").strip()
        label = str(payload.get("label") or "").strip()
        if not label or not prompt:
            raise ValueError(f"candidate {posture} must include label and prompt")
        action_schema = _posture_action_schema(
            posture,
            _action_schema_from_prompt(
                prompt,
                branch_event=item.branch_event,
                historical_action=historical_action,
            ),
        )
        candidates.append(
            WhatIfBenchmarkCandidate(
                candidate_id=posture,
                label=label,
                prompt=prompt,
                action_schema=action_schema,
                expected_hypotheses=_expected_hypotheses_for_posture(posture),
                metadata={
                    "posture": posture,
                    "generation_source": source,
                    "generation_model": model,
                    "generation_prompt_sha256": prompt_hash,
                    "pre_branch_evidence_sha256": evidence_hash,
                    "no_future_context": True,
                },
            )
        )
    return candidates


def _posture_action_schema(
    posture: str,
    base: WhatIfActionSchema,
) -> WhatIfActionSchema:
    if posture == "containment_hold":
        return base.model_copy(
            update={
                "recipient_scope": "internal",
                "external_recipient_count": 0,
                "hold_required": True,
                "legal_review_required": True,
                "review_path": "internal_legal",
                "coordination_breadth": "narrow",
                "outside_sharing_posture": "internal_only",
                "decision_posture": "hold",
                "action_tags": sorted(set(base.action_tags) | {"hold", "legal"}),
            }
        )
    if posture == "narrow_controlled_response":
        return base.model_copy(
            update={
                "recipient_scope": "external",
                "external_recipient_count": 1,
                "attachment_policy": "sanitized",
                "owner_clarity": "single_owner",
                "review_path": "business_owner",
                "coordination_breadth": "single_owner",
                "outside_sharing_posture": "status_only",
                "decision_posture": "resolve",
                "action_tags": sorted(
                    set(base.action_tags) | {"status_only", "clarify_owner"}
                ),
            }
        )
    if posture == "escalate_expert_review":
        return base.model_copy(
            update={
                "escalation_level": "manager",
                "owner_clarity": "single_owner",
                "review_path": "cross_functional",
                "coordination_breadth": "targeted",
                "outside_sharing_posture": "limited_external",
                "decision_posture": "escalate",
                "action_tags": sorted(
                    set(base.action_tags) | {"expert_review", "executive_gate"}
                ),
            }
        )
    return base.model_copy(
        update={
            "recipient_scope": "mixed",
            "external_recipient_count": max(2, base.external_recipient_count),
            "escalation_level": "executive",
            "owner_clarity": "multi_owner",
            "review_path": "executive",
            "coordination_breadth": "broad",
            "outside_sharing_posture": "broad_external",
            "decision_posture": "resolve",
            "action_tags": sorted(set(base.action_tags) | {"send_now", "widen_loop"}),
        }
    )


def _expected_hypotheses_for_posture(
    posture: str,
) -> dict[WhatIfBusinessObjectivePackId, WhatIfResearchHypothesisLabel]:
    if posture == "containment_hold":
        return {
            "minimize_enterprise_risk": "best_expected",
            "protect_commercial_position": "middle_expected",
            "reduce_org_strain": "middle_expected",
            "preserve_stakeholder_trust": "middle_expected",
            "maintain_execution_velocity": "worst_expected",
        }
    if posture == "narrow_controlled_response":
        return {
            "minimize_enterprise_risk": "middle_expected",
            "protect_commercial_position": "best_expected",
            "reduce_org_strain": "middle_expected",
            "preserve_stakeholder_trust": "best_expected",
            "maintain_execution_velocity": "middle_expected",
        }
    if posture == "escalate_expert_review":
        return {
            "minimize_enterprise_risk": "middle_expected",
            "protect_commercial_position": "middle_expected",
            "reduce_org_strain": "worst_expected",
            "preserve_stakeholder_trust": "middle_expected",
            "maintain_execution_velocity": "middle_expected",
        }
    return {
        "minimize_enterprise_risk": "worst_expected",
        "protect_commercial_position": "middle_expected",
        "reduce_org_strain": "worst_expected",
        "preserve_stakeholder_trust": "worst_expected",
        "maintain_execution_velocity": "best_expected",
    }


def _template_candidate_payloads(item: _RowCandidate) -> list[dict[str, str]]:
    subject = item.subject or item.branch_event.subject or "this decision"
    return [
        {
            "posture": "containment_hold",
            "label": "Hold and contain internally",
            "prompt": (
                f'Hold "{subject}" inside the company, stop external sharing, '
                "route it through legal or the accountable owner, and wait for one clean approval."
            ),
        },
        {
            "posture": "narrow_controlled_response",
            "label": "Send one controlled status note",
            "prompt": (
                f'Send one narrow no-attachment status note on "{subject}", '
                "name one owner, and promise a concrete follow-up."
            ),
        },
        {
            "posture": "escalate_expert_review",
            "label": "Escalate to expert review",
            "prompt": (
                f'Escalate "{subject}" to a focused expert review group, '
                "ask for one accountable recommendation, and keep distribution targeted."
            ),
        },
        {
            "posture": "speed_broad_coordination",
            "label": "Move fast with a broad loop",
            "prompt": (
                f'Push "{subject}" forward immediately, widen the loop for rapid input, '
                "and keep external coordination active."
            ),
        },
    ]


def _case_leakage_payload(
    *,
    item: _RowCandidate,
    generation_prompt: str,
    dossier_paths: dict[str, str],
) -> dict[str, Any]:
    tail_events = [
        event
        for event in item.future_events
        if event.event_id != item.raw_branch_event_id
    ]
    future_event_ids = [event.event_id for event in tail_events]
    allowed_evidence_text = _allowed_pre_branch_marker_text(item)
    future_markers = [
        marker
        for event in tail_events
        for marker in _future_unique_markers(event, allowed_text=allowed_evidence_text)
        if marker
    ]
    prompt_hits = sorted(
        {marker for marker in future_markers if marker in generation_prompt}
    )
    dossier_text = "\n".join(
        Path(path).read_text(encoding="utf-8") for path in dossier_paths.values()
    )
    dossier_hits = sorted(
        {marker for marker in future_markers if marker in dossier_text}
    )
    return {
        "case_id": item.row.contract.case_id,
        "tenant_id": item.tenant_id,
        "branch_event_id": item.row.branch_event_id,
        "future_event_count": len(future_event_ids),
        "future_marker_count": len(future_markers),
        "candidate_prompt_future_marker_hits": prompt_hits,
        "judge_dossier_future_marker_hits": dossier_hits,
        "candidate_prompt_excludes_future_event_ids": not prompt_hits,
        "judge_dossiers_exclude_future_event_ids": not dossier_hits,
    }


def _allowed_pre_branch_marker_text(item: _RowCandidate) -> str:
    values: list[str] = []
    branch = item.row.contract.branch_event
    values.extend(
        [
            branch.event_id,
            branch.timestamp,
            branch.subject,
            branch.snippet,
        ]
    )
    for event in item.history_events:
        values.extend(
            [
                event.event_id,
                event.timestamp,
                event.subject,
                event.snippet,
            ]
        )
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


def _group_events_by_thread(
    events: Sequence[WhatIfEvent],
) -> dict[str, list[WhatIfEvent]]:
    grouped: dict[str, list[WhatIfEvent]] = defaultdict(list)
    for event in sorted(events, key=lambda item: (item.timestamp_ms, item.event_id)):
        grouped[event.thread_id].append(event)
    return grouped


def _safe_id(tenant_id: str, value: str) -> str:
    normalized_tenant = _slug(tenant_id)
    normalized_value = str(value or "unknown").strip()
    return f"{normalized_tenant}:{normalized_value}"


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


__all__ = [
    "CandidateGenerationMode",
    "MultiTenantBenchmarkSource",
    "build_candidate_generation_prompt",
    "build_multitenant_world_model_benchmark",
    "validate_candidate_diversity",
]
