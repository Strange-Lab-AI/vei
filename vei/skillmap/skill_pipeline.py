from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast


from vei.context.api import (
    ContextSnapshot,
    build_canonical_history_bundle,
    hydrate_blueprint,
    load_canonical_history_bundle,
)
from vei.llm.providers import plan_once_with_usage
from vei.project_settings import resolve_llm_defaults
from vei.structure.api import build_structure_view_from_canonical_events
from vei.whatif.api import WhatIfWorld, list_branch_candidates, load_world
from vei.world.api import WorldSessionAPI

from vei.skillmap.models import (
    CONTEXT_SNAPSHOT_FILE,
    SKILLMAP_CLUSTER_TOOL,
    SKILLMAP_LLM_TOOL,
    USEFULNESS_SCORE_KEYS,
    CompanySkill,
    CompanySkillMap,
    SkillCandidateType,
    SkillDeploymentReadiness,
    SkillEvidenceRef,
    SkillExecutionMode,
    SkillFreshnessStatus,
    SkillMapGap,
    SkillMapValidation,
    SkillOutputArtifact,
    SkillReplayOutcome,
    SkillReplayResult,
    SkillStep,
    SkillTrigger,
    SkillValidationIssue,
)


def build_company_skill_map_from_context_path(
    path: str | Path,
    *,
    limit: int = 12,
    include_replay: bool = True,
    provider: str | None = None,
    model: str | None = None,
    previous_map_path: str | Path | None = None,
    timeout_s: int = 240,
    catalog_shard_size: int = 80,
) -> CompanySkillMap:
    """Build a deployable shadow-mode skill map from a context bundle."""
    snapshot_path = _resolve_snapshot_path(path)
    snapshot = ContextSnapshot.model_validate_json(
        snapshot_path.read_text(encoding="utf-8")
    )
    bundle = load_canonical_history_bundle(snapshot_path)
    if bundle is None:
        bundle = build_canonical_history_bundle(snapshot)
    structure_view = build_structure_view_from_canonical_events(
        bundle.events,
        source_mode="canonical_history",
    )
    blueprint = hydrate_blueprint(
        snapshot,
        scenario_name="multi_channel",
        workflow_name="company_skill_map",
    )
    event_index = {
        row.event_id: row.model_dump(mode="json") for row in bundle.index.rows
    }
    structure_payload = _model_dump(structure_view)
    graphs_payload = _model_dump(blueprint.capability_graphs)
    evidence_catalog = _build_skill_evidence_catalog(
        structure_payload=structure_payload,
        graphs_payload=graphs_payload,
        event_index=event_index,
        limit=None,
    )
    llm_evidence_catalog = _compact_evidence_catalog_for_llm(evidence_catalog)
    skills, extraction_metadata, extraction_gaps = _extract_context_skills_with_llm(
        organization_name=snapshot.organization_name,
        organization_domain=snapshot.organization_domain,
        source_providers=list(bundle.index.source_providers),
        canonical_event_count=len(bundle.events),
        evidence_catalog=llm_evidence_catalog,
        limit=limit,
        provider=provider,
        model=model,
        timeout_s=timeout_s,
        catalog_shard_size=catalog_shard_size,
    )
    compacted_processed_count = int(
        extraction_metadata.get("evidence_catalog_processed_count") or 0
    )
    extraction_metadata["raw_evidence_catalog_count"] = len(evidence_catalog)
    extraction_metadata["llm_evidence_catalog_count"] = len(llm_evidence_catalog)
    extraction_metadata["llm_evidence_catalog_processed_count"] = (
        compacted_processed_count
    )
    extraction_metadata["evidence_catalog_count"] = len(evidence_catalog)
    extraction_metadata["evidence_catalog_processed_count"] = len(evidence_catalog)
    gaps = _build_map_gaps(
        structure_payload=structure_payload,
        graphs_payload=graphs_payload,
        event_index=event_index,
        source_statuses=[source.model_dump(mode="json") for source in snapshot.sources],
        canonical_event_count=len(bundle.events),
        skills=skills,
    )
    gaps.extend(extraction_gaps)
    skill_map = CompanySkillMap(
        organization_name=snapshot.organization_name or "Unknown organization",
        organization_domain=snapshot.organization_domain,
        generated_at=_utc_now_iso(),
        source_ref=str(snapshot_path),
        source_providers=_unique(list(bundle.index.source_providers)),
        canonical_event_count=len(bundle.events),
        skill_count=len(skills),
        skills=skills,
        gaps=gaps,
        metadata={
            "builder": "context_bundle",
            "bundle_role": str(snapshot.metadata.get("snapshot_role", "")),
            "skill_source_policy": "context_bundle_only",
            "skill_extractor": "llm",
            "graph_plan_skills": "omitted_for_context_bundle_to_avoid_template_leakage",
            **extraction_metadata,
        },
    )
    if include_replay:
        replay_world, replay_error = _load_replay_world(snapshot_path)
        if replay_world is not None:
            _attach_replay_results(skill_map, replay_world, limit=limit)
        else:
            skill_map.gaps.append(
                SkillMapGap(
                    gap_id="gap:replay_world_unavailable",
                    title="Replay world unavailable",
                    severity="warning",
                    reason=(
                        "The skill map could not load a historical what-if world for shadow replay"
                        + (f": {replay_error}" if replay_error else ".")
                    ),
                    recommendation="Ensure the context bundle has canonical history sidecars before using replay scores for activation.",
                )
            )
    previous_map = _load_previous_skill_map(previous_map_path)
    if previous_map is not None:
        _apply_previous_skill_map(skill_map, previous_map)
    return _finalize_skill_map(skill_map)


def build_company_skill_map_from_session(
    session: WorldSessionAPI, *, limit: int = 12
) -> CompanySkillMap:
    """Expose the runtime skill-map schema without deterministic synthesis."""
    structure_view = session.structure_view()
    graphs = session.capability_graphs()
    graph_payload = _model_dump(graphs)
    metadata = dict(graph_payload.get("metadata") or {})
    organization_domain = str(metadata.get("organization_domain") or "")
    skill_map = CompanySkillMap(
        organization_name="Current VEI World",
        organization_domain=organization_domain,
        generated_at=_utc_now_iso(),
        source_ref="world_session",
        source_providers=list(graph_payload.get("available_domains") or []),
        canonical_event_count=int(
            _model_dump(structure_view).get("total_event_count") or 0
        ),
        skill_count=0,
        skills=[],
        gaps=[
            SkillMapGap(
                gap_id="gap:runtime_skill_synthesis_disabled",
                title="Runtime skill synthesis disabled",
                severity="warning",
                reason=(
                    "The MCP world session does not synthesize skills with "
                    "deterministic templates. Company skills require an LLM pass "
                    "over a normalized context bundle."
                ),
                recommendation=(
                    "Run `vei skillmap build --source-dir <context_snapshot.json>` "
                    "with a configured LLM provider."
                ),
            )
        ],
        metadata={
            "builder": "world_session",
            "replay_available": False,
            "replay_note": "Session maps are read-only runtime snapshots; replay scoring requires a context bundle.",
            "skill_source_policy": "current_world_session",
            "graph_plan_skills": "omitted_to_avoid_template_leakage",
            "skill_extractor": "disabled_without_context_bundle_llm",
            "requested_limit": limit,
        },
    )
    return _finalize_skill_map(skill_map)


def validate_company_skill_map(skill_map: CompanySkillMap) -> SkillMapValidation:
    issues: list[SkillValidationIssue] = []
    skill_ids: set[str] = set()
    active_skill_count = 0
    draft_skill_count = 0

    if not skill_map.skills:
        issues.append(
            SkillValidationIssue(
                severity="error",
                code="map_without_skills",
                message="A deployable company skill map must contain at least one skill.",
            )
        )

    for skill in skill_map.skills:
        if skill.skill_id in skill_ids:
            issues.append(
                SkillValidationIssue(
                    severity="error",
                    code="duplicate_skill_id",
                    message=f"Duplicate skill id: {skill.skill_id}",
                    skill_id=skill.skill_id,
                )
            )
        skill_ids.add(skill.skill_id)
        if skill.status == "active":
            active_skill_count += 1
        if skill.status == "draft":
            draft_skill_count += 1
        if not skill.evidence_refs:
            issues.append(
                SkillValidationIssue(
                    severity="error",
                    code="skill_without_evidence",
                    message="Every company skill must cite source evidence.",
                    skill_id=skill.skill_id,
                )
            )
        if not skill.steps:
            issues.append(
                SkillValidationIssue(
                    severity="error",
                    code="skill_without_steps",
                    message="Every company skill must define executable steps.",
                    skill_id=skill.skill_id,
                )
            )
        if skill.candidate_type == "gap":
            issues.append(
                SkillValidationIssue(
                    severity="error",
                    code="gap_candidate_promoted_to_skill",
                    message="Gap candidates should be emitted as map gaps, not skills.",
                    skill_id=skill.skill_id,
                )
            )
        if not skill.output_artifacts:
            issues.append(
                SkillValidationIssue(
                    severity="warning",
                    code="skill_without_output_artifact",
                    message="Useful skills should define the artifact they produce.",
                    skill_id=skill.skill_id,
                )
            )
        if not skill.negative_triggers:
            issues.append(
                SkillValidationIssue(
                    severity="warning",
                    code="skill_without_negative_triggers",
                    message="Skills should define when not to use them.",
                    skill_id=skill.skill_id,
                )
            )
        if not skill.replay_checks:
            issues.append(
                SkillValidationIssue(
                    severity="warning",
                    code="skill_without_replay_checks",
                    message="Skills should define skill-specific replay checks.",
                    skill_id=skill.skill_id,
                )
            )
        if skill.status == "active":
            if not skill.owner or not skill.reviewer:
                issues.append(
                    SkillValidationIssue(
                        severity="error",
                        code="active_skill_missing_owner_or_reviewer",
                        message="Active skills require a named owner and reviewer.",
                        skill_id=skill.skill_id,
                    )
                )
            if skill.replay_score < 0.6 or not skill.replay_results:
                issues.append(
                    SkillValidationIssue(
                        severity="error",
                        code="active_skill_missing_replay_proof",
                        message=(
                            "Active skills require at least one historical replay "
                            "result and replay_score >= 0.60."
                        ),
                        skill_id=skill.skill_id,
                    )
                )
            if skill.review_status != "reviewed":
                issues.append(
                    SkillValidationIssue(
                        severity="error",
                        code="active_skill_not_reviewed",
                        message="Active skills must be reviewed before deployment.",
                        skill_id=skill.skill_id,
                    )
                )
            if skill.freshness_status == "stale":
                issues.append(
                    SkillValidationIssue(
                        severity="error",
                        code="active_skill_stale",
                        message="Active skills cannot be backed only by stale evidence.",
                        skill_id=skill.skill_id,
                    )
                )
        elif skill.status == "draft":
            if not skill.owner or not skill.reviewer:
                issues.append(
                    SkillValidationIssue(
                        severity="warning",
                        code="draft_skill_needs_owner_and_reviewer",
                        message="Draft skills should name an owner and reviewer before activation.",
                        skill_id=skill.skill_id,
                    )
                )
        for step in skill.steps:
            if step.read_only:
                continue
            if step.requires_approval:
                continue
            if skill.execution_mode == "shadow":
                continue
            issues.append(
                SkillValidationIssue(
                    severity="error",
                    code="write_step_without_approval",
                    message=(
                        "Non-read-only steps must either stay in shadow mode or "
                        "require approval."
                    ),
                    skill_id=skill.skill_id,
                )
            )

    for gap in skill_map.gaps:
        issues.append(
            SkillValidationIssue(
                severity=gap.severity,
                code="map_gap",
                message=gap.reason,
                gap_id=gap.gap_id,
            )
        )

    error_count = sum(1 for issue in issues if issue.severity == "error")
    warning_count = sum(1 for issue in issues if issue.severity == "warning")
    return SkillMapValidation(
        ok=error_count == 0,
        error_count=error_count,
        warning_count=warning_count,
        active_skill_count=active_skill_count,
        draft_skill_count=draft_skill_count,
        gap_count=len(skill_map.gaps),
        issues=issues,
    )


def render_company_skill_map_markdown(skill_map: CompanySkillMap) -> str:
    lines = [
        f"# Company Skill Map: {skill_map.organization_name}",
        "",
        f"- Schema: `{skill_map.schema_version}`",
        f"- Source: `{skill_map.source_ref}`",
        f"- Sources: {', '.join(skill_map.source_providers) or 'none'}",
        f"- Canonical events: {skill_map.canonical_event_count}",
        f"- Skills: {skill_map.skill_count}",
        f"- Validation: {skill_map.validation.error_count} errors, {skill_map.validation.warning_count} warnings",
        "",
        "## Skills",
    ]
    if not skill_map.skills:
        lines.extend(["", "No skills were inferred from the supplied evidence."])
    for skill in skill_map.skills:
        lines.extend(
            [
                "",
                f"### {skill.title}",
                "",
                f"- ID: `{skill.skill_id}`",
                f"- Status: `{skill.status}`",
                f"- Mode: `{skill.execution_mode}`",
                f"- Candidate type: `{skill.candidate_type}`",
                f"- Domain: `{skill.domain or 'unknown'}`",
                f"- Usefulness: {skill.usefulness_score:.2f}",
                f"- Confidence: {skill.confidence:.2f}",
                f"- Freshness: `{skill.freshness_status}`",
                f"- Replay score: {skill.replay_score:.2f}",
                f"- Readiness: `{skill.deployment_readiness}`",
                f"- Trigger: {skill.trigger.description}",
                f"- Goal: {skill.goal}",
                "",
                "Negative triggers:",
            ]
        )
        if skill.negative_triggers:
            for item in skill.negative_triggers:
                lines.append(f"- {item}")
        else:
            lines.append("- none")
        lines.extend(
            [
                "",
                "Steps:",
            ]
        )
        for index, step in enumerate(skill.steps, start=1):
            guard = "read-only" if step.read_only else "approval required"
            lines.append(
                f"{index}. {step.instruction} (`{step.tool or step.graph_action or 'manual'}`, {guard})"
            )
        lines.extend(["", "Output artifacts:"])
        if skill.output_artifacts:
            for artifact in skill.output_artifacts:
                hint = f" - {artifact.schema_hint}" if artifact.schema_hint else ""
                lines.append(
                    f"- `{artifact.artifact_id}` {artifact.title} ({artifact.kind or 'artifact'}){hint}"
                )
        else:
            lines.append("- none")
        lines.extend(["", "Replay checks:"])
        if skill.replay_checks:
            for check in skill.replay_checks:
                lines.append(f"- {check}")
        else:
            lines.append("- none")
        lines.extend(["", "Evidence:"])
        for evidence in skill.evidence_refs:
            label = evidence.title or evidence.ref_id
            details = []
            if evidence.surface:
                details.append(evidence.surface)
            if evidence.timestamp:
                details.append(evidence.timestamp)
            suffix = f" ({', '.join(details)})" if details else ""
            lines.append(
                f"- `{evidence.ref_type}:{evidence.ref_id}` {label}{suffix}".rstrip()
            )
        if skill.replay_results:
            lines.extend(["", "Replay:"])
            for replay in skill.replay_results:
                lines.append(
                    f"- `{replay.outcome}` score={replay.alignment_score:.2f} case={replay.case_id or 'unknown'} branch={replay.branch_event_id or 'n/a'}"
                )
    if skill_map.gaps:
        lines.extend(["", "## Gaps"])
        for gap in skill_map.gaps:
            lines.extend(
                [
                    "",
                    f"### {gap.title}",
                    "",
                    f"- Severity: `{gap.severity}`",
                    f"- Reason: {gap.reason}",
                    f"- Recommendation: {gap.recommendation}",
                ]
            )
    return "\n".join(lines).strip() + "\n"


def render_skill_evidence_report(skill_map: CompanySkillMap) -> str:
    lines = [
        f"# Skill Evidence Report: {skill_map.organization_name}",
        "",
        f"Generated: {skill_map.generated_at}",
        "",
    ]
    for skill in skill_map.skills:
        lines.extend([f"## {skill.title}", ""])
        if not skill.evidence_refs:
            lines.extend(["No evidence refs.", ""])
            continue
        for evidence in skill.evidence_refs:
            lines.append(
                f"- `{evidence.ref_type}:{evidence.ref_id}` {evidence.title or evidence.snippet}"
            )
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def render_skill_gap_report(skill_map: CompanySkillMap) -> str:
    lines = [
        f"# Skill Gap Report: {skill_map.organization_name}",
        "",
        f"Validation: {skill_map.validation.error_count} errors, {skill_map.validation.warning_count} warnings",
        "",
    ]
    if not skill_map.gaps:
        lines.append("No structural gaps were detected.")
    for gap in skill_map.gaps:
        lines.extend(
            [
                f"## {gap.title}",
                "",
                f"- ID: `{gap.gap_id}`",
                f"- Severity: `{gap.severity}`",
                f"- Reason: {gap.reason}",
                f"- Recommendation: {gap.recommendation}",
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def render_skill_replay_report(skill_map: CompanySkillMap) -> str:
    lines = [
        f"# Skill Replay Report: {skill_map.organization_name}",
        "",
        "Historical replay tests compare each candidate skill with the cases and "
        "future events already present in the company history. They are "
        "decision-support checks, not causal proof.",
        "",
    ]
    if not skill_map.skills:
        lines.append("No skills were available for replay.")
    for skill in skill_map.skills:
        lines.extend(
            [
                f"## {skill.title}",
                "",
                f"- Skill ID: `{skill.skill_id}`",
                f"- Replay score: {skill.replay_score:.2f}",
                f"- Readiness: `{skill.deployment_readiness}`",
                "",
            ]
        )
        if not skill.replay_results:
            lines.extend(["No replay results attached.", ""])
            continue
        for replay in skill.replay_results:
            surfaces = ", ".join(replay.observed_future_surfaces) or "none"
            lines.extend(
                [
                    f"### {replay.replay_id}",
                    "",
                    f"- Outcome: `{replay.outcome}`",
                    f"- Backend: `{replay.backend}`",
                    f"- Score: {replay.alignment_score:.2f}",
                    f"- Case: `{replay.case_id or 'unknown'}`",
                    f"- Branch event: `{replay.branch_event_id or 'n/a'}`",
                    f"- Historical events: {replay.historical_event_count}",
                    f"- Future events: {replay.future_event_count}",
                    f"- Future surfaces: {surfaces}",
                    f"- Counterfactual prompt: {replay.counterfactual_prompt}",
                    "",
                ]
            )
            for note in replay.notes:
                lines.append(f"- {note}")
            lines.append("")
    return "\n".join(lines).strip() + "\n"


def render_skill_refresh_report(skill_map: CompanySkillMap) -> str:
    refresh = _model_dump(skill_map.metadata.get("refresh"))
    lines = [
        f"# Skill Refresh Report: {skill_map.organization_name}",
        "",
        "Refresh compares the new LLM-synthesized skill map with a previous "
        "company map so review state survives evidence refreshes only when the "
        "underlying skill is still materially the same.",
        "",
    ]
    if not refresh:
        lines.append("No previous skill map was supplied for this build.")
        return "\n".join(lines).strip() + "\n"

    lines.extend(
        [
            f"- Previous source: `{refresh.get('previous_source_ref') or 'unknown'}`",
            f"- Previous skills: {refresh.get('previous_skill_count', 0)}",
            f"- Preserved: {refresh.get('preserved_skill_count', 0)}",
            f"- Changed: {refresh.get('changed_skill_count', 0)}",
            f"- New: {refresh.get('new_skill_count', 0)}",
            f"- Retired: {refresh.get('retired_skill_count', 0)}",
            "",
        ]
    )
    buckets = {
        "new": "New Skills",
        "preserved": "Preserved Skills",
        "changed": "Changed Skills",
        "retired": "Retired Skills",
    }
    for status, title in buckets.items():
        matching = [
            skill
            for skill in skill_map.skills
            if str(skill.metadata.get("refresh_status") or "new") == status
        ]
        if not matching:
            continue
        lines.extend([f"## {title}", ""])
        for skill in matching:
            match_id = str(skill.metadata.get("previous_skill_id") or "")
            suffix = f" previous=`{match_id}`" if match_id else ""
            lines.append(f"- `{skill.skill_id}` {skill.title}{suffix}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_skill_evidence_catalog(
    *,
    structure_payload: dict[str, Any],
    graphs_payload: dict[str, Any],
    event_index: dict[str, dict[str, Any]],
    limit: int | None,
) -> list[dict[str, Any]]:
    catalog: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_item(
        *,
        evidence_ref: SkillEvidenceRef,
        summary: str,
        facts: dict[str, Any] | None = None,
        related_evidence_ids: list[str] | None = None,
    ) -> None:
        evidence_id = _evidence_id(evidence_ref)
        if not evidence_ref.ref_id or evidence_id in seen:
            return
        if limit is not None and len(catalog) >= limit:
            return
        seen.add(evidence_id)
        catalog.append(
            {
                "evidence_id": evidence_id,
                "ref_type": evidence_ref.ref_type,
                "ref_id": evidence_ref.ref_id,
                "title": evidence_ref.title,
                "source": evidence_ref.source,
                "surface": evidence_ref.surface,
                "timestamp": evidence_ref.timestamp,
                "snippet": _truncate_text(evidence_ref.snippet or summary, 700),
                "summary": _truncate_text(summary, 900),
                "facts": facts or {},
                "related_evidence_ids": _unique(related_evidence_ids or []),
                "evidence_ref": evidence_ref.model_dump(mode="json"),
            }
        )

    cases = sorted(
        [_model_dump(item) for item in structure_payload.get("cases") or []],
        key=lambda item: (
            -len(_safe_list(item.get("event_ids"))),
            -len(_safe_list(item.get("surfaces"))),
            str(item.get("case_id") or ""),
        ),
    )
    for case in cases:
        event_ids = _safe_str_list(case.get("event_ids"))
        surfaces = _safe_str_list(case.get("surfaces"))
        if not event_ids and not surfaces:
            continue
        case_id = str(case.get("case_id") or "")
        case_title = str(case.get("title") or case_id or "Untitled case")
        event_summaries = [
            _event_summary(event_index[event_id])
            for event_id in event_ids[:6]
            if event_id in event_index
        ]
        add_item(
            evidence_ref=SkillEvidenceRef(
                ref_type="case",
                ref_id=case_id,
                title=case_title,
                surface=", ".join(surfaces),
                snippet=" | ".join(event_summaries),
                metadata={
                    "case_source": str(case.get("case_source") or ""),
                    "confidence": float(case.get("confidence") or 0.0),
                    "event_ids": event_ids,
                    "surfaces": surfaces,
                },
            ),
            summary=(
                f"{case_title} spans {', '.join(surfaces) or 'unknown surfaces'} "
                f"with {len(event_ids)} cited event(s). "
                + " ".join(event_summaries[:3])
            ),
            facts={
                "case_id": case_id,
                "surfaces": surfaces,
                "event_ids": event_ids,
                "anchor_refs": _safe_str_list(case.get("anchor_refs")),
                "entity_ids": _safe_str_list(case.get("entity_ids")),
            },
            related_evidence_ids=[f"event:{event_id}" for event_id in event_ids],
        )

    knowledge_graph = _model_dump(graphs_payload.get("knowledge_graph"))
    assets = sorted(
        [_model_dump(item) for item in knowledge_graph.get("assets") or []],
        key=lambda item: (
            str(item.get("status") or "active") != "active",
            str(item.get("title") or ""),
            str(item.get("asset_id") or ""),
        ),
    )
    for asset in assets:
        asset_id = str(asset.get("asset_id") or "")
        if not asset_id:
            continue
        provenance = _model_dump(asset.get("provenance"))
        linked_refs = _safe_str_list(asset.get("linked_object_refs"))
        add_item(
            evidence_ref=SkillEvidenceRef(
                ref_type="knowledge_asset",
                ref_id=asset_id,
                source=str(provenance.get("source") or asset.get("source") or ""),
                title=str(asset.get("title") or asset_id),
                timestamp=str(provenance.get("captured_at") or ""),
                snippet=str(asset.get("summary") or asset.get("body") or ""),
                metadata={
                    "kind": str(asset.get("kind") or ""),
                    "status": str(asset.get("status") or "active"),
                    "tags": _safe_str_list(asset.get("tags")),
                    "linked_object_refs": linked_refs,
                },
            ),
            summary=(
                f"{asset.get('title') or asset_id}: "
                f"{asset.get('summary') or _truncate_text(str(asset.get('body') or ''), 500)}"
            ),
            facts={
                "asset_id": asset_id,
                "kind": str(asset.get("kind") or ""),
                "status": str(asset.get("status") or "active"),
                "tags": _safe_str_list(asset.get("tags")),
                "linked_object_refs": linked_refs,
            },
            related_evidence_ids=[
                str(ref) if ":" in str(ref) else f"case:{ref}" for ref in linked_refs
            ],
        )

    rows = sorted(
        event_index.values(),
        key=lambda item: (
            int(item.get("ts_ms") or 0),
            str(item.get("event_id") or ""),
        ),
    )
    for row in rows:
        event_id = str(row.get("event_id") or "")
        if not event_id:
            continue
        case_id = str(row.get("case_id") or "")
        add_item(
            evidence_ref=SkillEvidenceRef(
                ref_type="event",
                ref_id=event_id,
                source=str(row.get("provider") or ""),
                surface=str(row.get("surface") or ""),
                title=str(row.get("subject") or row.get("kind") or event_id),
                timestamp=str(row.get("timestamp") or ""),
                snippet=str(row.get("snippet") or ""),
                metadata={
                    "case_id": case_id,
                    "thread_ref": row.get("thread_ref"),
                    "conversation_anchor": row.get("conversation_anchor"),
                    "kind": row.get("kind"),
                    "actor_id": row.get("actor_id"),
                    "target_id": row.get("target_id"),
                },
            ),
            summary=_event_summary(row),
            facts={
                "case_id": case_id,
                "thread_ref": str(row.get("thread_ref") or ""),
                "surface": str(row.get("surface") or ""),
                "provider": str(row.get("provider") or ""),
                "kind": str(row.get("kind") or ""),
                "actor_id": str(row.get("actor_id") or ""),
                "participant_ids": _safe_str_list(row.get("participant_ids")),
                "search_terms": _safe_str_list(row.get("search_terms")),
            },
            related_evidence_ids=[f"case:{case_id}"] if case_id else [],
        )

    return catalog


def _compact_evidence_catalog_for_llm(
    evidence_catalog: list[dict[str, Any]], *, target_items: int = 720
) -> list[dict[str, Any]]:
    """Represent the full catalog with high-signal items plus source digests."""
    if len(evidence_catalog) <= target_items:
        return list(evidence_catalog)

    keep_budget = max(120, target_items // 2)
    digest_budget = max(80, target_items - keep_budget)
    ranked = sorted(evidence_catalog, key=_evidence_item_rank)
    kept = ranked[:keep_budget]
    kept_ids = {str(item.get("evidence_id") or "") for item in kept}
    remainder = [
        item
        for item in evidence_catalog
        if str(item.get("evidence_id") or "") not in kept_ids
    ]
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for item in remainder:
        key = (
            str(item.get("source") or "unknown"),
            str(item.get("surface") or "unknown"),
            str(item.get("ref_type") or "unknown"),
        )
        grouped.setdefault(key, []).append(item)

    digests: list[dict[str, Any]] = []
    group_count = max(len(grouped), 1)
    per_group_budget = max(1, digest_budget // group_count)
    for (source, surface, ref_type), items in sorted(grouped.items()):
        sorted_items = sorted(
            items,
            key=lambda item: (
                str(item.get("timestamp") or ""),
                str(item.get("evidence_id") or ""),
            ),
        )
        chunk_size = max(
            1, (len(sorted_items) + per_group_budget - 1) // per_group_budget
        )
        for index in range(0, len(sorted_items), chunk_size):
            chunk = sorted_items[index : index + chunk_size]
            digests.append(
                _build_evidence_digest(
                    source=source,
                    surface=surface,
                    ref_type=ref_type,
                    items=chunk,
                    chunk_index=(index // chunk_size) + 1,
                )
            )

    compacted = kept + sorted(digests, key=_evidence_item_rank)
    return compacted[:target_items]


def _evidence_item_rank(item: dict[str, Any]) -> tuple[int, int, int, str, str]:
    ref_type = str(item.get("ref_type") or "")
    type_rank = {
        "case": 0,
        "knowledge_asset": 1,
        "evidence_digest": 2,
        "event": 3,
    }.get(ref_type, 4)
    facts = _model_dump(item.get("facts"))
    related = _safe_str_list(item.get("related_evidence_ids"))
    event_ids = _safe_str_list(facts.get("event_ids"))
    snippet_len = len(str(item.get("summary") or item.get("snippet") or ""))
    return (
        type_rank,
        -(len(event_ids) + len(related)),
        -snippet_len,
        str(item.get("timestamp") or ""),
        str(item.get("evidence_id") or ""),
    )


def _build_evidence_digest(
    *,
    source: str,
    surface: str,
    ref_type: str,
    items: list[dict[str, Any]],
    chunk_index: int,
) -> dict[str, Any]:
    evidence_ids = [str(item.get("evidence_id") or "") for item in items]
    digest_id = _stable_id(
        "digest",
        source,
        surface,
        ref_type,
        chunk_index,
        "|".join(evidence_ids[:8]),
    )
    title = f"{source}/{surface}/{ref_type} digest {chunk_index}"
    timestamps = [
        str(item.get("timestamp") or "") for item in items if item.get("timestamp")
    ]
    title_samples = [
        _truncate_text(str(item.get("title") or item.get("ref_id") or ""), 80)
        for item in items[:18]
    ]
    summary_samples = [
        _truncate_text(str(item.get("summary") or item.get("snippet") or ""), 160)
        for item in items[:10]
    ]
    snippet = " | ".join(item for item in title_samples if item)
    summary = (
        f"Digest representing {len(items)} {ref_type} evidence item(s) from "
        f"{source}/{surface}. Titles: {snippet}. "
        f"Samples: {' | '.join(item for item in summary_samples if item)}"
    )
    evidence_ref = SkillEvidenceRef(
        ref_type="evidence_digest",
        ref_id=digest_id,
        source=source,
        surface=surface,
        title=title,
        timestamp=min(timestamps) if timestamps else "",
        snippet=_truncate_text(summary, 700),
        metadata={
            "represented_count": len(items),
            "represented_ref_type": ref_type,
            "represented_evidence_ids_sample": evidence_ids[:80],
        },
    )
    return {
        "evidence_id": f"evidence_digest:{digest_id}",
        "ref_type": "evidence_digest",
        "ref_id": digest_id,
        "title": title,
        "source": source,
        "surface": surface,
        "timestamp": min(timestamps) if timestamps else "",
        "snippet": _truncate_text(snippet, 700),
        "summary": _truncate_text(summary, 900),
        "facts": {
            "represented_count": len(items),
            "represented_ref_type": ref_type,
            "represented_evidence_ids_sample": evidence_ids[:80],
        },
        "related_evidence_ids": evidence_ids[:80],
        "evidence_ref": evidence_ref.model_dump(mode="json"),
    }


def _extract_context_skills_with_llm(
    *,
    organization_name: str,
    organization_domain: str,
    source_providers: list[str],
    canonical_event_count: int,
    evidence_catalog: list[dict[str, Any]],
    limit: int,
    provider: str | None,
    model: str | None,
    timeout_s: int,
    catalog_shard_size: int,
) -> tuple[list[CompanySkill], dict[str, Any], list[SkillMapGap]]:
    resolved_provider, resolved_model = resolve_llm_defaults(
        provider=provider,
        model=model,
    )
    metadata: dict[str, Any] = {
        "llm_provider": resolved_provider,
        "llm_model": resolved_model,
        "evidence_catalog_count": len(evidence_catalog),
        "evidence_catalog_policy": "full_normalized_bundle_sharded_then_globalized",
        "evidence_catalog_shard_size": max(catalog_shard_size, 0),
        "llm_skill_limit": limit,
        "llm_pipeline": "evidence_clusters_then_global_skills",
    }
    if not evidence_catalog:
        return (
            [],
            metadata,
            [
                SkillMapGap(
                    gap_id="gap:no_skill_evidence_catalog",
                    title="No skill evidence catalog",
                    severity="error",
                    reason="The company bundle did not expose case, event, or knowledge evidence for LLM skill synthesis.",
                    recommendation="Provide canonical history sidecars or company knowledge assets before building skills.",
                )
            ],
        )

    if not _llm_available(resolved_provider):
        env_hint = (
            ", ".join(_provider_env_names(resolved_provider)) or "provider credentials"
        )
        raise RuntimeError(
            "LLM skill extraction requires credentials because deterministic "
            "skill extraction is disabled. Configure "
            f"{env_hint}, or pass --provider/--model for an available LLM."
        )

    cluster_system = (
        "You identify reusable company-specific operating know-how from one "
        "shard of a company's evidence catalog. Emit evidence clusters and "
        "candidate types, not final skills. Use only supplied evidence IDs. "
        "Do not invent facts, do not create generic business skills, and do "
        "not use scenario templates. Do not let high-volume routine inbox "
        "categories crowd out rare high-consequence signals such as customer "
        "pilots, enterprise buyer asks, legal holds, privacy/data-rights "
        "commitments, security reviews, pricing, or external commitments."
    )
    final_system = (
        "You synthesize the final company skill map from candidate evidence "
        "clusters. Prefer high-consequence, company-specific decision gates "
        "over commodity summaries. Separate flagship skills, support skills, "
        "workflows, and preprocessors. A valid skill has positive and negative "
        "triggers, a concrete output artifact, cited evidence IDs, replay "
        "checks, and approval boundaries for side effects."
        " A rare high-consequence company decision gate can outrank a frequent "
        "administrative pattern. Do not fill the map with recruiting, billing, "
        "vendor, newsletter, or account-notice skills unless those are clearly "
        "central to how this company creates value or avoids material risk."
    )
    cluster_schema = _cluster_plan_schema()
    final_schema = _skill_plan_schema()
    shards = _catalog_shards(evidence_catalog, catalog_shard_size)
    all_clusters: list[dict[str, Any]] = []
    all_skills: list[CompanySkill] = []
    rejected: list[str] = []
    returned_cluster_count = 0
    returned_skill_count = 0
    prompt_tokens = 0
    completion_tokens = 0
    total_tokens = 0
    cost_total = 0.0
    cost_available = True

    for shard_index, shard in enumerate(shards, start=1):
        user_payload = _skillmap_cluster_payload(
            organization_name=organization_name,
            organization_domain=organization_domain,
            source_providers=source_providers,
            canonical_event_count=canonical_event_count,
            evidence_catalog=shard,
            cluster_limit=_cluster_limit_per_shard(limit),
            shard_index=shard_index,
            shard_count=len(shards),
        )
        result = _run_async(
            plan_once_with_usage(
                provider=resolved_provider,
                model=resolved_model,
                system=cluster_system,
                user=json.dumps(user_payload, indent=2),
                plan_schema=cluster_schema,
                timeout_s=timeout_s,
            )
        )
        prompt_tokens, completion_tokens, total_tokens, cost_total, cost_available = (
            _accumulate_usage(
                result,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens,
                cost_total=cost_total,
                cost_available=cost_available,
            )
        )
        raw_clusters = _raw_llm_clusters(getattr(result, "plan", {}))
        returned_cluster_count += len(raw_clusters)
        shard_clusters, shard_rejected = _coerce_llm_clusters(
            raw_clusters,
            evidence_catalog=shard,
            organization_domain=organization_domain,
        )
        for cluster in shard_clusters:
            cluster["catalog_shard_index"] = shard_index
            cluster["catalog_shard_count"] = len(shards)
        all_clusters.extend(shard_clusters)
        rejected.extend(
            [
                f"cluster shard {shard_index}/{len(shards)}: {reason}"
                for reason in shard_rejected
            ]
        )

    selected_clusters = _select_final_clusters(all_clusters, limit=limit)
    if selected_clusters:
        final_payload = _skillmap_final_payload(
            organization_name=organization_name,
            organization_domain=organization_domain,
            source_providers=source_providers,
            canonical_event_count=canonical_event_count,
            candidate_clusters=selected_clusters,
            skill_limit=limit,
        )
        final_result = _run_async(
            plan_once_with_usage(
                provider=resolved_provider,
                model=resolved_model,
                system=final_system,
                user=json.dumps(final_payload, indent=2),
                plan_schema=final_schema,
                timeout_s=timeout_s,
            )
        )
        prompt_tokens, completion_tokens, total_tokens, cost_total, cost_available = (
            _accumulate_usage(
                final_result,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens,
                cost_total=cost_total,
                cost_available=cost_available,
            )
        )
        raw_skills = _raw_llm_skills(getattr(final_result, "plan", {}))
        returned_skill_count += len(raw_skills)
        all_skills, skill_rejected = _coerce_llm_skills(
            raw_skills,
            evidence_catalog=evidence_catalog,
            limit=max(limit * 2, limit),
            organization_domain=organization_domain,
        )
        rejected.extend([f"final skill pass: {reason}" for reason in skill_rejected])

    skills = _select_final_skills(all_skills, limit=limit)
    metadata.update(
        {
            "evidence_catalog_processed_count": sum(len(shard) for shard in shards),
            "llm_shard_count": len(shards),
            "llm_returned_cluster_count": returned_cluster_count,
            "llm_accepted_cluster_count": len(all_clusters),
            "llm_selected_cluster_count": len(selected_clusters),
            "llm_prompt_tokens": prompt_tokens,
            "llm_completion_tokens": completion_tokens,
            "llm_total_tokens": total_tokens,
            "llm_estimated_cost_usd": round(cost_total, 6) if cost_available else None,
        }
    )
    metadata["llm_returned_skill_count"] = returned_skill_count
    metadata["llm_accepted_before_dedupe_count"] = len(all_skills)
    metadata["llm_accepted_skill_count"] = len(skills)
    metadata["llm_rejected_skill_count"] = len(rejected)
    gaps = [
        SkillMapGap(
            gap_id=_stable_id("gap", "rejected_llm_skill", str(index), reason),
            title="Rejected LLM skill candidate",
            severity="warning",
            reason=reason,
            recommendation="Inspect the LLM skill synthesis prompt and company evidence catalog before retrying.",
        )
        for index, reason in enumerate(rejected[:8], start=1)
    ]
    if not skills:
        gaps.append(
            SkillMapGap(
                gap_id="gap:no_accepted_llm_skills",
                title="No accepted LLM skill candidates",
                severity="error",
                reason="The LLM response did not produce any evidence-grounded company skills that passed validation.",
                recommendation="Add richer company evidence or retry with a stronger model before deployment.",
            )
        )
    return skills, metadata, gaps


def _accumulate_usage(
    result: Any,
    *,
    prompt_tokens: int,
    completion_tokens: int,
    total_tokens: int,
    cost_total: float,
    cost_available: bool,
) -> tuple[int, int, int, float, bool]:
    usage = getattr(result, "usage", None)
    prompt_tokens += int(getattr(usage, "prompt_tokens", 0) or 0)
    completion_tokens += int(getattr(usage, "completion_tokens", 0) or 0)
    total_tokens += int(getattr(usage, "total_tokens", 0) or 0)
    cost = getattr(usage, "estimated_cost_usd", None)
    if cost is None:
        cost_available = False
    else:
        cost_total += float(cost)
    return prompt_tokens, completion_tokens, total_tokens, cost_total, cost_available


def _cluster_plan_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "tool": {"type": "string"},
            "args": {
                "type": "object",
                "properties": {
                    "clusters": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "title": {"type": "string"},
                                "summary": {"type": "string"},
                                "candidate_type": {"type": "string"},
                                "domain": {"type": "string"},
                                "positive_triggers": _string_array_schema(),
                                "negative_triggers": _string_array_schema(),
                                "reuse_pattern": {"type": "string"},
                                "evidence_ids": _string_array_schema(),
                                "allowed_actions": _string_array_schema(),
                                "blocked_actions": _string_array_schema(),
                                "output_artifacts": _output_artifact_array_schema(),
                                "replay_checks": _string_array_schema(),
                                "usefulness_scores": _usefulness_schema(),
                                "usefulness_rationale": {"type": "string"},
                                "confidence": {"type": "number"},
                            },
                        },
                    }
                },
            },
        },
    }


def _skill_plan_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "tool": {"type": "string"},
            "args": {
                "type": "object",
                "properties": {
                    "skills": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "title": {"type": "string"},
                                "summary": {"type": "string"},
                                "candidate_type": {"type": "string"},
                                "domain": {"type": "string"},
                                "trigger": {
                                    "type": "object",
                                    "properties": {
                                        "description": {"type": "string"},
                                        "signals": _string_array_schema(),
                                    },
                                },
                                "negative_triggers": _string_array_schema(),
                                "goal": {"type": "string"},
                                "reuse_pattern": {"type": "string"},
                                "evidence_ids": _string_array_schema(),
                                "steps": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "instruction": {"type": "string"},
                                            "tool": {"type": "string"},
                                            "graph_domain": {"type": "string"},
                                            "graph_action": {"type": "string"},
                                            "args": {
                                                "type": "object",
                                                "properties": {},
                                            },
                                            "read_only": {"type": "boolean"},
                                            "requires_approval": {"type": "boolean"},
                                        },
                                    },
                                },
                                "output_artifacts": _output_artifact_array_schema(),
                                "replay_checks": _string_array_schema(),
                                "allowed_actions": _string_array_schema(),
                                "blocked_actions": _string_array_schema(),
                                "execution_mode": {"type": "string"},
                                "tags": _string_array_schema(),
                                "usefulness_scores": _usefulness_schema(),
                                "usefulness_rationale": {"type": "string"},
                                "confidence": {"type": "number"},
                            },
                        },
                    }
                },
            },
        },
    }


def _string_array_schema() -> dict[str, Any]:
    return {"type": "array", "items": {"type": "string"}}


def _output_artifact_array_schema() -> dict[str, Any]:
    return {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "artifact_id": {"type": "string"},
                "title": {"type": "string"},
                "kind": {"type": "string"},
                "schema_hint": {"type": "string"},
                "required": {"type": "boolean"},
            },
        },
    }


def _usefulness_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {key: {"type": "number"} for key in USEFULNESS_SCORE_KEYS},
    }


def _catalog_shards(
    evidence_catalog: list[dict[str, Any]], shard_size: int
) -> list[list[dict[str, Any]]]:
    if not evidence_catalog:
        return []
    if shard_size <= 0:
        return [evidence_catalog]
    return [
        evidence_catalog[index : index + shard_size]
        for index in range(0, len(evidence_catalog), shard_size)
    ]


def _cluster_limit_per_shard(limit: int) -> int:
    return max(2, min(6, max(limit, 1)))


def _skillmap_cluster_payload(
    *,
    organization_name: str,
    organization_domain: str,
    source_providers: list[str],
    canonical_event_count: int,
    evidence_catalog: list[dict[str, Any]],
    cluster_limit: int,
    shard_index: int,
    shard_count: int,
) -> dict[str, Any]:
    return {
        "organization": {
            "name": organization_name,
            "domain": organization_domain,
            "source_providers": source_providers,
            "canonical_event_count": canonical_event_count,
        },
        "catalog_shard": {
            "index": shard_index,
            "count": shard_count,
            "policy": "Every shard is processed; emit evidence clusters only from evidence IDs in this shard.",
        },
        "cluster_limit": cluster_limit,
        "evidence_catalog": [_prompt_evidence_item(item) for item in evidence_catalog],
        "return_contract": {
            "tool": SKILLMAP_CLUSTER_TOOL,
            "args": {
                "clusters": [
                    {
                        "title": "company-specific evidence cluster title",
                        "summary": "what this evidence says about how the company works",
                        "candidate_type": "flagship_skill|support_skill|workflow|preprocessor|gap",
                        "domain": "short company workflow domain",
                        "positive_triggers": ["when this know-how applies"],
                        "negative_triggers": ["when this know-how should not apply"],
                        "reuse_pattern": "what repeats across evidence",
                        "evidence_ids": [
                            "case:...",
                            "event:...",
                            "knowledge_asset:...",
                        ],
                        "allowed_actions": ["bounded action names"],
                        "blocked_actions": ["live_write_without_approval"],
                        "output_artifacts": [
                            {
                                "artifact_id": "stable short id",
                                "title": "artifact name",
                                "kind": "json|markdown|checklist|table|draft|log",
                                "schema_hint": "expected fields",
                            }
                        ],
                        "replay_checks": [
                            "what historical/counterfactual replay should check"
                        ],
                        "usefulness_scores": {
                            "company_specificity": 0.0,
                            "repeat_frequency": 0.0,
                            "business_consequence": 0.0,
                            "actionability": 0.0,
                            "evidence_coverage": 0.0,
                            "risk_if_wrong": 0.0,
                            "replay_testability": 0.0,
                        },
                        "usefulness_rationale": "why this cluster matters or does not matter",
                        "confidence": 0.0,
                    }
                ]
            },
        },
        "quality_bar": [
            "Every cluster must cite at least one supplied evidence_id.",
            "Flagship skills are company-specific decision gates with clear business consequence.",
            "Rare high-consequence customer, legal, security, privacy, pricing, or data-rights signals can be flagship even if they occur once.",
            "Support skills and preprocessors are useful but should not crowd out flagship skills.",
            "Workflow candidates should involve approvals, side effects, or multi-step routing.",
            "Do not over-rank high-volume recruiting, vendor, billing, newsletter, or account-notice traffic unless it is core company work or material risk.",
            "Gap candidates should describe missing evidence needed before a skill can be trusted.",
        ],
    }


def _skillmap_final_payload(
    *,
    organization_name: str,
    organization_domain: str,
    source_providers: list[str],
    canonical_event_count: int,
    candidate_clusters: list[dict[str, Any]],
    skill_limit: int,
) -> dict[str, Any]:
    return {
        "organization": {
            "name": organization_name,
            "domain": organization_domain,
            "source_providers": source_providers,
            "canonical_event_count": canonical_event_count,
        },
        "selection_policy": {
            "skill_limit": skill_limit,
            "instructions": [
                "Choose the strongest deployable company-specific candidates.",
                "Prefer flagship skills over support skills when evidence is comparable.",
                "Prefer rare high-consequence decision gates over high-volume routine inbox patterns.",
                "Select a diverse set across customer/revenue, trust/legal/security, product/ops, and support/preprocessing when those signals exist.",
                "Do not promote preprocessor or workflow candidates unless they are genuinely useful as skills.",
                "Avoid more than two recruiting, billing, vendor, newsletter, or account-notice skills unless no stronger company operating doctrine is present.",
                "Do not promote gap candidates to skills.",
            ],
        },
        "candidate_clusters": candidate_clusters,
        "return_contract": {
            "tool": SKILLMAP_LLM_TOOL,
            "args": {
                "skills": [
                    {
                        "title": "company-specific reusable skill title",
                        "summary": "why this know-how matters here",
                        "candidate_type": "flagship_skill|support_skill|workflow|preprocessor",
                        "domain": "short company workflow domain",
                        "trigger": {
                            "description": "when an agent should consider this skill",
                            "signals": ["company-specific signal"],
                        },
                        "negative_triggers": [
                            "when an agent should not use this skill"
                        ],
                        "goal": "desired outcome",
                        "reuse_pattern": "what repeats across evidence",
                        "evidence_ids": [
                            "case:...",
                            "event:...",
                            "knowledge_asset:...",
                        ],
                        "steps": [
                            {
                                "instruction": "specific agent step",
                                "tool": "read or shadow tool name",
                                "graph_domain": "",
                                "graph_action": "",
                                "args": {},
                                "read_only": True,
                                "requires_approval": False,
                            }
                        ],
                        "output_artifacts": [
                            {
                                "artifact_id": "stable short id",
                                "title": "artifact name",
                                "kind": "json|markdown|checklist|table|draft|log",
                                "schema_hint": "expected fields",
                                "required": True,
                            }
                        ],
                        "replay_checks": [
                            "skill-specific replay or counterfactual check"
                        ],
                        "allowed_actions": ["bounded action names"],
                        "blocked_actions": ["live_write_without_approval"],
                        "execution_mode": "read_only|shadow|approval_gated",
                        "tags": ["company-specific tag"],
                        "usefulness_scores": {
                            "company_specificity": 0.0,
                            "repeat_frequency": 0.0,
                            "business_consequence": 0.0,
                            "actionability": 0.0,
                            "evidence_coverage": 0.0,
                            "risk_if_wrong": 0.0,
                            "replay_testability": 0.0,
                        },
                        "usefulness_rationale": "why this should be a skill rather than a workflow/gap/preprocessor",
                        "confidence": 0.0,
                    }
                ]
            },
        },
        "quality_bar": [
            "Every skill must cite evidence IDs present in candidate_clusters.",
            "Every skill needs positive and negative triggers.",
            "Every skill needs at least one concrete output artifact.",
            "Every skill needs skill-specific replay checks.",
            "A skill should encode company-specific operating doctrine, not just summarize a noisy inbox category.",
            "Side-effecting actions must stay shadow or approval-gated.",
        ],
    }


def _prompt_evidence_item(item: dict[str, Any]) -> dict[str, Any]:
    facts = _model_dump(item.get("facts"))
    compact_facts = {
        "case_id": facts.get("case_id"),
        "surfaces": _safe_str_list(facts.get("surfaces")),
        "event_count": len(_safe_str_list(facts.get("event_ids"))),
        "represented_count": facts.get("represented_count"),
        "represented_ref_type": facts.get("represented_ref_type"),
        "linked_object_refs": _safe_str_list(facts.get("linked_object_refs"))[:10],
    }
    return {
        "evidence_id": str(item.get("evidence_id") or ""),
        "ref_type": str(item.get("ref_type") or ""),
        "title": _truncate_text(str(item.get("title") or ""), 160),
        "source": str(item.get("source") or ""),
        "surface": str(item.get("surface") or ""),
        "timestamp": str(item.get("timestamp") or ""),
        "snippet": _truncate_text(str(item.get("snippet") or ""), 500),
        "summary": _truncate_text(str(item.get("summary") or ""), 650),
        "facts": {key: value for key, value in compact_facts.items() if value},
        "related_evidence_ids": _safe_str_list(item.get("related_evidence_ids"))[:10],
    }


def _select_final_clusters(
    clusters: list[dict[str, Any]], *, limit: int
) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for cluster in clusters:
        key = _cluster_signature(cluster)
        existing = deduped.get(key)
        if existing is None or _cluster_rank_key(cluster) < _cluster_rank_key(existing):
            deduped[key] = cluster
    ranked = [
        cluster
        for cluster in sorted(deduped.values(), key=_cluster_rank_key)
        if cluster.get("candidate_type") != "gap"
    ]
    return ranked[: max(limit * 6, limit, 1)]


def _select_final_skills(
    skills: list[CompanySkill], *, limit: int
) -> list[CompanySkill]:
    deduped: dict[str, CompanySkill] = {}
    for skill in skills:
        existing = deduped.get(skill.skill_id)
        if existing is None or _skill_rank_key(skill) < _skill_rank_key(existing):
            deduped[skill.skill_id] = skill
    ranked = sorted(deduped.values(), key=_skill_rank_key)
    return ranked[: max(limit, 0)]


def _skill_rank_key(skill: CompanySkill) -> tuple[int, float, float, int, str]:
    return (
        _candidate_type_rank(skill.candidate_type),
        -skill.usefulness_score,
        -skill.confidence,
        -len(skill.evidence_refs),
        skill.title,
    )


def _cluster_rank_key(cluster: dict[str, Any]) -> tuple[int, float, float, int, str]:
    candidate_type = str(cluster.get("candidate_type") or "support_skill")
    return (
        _candidate_type_rank(candidate_type),
        -_bounded_float(cluster.get("usefulness_score"), default=0.0),
        -_bounded_float(cluster.get("confidence"), default=0.0),
        -len(_safe_str_list(cluster.get("evidence_ids"))),
        str(cluster.get("title") or ""),
    )


def _candidate_type_rank(candidate_type: str) -> int:
    ranks = {
        "flagship_skill": 0,
        "workflow": 1,
        "support_skill": 2,
        "preprocessor": 3,
        "gap": 4,
    }
    return ranks.get(str(candidate_type or "").strip(), 2)


def _cluster_signature(cluster: dict[str, Any]) -> str:
    title_tokens = sorted(set(_tokenize(str(cluster.get("title") or ""))))[:8]
    evidence_ids = _safe_str_list(cluster.get("evidence_ids"))[:4]
    return "|".join(
        [
            str(cluster.get("candidate_type") or "support_skill"),
            str(cluster.get("domain") or ""),
            " ".join(title_tokens),
            "|".join(evidence_ids),
        ]
    )


def _raw_llm_clusters(plan: Any) -> list[dict[str, Any]]:
    payload = _model_dump(plan)
    args = payload.get("args") if isinstance(payload.get("args"), dict) else None
    if str(payload.get("tool") or "") == SKILLMAP_CLUSTER_TOOL and args is not None:
        clusters = args.get("clusters")
    elif "clusters" in payload:
        clusters = payload.get("clusters")
    else:
        clusters = None
    if not isinstance(clusters, list):
        return []
    return [_model_dump(item) for item in clusters if isinstance(item, dict)]


def _raw_llm_skills(plan: Any) -> list[dict[str, Any]]:
    payload = _model_dump(plan)
    args = payload.get("args") if isinstance(payload.get("args"), dict) else None
    if str(payload.get("tool") or "") == SKILLMAP_LLM_TOOL and args is not None:
        skills = args.get("skills")
    elif "skills" in payload:
        skills = payload.get("skills")
    else:
        skills = None
    if not isinstance(skills, list):
        return []
    return [_model_dump(item) for item in skills if isinstance(item, dict)]


def _coerce_llm_clusters(
    raw_clusters: list[dict[str, Any]],
    *,
    evidence_catalog: list[dict[str, Any]],
    organization_domain: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    evidence_by_id = {
        str(item.get("evidence_id") or ""): item
        for item in evidence_catalog
        if str(item.get("evidence_id") or "")
    }
    clusters: list[dict[str, Any]] = []
    rejected: list[str] = []
    seen_ids: set[str] = set()
    for index, raw in enumerate(raw_clusters, start=1):
        title = _clean_llm_text(raw.get("title"), max_len=140)
        summary = _clean_llm_text(raw.get("summary"), max_len=900)
        if not title or not summary:
            rejected.append(f"cluster {index} missing title or summary")
            continue
        candidate_type = _coerce_candidate_type(raw.get("candidate_type"))
        evidence_ids = [
            str(item).strip()
            for item in _safe_list(raw.get("evidence_ids"))
            if str(item).strip()
        ]
        valid_evidence_ids = _unique(
            [
                evidence_id
                for evidence_id in evidence_ids
                if evidence_id in evidence_by_id
            ]
        )
        if not valid_evidence_ids:
            rejected.append(f"{title}: no supplied evidence_id matched the catalog")
            continue
        usefulness_score, usefulness_scores = _coerce_usefulness_scores(
            raw.get("usefulness_scores")
        )
        cluster_id = _stable_id(
            "cluster",
            organization_domain,
            title,
            "|".join(valid_evidence_ids[:4]),
        )
        if cluster_id in seen_ids:
            rejected.append(f"{title}: duplicate cluster candidate")
            continue
        seen_ids.add(cluster_id)
        clusters.append(
            {
                "cluster_id": cluster_id,
                "title": title,
                "summary": summary,
                "candidate_type": candidate_type,
                "domain": _clean_llm_text(raw.get("domain"), max_len=80) or "company",
                "positive_triggers": _unique(
                    _safe_str_list(raw.get("positive_triggers"))
                ),
                "negative_triggers": _unique(
                    _safe_str_list(raw.get("negative_triggers"))
                ),
                "reuse_pattern": _clean_llm_text(raw.get("reuse_pattern"), max_len=700),
                "evidence_ids": valid_evidence_ids,
                "evidence": [
                    _compact_cluster_evidence(evidence_by_id[evidence_id])
                    for evidence_id in valid_evidence_ids[:8]
                ],
                "allowed_actions": _unique(_safe_str_list(raw.get("allowed_actions"))),
                "blocked_actions": _unique(
                    _safe_str_list(raw.get("blocked_actions"))
                    + ["live_write_without_approval"]
                ),
                "output_artifacts": [
                    item.model_dump(mode="json")
                    for item in _coerce_output_artifacts(raw.get("output_artifacts"))
                ],
                "replay_checks": _unique(_safe_str_list(raw.get("replay_checks"))),
                "usefulness_score": usefulness_score,
                "usefulness_scores": usefulness_scores,
                "usefulness_rationale": _clean_llm_text(
                    raw.get("usefulness_rationale"), max_len=700
                ),
                "confidence": _bounded_float(raw.get("confidence"), default=0.5),
                "discarded_evidence_ids": [
                    evidence_id
                    for evidence_id in evidence_ids
                    if evidence_id not in valid_evidence_ids
                ],
            }
        )
    return clusters, rejected


def _compact_cluster_evidence(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "evidence_id": str(item.get("evidence_id") or ""),
        "ref_type": str(item.get("ref_type") or ""),
        "title": _truncate_text(str(item.get("title") or ""), 160),
        "source": str(item.get("source") or ""),
        "surface": str(item.get("surface") or ""),
        "timestamp": str(item.get("timestamp") or ""),
        "summary": _truncate_text(str(item.get("summary") or ""), 260),
        "snippet": _truncate_text(str(item.get("snippet") or ""), 220),
    }


def _coerce_llm_skills(
    raw_skills: list[dict[str, Any]],
    *,
    evidence_catalog: list[dict[str, Any]],
    limit: int,
    organization_domain: str,
) -> tuple[list[CompanySkill], list[str]]:
    evidence_by_id = {
        str(item.get("evidence_id") or ""): SkillEvidenceRef.model_validate(
            item.get("evidence_ref") or {}
        )
        for item in evidence_catalog
        if str(item.get("evidence_id") or "")
    }
    skills: list[CompanySkill] = []
    rejected: list[str] = []
    seen_ids: set[str] = set()
    for index, raw in enumerate(raw_skills, start=1):
        if len(skills) >= limit:
            break
        title = _clean_llm_text(raw.get("title"), max_len=140)
        goal = _clean_llm_text(raw.get("goal"), max_len=500)
        summary = _clean_llm_text(raw.get("summary"), max_len=700)
        if not title or not goal:
            rejected.append(f"candidate {index} missing title or goal")
            continue
        candidate_type = _coerce_candidate_type(raw.get("candidate_type"))
        if candidate_type == "gap":
            rejected.append(f"{title}: gap candidate cannot be promoted to a skill")
            continue
        evidence_ids = [
            str(item).strip()
            for item in _safe_list(raw.get("evidence_ids"))
            if str(item).strip()
        ]
        valid_evidence_ids = _unique(
            [
                evidence_id
                for evidence_id in evidence_ids
                if evidence_id in evidence_by_id
            ]
        )
        if not valid_evidence_ids:
            rejected.append(f"{title}: no supplied evidence_id matched the catalog")
            continue
        steps, step_error = _coerce_llm_steps(raw.get("steps"), title=title)
        if step_error:
            rejected.append(f"{title}: {step_error}")
            continue
        requested_mode = str(raw.get("execution_mode") or "shadow").strip()
        execution_mode: SkillExecutionMode = (
            cast(SkillExecutionMode, requested_mode)
            if requested_mode in {"read_only", "shadow", "approval_gated"}
            else "shadow"
        )
        has_write_step = any(not step.read_only for step in steps)
        if has_write_step:
            for step in steps:
                if not step.read_only:
                    step.requires_approval = True
            if execution_mode == "read_only":
                execution_mode = "shadow"
        evidence_refs = [
            evidence_by_id[evidence_id] for evidence_id in valid_evidence_ids
        ]
        skill_id = _stable_id(
            "skill",
            "llm",
            organization_domain,
            title,
            "|".join(valid_evidence_ids[:4]),
        )
        if skill_id in seen_ids:
            rejected.append(f"{title}: duplicate skill candidate")
            continue
        seen_ids.add(skill_id)
        trigger_payload = _model_dump(raw.get("trigger"))
        trigger_description = (
            _clean_llm_text(trigger_payload.get("description"), max_len=500)
            or f"Relevant company evidence appears for {title}."
        )
        confidence = _bounded_float(raw.get("confidence"), default=0.5)
        usefulness_score, usefulness_scores = _coerce_usefulness_scores(
            raw.get("usefulness_scores")
        )
        tags = _unique(
            _safe_str_list(raw.get("tags"))
            + [str(ref.surface) for ref in evidence_refs if ref.surface]
            + [str(ref.metadata.get("case_id") or "") for ref in evidence_refs]
        )
        skill = CompanySkill(
            skill_id=skill_id,
            title=title,
            summary=summary
            or "LLM-synthesized company skill grounded in the cited evidence.",
            status="draft",
            domain=_clean_llm_text(raw.get("domain"), max_len=80) or "company",
            candidate_type=candidate_type,
            usefulness_score=usefulness_score,
            usefulness_rationale=_clean_llm_text(
                raw.get("usefulness_rationale"), max_len=700
            ),
            trigger=SkillTrigger(
                description=trigger_description,
                signals=_unique(_safe_str_list(trigger_payload.get("signals"))),
            ),
            negative_triggers=_unique(_safe_str_list(raw.get("negative_triggers"))),
            goal=goal,
            prerequisites=[
                "Cited company evidence remains available in the context bundle.",
                "A human owner reviews the draft before activation.",
            ],
            steps=steps,
            output_artifacts=_coerce_output_artifacts(raw.get("output_artifacts")),
            evidence_refs=evidence_refs,
            allowed_actions=_unique(_safe_str_list(raw.get("allowed_actions"))),
            blocked_actions=_unique(
                _safe_str_list(raw.get("blocked_actions"))
                + ["live_write_without_approval"]
            ),
            freshness_status=_freshness_from_evidence_refs(evidence_refs),
            replay_checks=_unique(_safe_str_list(raw.get("replay_checks"))),
            confidence=confidence,
            execution_mode=execution_mode,
            tags=tags,
            metadata={
                "extractor": "llm",
                "llm_candidate_index": index,
                "reuse_pattern": _clean_llm_text(raw.get("reuse_pattern"), max_len=700),
                "usefulness_scores": usefulness_scores,
                "evidence_ids": valid_evidence_ids,
                "discarded_evidence_ids": [
                    evidence_id
                    for evidence_id in evidence_ids
                    if evidence_id not in valid_evidence_ids
                ],
            },
        )
        skills.append(skill)
    return skills, rejected


def _coerce_candidate_type(raw: Any) -> SkillCandidateType:
    value = str(raw or "support_skill").strip()
    if value in {
        "flagship_skill",
        "support_skill",
        "workflow",
        "preprocessor",
        "gap",
    }:
        return value  # type: ignore[return-value]
    return "support_skill"


def _coerce_usefulness_scores(raw: Any) -> tuple[float, dict[str, float]]:
    payload = _model_dump(raw)
    scores: dict[str, float] = {}
    for key in USEFULNESS_SCORE_KEYS:
        scores[key] = _bounded_float(payload.get(key), default=0.5)
    overall = payload.get("overall")
    if overall is None:
        overall = sum(scores.values()) / max(len(scores), 1)
    return round(_bounded_float(overall, default=0.5), 3), scores


def _coerce_output_artifacts(raw: Any) -> list[SkillOutputArtifact]:
    artifacts: list[SkillOutputArtifact] = []
    for index, item in enumerate(_safe_list(raw), start=1):
        payload = _model_dump(item)
        title = _clean_llm_text(payload.get("title"), max_len=160)
        schema_hint = _clean_llm_text(payload.get("schema_hint"), max_len=400)
        if not title and not schema_hint:
            continue
        artifact_id = _clean_llm_text(payload.get("artifact_id"), max_len=80)
        if not artifact_id:
            artifact_id = _stable_id("artifact", index, title or schema_hint)
        artifacts.append(
            SkillOutputArtifact(
                artifact_id=artifact_id,
                title=title or artifact_id,
                kind=_clean_llm_text(payload.get("kind"), max_len=80) or "artifact",
                schema_hint=schema_hint,
                required=bool(payload.get("required", True)),
            )
        )
    return artifacts


def _coerce_llm_steps(raw_steps: Any, *, title: str) -> tuple[list[SkillStep], str]:
    if not isinstance(raw_steps, list) or not raw_steps:
        return [], "missing executable steps"
    steps: list[SkillStep] = []
    for index, raw_step in enumerate(raw_steps, start=1):
        payload = _model_dump(raw_step)
        instruction = _clean_llm_text(payload.get("instruction"), max_len=700)
        if not instruction:
            continue
        read_only = bool(payload.get("read_only", True))
        requires_approval = bool(payload.get("requires_approval", False))
        if not read_only:
            requires_approval = True
        raw_args = payload.get("args")
        args = raw_args if isinstance(raw_args, dict) else {}
        steps.append(
            SkillStep(
                step_id=_clean_llm_text(payload.get("step_id"), max_len=80)
                or _stable_id("step", title, index, instruction),
                instruction=instruction,
                tool=_clean_llm_text(payload.get("tool"), max_len=120),
                graph_domain=_clean_llm_text(payload.get("graph_domain"), max_len=80),
                graph_action=_clean_llm_text(payload.get("graph_action"), max_len=80),
                args=dict(args),
                read_only=read_only,
                requires_approval=requires_approval,
            )
        )
    if not steps:
        return [], "no step had an instruction"
    return steps, ""


def _build_map_gaps(
    *,
    structure_payload: dict[str, Any],
    graphs_payload: dict[str, Any],
    event_index: dict[str, dict[str, Any]],
    source_statuses: list[dict[str, Any]],
    canonical_event_count: int,
    skills: list[CompanySkill],
) -> list[SkillMapGap]:
    gaps: list[SkillMapGap] = []
    if canonical_event_count == 0:
        gaps.append(
            SkillMapGap(
                gap_id="gap:no_canonical_events",
                title="No canonical event history",
                severity="error",
                reason="The map has no canonical events to ground company skills.",
                recommendation=(
                    "Run `vei context normalize` or provide a context snapshot with "
                    "canonical sidecars before deploying skills."
                ),
            )
        )
    surface_count = len(
        {
            str(row.get("surface") or "")
            for row in event_index.values()
            if str(row.get("surface") or "")
        }
    )
    if canonical_event_count > 0 and surface_count <= 1:
        gaps.append(
            SkillMapGap(
                gap_id="gap:single_surface_history",
                title="Only one source surface is represented",
                severity="warning",
                reason=(
                    "The map cannot validate cross-system procedures when history "
                    "comes from only one surface."
                ),
                recommendation="Add at least one second surface, such as Slack, tickets, CRM, or docs.",
            )
        )
    for source in source_statuses:
        if str(source.get("status") or "ok") != "error":
            continue
        provider = str(source.get("provider") or "unknown")
        gaps.append(
            SkillMapGap(
                gap_id=_stable_id("gap", "provider_error", provider),
                title=f"{provider} capture failed",
                severity="error",
                reason=str(
                    source.get("error") or "A source provider returned an error."
                ),
                recommendation=f"Fix the {provider} capture before activating related skills.",
                evidence_refs=[
                    SkillEvidenceRef(
                        ref_type="source",
                        ref_id=provider,
                        title=f"{provider} provider",
                    )
                ],
            )
        )
    for hypothesis in [
        _model_dump(item) for item in structure_payload.get("hypotheses") or []
    ]:
        gaps.append(
            SkillMapGap(
                gap_id=_stable_id(
                    "gap",
                    "structure_hypothesis",
                    str(hypothesis.get("hypothesis_id") or ""),
                ),
                title=str(hypothesis.get("title") or "Open structure ambiguity"),
                severity="warning",
                reason=str(hypothesis.get("summary") or "Open structure ambiguity."),
                recommendation="Resolve this ambiguity before activating skills that depend on the entity.",
                evidence_refs=[
                    SkillEvidenceRef(
                        ref_type="structure_hypothesis",
                        ref_id=str(hypothesis.get("hypothesis_id") or ""),
                        title=str(hypothesis.get("title") or ""),
                        metadata={"confidence": hypothesis.get("confidence")},
                    )
                ],
            )
        )
    knowledge_graph = _model_dump(graphs_payload.get("knowledge_graph"))
    stale_assets = [
        _model_dump(asset)
        for asset in knowledge_graph.get("assets") or []
        if str(asset.get("status") or "active") in {"stale", "expired"}
    ]
    for asset in stale_assets[:5]:
        gaps.append(
            SkillMapGap(
                gap_id=_stable_id(
                    "gap", "stale_asset", str(asset.get("asset_id") or "")
                ),
                title=f"Stale evidence: {asset.get('title') or asset.get('asset_id')}",
                severity="warning",
                reason="A knowledge asset backing the company map is stale or expired.",
                recommendation="Refresh or supersede this asset before activating dependent skills.",
                evidence_refs=[
                    SkillEvidenceRef(
                        ref_type="knowledge_asset",
                        ref_id=str(asset.get("asset_id") or ""),
                        title=str(asset.get("title") or ""),
                        source=str(asset.get("source") or ""),
                    )
                ],
            )
        )
    if not skills:
        gaps.append(
            SkillMapGap(
                gap_id="gap:no_candidate_skills",
                title="No candidate skills inferred",
                severity="error",
                reason="The available evidence did not produce any deployable draft skills.",
                recommendation=(
                    "Add richer cross-surface history, subject-linked knowledge assets, "
                    "or capability graph state."
                ),
            )
        )
    return gaps


def _load_replay_world(snapshot_path: Path) -> tuple[WhatIfWorld | None, str]:
    try:
        return (
            load_world(
                source="company_history",
                source_dir=snapshot_path,
                include_content=False,
                include_situation_graph=True,
            ),
            "",
        )
    except (FileNotFoundError, ValueError, OSError) as exc:
        return None, str(exc)
    except Exception as exc:  # noqa: BLE001 - preserve gap detail for replay setup
        return None, f"{type(exc).__name__}: {exc}"


def _attach_replay_results(
    skill_map: CompanySkillMap, world: WhatIfWorld, *, limit: int
) -> None:
    candidate_result = list_branch_candidates(world, limit=max(limit * 4, 20))
    candidates = [_model_dump(item) for item in candidate_result.candidates]
    if not candidates:
        skill_map.gaps.append(
            SkillMapGap(
                gap_id="gap:no_replay_candidates",
                title="No replayable branch candidates",
                severity="warning",
                reason=(
                    "The history loaded successfully, but it did not expose a "
                    "past/future split for shadow-testing inferred skills."
                ),
                recommendation="Add longer case histories with visible follow-up events.",
            )
        )
        return

    events_by_id = {_event_attr(event, "event_id"): event for event in world.events}
    events_by_case: dict[str, list[Any]] = {}
    events_by_thread: dict[str, list[Any]] = {}
    for event in world.events:
        case_id = _event_attr(event, "case_id")
        thread_id = _event_attr(event, "thread_id")
        if case_id:
            events_by_case.setdefault(case_id, []).append(event)
        if thread_id:
            events_by_thread.setdefault(thread_id, []).append(event)

    replayed_count = 0
    for skill in skill_map.skills:
        results = _replay_results_for_skill(
            skill,
            candidates=candidates,
            events_by_id=events_by_id,
            events_by_case=events_by_case,
            events_by_thread=events_by_thread,
        )
        skill.replay_results = results
        if results:
            replayed_count += 1
            skill.replay_score = round(
                sum(result.alignment_score for result in results) / len(results),
                3,
            )
        skill.deployment_readiness = _deployment_readiness(skill)
    skill_map.metadata["replay_available"] = True
    skill_map.metadata["replay_world_source"] = world.source
    skill_map.metadata["replay_candidate_count"] = len(candidates)
    skill_map.metadata["replayed_skill_count"] = replayed_count


def _replay_results_for_skill(
    skill: CompanySkill,
    *,
    candidates: list[dict[str, Any]],
    events_by_id: dict[str, Any],
    events_by_case: dict[str, list[Any]],
    events_by_thread: dict[str, list[Any]],
) -> list[SkillReplayResult]:
    scored: list[tuple[float, dict[str, Any]]] = []
    for candidate in candidates:
        score = _candidate_match_score(skill, candidate, events_by_id)
        if score < 0.3:
            continue
        scored.append((score, candidate))
    if not scored:
        return []
    scored.sort(
        key=lambda item: (
            -item[0],
            -int(item[1].get("future_event_count") or 0),
            str(item[1].get("thread_id") or ""),
        )
    )
    return [
        _build_replay_result(
            skill,
            candidate,
            match_score=match_score,
            events_by_id=events_by_id,
            events_by_case=events_by_case,
            events_by_thread=events_by_thread,
        )
        for match_score, candidate in scored[:3]
    ]


def _candidate_match_score(
    skill: CompanySkill,
    candidate: dict[str, Any],
    events_by_id: dict[str, Any],
) -> float:
    skill_case_refs = _normalized_ref_set(_skill_case_refs(skill))
    candidate_case_id = str(candidate.get("case_id") or "")
    candidate_thread_id = str(candidate.get("thread_id") or "")
    candidate_refs = _normalized_ref_set([candidate_case_id, candidate_thread_id])
    candidate_event_id = str(candidate.get("branch_event_id") or "")
    score = 0.0
    if candidate_case_id and skill_case_refs.intersection(candidate_refs):
        score += 0.55
    if candidate_thread_id and _normalize_ref(candidate_thread_id) in skill_case_refs:
        score += 0.35
    if candidate_event_id in _skill_event_refs(skill):
        score += 0.35
    branch_event = events_by_id.get(candidate_event_id)
    if branch_event is not None:
        surface = _event_attr(branch_event, "surface")
        if surface and surface in set(skill.tags + [skill.domain]):
            score += 0.15
        subject = _event_attr(branch_event, "subject").lower()
        title_tokens = set(_tokenize(skill.title))
        if title_tokens and title_tokens.intersection(_tokenize(subject)):
            score += 0.2
    return min(score, 1.0)


def _build_replay_result(
    skill: CompanySkill,
    candidate: dict[str, Any],
    *,
    match_score: float,
    events_by_id: dict[str, Any],
    events_by_case: dict[str, list[Any]],
    events_by_thread: dict[str, list[Any]],
) -> SkillReplayResult:
    branch_event_id = str(candidate.get("branch_event_id") or "")
    branch_event = events_by_id.get(branch_event_id)
    branch_ts = int(_event_attr(branch_event, "timestamp_ms") or 0)
    case_id = str(candidate.get("case_id") or "")
    thread_id = str(candidate.get("thread_id") or "")
    related_events = (
        list(events_by_case.get(case_id, []))
        if case_id
        else list(events_by_thread.get(thread_id, []))
    )
    if not related_events and thread_id:
        related_events = list(events_by_thread.get(thread_id, []))
    future_events = [
        event
        for event in related_events
        if int(_event_attr(event, "timestamp_ms") or 0) > branch_ts
    ]
    past_events = [
        event
        for event in related_events
        if int(_event_attr(event, "timestamp_ms") or 0) <= branch_ts
    ]
    observed_surfaces = _unique(
        [_event_attr(event, "surface") for event in future_events]
    )
    expected_actions = _expected_actions(skill)
    action_alignment = _action_alignment_score(
        skill=skill,
        expected_actions=expected_actions,
        future_events=future_events,
        observed_surfaces=observed_surfaces,
    )
    future_tail_score = min(len(future_events) / 3.0, 1.0)
    guard_score = _write_guard_score(skill)
    alignment_score = round(
        (match_score * 0.35)
        + (future_tail_score * 0.2)
        + (action_alignment * 0.25)
        + (guard_score * 0.2),
        3,
    )
    outcome = _replay_outcome(alignment_score)
    return SkillReplayResult(
        replay_id=_stable_id("replay", skill.skill_id, branch_event_id or thread_id),
        backend="historical_replay",
        outcome=outcome,
        case_id=case_id,
        thread_id=thread_id,
        branch_event_id=branch_event_id,
        branch_timestamp=_event_attr(branch_event, "timestamp"),
        historical_event_count=len(past_events),
        future_event_count=len(future_events),
        matched_event_count=len(related_events),
        alignment_score=alignment_score,
        counterfactual_prompt=_counterfactual_prompt(skill, candidate),
        observed_future_surfaces=observed_surfaces,
        expected_actions=expected_actions,
        evidence_refs=_candidate_evidence_refs(
            candidate=candidate,
            branch_event=branch_event,
        ),
        notes=_replay_notes(
            skill=skill,
            future_events=future_events,
            match_score=match_score,
            action_alignment=action_alignment,
            guard_score=guard_score,
        ),
    )


def _skill_case_refs(skill: CompanySkill) -> set[str]:
    refs = {
        str(skill.metadata.get("case_id") or ""),
        str(skill.metadata.get("subject_object_ref") or ""),
    }
    for evidence in skill.evidence_refs:
        if evidence.ref_type == "case":
            refs.add(evidence.ref_id)
        case_id = evidence.metadata.get("case_id")
        if case_id:
            refs.add(str(case_id))
        thread_ref = evidence.metadata.get("thread_ref")
        if thread_ref:
            refs.add(str(thread_ref))
    return {ref for ref in refs if ref}


def _skill_event_refs(skill: CompanySkill) -> set[str]:
    return {
        evidence.ref_id
        for evidence in skill.evidence_refs
        if evidence.ref_type == "event"
    }


def _expected_actions(skill: CompanySkill) -> list[str]:
    actions = list(skill.allowed_actions)
    for step in skill.steps:
        if step.graph_domain and step.graph_action:
            actions.append(f"{step.graph_domain}.{step.graph_action}")
        elif step.graph_action:
            actions.append(step.graph_action)
        elif step.tool:
            actions.append(step.tool)
    return _unique(actions)


def _action_alignment_score(
    *,
    skill: CompanySkill,
    expected_actions: list[str],
    future_events: list[Any],
    observed_surfaces: list[str],
) -> float:
    if not future_events:
        return 0.0
    score = 0.0
    if skill.domain in observed_surfaces:
        score += 0.35
    surface_tags = {tag for tag in skill.tags if tag in observed_surfaces}
    if surface_tags:
        score += 0.25
    future_text = " ".join(
        " ".join(
            [
                _event_attr(event, "event_type"),
                _event_attr(event, "subject"),
                _event_attr(event, "snippet"),
                _event_attr(event, "surface"),
            ]
        )
        for event in future_events
    ).lower()
    action_tokens = {
        token
        for action in expected_actions
        for token in _tokenize(action.replace(".", " "))
        if len(token) >= 4
    }
    if action_tokens and any(token in future_text for token in action_tokens):
        score += 0.3
    if skill.execution_mode in {"read_only", "shadow", "approval_gated"}:
        score += 0.1
    return min(score, 1.0)


def _write_guard_score(skill: CompanySkill) -> float:
    for step in skill.steps:
        if step.read_only:
            continue
        if step.requires_approval or skill.execution_mode == "shadow":
            continue
        return 0.0
    return 1.0


def _replay_outcome(score: float) -> SkillReplayOutcome:
    if score >= 0.72:
        return "supported"
    if score >= 0.45:
        return "partial"
    return "unsupported"


def _deployment_readiness(skill: CompanySkill) -> SkillDeploymentReadiness:
    if not skill.replay_results:
        return "not_tested"
    if skill.replay_score >= 0.82 and skill.freshness_status != "stale":
        return "activation_candidate"
    if skill.replay_score >= 0.6:
        return "shadow_ready"
    return "needs_review"


def _counterfactual_prompt(skill: CompanySkill, candidate: dict[str, Any]) -> str:
    step_lines = "; ".join(step.instruction for step in skill.steps[:3])
    replay_checks = "; ".join(skill.replay_checks[:3])
    subject = str(candidate.get("subject") or candidate.get("thread_id") or "this case")
    checks_suffix = f" Skill-specific checks: {replay_checks}." if replay_checks else ""
    return (
        f"If an agent followed the skill '{skill.title}' at branch event "
        f"{candidate.get('branch_event_id') or 'unknown'} for {subject}, it would: "
        f"{step_lines}.{checks_suffix} Compare that proposed path against the historical future."
    )


def _candidate_evidence_refs(
    *, candidate: dict[str, Any], branch_event: Any
) -> list[SkillEvidenceRef]:
    refs: list[SkillEvidenceRef] = []
    branch_event_id = str(candidate.get("branch_event_id") or "")
    if branch_event_id:
        refs.append(
            SkillEvidenceRef(
                ref_type="event",
                ref_id=branch_event_id,
                surface=_event_attr(branch_event, "surface"),
                title=str(
                    candidate.get("subject") or _event_attr(branch_event, "subject")
                ),
                timestamp=_event_attr(branch_event, "timestamp"),
                snippet=_event_attr(branch_event, "snippet"),
                metadata={
                    "thread_id": candidate.get("thread_id"),
                    "case_id": candidate.get("case_id"),
                },
            )
        )
    case_id = str(candidate.get("case_id") or "")
    if case_id:
        refs.append(
            SkillEvidenceRef(
                ref_type="case",
                ref_id=case_id,
                title=str(candidate.get("subject") or case_id),
            )
        )
    return refs


def _replay_notes(
    *,
    skill: CompanySkill,
    future_events: list[Any],
    match_score: float,
    action_alignment: float,
    guard_score: float,
) -> list[str]:
    notes = [
        f"Matched historical branch with score {match_score:.2f}.",
        f"Observed {len(future_events)} future event(s) after the branch point.",
        f"Action alignment score {action_alignment:.2f}.",
    ]
    if guard_score < 1.0:
        notes.append("A write-capable step is not approval-gated.")
    if skill.execution_mode == "read_only":
        notes.append("Skill stays read-only during replay.")
    elif skill.execution_mode == "shadow":
        notes.append("Write-capable behavior remains shadow-mode.")
    else:
        notes.append("Write-capable behavior requires approval.")
    return notes


def _event_attr(event: Any, name: str) -> str:
    if event is None:
        return ""
    value = getattr(event, name, "")
    if value is None:
        return ""
    return str(value)


def _tokenize(value: str) -> list[str]:
    return [token for token in re.split(r"[^a-z0-9]+", value.lower()) if token]


def _normalized_ref_set(values: list[str] | set[str]) -> set[str]:
    normalized: set[str] = set()
    for value in values:
        ref = _normalize_ref(value)
        if ref:
            normalized.add(ref)
        if ref.startswith("case:"):
            normalized.add(ref.removeprefix("case:"))
        if ref.startswith("thread:"):
            normalized.add(ref.removeprefix("thread:"))
    return normalized


def _normalize_ref(value: str) -> str:
    ref = str(value or "").strip()
    while ref.startswith("case:case:"):
        ref = "case:" + ref.removeprefix("case:case:")
    return ref


def _load_previous_skill_map(path: str | Path | None) -> CompanySkillMap | None:
    if path is None:
        return None
    resolved = Path(path).expanduser().resolve()
    if resolved.is_dir():
        resolved = resolved / "company_skill_map.json"
    if not resolved.exists():
        raise FileNotFoundError(f"previous skill map not found: {resolved}")
    return CompanySkillMap.model_validate_json(resolved.read_text(encoding="utf-8"))


def _apply_previous_skill_map(
    skill_map: CompanySkillMap, previous_map: CompanySkillMap
) -> None:
    previous_by_id = {skill.skill_id: skill for skill in previous_map.skills}
    matched_previous_ids: set[str] = set()
    preserved_count = 0
    changed_count = 0
    new_count = 0

    for skill in skill_map.skills:
        previous = previous_by_id.get(skill.skill_id)
        if previous is None:
            previous = _best_previous_skill_match(
                skill,
                previous_map.skills,
                matched_previous_ids=matched_previous_ids,
            )
        if previous is None:
            skill.metadata["refresh_status"] = "new"
            new_count += 1
            continue
        matched_previous_ids.add(previous.skill_id)
        skill.metadata["previous_skill_id"] = previous.skill_id
        if _skill_signature(skill) == _skill_signature(previous):
            _preserve_previous_skill_state(skill, previous, preserve_review=True)
            skill.metadata["refresh_status"] = "preserved"
            preserved_count += 1
        else:
            _preserve_previous_skill_state(skill, previous, preserve_review=False)
            skill.metadata["refresh_status"] = "changed"
            changed_count += 1

    retired: list[CompanySkill] = []
    for previous in previous_map.skills:
        if previous.skill_id in matched_previous_ids:
            continue
        retired_skill = previous.model_copy(deep=True)
        retired_skill.status = "retired"
        retired_skill.deployment_readiness = "not_tested"
        retired_skill.metadata = dict(retired_skill.metadata)
        retired_skill.metadata.update(
            {
                "refresh_status": "retired",
                "retired_reason": "No evidence-grounded matching skill was synthesized from the refreshed company bundle.",
                "previous_skill_id": previous.skill_id,
            }
        )
        retired.append(retired_skill)

    if retired:
        skill_map.skills.extend(retired)
    skill_map.metadata["refresh"] = {
        "previous_source_ref": previous_map.source_ref,
        "previous_generated_at": previous_map.generated_at,
        "previous_skill_count": len(previous_map.skills),
        "preserved_skill_count": preserved_count,
        "changed_skill_count": changed_count,
        "new_skill_count": new_count,
        "retired_skill_count": len(retired),
    }


def _best_previous_skill_match(
    skill: CompanySkill,
    previous_skills: list[CompanySkill],
    *,
    matched_previous_ids: set[str],
) -> CompanySkill | None:
    skill_evidence = set(_skill_evidence_ids(skill))
    skill_title = _slug(skill.title)
    best: tuple[float, CompanySkill] | None = None
    for previous in previous_skills:
        if previous.skill_id in matched_previous_ids:
            continue
        previous_evidence = set(_skill_evidence_ids(previous))
        overlap_score = 0.0
        if skill_evidence and previous_evidence:
            overlap_score = len(skill_evidence & previous_evidence) / max(
                len(skill_evidence | previous_evidence), 1
            )
        title_score = (
            0.25 if skill_title and skill_title == _slug(previous.title) else 0.0
        )
        score = overlap_score + title_score
        if score <= 0:
            continue
        if best is None or score > best[0]:
            best = (score, previous)
    if best is None or best[0] < 0.3:
        return None
    return best[1]


def _preserve_previous_skill_state(
    skill: CompanySkill, previous: CompanySkill, *, preserve_review: bool
) -> None:
    skill.owner = previous.owner
    skill.reviewer = previous.reviewer
    if preserve_review:
        skill.review_status = previous.review_status
        if previous.status == "active" and _can_preserve_active_status(skill):
            skill.status = "active"
        elif previous.status in {"draft", "retired"}:
            skill.status = previous.status
        if previous.execution_mode in {"read_only", "shadow", "approval_gated"}:
            skill.execution_mode = previous.execution_mode
    else:
        skill.review_status = "unreviewed"
        skill.status = "draft"


def _can_preserve_active_status(skill: CompanySkill) -> bool:
    return (
        bool(skill.owner)
        and bool(skill.reviewer)
        and skill.review_status == "reviewed"
        and skill.replay_score >= 0.6
        and bool(skill.replay_results)
        and skill.freshness_status != "stale"
    )


def _skill_signature(skill: CompanySkill) -> str:
    payload = {
        "title": skill.title,
        "candidate_type": skill.candidate_type,
        "goal": skill.goal,
        "negative_triggers": sorted(skill.negative_triggers),
        "output_artifacts": [
            artifact.model_dump(mode="json") for artifact in skill.output_artifacts
        ],
        "replay_checks": sorted(skill.replay_checks),
        "evidence_ids": sorted(_skill_evidence_ids(skill)),
        "steps": [
            {
                "instruction": step.instruction,
                "tool": step.tool,
                "graph_domain": step.graph_domain,
                "graph_action": step.graph_action,
                "read_only": step.read_only,
                "requires_approval": step.requires_approval,
            }
            for step in skill.steps
        ],
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()


def _skill_evidence_ids(skill: CompanySkill) -> list[str]:
    ids = [_evidence_id(evidence) for evidence in skill.evidence_refs]
    ids.extend(str(item) for item in _safe_list(skill.metadata.get("evidence_ids")))
    return _unique(ids)


def _llm_available(provider: str) -> bool:
    normalized = provider.strip().lower()
    if normalized == "codex":
        return True
    return any(
        os.environ.get(name, "").strip() for name in _provider_env_names(normalized)
    )


def _provider_env_names(provider: str) -> tuple[str, ...]:
    provider_key_map = {
        "openai": ("OPENAI_API_KEY",),
        "anthropic": ("ANTHROPIC_API_KEY",),
        "google": ("GOOGLE_API_KEY", "GEMINI_API_KEY"),
        "openrouter": ("OPENROUTER_API_KEY",),
    }
    return provider_key_map.get(provider.strip().lower(), ())


def _run_async(awaitable: Any) -> Any:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)
    if not loop.is_running():
        return asyncio.run(awaitable)
    return _run_async_in_thread(awaitable)


def _run_async_in_thread(awaitable: Any) -> Any:
    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}

    def runner() -> None:
        try:
            result["value"] = asyncio.run(awaitable)
        except BaseException as exc:  # pragma: no cover - surfaced in caller
            error["value"] = exc

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    thread.join()
    if "value" in error:
        raise error["value"]
    if "value" not in result:
        raise RuntimeError("LLM worker exited without a result")
    return result["value"]


def _event_summary(row: dict[str, Any]) -> str:
    parts = [
        str(row.get("timestamp") or ""),
        str(row.get("surface") or ""),
        str(row.get("kind") or ""),
        str(row.get("subject") or ""),
        str(row.get("snippet") or ""),
    ]
    return _truncate_text(" ".join(part for part in parts if part), 700)


def _evidence_id(evidence: SkillEvidenceRef) -> str:
    return f"{evidence.ref_type}:{evidence.ref_id}"


def _freshness_from_evidence_refs(
    evidence_refs: list[SkillEvidenceRef],
) -> SkillFreshnessStatus:
    statuses = {
        str(evidence.metadata.get("status") or "").lower()
        for evidence in evidence_refs
        if evidence.ref_type == "knowledge_asset"
    }
    if statuses & {"stale", "expired", "superseded"}:
        return "stale"
    if statuses & {"active", "fresh"}:
        return "fresh"
    return "unknown"


def _clean_llm_text(value: Any, *, max_len: int) -> str:
    if value is None:
        return ""
    text = re.sub(r"\s+", " ", str(value)).strip()
    return _truncate_text(text, max_len)


def _truncate_text(value: str, max_len: int) -> str:
    text = str(value or "").strip()
    if len(text) <= max_len:
        return text
    return text[: max(max_len - 3, 0)].rstrip() + "..."


def _bounded_float(value: Any, *, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, 0.0), 1.0)


def _finalize_skill_map(skill_map: CompanySkillMap) -> CompanySkillMap:
    skill_map.skill_count = len(skill_map.skills)
    skill_map.validation = validate_company_skill_map(skill_map)
    return skill_map


def _resolve_snapshot_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser().resolve()
    if resolved.is_dir():
        resolved = resolved / CONTEXT_SNAPSHOT_FILE
    if not resolved.exists():
        raise FileNotFoundError(f"context snapshot not found: {resolved}")
    return resolved


def _model_dump(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if hasattr(value, "model_dump"):
        payload = value.model_dump(mode="json")
        return payload if isinstance(payload, dict) else {}
    if isinstance(value, dict):
        return dict(value)
    return {}


def _safe_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _safe_str_list(value: Any) -> list[str]:
    return [str(item) for item in _safe_list(value) if str(item)]


def _unique(values: list[str]) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values if str(value)))


def _stable_id(prefix: str, *parts: object) -> str:
    raw = ":".join(str(part) for part in parts if str(part))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
    slug_source = raw or digest
    return f"{prefix}:{_slug(slug_source)[:48]}:{digest}"


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "item"


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def write_company_skill_map_outputs(
    skill_map: CompanySkillMap, output_dir: str | Path
) -> dict[str, Path]:
    destination = Path(output_dir).expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": destination / "company_skill_map.json",
        "skills": destination / "company_skills.md",
        "evidence": destination / "skill_evidence_report.md",
        "replay": destination / "skill_replay_report.md",
        "refresh": destination / "skill_refresh_report.md",
        "gaps": destination / "skill_gap_report.md",
    }
    paths["json"].write_text(
        json.dumps(skill_map.model_dump(mode="json"), indent=2) + "\n",
        encoding="utf-8",
    )
    paths["skills"].write_text(
        render_company_skill_map_markdown(skill_map), encoding="utf-8"
    )
    paths["evidence"].write_text(
        render_skill_evidence_report(skill_map), encoding="utf-8"
    )
    paths["replay"].write_text(render_skill_replay_report(skill_map), encoding="utf-8")
    paths["refresh"].write_text(
        render_skill_refresh_report(skill_map), encoding="utf-8"
    )
    paths["gaps"].write_text(render_skill_gap_report(skill_map), encoding="utf-8")
    return paths
