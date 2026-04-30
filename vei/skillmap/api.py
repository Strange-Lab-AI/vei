"""Public skill-map API (thin barrel over implementation modules)."""

from __future__ import annotations

from pathlib import Path

from vei.llm.providers import plan_once_with_usage
from vei.world.api import WorldSessionAPI

from vei.skillmap import skill_pipeline as _skill_pipeline
from vei.skillmap.models import (
    CompanySkill,
    CompanySkillMap,
    SkillEvidenceRef,
    SkillMapGap,
    SkillMapValidation,
    SkillReplayResult,
    SkillStep,
    SkillTrigger,
    SkillValidationIssue,
)

from vei.skillmap.skill_pipeline import (
    render_company_skill_map_markdown,
    render_skill_evidence_report,
    render_skill_gap_report,
    render_skill_replay_report,
    render_skill_refresh_report,
    validate_company_skill_map,
    write_company_skill_map_outputs,
)

_ORIGINAL_PLAN_ONCE_WITH_USAGE = _skill_pipeline.plan_once_with_usage
_ORIGINAL_LLM_AVAILABLE = _skill_pipeline._llm_available

_llm_available = _ORIGINAL_LLM_AVAILABLE


def _sync_test_hook_globals() -> None:
    """Keep pre-split API monkeypatch hooks working without clobbering new ones."""
    if plan_once_with_usage is not _ORIGINAL_PLAN_ONCE_WITH_USAGE:
        _skill_pipeline.plan_once_with_usage = plan_once_with_usage
    if _llm_available is not _ORIGINAL_LLM_AVAILABLE:
        _skill_pipeline._llm_available = _llm_available


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
    _sync_test_hook_globals()
    return _skill_pipeline.build_company_skill_map_from_context_path(
        path,
        limit=limit,
        include_replay=include_replay,
        provider=provider,
        model=model,
        previous_map_path=previous_map_path,
        timeout_s=timeout_s,
        catalog_shard_size=catalog_shard_size,
    )


def build_company_skill_map_from_session(
    session: WorldSessionAPI,
    *,
    limit: int = 12,
) -> CompanySkillMap:
    return _skill_pipeline.build_company_skill_map_from_session(session, limit=limit)


__all__ = [
    "CompanySkill",
    "CompanySkillMap",
    "SkillEvidenceRef",
    "SkillMapGap",
    "SkillMapValidation",
    "SkillReplayResult",
    "SkillStep",
    "SkillTrigger",
    "SkillValidationIssue",
    "build_company_skill_map_from_context_path",
    "build_company_skill_map_from_session",
    "render_company_skill_map_markdown",
    "render_skill_evidence_report",
    "render_skill_gap_report",
    "render_skill_replay_report",
    "render_skill_refresh_report",
    "validate_company_skill_map",
    "write_company_skill_map_outputs",
]
