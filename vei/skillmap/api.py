"""Public skill-map API (thin barrel over implementation modules)."""

from __future__ import annotations

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
    build_company_skill_map_from_context_path,
    build_company_skill_map_from_session,
    render_company_skill_map_markdown,
    render_skill_evidence_report,
    render_skill_gap_report,
    render_skill_replay_report,
    render_skill_refresh_report,
    validate_company_skill_map,
    write_company_skill_map_outputs,
)

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
