from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from vei.context.api import (
    ContextSnapshot,
    ContextSourceResult,
    write_canonical_history_sidecars,
)
from vei.skillmap.api import (
    CompanySkill,
    CompanySkillMap,
    SkillEvidenceRef,
    SkillStep,
    SkillTrigger,
    build_company_skill_map_from_context_path,
    render_company_skill_map_markdown,
    render_skill_refresh_report,
    validate_company_skill_map,
)


def test_skill_map_builds_shadow_skills_from_context_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_skillmap_llm(monkeypatch)
    snapshot_path = _write_skillmap_snapshot(tmp_path)

    skill_map = build_company_skill_map_from_context_path(snapshot_path, limit=8)

    assert skill_map.schema_version == "company_skill_map_v1"
    assert skill_map.organization_name == "Acme Ops"
    assert skill_map.canonical_event_count >= 3
    assert skill_map.metadata["skill_extractor"] == "llm"
    assert skill_map.metadata["llm_provider"] == "openai"
    assert skill_map.metadata["llm_accepted_skill_count"] == 1
    assert skill_map.validation.ok is True
    assert all(skill.status == "draft" for skill in skill_map.skills)
    assert all(skill.evidence_refs for skill in skill_map.skills)
    replayed = [skill for skill in skill_map.skills if skill.replay_results]
    assert replayed
    assert replayed[0].replay_score > 0
    assert replayed[0].deployment_readiness in {
        "needs_review",
        "shadow_ready",
        "activation_candidate",
    }
    assert replayed[0].replay_results[0].counterfactual_prompt
    assert any(skill.domain == "renewal_ops" for skill in skill_map.skills)
    assert any(
        not step.read_only and step.requires_approval
        for skill in skill_map.skills
        for step in skill.steps
    )
    serialized = json.dumps(skill_map.model_dump(mode="json"))
    for forbidden_default_world_value in [
        "checkout_v2",
        "PD-9001",
        "Q2 Territory Plan",
        "REQ-8801",
        "TCK-42",
    ]:
        assert forbidden_default_world_value not in serialized
    assert all(
        evidence.ref_type != "graph_plan"
        for skill in skill_map.skills
        for evidence in skill.evidence_refs
    )
    assert (
        skill_map.metadata["graph_plan_skills"]
        == "omitted_for_context_bundle_to_avoid_template_leakage"
    )

    markdown = render_company_skill_map_markdown(skill_map)
    assert "Company Skill Map: Acme Ops" in markdown
    assert "Evidence:" in markdown


def test_skill_map_requires_llm_credentials_when_building_context_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot_path = _write_skillmap_snapshot(tmp_path)
    monkeypatch.setattr("vei.skillmap.api._llm_available", lambda provider: False)

    with pytest.raises(
        RuntimeError, match="deterministic skill extraction is disabled"
    ):
        build_company_skill_map_from_context_path(snapshot_path, limit=4)


def test_skill_map_processes_entire_evidence_catalog_across_shards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_skillmap_llm(monkeypatch)
    snapshot_path = _write_skillmap_snapshot(tmp_path)

    skill_map = build_company_skill_map_from_context_path(
        snapshot_path,
        limit=8,
        include_replay=False,
        catalog_shard_size=2,
    )

    assert skill_map.metadata["evidence_catalog_count"] > 2
    assert skill_map.metadata["llm_shard_count"] > 1
    assert (
        skill_map.metadata["evidence_catalog_processed_count"]
        == skill_map.metadata["evidence_catalog_count"]
    )
    assert (
        skill_map.metadata["evidence_catalog_policy"]
        == "full_normalized_bundle_sharded_then_globalized"
    )
    assert skill_map.metadata["llm_pipeline"] == "evidence_clusters_then_global_skills"
    assert skill_map.metadata["llm_accepted_cluster_count"] >= 2
    assert skill_map.metadata["llm_selected_cluster_count"] >= 1


def test_skill_map_refresh_preserves_review_state_and_retires_missing_skills(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_skillmap_llm(monkeypatch)
    snapshot_path = _write_skillmap_snapshot(tmp_path)
    first_map = build_company_skill_map_from_context_path(snapshot_path, limit=4)
    previous_map = first_map.model_copy(deep=True)
    previous_map.skills[0].owner = "maya@acme.example"
    previous_map.skills[0].reviewer = "legal@acme.example"
    previous_map.skills[0].review_status = "reviewed"
    previous_map.skills.append(
        CompanySkill(
            skill_id="skill:legacy-retired",
            title="Legacy removed skill",
            summary="A previously reviewed skill no longer grounded in the bundle.",
            domain="legacy",
            trigger=SkillTrigger(description="Legacy trigger."),
            goal="Retire if no refreshed evidence supports it.",
            steps=[
                SkillStep(
                    step_id="legacy-review",
                    instruction="Review the legacy map entry.",
                    read_only=True,
                )
            ],
            evidence_refs=[
                SkillEvidenceRef(
                    ref_type="source",
                    ref_id="legacy-map",
                    title="Legacy skill map",
                )
            ],
        )
    )
    previous_path = tmp_path / "previous_skill_map.json"
    previous_path.write_text(
        json.dumps(previous_map.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )

    refreshed = build_company_skill_map_from_context_path(
        snapshot_path,
        limit=4,
        previous_map_path=previous_path,
    )

    preserved = next(
        skill
        for skill in refreshed.skills
        if skill.metadata.get("refresh_status") == "preserved"
    )
    assert preserved.owner == "maya@acme.example"
    assert preserved.reviewer == "legal@acme.example"
    assert preserved.review_status == "reviewed"
    retired = [
        skill
        for skill in refreshed.skills
        if skill.metadata.get("refresh_status") == "retired"
    ]
    assert retired and retired[0].status == "retired"
    assert refreshed.metadata["refresh"]["preserved_skill_count"] == 1
    assert refreshed.metadata["refresh"]["retired_skill_count"] == 1
    assert "Retired Skills" in render_skill_refresh_report(refreshed)


def test_skill_map_validation_rejects_unreviewed_active_skill() -> None:
    skill = CompanySkill(
        skill_id="skill:test",
        title="Send renewal update",
        summary="Demo active skill.",
        status="active",
        domain="comm_graph",
        trigger=SkillTrigger(description="A renewal update is due."),
        goal="Send a renewal update.",
        steps=[],
        evidence_refs=[],
        execution_mode="approval_gated",
    )
    skill_map = CompanySkillMap(
        organization_name="Acme Ops",
        generated_at="2026-01-01T00:00:00Z",
        source_ref="unit-test",
        skills=[skill],
    )

    validation = validate_company_skill_map(skill_map)

    assert validation.ok is False
    assert validation.error_count >= 3
    assert {issue.code for issue in validation.issues if issue.severity == "error"} >= {
        "skill_without_evidence",
        "skill_without_steps",
        "active_skill_missing_owner_or_reviewer",
        "active_skill_not_reviewed",
        "active_skill_missing_replay_proof",
    }


def _patch_skillmap_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_plan_once_with_usage(**kwargs: object) -> SimpleNamespace:
        payload = json.loads(str(kwargs["user"]))
        if "evidence_catalog" in payload:
            evidence_ids = [
                str(item["evidence_id"])
                for item in payload["evidence_catalog"]
                if str(item.get("evidence_id") or "").startswith(
                    ("case:", "event:", "knowledge_asset:")
                )
            ]
            return SimpleNamespace(
                plan={
                    "tool": "skillmap.cluster",
                    "args": {
                        "clusters": [
                            {
                                "title": "CASE-123 renewal risk operating cluster",
                                "summary": "Acme renewal risks repeat across customer mail, internal coordination, and legal SOP evidence.",
                                "candidate_type": "flagship_skill",
                                "domain": "renewal_ops",
                                "positive_triggers": ["CASE-123 renewal risk"],
                                "negative_triggers": [
                                    "Routine renewal without legal or finance blocker"
                                ],
                                "reuse_pattern": "Renewal risks need legal and finance review before customer response.",
                                "evidence_ids": evidence_ids[:5],
                                "allowed_actions": ["review_case_timeline"],
                                "blocked_actions": ["live_write_without_approval"],
                                "output_artifacts": [
                                    {
                                        "artifact_id": "renewal_update",
                                        "title": "Internal renewal update",
                                        "kind": "markdown",
                                        "schema_hint": "case, blocker, owner, next action",
                                    }
                                ],
                                "replay_checks": [
                                    "Future events should show legal or finance review before customer-facing response."
                                ],
                                "usefulness_scores": {
                                    "company_specificity": 0.9,
                                    "repeat_frequency": 0.7,
                                    "business_consequence": 0.8,
                                    "actionability": 0.9,
                                    "evidence_coverage": 0.8,
                                    "risk_if_wrong": 0.7,
                                    "replay_testability": 0.8,
                                },
                                "usefulness_rationale": "This is a high-risk repeatable renewal decision gate.",
                                "confidence": 0.82,
                            }
                        ]
                    },
                },
                usage=SimpleNamespace(
                    provider="openai",
                    model="test-model",
                    prompt_tokens=10,
                    completion_tokens=20,
                    total_tokens=30,
                    estimated_cost_usd=0.01,
                ),
            )
        evidence_ids = [
            evidence_id
            for cluster in payload["candidate_clusters"]
            for evidence_id in cluster.get("evidence_ids", [])
        ]
        return SimpleNamespace(
            plan={
                "tool": "skillmap.propose",
                "args": {
                    "skills": [
                        {
                            "title": "Coordinate CASE-123 renewal risk review",
                            "summary": "Use Acme's renewal thread, legal response, and SOP before drafting the customer update.",
                            "candidate_type": "flagship_skill",
                            "domain": "renewal_ops",
                            "trigger": {
                                "description": "A renewal risk case appears across mail, Slack, and the renewal SOP.",
                                "signals": ["CASE-123", "#renewals", "legal review"],
                            },
                            "negative_triggers": [
                                "Routine renewal with no legal or finance blocker."
                            ],
                            "goal": "Prepare a grounded internal renewal update before any customer-facing response.",
                            "reuse_pattern": "Acme renewal risks repeat across customer mail, internal coordination, and legal SOP evidence.",
                            "evidence_ids": evidence_ids[:5],
                            "steps": [
                                {
                                    "instruction": "Review the cited CASE-123 timeline and linked renewal SOP.",
                                    "tool": "vei.structure_view",
                                    "read_only": True,
                                },
                                {
                                    "instruction": "Draft an internal renewal update for human approval.",
                                    "tool": "knowledge.compose_artifact",
                                    "graph_domain": "knowledge_graph",
                                    "graph_action": "compose_artifact",
                                    "args": {"target": "internal_update"},
                                    "read_only": False,
                                    "requires_approval": True,
                                },
                            ],
                            "output_artifacts": [
                                {
                                    "artifact_id": "renewal_update",
                                    "title": "Internal renewal update",
                                    "kind": "markdown",
                                    "schema_hint": "case, blocker, owner, next action",
                                }
                            ],
                            "replay_checks": [
                                "Future events should show legal or finance review before customer-facing response."
                            ],
                            "allowed_actions": [
                                "review_case_timeline",
                                "compose_shadow_draft",
                            ],
                            "blocked_actions": ["live_write_without_approval"],
                            "execution_mode": "shadow",
                            "tags": ["renewal", "legal_review", "CASE-123"],
                            "usefulness_scores": {
                                "company_specificity": 0.9,
                                "repeat_frequency": 0.7,
                                "business_consequence": 0.8,
                                "actionability": 0.9,
                                "evidence_coverage": 0.8,
                                "risk_if_wrong": 0.7,
                                "replay_testability": 0.8,
                            },
                            "usefulness_rationale": "This is a high-risk repeatable renewal decision gate.",
                            "confidence": 0.82,
                        }
                    ]
                },
            },
            usage=SimpleNamespace(
                provider="openai",
                model="test-model",
                prompt_tokens=10,
                completion_tokens=20,
                total_tokens=30,
                estimated_cost_usd=0.01,
            ),
        )

    monkeypatch.setattr(
        "vei.skillmap.api.plan_once_with_usage", fake_plan_once_with_usage
    )
    monkeypatch.setattr("vei.skillmap.api._llm_available", lambda provider: True)


def _write_skillmap_snapshot(tmp_path: Path) -> Path:
    snapshot = ContextSnapshot(
        organization_name="Acme Ops",
        organization_domain="acme.example",
        captured_at="2026-01-03T00:00:00Z",
        metadata={"snapshot_role": "company_history_bundle"},
        sources=[
            ContextSourceResult(
                provider="mail_archive",
                captured_at="2026-01-03T00:00:00Z",
                record_counts={"threads": 1, "messages": 1},
                data={
                    "threads": [
                        {
                            "thread_id": "thr-CASE-123",
                            "subject": "CASE-123 renewal risk",
                            "messages": [
                                {
                                    "from": "maya@acme.example",
                                    "to": ["legal@acme.example"],
                                    "date": "2026-01-01T10:00:00Z",
                                    "subject": "CASE-123 renewal risk",
                                    "body_text": (
                                        "CASE-123 renewal risk needs legal review "
                                        "before the customer reply."
                                    ),
                                },
                                {
                                    "from": "legal@acme.example",
                                    "to": ["maya@acme.example"],
                                    "date": "2026-01-01T10:30:00Z",
                                    "subject": "CASE-123 renewal risk",
                                    "body_text": (
                                        "Legal review complete. Keep the first "
                                        "reply internal until finance confirms."
                                    ),
                                },
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="slack",
                captured_at="2026-01-03T00:00:00Z",
                record_counts={"channels": 1, "messages": 1},
                data={
                    "channels": [
                        {
                            "channel": "#renewals",
                            "messages": [
                                {
                                    "user": "maya@acme.example",
                                    "ts": "2026-01-01T11:00:00Z",
                                    "text": (
                                        "CASE-123 renewal risk has a customer "
                                        "thread and legal review."
                                    ),
                                }
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="notion",
                captured_at="2026-01-03T00:00:00Z",
                record_counts={"pages": 1},
                data={
                    "pages": [
                        {
                            "page_id": "page-1",
                            "title": "CASE-123 renewal SOP",
                            "body": "Use legal review for renewal risk.",
                            "tags": ["renewal", "sop"],
                            "linked_object_refs": ["case:CASE-123"],
                            "updated_at": "2026-01-01T12:00:00Z",
                        }
                    ]
                },
            ),
        ],
    )
    snapshot_path = tmp_path / "context_snapshot.json"
    snapshot_path.write_text(
        json.dumps(snapshot.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )
    write_canonical_history_sidecars(snapshot, snapshot_path)
    return snapshot_path
