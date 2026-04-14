from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from vei.blueprint.models import BlueprintAsset
from vei.context.models import ContextSnapshot
from vei.cli.vei import app as cli_app
from vei.twin import load_customer_twin
from vei.whatif import (
    build_branch_point_benchmark,
    evaluate_branch_point_benchmark_model,
    get_research_pack,
    judge_branch_point_benchmark,
    list_branch_point_benchmark_models,
    load_branch_point_benchmark_build_result,
    load_branch_point_benchmark_eval_result,
    load_branch_point_benchmark_judge_result,
    load_branch_point_benchmark_study_result,
    load_branch_point_benchmark_train_result,
    load_research_pack_run_result,
    load_episode_manifest,
    list_research_packs,
    load_world,
    materialize_episode,
    run_research_pack,
    run_branch_point_benchmark_study,
    search_events,
    train_branch_point_benchmark_model,
    run_whatif,
)
from vei.whatif.models import (
    WhatIfBenchmarkCaseEvaluation,
    WhatIfBenchmarkEvalArtifacts,
    WhatIfBenchmarkEvalResult,
    WhatIfBenchmarkJudgeArtifacts,
    WhatIfBenchmarkJudgeResult,
    WhatIfBenchmarkMetricSummary,
    WhatIfBenchmarkStudyArtifacts,
    WhatIfBenchmarkStudyModelSummary,
    WhatIfBenchmarkStudyResult,
    WhatIfBenchmarkTrainArtifacts,
    WhatIfBenchmarkTrainResult,
    WhatIfCounterfactualCandidatePrediction,
    WhatIfCounterfactualObjectiveEvaluation,
    WhatIfObservedEvidenceHeads,
    WhatIfObservedForecastMetrics,
    WhatIfAuditRecord,
    WhatIfEventReference,
    WhatIfHistoricalScore,
    WhatIfCounterfactualEstimateDelta,
    WhatIfCounterfactualEstimateResult,
    WhatIfLLMGeneratedMessage,
    WhatIfLLMReplayResult,
    WhatIfOutcomeSignals,
    WhatIfResearchCandidate,
    WhatIfResearchCase,
    WhatIfResearchPack,
)
from vei.whatif.benchmark_business import (
    evidence_to_business_outcomes,
    get_business_objective_pack,
    list_business_objective_packs,
    score_business_objective,
    summarize_observed_evidence,
)
from vei.whatif.benchmark_bridge import (
    _BenchmarkPreprocessor,
    _EVIDENCE_TARGET_NAMES,
    _SEQUENCE_NUMERIC_WIDTH,
    _SEQUENCE_TOKEN_LIMIT,
    _TorchTrainer,
    _action_vector_width,
)
from vei.whatif.corpus import has_external_recipients, recipient_scope
from vei.whatif.ranking import (
    aggregate_outcome_signals,
    get_objective_pack,
    score_outcome_signals,
    sort_candidates_for_rank,
    summarize_llm_branch,
)


def _write_rosetta_fixture(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    metadata_rows = [
        {
            "event_id": "evt-001",
            "timestamp": "2001-05-01T10:00:00Z",
            "actor_id": "vince.kaminski@enron.com",
            "target_id": "sara.shackleton@enron.com",
            "event_type": "message",
            "thread_task_id": "thr-legal-trading",
            "artifacts": json.dumps(
                {
                    "subject": "Gas Position Limits",
                    "norm_subject": "gas position limits",
                    "to_recipients": ["sara.shackleton@enron.com"],
                    "to_count": 1,
                    "consult_legal_specialist": True,
                    "custodian_id": "kaminski-v",
                }
            ),
        },
        {
            "event_id": "evt-002",
            "timestamp": "2001-05-01T10:00:01Z",
            "actor_id": "sara.shackleton@enron.com",
            "target_id": "mark.taylor@enron.com",
            "event_type": "reply",
            "thread_task_id": "thr-legal-trading",
            "artifacts": json.dumps(
                {
                    "subject": "Gas Position Limits",
                    "norm_subject": "gas position limits",
                    "to_recipients": ["mark.taylor@enron.com"],
                    "to_count": 1,
                    "consult_trading_specialist": True,
                    "is_forward": True,
                    "custodian_id": "shackleton-s",
                }
            ),
        },
        {
            "event_id": "evt-003",
            "timestamp": "2001-05-01T10:00:02Z",
            "actor_id": "mark.taylor@enron.com",
            "target_id": "ops.review@enron.com",
            "event_type": "assignment",
            "thread_task_id": "thr-legal-trading",
            "artifacts": json.dumps(
                {
                    "subject": "Gas Position Limits",
                    "norm_subject": "gas position limits",
                    "to_recipients": ["ops.review@enron.com"],
                    "to_count": 1,
                    "custodian_id": "taylor-m",
                }
            ),
        },
        {
            "event_id": "evt-004",
            "timestamp": "2001-05-01T10:00:03Z",
            "actor_id": "assistant@enron.com",
            "target_id": "kenneth.lay@enron.com",
            "event_type": "escalation",
            "thread_task_id": "thr-exec",
            "artifacts": json.dumps(
                {
                    "subject": "Escalate to leadership",
                    "to_recipients": ["kenneth.lay@enron.com"],
                    "to_count": 1,
                    "is_escalation": True,
                }
            ),
        },
        {
            "event_id": "evt-005",
            "timestamp": "2001-05-01T10:00:04Z",
            "actor_id": "jeff.skilling@enron.com",
            "target_id": "outside@lawfirm.com",
            "event_type": "message",
            "thread_task_id": "thr-external",
            "artifacts": json.dumps(
                {
                    "subject": "Draft term sheet",
                    "to_recipients": ["outside@lawfirm.com"],
                    "to_count": 1,
                    "has_attachment_reference": True,
                }
            ),
        },
    ]
    content_rows = [
        {"event_id": "evt-001", "content": "Need legal eyes on this position update."},
        {"event_id": "evt-002", "content": "Forwarding with trading context attached."},
        {"event_id": "evt-003", "content": "Assigning ops review before we proceed."},
        {"event_id": "evt-004", "content": "Escalating to executive review."},
        {"event_id": "evt-005", "content": "External draft attached for review."},
    ]
    pq.write_table(
        pa.Table.from_pylist(metadata_rows),
        root / "enron_rosetta_events_metadata.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(content_rows),
        root / "enron_rosetta_events_content.parquet",
    )


def _write_mail_archive_fixture(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    archive_path = root / "context_snapshot.json"
    archive_path.write_text(
        json.dumps(
            {
                "organization_name": "Py Corp",
                "organization_domain": "pycorp.example.com",
                "captured_at": "2026-03-01T09:15:00Z",
                "threads": [
                    {
                        "thread_id": "py-legal-001",
                        "subject": "Pricing addendum",
                        "category": "historical",
                        "messages": [
                            {
                                "message_id": "py-msg-001",
                                "from": "emma@pycorp.example.com",
                                "to": "legal@pycorp.example.com",
                                "subject": "Pricing addendum",
                                "body_text": "Please review before we send this draft to Redwood.",
                                "timestamp": "2026-03-01T09:00:00Z",
                            },
                            {
                                "message_id": "py-msg-002",
                                "from": "legal@pycorp.example.com",
                                "to": "emma@pycorp.example.com",
                                "subject": "Re: Pricing addendum",
                                "body_text": "Hold for one markup round. Counsel wants one more pass.",
                                "timestamp": "2026-03-01T09:05:00Z",
                            },
                            {
                                "message_id": "py-msg-003",
                                "from": "emma@pycorp.example.com",
                                "to": "partner@redwoodcapital.com",
                                "subject": "Pricing addendum",
                                "body_text": "Sharing the draft addendum now.",
                                "timestamp": "2026-03-01T09:10:00Z",
                                "has_attachment_reference": True,
                            },
                        ],
                    }
                ],
                "actors": [
                    {
                        "actor_id": "emma@pycorp.example.com",
                        "email": "emma@pycorp.example.com",
                        "display_name": "Emma Rowan",
                    },
                    {
                        "actor_id": "legal@pycorp.example.com",
                        "email": "legal@pycorp.example.com",
                        "display_name": "Legal Team",
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return archive_path


def _write_company_history_fixture(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    snapshot_path = root / "context_snapshot.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "version": "1",
                "organization_name": "Py Corp",
                "organization_domain": "pycorp.example.com",
                "captured_at": "2026-03-01T10:15:00Z",
                "sources": [
                    {
                        "provider": "slack",
                        "captured_at": "2026-03-01T10:15:00Z",
                        "status": "ok",
                        "record_counts": {"channels": 1, "messages": 3, "users": 2},
                        "data": {
                            "channels": [
                                {
                                    "channel": "#deal-desk",
                                    "channel_id": "C001",
                                    "unread": 0,
                                    "messages": [
                                        {
                                            "ts": "2026-03-01T09:00:00Z",
                                            "user": "emma@pycorp.example.com",
                                            "text": "Need a clean internal review thread for LEGAL-7 before we update Redwood.",
                                        },
                                        {
                                            "ts": "2026-03-01T09:05:00Z",
                                            "user": "legal@pycorp.example.com",
                                            "text": "Hold LEGAL-7 internally until legal signs off.",
                                            "thread_ts": "2026-03-01T09:00:00Z",
                                        },
                                        {
                                            "ts": "2026-03-01T09:10:00Z",
                                            "user": "emma@pycorp.example.com",
                                            "text": "I will send Redwood a short LEGAL-7 status note after that review.",
                                            "thread_ts": "2026-03-01T09:00:00Z",
                                        },
                                    ],
                                }
                            ],
                            "users": [
                                {
                                    "id": "U001",
                                    "name": "emma",
                                    "real_name": "Emma Rowan",
                                    "email": "emma@pycorp.example.com",
                                },
                                {
                                    "id": "U002",
                                    "name": "legal",
                                    "real_name": "Legal Team",
                                    "email": "legal@pycorp.example.com",
                                },
                            ],
                        },
                    },
                    {
                        "provider": "jira",
                        "captured_at": "2026-03-01T10:15:00Z",
                        "status": "ok",
                        "record_counts": {"issues": 1, "projects": 1},
                        "data": {
                            "issues": [
                                {
                                    "ticket_id": "LEGAL-7",
                                    "title": "LEGAL-7 pricing addendum review",
                                    "status": "in_progress",
                                    "assignee": "emma@pycorp.example.com",
                                    "description": "Check LEGAL-7 pricing addendum before any outside update.",
                                    "updated": "2026-03-01T10:00:00Z",
                                    "comments": [
                                        {
                                            "id": "c1",
                                            "author": "legal@pycorp.example.com",
                                            "body": "Need one more LEGAL-7 markup pass before we send anything outside.",
                                            "created": "2026-03-01T09:02:00Z",
                                        },
                                        {
                                            "id": "c2",
                                            "author": "emma@pycorp.example.com",
                                            "body": "Holding the LEGAL-7 response until the markup is done.",
                                            "created": "2026-03-01T09:06:00Z",
                                        },
                                    ],
                                }
                            ],
                            "projects": [{"key": "LEGAL", "name": "Legal"}],
                        },
                    },
                    {
                        "provider": "google",
                        "captured_at": "2026-03-01T10:15:00Z",
                        "status": "ok",
                        "record_counts": {"documents": 1},
                        "data": {
                            "documents": [
                                {
                                    "doc_id": "DOC-LEGAL-7",
                                    "title": "LEGAL-7 markup tracker",
                                    "mime_type": "application/vnd.google-apps.document",
                                    "body": "Markup notes for LEGAL-7 pricing addendum review.",
                                }
                            ]
                        },
                    },
                    {
                        "provider": "salesforce",
                        "captured_at": "2026-03-01T10:15:00Z",
                        "status": "ok",
                        "record_counts": {"deals": 1},
                        "data": {
                            "deals": [
                                {
                                    "id": "DEAL-LEGAL-7",
                                    "name": "LEGAL-7 Redwood renewal",
                                    "stage": "legal_review",
                                    "owner": "emma@pycorp.example.com",
                                    "amount": 240000,
                                }
                            ]
                        },
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return snapshot_path


def _write_company_history_with_mixed_mail_fixture(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    snapshot_path = root / "context_snapshot.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "version": "1",
                "organization_name": "Mixed Mail Corp",
                "organization_domain": "mixed.example.com",
                "captured_at": "2026-03-01T10:15:00Z",
                "sources": [
                    {
                        "provider": "mail_archive",
                        "captured_at": "2026-03-01T10:15:00Z",
                        "status": "ok",
                        "data": {
                            "threads": [
                                {
                                    "thread_id": "archive-thread-001",
                                    "subject": "Archive pricing review",
                                    "category": "historical",
                                    "messages": [
                                        {
                                            "message_id": "archive-msg-001",
                                            "from": "emma@mixed.example.com",
                                            "to": "legal@mixed.example.com",
                                            "subject": "Archive pricing review",
                                            "body_text": "Archive side history",
                                            "timestamp": "2026-03-01T09:00:00Z",
                                        }
                                    ],
                                }
                            ],
                            "actors": [],
                        },
                    },
                    {
                        "provider": "gmail",
                        "captured_at": "2026-03-01T10:15:00Z",
                        "status": "ok",
                        "data": {
                            "threads": [
                                {
                                    "thread_id": "gmail-thread-001",
                                    "subject": "Gmail renewal follow-up",
                                    "messages": [
                                        {
                                            "id": "gmail-msg-001",
                                            "from": "sales@mixed.example.com",
                                            "to": "buyer@example.net",
                                            "subject": "Gmail renewal follow-up",
                                            "snippet": "Gmail side history",
                                            "internal_date": 1772355900000,
                                        }
                                    ],
                                }
                            ]
                        },
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return snapshot_path


def _make_llm_replay_result(
    *,
    prompt: str,
    to: str,
    subject: str,
    body_text: str,
    delay_ms: int,
    summary: str,
    notes: list[str] | None = None,
) -> WhatIfLLMReplayResult:
    return WhatIfLLMReplayResult(
        status="ok",
        provider="openai",
        model="gpt-5-mini",
        prompt=prompt,
        summary=summary,
        messages=[
            WhatIfLLMGeneratedMessage(
                actor_id="jeff.skilling@enron.com",
                to=to,
                subject=subject,
                body_text=body_text,
                delay_ms=delay_ms,
            )
        ],
        scheduled_event_count=1,
        delivered_event_count=1,
        inbox_count=1,
        notes=notes or [],
    )


def _make_forecast_result(
    *,
    prompt: str,
    risk_score: float,
    future_event_count: int,
    future_external_event_count: int,
    summary: str,
) -> WhatIfCounterfactualEstimateResult:
    baseline = WhatIfHistoricalScore(
        backend="historical",
        future_event_count=2,
        future_external_event_count=1,
        risk_score=0.6,
    )
    predicted = WhatIfHistoricalScore(
        backend="e_jepa_proxy",
        future_event_count=future_event_count,
        future_external_event_count=future_external_event_count,
        risk_score=risk_score,
    )
    return WhatIfCounterfactualEstimateResult(
        status="ok",
        backend="e_jepa_proxy",
        prompt=prompt,
        summary=summary,
        baseline=baseline,
        predicted=predicted,
        delta=WhatIfCounterfactualEstimateDelta(
            risk_score_delta=round(risk_score - baseline.risk_score, 3),
            future_event_delta=future_event_count - baseline.future_event_count,
            external_event_delta=(
                future_external_event_count - baseline.future_external_event_count
            ),
        ),
    )


def _make_research_pack() -> WhatIfResearchPack:
    return WhatIfResearchPack(
        pack_id="fixture_pack",
        title="Fixture Pack",
        summary="Small fixture pack for the research runner tests.",
        objective_pack_ids=[
            "contain_exposure",
            "reduce_delay",
            "protect_relationship",
        ],
        rollout_seeds=[42042, 42043],
        cases=[
            WhatIfResearchCase(
                case_id="external_hold_case",
                title="External Hold Case",
                event_id="evt-005",
                thread_id="thr-external",
                summary="A small held-out external thread.",
                candidates=[
                    WhatIfResearchCandidate(
                        candidate_id="legal_hold_internal",
                        label="Legal hold internal",
                        prompt=(
                            "Keep this internal, hold the send, and ask legal to "
                            "review before anything leaves Enron."
                        ),
                        expected_hypotheses={
                            "contain_exposure": "best_expected",
                            "reduce_delay": "worst_expected",
                            "protect_relationship": "middle_expected",
                        },
                    ),
                    WhatIfResearchCandidate(
                        candidate_id="narrow_external_status",
                        label="Narrow external status",
                        prompt=(
                            "Send a short external status note right away, promise a "
                            "clean draft later, and keep the attachment inside."
                        ),
                        expected_hypotheses={
                            "contain_exposure": "middle_expected",
                            "reduce_delay": "best_expected",
                            "protect_relationship": "best_expected",
                        },
                    ),
                    WhatIfResearchCandidate(
                        candidate_id="broad_external_send",
                        label="Broad external send",
                        prompt=(
                            "Send the draft outside now, include the attachment, and "
                            "widen circulation for urgent comments."
                        ),
                        expected_hypotheses={
                            "contain_exposure": "worst_expected",
                            "reduce_delay": "middle_expected",
                            "protect_relationship": "worst_expected",
                        },
                    ),
                ],
            )
        ],
    )


def _make_generic_research_pack() -> WhatIfResearchPack:
    return WhatIfResearchPack(
        pack_id="pycorp_fixture_pack",
        title="Py Corp Fixture Pack",
        summary="Small fixture pack for a generic mail archive company.",
        objective_pack_ids=[
            "contain_exposure",
            "reduce_delay",
            "protect_relationship",
        ],
        rollout_seeds=[42042],
        cases=[
            WhatIfResearchCase(
                case_id="pycorp_external_hold",
                title="Py Corp External Hold",
                event_id="py-msg-002",
                thread_id="py-legal-001",
                summary="A held-out external send from a generic mail archive.",
                candidates=[
                    WhatIfResearchCandidate(
                        candidate_id="internal_hold",
                        label="Internal hold",
                        prompt=(
                            "Keep this internal, hold the send, and ask legal to review "
                            "before anything leaves Py Corp."
                        ),
                        expected_hypotheses={
                            "contain_exposure": "best_expected",
                            "reduce_delay": "worst_expected",
                            "protect_relationship": "middle_expected",
                        },
                    ),
                    WhatIfResearchCandidate(
                        candidate_id="status_note",
                        label="Status note",
                        prompt=(
                            "Send a short status note outside, promise a clean draft "
                            "later, and keep the attachment inside."
                        ),
                        expected_hypotheses={
                            "contain_exposure": "middle_expected",
                            "reduce_delay": "best_expected",
                            "protect_relationship": "best_expected",
                        },
                    ),
                ],
            )
        ],
    )


def _write_research_pack_file(path: Path, pack: WhatIfResearchPack) -> Path:
    path.write_text(pack.model_dump_json(indent=2), encoding="utf-8")
    return path


def test_load_world_and_run_compliance_whatif(tmp_path: Path) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)

    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    result = run_whatif(world, scenario="compliance_gateway")
    prompt_result = run_whatif(
        world,
        prompt="What if compliance reviewed every legal and trading thread?",
    )

    assert world.summary.event_count == 5
    assert world.summary.thread_count == 3
    assert result.affected_thread_count == 1
    assert result.blocked_forward_count == 1
    assert result.delayed_assignment_count == 1
    assert result.top_threads[0].thread_id == "thr-legal-trading"
    assert prompt_result.scenario.scenario_id == "compliance_gateway"


def test_company_history_merges_mail_archive_and_gmail_threads(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_company_history_with_mixed_mail_fixture(
        tmp_path / "company_history_mixed_mail"
    )

    world = load_world(source="company_history", source_dir=snapshot_path)

    assert world.source == "company_history"
    assert {
        thread.thread_id for thread in world.threads if thread.surface == "mail"
    } == {"mail:archive-thread-001", "mail:gmail-thread-001"}


def test_company_history_uses_gmail_date_header_when_internal_date_is_missing(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "gmail_date_snapshot.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "version": "1",
                "organization_name": "Dispatch",
                "organization_domain": "thedispatch.ai",
                "captured_at": "2026-04-13T10:00:00Z",
                "sources": [
                    {
                        "provider": "gmail",
                        "captured_at": "2026-04-13T10:00:00Z",
                        "status": "ok",
                        "data": {
                            "threads": [
                                {
                                    "thread_id": "dispatch-thread-001",
                                    "subject": "Fundraising follow-up",
                                    "messages": [
                                        {
                                            "message_id": "<dispatch-1@thedispatch.ai>",
                                            "from": "jon@thedispatch.ai",
                                            "to": "investor@example.com",
                                            "subject": "Fundraising follow-up",
                                            "snippet": "Sharing the deck and next steps.",
                                            "date": "Tue, 13 Aug 2024 14:32:10 +0000",
                                        }
                                    ],
                                }
                            ]
                        },
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    world = load_world(source="company_history", source_dir=snapshot_path)

    event = next(
        item for item in world.events if item.thread_id == "mail:dispatch-thread-001"
    )
    assert event.timestamp.startswith("2024-08-13T14:32:10")
    assert event.timestamp_ms > 1_700_000_000_000


def test_company_history_case_context_links_related_surfaces_and_records(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_company_history_fixture(tmp_path / "company_history_case")
    world = load_world(source="company_history", source_dir=snapshot_path)

    slack_thread = next(thread for thread in world.threads if thread.surface == "slack")
    workspace_root = tmp_path / "company_history_case_episode"
    materialization = materialize_episode(
        world,
        root=workspace_root,
        thread_id=slack_thread.thread_id,
    )
    manifest = load_episode_manifest(workspace_root)
    bundle = load_customer_twin(workspace_root)
    snapshot = ContextSnapshot.model_validate_json(
        materialization.context_snapshot_path.read_text(encoding="utf-8")
    )
    asset = BlueprintAsset.model_validate_json(
        (workspace_root / bundle.blueprint_asset_path).read_text(encoding="utf-8")
    )

    assert any(case.case_id == "case:LEGAL-7" for case in world.cases)
    assert world.situation_graph is not None
    assert any(
        {"crm", "docs", "slack"} <= set(cluster.surfaces)
        for cluster in world.situation_graph.clusters
    )
    assert materialization.case_id == "case:LEGAL-7"
    assert manifest.case_context is not None
    assert len(manifest.case_context.related_history) >= 1
    assert {record.surface for record in manifest.case_context.records} >= {
        "docs",
        "crm",
    }
    assert manifest.situation_context is not None
    assert {
        thread.surface for thread in manifest.situation_context.related_threads
    } >= {
        "docs",
        "crm",
    }
    assert {source.provider for source in snapshot.sources} >= {
        "jira",
        "slack",
        "google",
        "salesforce",
    }
    assert asset.capability_graphs is not None
    assert asset.capability_graphs.work_graph is not None
    assert {
        ticket.ticket_id for ticket in asset.capability_graphs.work_graph.tickets
    } >= {"LEGAL-7"}
    assert "jira" in asset.requested_facades
    assert "docs" in asset.requested_facades
    assert "crm" in asset.requested_facades


def test_load_company_history_world_rejects_errored_event_sources(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "broken_company_history.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "organization_name": "Broken Corp",
                "organization_domain": "broken.example.com",
                "captured_at": "2026-03-01T10:15:00Z",
                "sources": [
                    {
                        "provider": "slack",
                        "captured_at": "2026-03-01T10:15:00Z",
                        "status": "error",
                        "error": "rate limited",
                        "record_counts": {},
                        "data": {},
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="could not detect historical source"):
        load_world(source="auto", source_dir=snapshot_path)
    with pytest.raises(ValueError, match="supported sources"):
        load_world(source="company_history", source_dir=snapshot_path)


def test_recipient_helpers_keep_legacy_enron_default_scope() -> None:
    assert not has_external_recipients(["alice@enron.com"])
    assert recipient_scope(["alice@enron.com"]) == "internal"


def test_load_mail_archive_world_prefers_sender_domain_for_company_inference(
    tmp_path: Path,
) -> None:
    root = tmp_path / "domain_inference_archive"
    root.mkdir(parents=True, exist_ok=True)
    archive_path = root / "context_snapshot.json"
    archive_path.write_text(
        json.dumps(
            {
                "threads": [
                    {
                        "thread_id": "py-legal-002",
                        "subject": "Draft policy",
                        "messages": [
                            {
                                "message_id": "sender-001",
                                "from": "owner@pycorp.example.com",
                                "to": [
                                    "person1@outside.example.com",
                                    "person2@outside.example.com",
                                    "person3@outside.example.com",
                                ],
                                "subject": "Draft policy",
                                "body_text": "Please review.",
                                "timestamp": "2026-03-02T10:00:00Z",
                            },
                            {
                                "message_id": "sender-002",
                                "from": "legal@pycorp.example.com",
                                "to": "owner@pycorp.example.com",
                                "subject": "Re: Draft policy",
                                "body_text": "One internal legal pass first.",
                                "timestamp": "2026-03-02T10:05:00Z",
                            },
                        ],
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    world = load_world(source="auto", source_dir=archive_path)

    assert world.summary.organization_domain == "pycorp.example.com"


def test_vei_whatif_cli_supports_generic_mail_archive_source(tmp_path: Path) -> None:
    archive_path = _write_mail_archive_fixture(tmp_path / "mail_archive_cli")
    runner = CliRunner()

    explore_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "explore",
            "--source-dir",
            str(archive_path),
            "--scenario",
            "external_dlp",
        ],
    )
    assert explore_result.exit_code == 0, explore_result.output
    explore_payload = json.loads(explore_result.output)
    assert explore_payload["world_summary"]["organization_name"] == "Py Corp"

    workspace_root = tmp_path / "py_episode_cli"
    open_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "open",
            "--source-dir",
            str(archive_path),
            "--root",
            str(workspace_root),
            "--thread-id",
            "py-legal-001",
        ],
    )
    assert open_result.exit_code == 0, open_result.output
    open_payload = json.loads(open_result.output)
    assert open_payload["organization_name"] == "Py Corp"
    assert open_payload["branch_event"]["event_id"] == "py-msg-002"

    replay_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "replay",
            "--root",
            str(workspace_root),
            "--tick-ms",
            "400000",
        ],
    )
    assert replay_result.exit_code == 0, replay_result.output
    replay_payload = json.loads(replay_result.output)
    assert replay_payload["scheduled_event_count"] == 2
    assert replay_payload["delivered_event_count"] == 2

    events_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "events",
            "--source-dir",
            str(archive_path),
            "--query",
            "Redwood draft",
        ],
    )
    assert events_result.exit_code == 0, events_result.output
    events_payload = json.loads(events_result.output)
    assert events_payload["match_count"] >= 1
    assert events_payload["matches"][0]["event"]["thread_id"] == "py-legal-001"


def test_vei_whatif_cli_rejects_enron_research_pack_for_generic_archive(
    tmp_path: Path,
) -> None:
    archive_path = _write_mail_archive_fixture(tmp_path / "mail_archive_cli_pack")
    runner = CliRunner()

    result = runner.invoke(
        cli_app,
        [
            "whatif",
            "pack",
            "run",
            "--source-dir",
            str(archive_path),
            "--label",
            "generic-pack",
        ],
    )

    assert result.exit_code != 0
    assert "requires an Enron historical source" in result.output


def test_vei_whatif_cli_accepts_file_backed_research_pack_for_generic_archive(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive_path = _write_mail_archive_fixture(tmp_path / "mail_archive_cli_pack")
    pack_path = _write_research_pack_file(
        tmp_path / "pycorp_research_pack.json",
        _make_generic_research_pack(),
    )
    runner = CliRunner()

    def fake_run_research_pack(*_args: object, **kwargs: object):
        research_pack = kwargs["research_pack"]
        assert isinstance(research_pack, WhatIfResearchPack)
        assert research_pack.pack_id == "pycorp_fixture_pack"
        return type(
            "PackRunStub",
            (),
            {
                "model_dump": lambda self, mode="json": {
                    "pack": {"pack_id": research_pack.pack_id}
                }
            },
        )()

    monkeypatch.setattr(
        "vei.cli.vei_whatif.run_research_pack",
        fake_run_research_pack,
    )

    result = runner.invoke(
        cli_app,
        [
            "whatif",
            "pack",
            "run",
            "--source-dir",
            str(archive_path),
            "--label",
            "generic-pack",
            "--pack-id",
            str(pack_path),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["pack"]["pack_id"] == "pycorp_fixture_pack"


def test_vei_whatif_cli_rejects_enron_benchmark_for_generic_archive(
    tmp_path: Path,
) -> None:
    archive_path = _write_mail_archive_fixture(tmp_path / "mail_archive_cli_benchmark")
    runner = CliRunner()

    result = runner.invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "build",
            "--source-dir",
            str(archive_path),
            "--label",
            "generic-benchmark",
        ],
    )

    assert result.exit_code != 0
    assert "requires an Enron historical source" in result.output


def test_vei_whatif_cli_builds_file_backed_benchmark_for_generic_archive(
    tmp_path: Path,
) -> None:
    archive_path = _write_mail_archive_fixture(tmp_path / "mail_archive_cli_benchmark")
    pack_path = _write_research_pack_file(
        tmp_path / "pycorp_research_pack.json",
        _make_generic_research_pack(),
    )
    runner = CliRunner()

    result = runner.invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "build",
            "--source-dir",
            str(archive_path),
            "--label",
            "generic-benchmark",
            "--heldout-pack-id",
            str(pack_path),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["cases"][0]["case_id"] == "pycorp_external_hold"


def test_branch_point_benchmark_build_uses_world_domain_for_generic_archive(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive_path = _write_mail_archive_fixture(tmp_path / "mail_archive_benchmark")
    world = load_world(source="mail_archive", source_dir=archive_path)
    pack = _make_generic_research_pack()

    monkeypatch.setattr(
        "vei.whatif.benchmark.get_research_pack",
        lambda _pack_id: pack,
    )

    result = build_branch_point_benchmark(
        world,
        artifacts_root=tmp_path / "generic_benchmark_artifacts",
        label="pycorp_benchmark",
        heldout_pack_id="pycorp_fixture_pack",
    )

    heldout_rows = [
        json.loads(line)
        for line in Path(result.dataset.split_paths["heldout"])
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    contract = heldout_rows[0]["contract"]

    assert contract["action_schema"]["recipient_scope"] == "internal"
    assert contract["action_schema"]["external_recipient_count"] == 0
    assert contract["sequence_steps"][0]["recipient_scope"] == "internal"
    assert contract["sequence_steps"][0]["external_recipient_count"] == 0


def test_search_events_finds_exact_branch_points(tmp_path: Path) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    result = search_events(
        world,
        actor="jeff.skilling",
        query="draft term sheet",
        flagged_only=True,
    )

    assert result.match_count == 1
    assert result.matches[0].event.event_id == "evt-005"
    assert result.matches[0].reason_labels == ["attachment", "external_recipient"]
    assert "External draft attached for review." in result.matches[0].event.snippet


def test_search_events_matches_human_name_queries(tmp_path: Path) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)

    result = search_events(
        world,
        query="Jeff Skilling draft term sheet",
    )

    assert result.match_count == 1
    assert result.matches[0].event.event_id == "evt-005"


def test_aggregate_outcome_signals_and_rank_tiebreaker_use_simple_contract() -> None:
    branch_event = WhatIfEventReference(
        event_id="evt-005",
        timestamp="2001-05-01T10:00:04Z",
        actor_id="jeff.skilling@enron.com",
        target_id="outside@lawfirm.com",
        event_type="message",
        thread_id="thr-external",
        subject="Draft term sheet",
        to_recipients=["outside@lawfirm.com"],
        has_attachment_reference=True,
    )
    first = summarize_llm_branch(
        branch_event=branch_event,
        llm_result=_make_llm_replay_result(
            prompt="Keep this internal and pause the send.",
            to="jeff.skilling@enron.com",
            subject="Please hold for review",
            body_text="Please hold this internally for review.",
            delay_ms=1000,
            summary="Internal review path.",
        ),
    )
    second = summarize_llm_branch(
        branch_event=branch_event,
        llm_result=_make_llm_replay_result(
            prompt="Please keep this inside and clarify ownership.",
            to="sara.shackleton@enron.com",
            subject="Please confirm next steps",
            body_text="Please confirm ownership before the draft moves.",
            delay_ms=2000,
            summary="Internal ownership clarification path.",
        ),
    )

    aggregate = aggregate_outcome_signals([first, second])
    assert aggregate.internal_only is True
    assert aggregate.avg_delay_ms == 1500
    assert aggregate.outside_message_count == 0

    pack = get_objective_pack("contain_exposure")
    ranked = sort_candidates_for_rank(
        [
            (
                "Option B",
                WhatIfOutcomeSignals(
                    exposure_risk=0.3,
                    delay_risk=0.2,
                    relationship_protection=0.7,
                ),
                score_outcome_signals(
                    pack=pack,
                    outcome=WhatIfOutcomeSignals(
                        exposure_risk=0.2,
                        delay_risk=0.2,
                        relationship_protection=0.7,
                    ),
                ),
            ),
            (
                "Option A",
                WhatIfOutcomeSignals(
                    exposure_risk=0.1,
                    delay_risk=0.5,
                    relationship_protection=0.7,
                ),
                score_outcome_signals(
                    pack=pack,
                    outcome=WhatIfOutcomeSignals(
                        exposure_risk=0.2,
                        delay_risk=0.2,
                        relationship_protection=0.7,
                    ),
                ),
            ),
        ]
    )

    assert ranked == ["Option A", "Option B"]


def test_research_pack_registry_exposes_built_in_enron_pack() -> None:
    packs = list_research_packs()
    pack_ids = {pack.pack_id for pack in packs}
    built_in = get_research_pack("enron_research_v1")

    assert "enron_research_v1" in pack_ids
    assert built_in.objective_pack_ids == [
        "contain_exposure",
        "reduce_delay",
        "protect_relationship",
    ]
    assert len(built_in.rollout_seeds) == 8
    assert len(built_in.cases) == 6
    assert built_in.cases[0].event_id == "enron_bcda1b925800af8c"


def test_research_pack_registry_loads_file_backed_pack(tmp_path: Path) -> None:
    pack_path = _write_research_pack_file(
        tmp_path / "pycorp_research_pack.json",
        _make_generic_research_pack(),
    )

    pack = get_research_pack(str(pack_path))

    assert pack.pack_id == "pycorp_fixture_pack"
    assert pack.cases[0].event_id == "py-msg-002"


def test_run_research_pack_writes_artifacts_and_scores_all_backends(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    def fake_run_llm_counterfactual(
        *_: object,
        prompt: str,
        provider: str = "openai",
        model: str = "gpt-5",
        seed: int = 42042,
    ) -> WhatIfLLMReplayResult:
        assert provider == "openai"
        assert model == "gpt-5-mini"
        if "internal" in prompt.lower():
            return _make_llm_replay_result(
                prompt=prompt,
                to="jeff.skilling@enron.com",
                subject="Please hold for review",
                body_text="Please hold this internally while legal reviews it.",
                delay_ms=8_000 + (seed % 2) * 1_000,
                summary="The draft stays inside Enron during legal review.",
            )
        if "status note" in prompt.lower():
            return _make_llm_replay_result(
                prompt=prompt,
                to="outside@lawfirm.com",
                subject="Status update",
                body_text="Please expect a revised copy soon. Thanks.",
                delay_ms=900 + (seed % 2) * 200,
                summary="A short external status note goes out quickly.",
            )
        return _make_llm_replay_result(
            prompt=prompt,
            to="outside@lawfirm.com",
            subject="Draft attached urgently",
            body_text="Sending the draft attachment outside now for urgent comments.",
            delay_ms=2_200 + (seed % 2) * 400,
            summary="The draft leaves Enron with broader outside circulation.",
        )

    def fake_run_ejepa_proxy_counterfactual(
        *_: object,
        prompt: str,
    ) -> WhatIfCounterfactualEstimateResult:
        if "internal" in prompt.lower():
            return _make_forecast_result(
                prompt=prompt,
                risk_score=0.15,
                future_event_count=2,
                future_external_event_count=0,
                summary="Proxy forecast prefers the internal legal hold.",
            )
        if "status note" in prompt.lower():
            return _make_forecast_result(
                prompt=prompt,
                risk_score=0.42,
                future_event_count=1,
                future_external_event_count=1,
                summary="Proxy forecast sees one controlled outside status touch.",
            )
        return _make_forecast_result(
            prompt=prompt,
            risk_score=0.9,
            future_event_count=3,
            future_external_event_count=2,
            summary="Proxy forecast expects broad outside circulation.",
        )

    def fake_run_ejepa_counterfactual(
        *_: object,
        prompt: str,
        **__: object,
    ) -> WhatIfCounterfactualEstimateResult:
        if "status note" in prompt.lower():
            return WhatIfCounterfactualEstimateResult(
                status="error",
                backend="e_jepa",
                prompt=prompt,
                summary="Real E-JEPA backend could not train on this tiny fixture.",
                error="fixture training error",
                notes=["Forced test fallback."],
            )
        if "internal" in prompt.lower():
            return WhatIfCounterfactualEstimateResult(
                status="ok",
                backend="e_jepa",
                prompt=prompt,
                summary="Real E-JEPA backend prefers the internal hold.",
                baseline=WhatIfHistoricalScore(
                    backend="historical",
                    future_event_count=2,
                    future_external_event_count=1,
                    risk_score=0.6,
                ),
                predicted=WhatIfHistoricalScore(
                    backend="e_jepa",
                    future_event_count=2,
                    future_external_event_count=0,
                    risk_score=0.18,
                ),
                delta=WhatIfCounterfactualEstimateDelta(
                    risk_score_delta=-0.42,
                    future_event_delta=0,
                    external_event_delta=-1,
                ),
                notes=["Real E-JEPA path used in the fixture test."],
            )
        return WhatIfCounterfactualEstimateResult(
            status="ok",
            backend="e_jepa",
            prompt=prompt,
            summary="Real E-JEPA backend dislikes the broad outside send.",
            baseline=WhatIfHistoricalScore(
                backend="historical",
                future_event_count=2,
                future_external_event_count=1,
                risk_score=0.6,
            ),
            predicted=WhatIfHistoricalScore(
                backend="e_jepa",
                future_event_count=3,
                future_external_event_count=2,
                risk_score=0.92,
            ),
            delta=WhatIfCounterfactualEstimateDelta(
                risk_score_delta=0.32,
                future_event_delta=1,
                external_event_delta=1,
            ),
            notes=["Real E-JEPA path used in the fixture test."],
        )

    monkeypatch.setattr(
        "vei.whatif.research.run_llm_counterfactual",
        fake_run_llm_counterfactual,
    )
    monkeypatch.setattr(
        "vei.whatif.research.estimate_counterfactual_delta",
        fake_run_ejepa_proxy_counterfactual,
    )
    monkeypatch.setattr(
        "vei.whatif.research.run_ejepa_counterfactual",
        fake_run_ejepa_counterfactual,
    )

    result = run_research_pack(
        world,
        artifacts_root=tmp_path / "research_artifacts",
        label="fixture_pack_run",
        research_pack=pack,
        provider="openai",
        model="gpt-5-mini",
    )
    loaded = load_research_pack_run_result(result.artifacts.root)

    assert result.pack.pack_id == "fixture_pack"
    assert result.dataset.heldout_thread_ids == ["thr-external"]
    assert result.dataset.historical_row_count == 1
    assert result.dataset.evaluation_row_count == 1
    assert result.hypothesis_pass_count == 3
    assert result.hypothesis_total_count == 3
    assert result.hypothesis_pass_rate == 1.0
    assert result.artifacts.result_json_path.exists()
    assert result.artifacts.overview_markdown_path.exists()
    assert result.artifacts.pilot_markdown_path.exists()
    assert loaded.pack.pack_id == "fixture_pack"

    objective_results = result.cases[0].objectives
    contain_exposure = next(
        objective
        for objective in objective_results
        if objective.objective_pack.pack_id == "contain_exposure"
    )
    reduce_delay = next(
        objective
        for objective in objective_results
        if objective.objective_pack.pack_id == "reduce_delay"
    )
    protect_relationship = next(
        objective
        for objective in objective_results
        if objective.objective_pack.pack_id == "protect_relationship"
    )

    assert contain_exposure.recommended_candidate_label == "Legal hold internal"
    assert reduce_delay.recommended_candidate_label == "Narrow external status"
    assert protect_relationship.recommended_candidate_label == "Narrow external status"

    for objective in objective_results:
        assert objective.expected_order_ok is True
        for candidate in objective.candidates:
            backend_ids = {score.backend for score in candidate.backend_scores}
            assert backend_ids == {
                "e_jepa",
                "e_jepa_proxy",
                "ft_transformer",
                "ts2vec",
                "g_transformer",
            }

    fallback_candidate = next(
        candidate
        for candidate in contain_exposure.candidates
        if candidate.candidate.candidate_id == "narrow_external_status"
    )
    fallback_score = next(
        score
        for score in fallback_candidate.backend_scores
        if score.backend == "e_jepa"
    )
    assert fallback_score.status == "fallback"
    assert fallback_score.effective_backend == "e_jepa_proxy"

    train_rows = []
    for split_name in ("train", "validation", "test"):
        split_path = Path(result.dataset.split_paths[split_name])
        if not split_path.exists():
            continue
        train_rows.extend(
            [
                json.loads(line)
                for line in split_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        )
    assert train_rows
    assert all(row["thread_id"] != "thr-external" for row in train_rows)

    scoreboard = result.artifacts.overview_markdown_path.read_text(encoding="utf-8")
    assert "Fixture Pack" in scoreboard
    assert "Hypothesis pass rate: 1.000 (3/3)" in scoreboard


def test_run_research_pack_uses_world_domain_for_generic_archive(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive_path = _write_mail_archive_fixture(tmp_path / "mail_archive_research")
    world = load_world(source="mail_archive", source_dir=archive_path)
    pack = _make_generic_research_pack()

    def fake_run_llm_counterfactual(
        *_: object,
        prompt: str,
        provider: str = "openai",
        model: str = "gpt-5",
        seed: int = 42042,
    ) -> WhatIfLLMReplayResult:
        assert provider == "openai"
        assert model == "gpt-5-mini"
        if "internal" in prompt.lower():
            return _make_llm_replay_result(
                prompt=prompt,
                to="legal@pycorp.example.com",
                subject="Re: Pricing addendum",
                body_text="Keep this internal until legal signs off.",
                delay_ms=1_000 + (seed % 2) * 100,
                summary="The draft stays inside Py Corp.",
            )
        return _make_llm_replay_result(
            prompt=prompt,
            to="partner@redwoodcapital.com",
            subject="Pricing addendum",
            body_text="Sending a short status note outside.",
            delay_ms=2_000 + (seed % 2) * 100,
            summary="A short outside status note goes out.",
        )

    def fake_run_ejepa_proxy_counterfactual(
        *_: object,
        prompt: str,
    ) -> WhatIfCounterfactualEstimateResult:
        if "internal" in prompt.lower():
            return _make_forecast_result(
                prompt=prompt,
                risk_score=0.2,
                future_event_count=2,
                future_external_event_count=0,
                summary="Proxy forecast prefers the internal hold.",
            )
        return _make_forecast_result(
            prompt=prompt,
            risk_score=0.5,
            future_event_count=1,
            future_external_event_count=1,
            summary="Proxy forecast allows one outside status note.",
        )

    def fake_run_ejepa_counterfactual(
        *_: object,
        prompt: str,
        **__: object,
    ) -> WhatIfCounterfactualEstimateResult:
        if "internal" in prompt.lower():
            return _make_forecast_result(
                prompt=prompt,
                risk_score=0.18,
                future_event_count=2,
                future_external_event_count=0,
                summary="Generic E-JEPA prefers the internal hold.",
            ).model_copy(update={"backend": "e_jepa"})
        return _make_forecast_result(
            prompt=prompt,
            risk_score=0.48,
            future_event_count=1,
            future_external_event_count=1,
            summary="Generic E-JEPA accepts one outside status note.",
        ).model_copy(update={"backend": "e_jepa"})

    monkeypatch.setattr(
        "vei.whatif.research.run_llm_counterfactual",
        fake_run_llm_counterfactual,
    )
    monkeypatch.setattr(
        "vei.whatif.research.estimate_counterfactual_delta",
        fake_run_ejepa_proxy_counterfactual,
    )
    monkeypatch.setattr(
        "vei.whatif.research.run_ejepa_counterfactual",
        fake_run_ejepa_counterfactual,
    )

    result = run_research_pack(
        world,
        artifacts_root=tmp_path / "generic_research_artifacts",
        label="pycorp_fixture_pack_run",
        research_pack=pack,
        provider="openai",
        model="gpt-5-mini",
    )

    contract_path = (
        result.artifacts.root
        / "cases"
        / "pycorp_external_hold"
        / "candidates"
        / "internal_hold_contract.json"
    )
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    generated_steps = [
        step for step in contract["sequence_steps"] if step["phase"] == "generated"
    ]
    branch_external = next(
        feature["value"]
        for feature in contract["summary_features"]
        if feature["name"] == "branch_external_count"
    )

    assert generated_steps[0]["recipient_scope"] == "internal"
    assert generated_steps[0]["external_recipient_count"] == 0
    assert branch_external == 0.0


def test_run_research_pack_reuses_completed_case_results(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    def fake_run_llm_counterfactual(
        *_: object,
        prompt: str,
        **__: object,
    ) -> WhatIfLLMReplayResult:
        return _make_llm_replay_result(
            prompt=prompt,
            to="jeff.skilling@enron.com",
            subject="Please hold for review",
            body_text="Please hold this internally while legal reviews it.",
            delay_ms=8_000,
            summary="The draft stays inside Enron during legal review.",
        )

    def fake_run_forecast(
        *_: object,
        prompt: str,
        **__: object,
    ) -> WhatIfCounterfactualEstimateResult:
        return _make_forecast_result(
            prompt=prompt,
            risk_score=0.15,
            future_event_count=2,
            future_external_event_count=0,
            summary="Fixture forecast completed.",
        )

    monkeypatch.setattr(
        "vei.whatif.research.run_llm_counterfactual",
        fake_run_llm_counterfactual,
    )
    monkeypatch.setattr(
        "vei.whatif.research.estimate_counterfactual_delta",
        fake_run_forecast,
    )
    monkeypatch.setattr(
        "vei.whatif.research.run_ejepa_counterfactual",
        fake_run_forecast,
    )

    first_result = run_research_pack(
        world,
        artifacts_root=tmp_path / "research_artifacts",
        label="fixture_pack_resume",
        research_pack=pack,
        rollout_workers=2,
    )

    monkeypatch.setattr(
        "vei.whatif.research.run_llm_counterfactual",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("cached case should skip rollout regeneration")
        ),
    )
    monkeypatch.setattr(
        "vei.whatif.research.estimate_counterfactual_delta",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("cached case should skip proxy scoring")
        ),
    )
    monkeypatch.setattr(
        "vei.whatif.research.run_ejepa_counterfactual",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("cached case should skip JEPA scoring")
        ),
    )

    second_result = run_research_pack(
        world,
        artifacts_root=tmp_path / "research_artifacts",
        label="fixture_pack_resume",
        research_pack=pack,
        rollout_workers=2,
    )

    assert second_result.cases[0].case.case_id == first_result.cases[0].case.case_id
    assert second_result.hypothesis_pass_count == first_result.hypothesis_pass_count
    assert second_result.hypothesis_total_count == first_result.hypothesis_total_count
    assert (
        second_result.artifacts.root
        / "cases"
        / pack.cases[0].case_id
        / "case_result.json"
    ).exists()


def test_vei_whatif_cli_pack_run_list_and_show(tmp_path: Path, monkeypatch) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    monkeypatch.setattr(
        "vei.whatif.research.run_llm_counterfactual",
        lambda *_args, prompt, **_kwargs: _make_llm_replay_result(
            prompt=prompt,
            to="jeff.skilling@enron.com",
            subject="Please hold for review",
            body_text="Please hold this internally while legal reviews it.",
            delay_ms=8_000,
            summary="The draft stays inside Enron during legal review.",
        ),
    )
    monkeypatch.setattr(
        "vei.whatif.research.estimate_counterfactual_delta",
        lambda *_args, prompt, **_kwargs: _make_forecast_result(
            prompt=prompt,
            risk_score=0.15,
            future_event_count=2,
            future_external_event_count=0,
            summary="Proxy forecast completed.",
        ),
    )
    monkeypatch.setattr(
        "vei.whatif.research.run_ejepa_counterfactual",
        lambda *_args, prompt, **_kwargs: WhatIfCounterfactualEstimateResult(
            status="ok",
            backend="e_jepa",
            prompt=prompt,
            summary="Real E-JEPA fixture forecast completed.",
            baseline=WhatIfHistoricalScore(
                backend="historical",
                future_event_count=2,
                future_external_event_count=1,
                risk_score=0.6,
            ),
            predicted=WhatIfHistoricalScore(
                backend="e_jepa",
                future_event_count=2,
                future_external_event_count=0,
                risk_score=0.2,
            ),
            delta=WhatIfCounterfactualEstimateDelta(
                risk_score_delta=-0.4,
                future_event_delta=0,
                external_event_delta=-1,
            ),
        ),
    )

    precomputed = run_research_pack(
        world,
        artifacts_root=tmp_path / "research_artifacts",
        label="fixture_pack_run",
        research_pack=pack,
    )

    runner = CliRunner()
    list_result = runner.invoke(
        cli_app,
        ["whatif", "pack", "list", "--format", "markdown"],
    )
    assert list_result.exit_code == 0, list_result.output
    assert "enron_research_v1" in list_result.output

    monkeypatch.setattr(
        "vei.cli.vei_whatif.get_research_pack",
        lambda _pack_id: pack,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.run_research_pack",
        lambda *args, **kwargs: precomputed,
    )

    run_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "pack",
            "run",
            "--rosetta-dir",
            str(rosetta_dir),
            "--artifacts-root",
            str(tmp_path / "cli_artifacts"),
            "--label",
            "fixture_cli",
            "--pack-id",
            "fixture_pack",
        ],
    )
    assert run_result.exit_code == 0, run_result.output
    run_payload = json.loads(run_result.output)
    assert run_payload["pack"]["pack_id"] == "fixture_pack"
    assert run_payload["cases"][0]["case"]["case_id"] == "external_hold_case"

    show_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "pack",
            "show",
            "--root",
            str(precomputed.artifacts.root),
            "--format",
            "markdown",
        ],
    )
    assert show_result.exit_code == 0, show_result.output
    assert "Fixture Pack" in show_result.output
    assert "Hypothesis pass rate" in show_result.output


def test_branch_point_benchmark_build_writes_prebranch_dataset_and_dossiers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    monkeypatch.setattr(
        "vei.whatif.benchmark.get_research_pack",
        lambda _pack_id: pack,
    )

    result = build_branch_point_benchmark(
        world,
        artifacts_root=tmp_path / "benchmark_artifacts",
        label="fixture_benchmark",
        heldout_pack_id="fixture_pack",
    )
    loaded = load_branch_point_benchmark_build_result(result.artifacts.root)

    assert list_branch_point_benchmark_models() == [
        "jepa_latent",
        "full_context_transformer",
        "ft_transformer",
        "sequence_transformer",
        "treatment_transformer",
    ]
    assert result.dataset.split_row_counts == {
        "train": 1,
        "validation": 0,
        "test": 0,
        "heldout": 1,
    }
    assert loaded.cases[0].case_id == "external_hold_case"
    assert result.artifacts.heldout_cases_path.exists()
    assert result.artifacts.judge_template_path.exists()
    assert result.artifacts.audit_template_path.exists()
    assert (
        result.artifacts.dossier_root
        / "external_hold_case"
        / "minimize_enterprise_risk.md"
    ).exists()

    train_rows = [
        json.loads(line)
        for line in Path(result.dataset.split_paths["train"])
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    heldout_rows = [
        json.loads(line)
        for line in Path(result.dataset.split_paths["heldout"])
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    assert train_rows[0]["thread_id"] == "thr-legal-trading"
    assert "observed_evidence_heads" in train_rows[0]
    assert "observed_business_outcomes" in train_rows[0]
    assert all(
        not feature["name"].startswith("branch_")
        for feature in train_rows[0]["contract"]["summary_features"]
    )
    assert all(
        step["phase"] == "history"
        for step in train_rows[0]["contract"]["sequence_steps"]
    )
    assert heldout_rows[0]["contract"]["case_id"] == "external_hold_case"
    assert heldout_rows[0]["contract"]["summary_features"]
    assert set(loaded.cases[0].objective_dossier_paths) == {
        pack.pack_id for pack in list_business_objective_packs()
    }
    assert "Legal hold internal" in (
        result.artifacts.dossier_root
        / "external_hold_case"
        / "minimize_enterprise_risk.md"
    ).read_text(encoding="utf-8")
    judged_template = json.loads(
        result.artifacts.judge_template_path.read_text(encoding="utf-8")
    )
    assert len(judged_template) == len(list_business_objective_packs())
    assert judged_template[0]["objective_pack_id"] == "minimize_enterprise_risk"


def test_business_evidence_and_objective_packs_use_future_email_evidence(
    tmp_path: Path,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    branch_event = next(event for event in world.events if event.event_id == "evt-001")
    future_events = [
        event
        for event in world.events
        if event.thread_id == branch_event.thread_id
        and event.timestamp_ms > branch_event.timestamp_ms
    ]

    evidence = summarize_observed_evidence(
        branch_event=branch_event,
        future_events=future_events,
    )
    business = evidence_to_business_outcomes(evidence)
    risk_pack = get_business_objective_pack("minimize_enterprise_risk")
    velocity_pack = get_business_objective_pack("maintain_execution_velocity")
    risk_score = score_business_objective(
        pack=risk_pack,
        outcomes=business,
        evidence=evidence,
    )
    velocity_score = score_business_objective(
        pack=velocity_pack,
        outcomes=business,
        evidence=evidence,
    )

    assert evidence.participant_fanout >= 2
    assert evidence.review_loop_count >= 1
    assert evidence.time_to_first_follow_up_ms > 0
    assert 0.0 <= business.enterprise_risk <= 1.0
    assert 0.0 <= business.execution_drag <= 1.0
    assert risk_score.overall_score != velocity_score.overall_score


def test_branch_point_benchmark_judge_writes_rankings_and_audit_queue(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    monkeypatch.setattr(
        "vei.whatif.benchmark.get_research_pack",
        lambda _pack_id: pack,
    )
    build = build_branch_point_benchmark(
        world,
        artifacts_root=tmp_path / "benchmark_artifacts",
        label="fixture_benchmark_judge",
        heldout_pack_id="fixture_pack",
    )

    def fake_judge_prompt(*_args, **_kwargs) -> str:
        return json.dumps(
            {
                "pairwise_comparisons": [
                    {
                        "left_candidate_id": "legal_hold_internal",
                        "right_candidate_id": "narrow_external_status",
                        "preferred_candidate_id": "legal_hold_internal",
                        "confidence": 0.45,
                        "evidence_references": ["inside Enron only"],
                        "rationale": "keep the material inside",
                    },
                    {
                        "left_candidate_id": "legal_hold_internal",
                        "right_candidate_id": "broad_external_send",
                        "preferred_candidate_id": "legal_hold_internal",
                        "confidence": 0.45,
                        "evidence_references": ["broad outside send raises spread"],
                        "rationale": "narrower spread",
                    },
                    {
                        "left_candidate_id": "narrow_external_status",
                        "right_candidate_id": "broad_external_send",
                        "preferred_candidate_id": "narrow_external_status",
                        "confidence": 0.45,
                        "evidence_references": ["status note avoids the draft"],
                        "rationale": "smaller external footprint",
                    },
                ],
                "confidence": 0.45,
                "evidence_references": ["thread stays narrower"],
                "notes": "low confidence on purpose",
            }
        )

    monkeypatch.setattr(
        "vei.whatif.benchmark.run_llm_judge_prompt",
        fake_judge_prompt,
    )

    result = judge_branch_point_benchmark(build.artifacts.root)
    loaded = load_branch_point_benchmark_judge_result(build.artifacts.root)

    assert len(result.judgments) == len(list_business_objective_packs())
    assert len(result.audit_queue) == len(result.judgments)
    assert result.artifacts.result_path.exists()
    assert result.artifacts.audit_queue_path.exists()
    assert loaded.judgments[0].ordered_candidate_ids[0] == "legal_hold_internal"
    assert loaded.audit_queue[0].status == "pending"


def test_branch_point_benchmark_train_and_eval_round_trip_with_stub_runtime(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    monkeypatch.setattr(
        "vei.whatif.benchmark.get_research_pack",
        lambda _pack_id: pack,
    )
    build = build_branch_point_benchmark(
        world,
        artifacts_root=tmp_path / "benchmark_artifacts",
        label="fixture_benchmark_eval",
        heldout_pack_id="fixture_pack",
    )

    def fake_train_runtime(**kwargs) -> WhatIfBenchmarkTrainResult:
        model_root = build.artifacts.root / "model_runs" / kwargs["model_id"]
        model_root.mkdir(parents=True, exist_ok=True)
        result = WhatIfBenchmarkTrainResult(
            model_id=kwargs["model_id"],
            dataset_root=build.dataset.root,
            train_loss=0.21,
            validation_loss=0.29,
            epoch_count=kwargs["epochs"],
            train_row_count=1,
            validation_row_count=0,
            notes=["stub runtime"],
            artifacts=WhatIfBenchmarkTrainArtifacts(
                root=model_root,
                model_path=model_root / "model.pt",
                metadata_path=model_root / "metadata.json",
                train_result_path=model_root / "train_result.json",
            ),
        )
        result.artifacts.model_path.write_text("stub", encoding="utf-8")
        result.artifacts.metadata_path.write_text("{}", encoding="utf-8")
        result.artifacts.train_result_path.write_text(
            result.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return result

    def fake_eval_runtime(**kwargs) -> WhatIfBenchmarkEvalResult:
        model_root = build.artifacts.root / "model_runs" / kwargs["model_id"]
        model_root.mkdir(parents=True, exist_ok=True)
        case = build.cases[0]
        objective_results: list[WhatIfCounterfactualObjectiveEvaluation] = []
        for objective_pack in list_business_objective_packs():
            candidate_predictions: list[WhatIfCounterfactualCandidatePrediction] = []
            for index, candidate in enumerate(case.candidates, start=1):
                evidence = WhatIfObservedEvidenceHeads(
                    outside_recipient_count=index - 1,
                    outside_forward_count=max(0, index - 2),
                    outside_attachment_spread_count=max(0, index - 2),
                    legal_follow_up_count=1 if index == 1 else 0,
                    review_loop_count=index,
                    markup_loop_count=max(0, index - 1),
                    executive_escalation_count=max(0, index - 2),
                    executive_mention_count=max(0, index - 2),
                    urgency_spike_count=max(0, index - 1),
                    participant_fanout=index + 1,
                    cc_expansion_count=index - 1,
                    cross_functional_loop_count=max(0, index - 1),
                    time_to_first_follow_up_ms=500 * index,
                    time_to_thread_end_ms=1_000 * index,
                    review_delay_burden_ms=750 * index,
                    reassurance_count=3 - index,
                    apology_repair_count=max(0, 2 - index),
                    commitment_clarity_count=index,
                    blame_pressure_count=max(0, index - 2),
                    internal_disagreement_count=max(0, index - 2),
                    attachment_recirculation_count=max(0, index - 2),
                    version_turn_count=index - 1,
                )
                business = evidence_to_business_outcomes(evidence)
                candidate_predictions.append(
                    WhatIfCounterfactualCandidatePrediction(
                        candidate=candidate,
                        expected_hypothesis=candidate.expected_hypotheses.get(
                            objective_pack.pack_id,
                            "middle_expected",
                        ),
                        rank=index,
                        predicted_evidence_heads=evidence,
                        predicted_business_outcomes=business,
                        predicted_objective_score=score_business_objective(
                            pack=objective_pack,
                            outcomes=business,
                            evidence=evidence,
                        ),
                    )
                )
            objective_results.append(
                WhatIfCounterfactualObjectiveEvaluation(
                    objective_pack=objective_pack,
                    recommended_candidate_label=candidate_predictions[
                        0
                    ].candidate.label,
                    candidates=candidate_predictions,
                    expected_order_ok=True,
                )
            )
        result = WhatIfBenchmarkEvalResult(
            model_id=kwargs["model_id"],
            dataset_root=build.dataset.root,
            observed_metrics=WhatIfObservedForecastMetrics(
                auroc_any_external_spread=0.82,
                brier_any_external_spread=0.11,
                calibration_error_any_external_spread=0.07,
                evidence_head_mae={
                    "outside_recipient_count": 0.4,
                    "review_loop_count": 0.6,
                },
                business_head_mae={
                    "enterprise_risk": 0.18,
                    "execution_drag": 0.12,
                },
                objective_score_mae={"minimize_enterprise_risk": 0.09},
            ),
            cases=[
                WhatIfBenchmarkCaseEvaluation(
                    case=case,
                    objectives=objective_results,
                )
            ],
            artifacts=WhatIfBenchmarkEvalArtifacts(
                root=model_root,
                eval_result_path=model_root / "eval_result.json",
                prediction_jsonl_path=model_root / "predictions.jsonl",
            ),
        )
        result.artifacts.prediction_jsonl_path.write_text("", encoding="utf-8")
        result.artifacts.eval_result_path.write_text(
            result.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return result

    monkeypatch.setattr(
        "vei.whatif.benchmark.run_branch_point_benchmark_training",
        fake_train_runtime,
    )
    monkeypatch.setattr(
        "vei.whatif.benchmark.run_branch_point_benchmark_evaluation",
        fake_eval_runtime,
    )

    train_result = train_branch_point_benchmark_model(
        build.artifacts.root,
        model_id="jepa_latent",
        epochs=5,
    )
    assert train_result.validation_loss == 0.29
    loaded_train = load_branch_point_benchmark_train_result(
        build.artifacts.root,
        model_id="jepa_latent",
    )
    assert loaded_train.model_id == "jepa_latent"

    def fake_judge_prompt(*_args, **_kwargs) -> str:
        return json.dumps(
            {
                "pairwise_comparisons": [
                    {
                        "left_candidate_id": "legal_hold_internal",
                        "right_candidate_id": "narrow_external_status",
                        "preferred_candidate_id": "legal_hold_internal",
                        "confidence": 0.45,
                        "evidence_references": ["keep the draft inside"],
                        "rationale": "narrower risk surface",
                    },
                    {
                        "left_candidate_id": "legal_hold_internal",
                        "right_candidate_id": "broad_external_send",
                        "preferred_candidate_id": "legal_hold_internal",
                        "confidence": 0.45,
                        "evidence_references": ["broad send raises spread"],
                        "rationale": "more control",
                    },
                    {
                        "left_candidate_id": "narrow_external_status",
                        "right_candidate_id": "broad_external_send",
                        "preferred_candidate_id": "narrow_external_status",
                        "confidence": 0.45,
                        "evidence_references": ["status note avoids attachment spread"],
                        "rationale": "lighter external touch",
                    },
                ],
                "confidence": 0.45,
                "evidence_references": ["keep the scope narrow"],
                "notes": "fixture judgment",
            }
        )

    monkeypatch.setattr(
        "vei.whatif.benchmark.run_llm_judge_prompt",
        fake_judge_prompt,
    )
    judge_result = judge_branch_point_benchmark(build.artifacts.root)
    audit_records = [
        WhatIfAuditRecord(
            case_id=item.case_id,
            objective_pack_id=item.objective_pack_id,
            status="completed",
            ordered_candidate_ids=item.ordered_candidate_ids,
            reviewer_id="auditor-1",
        )
        for item in judge_result.audit_queue
    ]
    audit_path = tmp_path / "audit_records.json"
    audit_path.write_text(
        json.dumps([item.model_dump(mode="json") for item in audit_records], indent=2),
        encoding="utf-8",
    )

    eval_result = evaluate_branch_point_benchmark_model(
        build.artifacts.root,
        model_id="jepa_latent",
        judged_rankings_path=judge_result.artifacts.result_path,
        audit_records_path=audit_path,
    )
    loaded_eval = load_branch_point_benchmark_eval_result(
        build.artifacts.root,
        model_id="jepa_latent",
    )

    assert eval_result.dominance_summary.pass_rate == 1.0
    assert eval_result.judge_summary.available is True
    assert eval_result.judge_summary.judgment_count == len(
        list_business_objective_packs()
    )
    assert eval_result.audit_summary.available is True
    assert eval_result.audit_summary.completed_count == len(audit_records)
    assert loaded_eval.model_id == "jepa_latent"
    assert eval_result.artifacts.eval_result_path.exists()


def test_branch_point_benchmark_study_writes_seeded_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    monkeypatch.setattr(
        "vei.whatif.benchmark.get_research_pack",
        lambda _pack_id: pack,
    )
    build = build_branch_point_benchmark(
        world,
        artifacts_root=tmp_path / "benchmark_artifacts",
        label="fixture_benchmark_study",
        heldout_pack_id="fixture_pack",
    )

    def fake_train_runtime(**kwargs) -> WhatIfBenchmarkTrainResult:
        model_root = Path(kwargs["output_root"])
        model_root.mkdir(parents=True, exist_ok=True)
        seed = int(kwargs["seed"])
        model_bonus = 0.05 if kwargs["model_id"] == "full_context_transformer" else 0.0
        result = WhatIfBenchmarkTrainResult(
            model_id=kwargs["model_id"],
            dataset_root=build.dataset.root,
            train_loss=round(0.30 - model_bonus + ((seed - 42042) * 0.01), 3),
            validation_loss=round(0.40 - model_bonus + ((seed - 42042) * 0.01), 3),
            epoch_count=kwargs["epochs"],
            train_row_count=1,
            validation_row_count=0,
            notes=[f"seed={seed}"],
            artifacts=WhatIfBenchmarkTrainArtifacts(
                root=model_root,
                model_path=model_root / "model.pt",
                metadata_path=model_root / "metadata.json",
                train_result_path=model_root / "train_result.json",
            ),
        )
        result.artifacts.model_path.write_text("stub", encoding="utf-8")
        result.artifacts.metadata_path.write_text("{}", encoding="utf-8")
        result.artifacts.train_result_path.write_text(
            result.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return result

    def fake_eval_runtime(**kwargs) -> WhatIfBenchmarkEvalResult:
        model_root = Path(kwargs["output_root"])
        model_root.mkdir(parents=True, exist_ok=True)
        case = build.cases[0]
        expected_order_ok = kwargs["model_id"] == "full_context_transformer"
        objective_results: list[WhatIfCounterfactualObjectiveEvaluation] = []
        for objective_pack in list_business_objective_packs():
            candidate_predictions: list[WhatIfCounterfactualCandidatePrediction] = []
            for index, candidate in enumerate(case.candidates, start=1):
                evidence = WhatIfObservedEvidenceHeads(
                    outside_recipient_count=index - 1,
                    review_loop_count=index,
                    participant_fanout=index + 1,
                    time_to_first_follow_up_ms=500 * index,
                    time_to_thread_end_ms=1_000 * index,
                    review_delay_burden_ms=750 * index,
                    commitment_clarity_count=index,
                )
                business = evidence_to_business_outcomes(evidence)
                candidate_predictions.append(
                    WhatIfCounterfactualCandidatePrediction(
                        candidate=candidate,
                        rank=index,
                        predicted_evidence_heads=evidence,
                        predicted_business_outcomes=business,
                        predicted_objective_score=score_business_objective(
                            pack=objective_pack,
                            outcomes=business,
                            evidence=evidence,
                        ),
                    )
                )
            objective_results.append(
                WhatIfCounterfactualObjectiveEvaluation(
                    objective_pack=objective_pack,
                    recommended_candidate_label=candidate_predictions[
                        0
                    ].candidate.label,
                    candidates=candidate_predictions,
                    expected_order_ok=expected_order_ok,
                )
            )
        result = WhatIfBenchmarkEvalResult(
            model_id=kwargs["model_id"],
            dataset_root=build.dataset.root,
            observed_metrics=WhatIfObservedForecastMetrics(
                auroc_any_external_spread=(
                    0.74 if kwargs["model_id"] == "full_context_transformer" else 0.68
                ),
                brier_any_external_spread=0.11,
                calibration_error_any_external_spread=0.07,
                business_head_mae={
                    "enterprise_risk": (
                        0.12
                        if kwargs["model_id"] == "full_context_transformer"
                        else 0.18
                    )
                },
            ),
            cases=[
                WhatIfBenchmarkCaseEvaluation(
                    case=case,
                    objectives=objective_results,
                )
            ],
            artifacts=WhatIfBenchmarkEvalArtifacts(
                root=model_root,
                eval_result_path=model_root / "eval_result.json",
                prediction_jsonl_path=model_root / "predictions.jsonl",
            ),
        )
        result.artifacts.prediction_jsonl_path.write_text("", encoding="utf-8")
        result.artifacts.eval_result_path.write_text(
            result.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return result

    monkeypatch.setattr(
        "vei.whatif.benchmark.run_branch_point_benchmark_training",
        fake_train_runtime,
    )
    monkeypatch.setattr(
        "vei.whatif.benchmark.run_branch_point_benchmark_evaluation",
        fake_eval_runtime,
    )

    result = run_branch_point_benchmark_study(
        build.artifacts.root,
        label="matched_input_fixture",
        model_ids=["jepa_latent", "full_context_transformer"],
        seeds=[42042, 42043],
        epochs=3,
    )
    loaded = load_branch_point_benchmark_study_result(result.artifacts.root)

    assert result.artifacts.result_path.exists()
    assert result.artifacts.overview_path.exists()
    assert len(result.runs) == 4
    assert result.ranked_model_ids[0] == "full_context_transformer"
    assert loaded.label == "matched_input_fixture"
    assert loaded.summaries[0].run_count == 2


def test_jepa_benchmark_model_uses_prebranch_sequence_signal() -> None:
    torch = pytest.importorskip("torch")
    torch.manual_seed(42042)
    preprocessor = _BenchmarkPreprocessor(
        summary_feature_names=["history_event_count"],
        summary_mean=[0.0],
        summary_std=[1.0],
        action_tag_names=["hold"],
        event_type_names=["__summary__", "message"],
        target_mean=[0.0] * len(_EVIDENCE_TARGET_NAMES),
        target_std=[1.0] * len(_EVIDENCE_TARGET_NAMES),
    )
    trainer = _TorchTrainer(model_id="jepa_latent", preprocessor=preprocessor)
    model = trainer.build_model(device="cpu")
    model.eval()

    summary = torch.zeros((1, 1), dtype=torch.float32)
    action = torch.zeros(
        (1, _action_vector_width(preprocessor)),
        dtype=torch.float32,
    )
    base_categorical = torch.zeros(
        (1, _SEQUENCE_TOKEN_LIMIT, 3),
        dtype=torch.long,
    )
    base_numeric = torch.zeros(
        (1, _SEQUENCE_TOKEN_LIMIT, _SEQUENCE_NUMERIC_WIDTH),
        dtype=torch.float32,
    )
    changed_categorical = base_categorical.clone()
    changed_numeric = base_numeric.clone()
    changed_categorical[0, 0, 0] = 1
    changed_categorical[0, 0, 1] = 1
    changed_categorical[0, 0, 2] = 1
    changed_numeric[0, 0, 0] = 2.0
    changed_numeric[0, 0, 1] = 1.0
    changed_numeric[0, 0, 3] = 1.0
    changed_numeric[0, 1, 10] = 1.0

    with torch.no_grad():
        base_outputs = model(summary, action, base_categorical, base_numeric)
        changed_outputs = model(
            summary,
            action,
            changed_categorical,
            changed_numeric,
        )

    assert (
        torch.abs(
            base_outputs["binary_logits"] - changed_outputs["binary_logits"]
        ).max()
        > 1e-6
    )
    assert (
        torch.abs(base_outputs["regression"] - changed_outputs["regression"]).max()
        > 1e-6
    )


def test_full_context_transformer_uses_summary_action_and_sequence_signal() -> None:
    torch = pytest.importorskip("torch")
    torch.manual_seed(42042)
    preprocessor = _BenchmarkPreprocessor(
        summary_feature_names=["history_event_count"],
        summary_mean=[0.0],
        summary_std=[1.0],
        action_tag_names=["hold"],
        event_type_names=["__summary__", "message"],
        target_mean=[0.0] * len(_EVIDENCE_TARGET_NAMES),
        target_std=[1.0] * len(_EVIDENCE_TARGET_NAMES),
    )
    trainer = _TorchTrainer(
        model_id="full_context_transformer",
        preprocessor=preprocessor,
    )
    model = trainer.build_model(device="cpu")
    model.eval()

    summary = torch.zeros((1, 1), dtype=torch.float32)
    action = torch.zeros(
        (1, _action_vector_width(preprocessor)),
        dtype=torch.float32,
    )
    token_categorical = torch.zeros(
        (1, _SEQUENCE_TOKEN_LIMIT, 3),
        dtype=torch.long,
    )
    token_numeric = torch.zeros(
        (1, _SEQUENCE_TOKEN_LIMIT, _SEQUENCE_NUMERIC_WIDTH),
        dtype=torch.float32,
    )

    changed_summary = summary.clone()
    changed_summary[0, 0] = 3.0
    changed_action = action.clone()
    changed_action[0, 0] = 1.0
    changed_categorical = token_categorical.clone()
    changed_numeric = token_numeric.clone()
    changed_categorical[0, 0, 0] = 1
    changed_categorical[0, 0, 1] = 1
    changed_categorical[0, 0, 2] = 1
    changed_numeric[0, 0, 0] = 2.0

    with torch.no_grad():
        base_outputs = model(summary, action, token_categorical, token_numeric)
        changed_summary_outputs = model(
            changed_summary,
            changed_action,
            token_categorical,
            token_numeric,
        )
        changed_sequence_outputs = model(
            summary,
            action,
            changed_categorical,
            changed_numeric,
        )

    assert (
        torch.abs(
            base_outputs["binary_logits"] - changed_summary_outputs["binary_logits"]
        ).max()
        > 1e-6
    )
    assert (
        torch.abs(
            base_outputs["binary_logits"] - changed_sequence_outputs["binary_logits"]
        ).max()
        > 1e-6
    )


def test_benchmark_preprocessor_decode_targets_clamps_overflow() -> None:
    preprocessor = _BenchmarkPreprocessor(
        summary_feature_names=["history_event_count"],
        summary_mean=[0.0],
        summary_std=[1.0],
        action_tag_names=["hold"],
        event_type_names=["__summary__", "message"],
        target_mean=[0.0] * len(_EVIDENCE_TARGET_NAMES),
        target_std=[1.0] * len(_EVIDENCE_TARGET_NAMES),
    )

    decoded = preprocessor.decode_targets(
        binary_probability=0.9,
        regression_values=[1e6] * len(_EVIDENCE_TARGET_NAMES),
    )

    assert decoded.any_external_spread is True
    assert decoded.outside_recipient_count > 0
    assert decoded.time_to_thread_end_ms > 0


def test_benchmark_preprocessor_decode_targets_handles_nonfinite_values() -> None:
    preprocessor = _BenchmarkPreprocessor(
        summary_feature_names=["history_event_count"],
        summary_mean=[0.0],
        summary_std=[1.0],
        action_tag_names=["hold"],
        event_type_names=["__summary__", "message"],
        target_mean=[0.0] * len(_EVIDENCE_TARGET_NAMES),
        target_std=[1.0] * len(_EVIDENCE_TARGET_NAMES),
    )

    decoded = preprocessor.decode_targets(
        binary_probability=0.1,
        regression_values=[float("nan"), float("inf"), float("-inf")]
        + [0.0] * (len(_EVIDENCE_TARGET_NAMES) - 3),
    )

    assert decoded.any_external_spread is False
    assert decoded.outside_recipient_count == 0
    assert decoded.outside_forward_count > 0
    assert decoded.outside_attachment_spread_count == 0


def test_vei_whatif_cli_benchmark_commands(tmp_path: Path, monkeypatch) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    monkeypatch.setattr(
        "vei.whatif.benchmark.get_research_pack",
        lambda _pack_id: pack,
    )
    build = build_branch_point_benchmark(
        world,
        artifacts_root=tmp_path / "benchmark_artifacts",
        label="fixture_benchmark_cli",
        heldout_pack_id="fixture_pack",
    )
    train_result = WhatIfBenchmarkTrainResult(
        model_id="jepa_latent",
        dataset_root=build.dataset.root,
        train_loss=0.2,
        validation_loss=0.3,
        epoch_count=4,
        train_row_count=1,
        validation_row_count=0,
        artifacts=WhatIfBenchmarkTrainArtifacts(
            root=build.artifacts.root / "model_runs" / "jepa_latent",
            model_path=build.artifacts.root / "model_runs" / "jepa_latent" / "model.pt",
            metadata_path=build.artifacts.root
            / "model_runs"
            / "jepa_latent"
            / "metadata.json",
            train_result_path=build.artifacts.root
            / "model_runs"
            / "jepa_latent"
            / "train_result.json",
        ),
    )
    eval_result = WhatIfBenchmarkEvalResult(
        model_id="jepa_latent",
        dataset_root=build.dataset.root,
        observed_metrics=WhatIfObservedForecastMetrics(
            auroc_any_external_spread=0.8,
            brier_any_external_spread=0.1,
            calibration_error_any_external_spread=0.05,
            evidence_head_mae={"outside_recipient_count": 0.2},
            business_head_mae={"enterprise_risk": 0.1},
        ),
        artifacts=WhatIfBenchmarkEvalArtifacts(
            root=build.artifacts.root / "model_runs" / "jepa_latent",
            eval_result_path=build.artifacts.root
            / "model_runs"
            / "jepa_latent"
            / "eval_result.json",
            prediction_jsonl_path=build.artifacts.root
            / "model_runs"
            / "jepa_latent"
            / "predictions.jsonl",
        ),
    )
    study_result = WhatIfBenchmarkStudyResult(
        label="fixture_study",
        build_root=build.artifacts.root,
        models=["jepa_latent", "full_context_transformer"],
        seeds=[42042, 42043],
        runs=[],
        summaries=[
            WhatIfBenchmarkStudyModelSummary(
                model_id="full_context_transformer",
                run_count=2,
                seeds=[42042, 42043],
                dominance_pass_rate=WhatIfBenchmarkMetricSummary(
                    count=2,
                    mean=0.9,
                    std=0.0,
                    min=0.9,
                    max=0.9,
                ),
                observed_auroc_any_external_spread=WhatIfBenchmarkMetricSummary(
                    count=2,
                    mean=0.8,
                    std=0.0,
                    min=0.8,
                    max=0.8,
                ),
            )
        ],
        ranked_model_ids=["full_context_transformer", "jepa_latent"],
        artifacts=WhatIfBenchmarkStudyArtifacts(
            root=build.artifacts.root / "studies" / "fixture_study",
            result_path=build.artifacts.root
            / "studies"
            / "fixture_study"
            / "benchmark_study_result.json",
            overview_path=build.artifacts.root
            / "studies"
            / "fixture_study"
            / "benchmark_study_overview.md",
        ),
    )

    monkeypatch.setattr(
        "vei.cli.vei_whatif.build_branch_point_benchmark",
        lambda *args, **kwargs: build,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.train_branch_point_benchmark_model",
        lambda *args, **kwargs: train_result,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.evaluate_branch_point_benchmark_model",
        lambda *args, **kwargs: eval_result,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.load_branch_point_benchmark_build_result",
        lambda *_args, **_kwargs: build,
    )
    judge_result = WhatIfBenchmarkJudgeResult(
        build_root=build.artifacts.root,
        judge_model="gpt-4.1-mini",
        judgments=[],
        audit_queue=[],
        artifacts=WhatIfBenchmarkJudgeArtifacts(
            root=build.artifacts.root,
            result_path=build.artifacts.root / "judge_result.json",
            audit_queue_path=build.artifacts.root / "audit_queue.json",
        ),
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.load_branch_point_benchmark_train_result",
        lambda *_args, **_kwargs: train_result,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.load_branch_point_benchmark_eval_result",
        lambda *_args, **_kwargs: eval_result,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.judge_branch_point_benchmark",
        lambda *_args, **_kwargs: judge_result,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.load_branch_point_benchmark_judge_result",
        lambda *_args, **_kwargs: judge_result,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.run_branch_point_benchmark_study",
        lambda *args, **kwargs: study_result,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.load_branch_point_benchmark_study_result",
        lambda *_args, **_kwargs: study_result,
    )

    runner = CliRunner()
    models_result = runner.invoke(
        cli_app,
        ["whatif", "benchmark", "models", "--format", "markdown"],
    )
    assert models_result.exit_code == 0, models_result.output
    assert "jepa_latent" in models_result.output

    build_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "build",
            "--rosetta-dir",
            str(rosetta_dir),
            "--label",
            "fixture_cli_benchmark",
            "--heldout-pack-id",
            "fixture_pack",
        ],
    )
    assert build_result.exit_code == 0, build_result.output
    assert json.loads(build_result.output)["label"] == "fixture_benchmark_cli"

    train_cli_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "train",
            "--root",
            str(build.artifacts.root),
            "--model-id",
            "jepa_latent",
        ],
    )
    assert train_cli_result.exit_code == 0, train_cli_result.output
    assert json.loads(train_cli_result.output)["model_id"] == "jepa_latent"

    judge_cli_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "judge",
            "--root",
            str(build.artifacts.root),
        ],
    )
    assert judge_cli_result.exit_code == 0, judge_cli_result.output
    assert json.loads(judge_cli_result.output)["judge_model"] == "gpt-4.1-mini"

    eval_cli_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "eval",
            "--root",
            str(build.artifacts.root),
            "--model-id",
            "jepa_latent",
            "--format",
            "markdown",
        ],
    )
    assert eval_cli_result.exit_code == 0, eval_cli_result.output
    assert "Observed Future Forecasting" in eval_cli_result.output

    study_cli_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "study",
            "--root",
            str(build.artifacts.root),
            "--label",
            "fixture_study",
            "--format",
            "markdown",
        ],
    )
    assert study_cli_result.exit_code == 0, study_cli_result.output
    assert "Ranked Models" in study_cli_result.output

    show_judge_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "show-judge",
            "--root",
            str(build.artifacts.root),
            "--format",
            "markdown",
        ],
    )
    assert show_judge_result.exit_code == 0, show_judge_result.output
    assert "Judged case-objectives" in show_judge_result.output

    show_study_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "show-study",
            "--root",
            str(study_result.artifacts.root),
            "--format",
            "markdown",
        ],
    )
    assert show_study_result.exit_code == 0, show_study_result.output
    assert "fixture_study" in show_study_result.output
