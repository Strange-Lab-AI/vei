from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from fastapi import HTTPException
from fastapi.testclient import TestClient
import pyarrow as pa
import pyarrow.parquet as pq

from vei.context.api import ContextSnapshot, write_canonical_history_sidecars
from vei.dataset.models import DatasetBuildSpec, DatasetBundle, DatasetSplitManifest
from vei.pilot import api as pilot_api
from vei.pilot.exercise_models import (
    ExerciseCatalogItem,
    ExerciseComparisonRow,
    ExerciseManifest,
)
from vei.imports.api import get_import_package_example_path
from vei.playable import prepare_playable_workspace
from vei.twin.models import (
    TwinLaunchManifest,
    TwinLaunchRuntime,
    TwinLaunchStatus,
    TwinOutcomeSummary,
    TwinServiceRecord,
)
from vei.run.api import launch_workspace_run
from vei.twin.models import CompatibilitySurfaceSpec, WorkspaceGovernorStatus
from vei.ui import api as ui_api
from vei.ui import _workspace_routes as workspace_routes
from vei.workspace.api import (
    create_workspace_from_template,
    generate_workspace_scenarios_from_import,
    import_workspace,
    load_workspace,
    sync_workspace_source,
    write_workspace,
)
from vei.whatif import load_world, materialize_episode
from vei.whatif.models import (
    WhatIfAuditRecord,
    WhatIfBenchmarkBuildArtifacts,
    WhatIfBenchmarkBuildResult,
    WhatIfBenchmarkCandidate,
    WhatIfBenchmarkCase,
    WhatIfBenchmarkDatasetManifest,
    WhatIfBenchmarkJudgeArtifacts,
    WhatIfBenchmarkJudgeResult,
    WhatIfEpisodeManifest,
    WhatIfEventReference,
    WhatIfHistoricalScore,
    WhatIfJudgedPairwiseComparison,
    WhatIfJudgedRanking,
    WhatIfPublicContext,
    WhatIfPublicFinancialSnapshot,
)


class _ImmediateThread:
    def __init__(self, *, target=None, daemon=None):
        self._target = target

    def start(self) -> None:
        if self._target is not None:
            self._target()


def test_ui_does_not_expose_legacy_skin_endpoint(tmp_path: Path) -> None:
    client = TestClient(ui_api.create_ui_app(tmp_path))

    response = client.get("/api/skin")

    assert response.status_code == 404


def test_ui_index_contains_company_subnav_and_whatif_steps(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    client = TestClient(ui_api.create_ui_app(root))

    response = client.get("/")

    assert response.status_code == 200
    body = response.text
    assert 'id="studio-view-helper"' in body
    assert 'id="company-subnav"' in body
    assert 'data-company-target="company-historical"' in body
    assert "Step 1" in body
    assert "Find Decision" in body
    assert "Compare Moves" in body
    assert "Review Forecast" in body


def _write_rosetta_fixture(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    metadata_rows = [
        {
            "event_id": "evt-001",
            "timestamp": "2001-05-01T10:00:00Z",
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
        {
            "event_id": "evt-002",
            "timestamp": "2001-05-01T10:05:00Z",
            "actor_id": "sara.shackleton@enron.com",
            "target_id": "ops.review@enron.com",
            "event_type": "assignment",
            "thread_task_id": "thr-external",
            "artifacts": json.dumps(
                {
                    "subject": "Draft term sheet",
                    "to_recipients": ["ops.review@enron.com"],
                    "to_count": 1,
                }
            ),
        },
    ]
    content_rows = [
        {"event_id": "evt-001", "content": "External draft attached for review."},
        {"event_id": "evt-002", "content": "Assigning ops review before we proceed."},
    ]
    pq.write_table(
        pa.Table.from_pylist(metadata_rows),
        root / "enron_rosetta_events_metadata.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(content_rows),
        root / "enron_rosetta_events_content.parquet",
    )


def _write_public_context_rosetta_fixture(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    metadata_rows = [
        {
            "event_id": "evt-200",
            "timestamp": "2001-04-17T14:00:00Z",
            "actor_id": "vince.kaminski@enron.com",
            "target_id": "sara.shackleton@enron.com",
            "event_type": "message",
            "thread_task_id": "thr-public-context",
            "artifacts": json.dumps(
                {
                    "subject": "Q1 numbers follow-up",
                    "to_recipients": ["sara.shackleton@enron.com"],
                    "to_count": 1,
                }
            ),
        },
        {
            "event_id": "evt-201",
            "timestamp": "2001-05-03T09:00:00Z",
            "actor_id": "jeff.skilling@enron.com",
            "target_id": "outside@lawfirm.com",
            "event_type": "message",
            "thread_task_id": "thr-public-context",
            "artifacts": json.dumps(
                {
                    "subject": "Draft term sheet",
                    "to_recipients": ["outside@lawfirm.com"],
                    "to_count": 1,
                    "has_attachment_reference": True,
                }
            ),
        },
        {
            "event_id": "evt-202",
            "timestamp": "2001-05-03T11:00:00Z",
            "actor_id": "sara.shackleton@enron.com",
            "target_id": "jeff.skilling@enron.com",
            "event_type": "reply",
            "thread_task_id": "thr-public-context",
            "artifacts": json.dumps(
                {
                    "subject": "Draft term sheet",
                    "to_recipients": ["jeff.skilling@enron.com"],
                    "to_count": 1,
                    "is_reply": True,
                }
            ),
        },
    ]
    content_rows = [
        {"event_id": "evt-200", "content": "Flagging the quarter numbers for review."},
        {"event_id": "evt-201", "content": "Sending the outside draft today."},
        {"event_id": "evt-202", "content": "Replying with legal concerns."},
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
    snapshot = ContextSnapshot.model_validate_json(
        snapshot_path.read_text(encoding="utf-8")
    )
    write_canonical_history_sidecars(snapshot, snapshot_path)
    return snapshot_path


def _write_saved_context_snapshot(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    snapshot_path = root / "context_snapshot.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "organization_name": "Enron Corporation",
                "organization_domain": "enron.com",
                "threads": [
                    {
                        "thread_id": "thr-master-agreement",
                        "subject": "Master Agreement",
                        "messages": [
                            {
                                "message_id": "msg-001",
                                "timestamp": "2000-09-26T15:00:00Z",
                                "from": "debra.perlingiere@enron.com",
                                "to": "marie.heard@enron.com",
                                "subject": "Master Agreement",
                                "body_text": "Draft is attached for internal prep before any outside send.",
                            },
                            {
                                "message_id": "msg-002",
                                "timestamp": "2000-09-27T13:42:00Z",
                                "from": "debra.perlingiere@enron.com",
                                "to": "kathy_gerken@cargill.com",
                                "subject": "Master Agreement",
                                "body_text": "Historical branch point.",
                            },
                        ],
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return snapshot_path


def _write_benchmark_audit_fixture(
    root: Path,
    *,
    include_judge: bool = True,
) -> None:
    root.mkdir(parents=True, exist_ok=True)
    dataset_root = root / "dataset"
    dataset_root.mkdir(parents=True, exist_ok=True)
    dossier_root = root / "dossiers" / "case_master_agreement"
    dossier_root.mkdir(parents=True, exist_ok=True)
    dossier_path = dossier_root / "minimize_enterprise_risk.md"
    dossier_path.write_text(
        (
            "# Master Agreement\n\n"
            "Review the internal hold against the external send.\n\n"
            "## Public Company Context\n"
            "- 1999-12-31 FY1999 selected financial data: Operating revenue was $40.11B and net income was $893M.\n"
        ),
        encoding="utf-8",
    )

    candidate_ids = [
        "legal_hold_internal",
        "narrow_external_status",
        "broad_external_send",
    ]
    build = WhatIfBenchmarkBuildResult(
        label="enron_benchmark_audit",
        heldout_pack_id="enron_fixture_pack",
        dataset=WhatIfBenchmarkDatasetManifest(
            root=dataset_root,
            heldout_cases_path=str(root / "heldout_cases.json"),
            judge_template_path=str(root / "judged_ranking_template.json"),
            audit_template_path=str(root / "audit_record_template.json"),
            dossier_root=str(root / "dossiers"),
        ),
        cases=[
            WhatIfBenchmarkCase(
                case_id="case_master_agreement",
                title="Master Agreement",
                event_id="enron_branch_001",
                thread_id="thr_master_agreement",
                summary="Review the draft before it goes outside Enron.",
                case_family="legal_review",
                branch_event=WhatIfEventReference(
                    event_id="enron_branch_001",
                    timestamp="2000-09-27T13:42:00Z",
                    actor_id="debra.perlingiere@enron.com",
                    target_id="kathy_gerken@cargill.com",
                    event_type="message",
                    thread_id="thr_master_agreement",
                    subject="Master Agreement",
                    snippet="Attached for your review is a draft Master Agreement.",
                ),
                public_context=WhatIfPublicContext(
                    pack_name="enron_public_context",
                    organization_name="Enron Corporation",
                    organization_domain="enron.com",
                    financial_snapshots=[
                        WhatIfPublicFinancialSnapshot(
                            snapshot_id="fy1999_selected_financial_data",
                            as_of="1999-12-31T00:00:00Z",
                            kind="annual",
                            label="FY1999 selected financial data",
                            summary="Operating revenue was $40.11B and net income was $893M.",
                        )
                    ],
                    public_news_events=[],
                ),
                objective_dossier_paths={"minimize_enterprise_risk": str(dossier_path)},
                candidates=[
                    WhatIfBenchmarkCandidate(
                        candidate_id="legal_hold_internal",
                        label="Legal hold internal",
                        prompt="Keep the draft inside Enron and ask legal for review.",
                    ),
                    WhatIfBenchmarkCandidate(
                        candidate_id="narrow_external_status",
                        label="Narrow external status",
                        prompt="Send a short status note without the draft.",
                    ),
                    WhatIfBenchmarkCandidate(
                        candidate_id="broad_external_send",
                        label="Broad external send",
                        prompt="Send the draft now and widen circulation.",
                    ),
                ],
            )
        ],
        artifacts=WhatIfBenchmarkBuildArtifacts(
            root=root,
            manifest_path=root / "branch_point_benchmark_build.json",
            heldout_cases_path=root / "heldout_cases.json",
            judge_template_path=root / "judged_ranking_template.json",
            audit_template_path=root / "audit_record_template.json",
            dossier_root=root / "dossiers",
        ),
    )
    build.artifacts.manifest_path.write_text(
        build.model_dump_json(indent=2),
        encoding="utf-8",
    )
    build.artifacts.heldout_cases_path.write_text("[]", encoding="utf-8")
    build.artifacts.judge_template_path.write_text("[]", encoding="utf-8")
    build.artifacts.audit_template_path.write_text("[]", encoding="utf-8")

    if not include_judge:
        return

    pairwise = [
        WhatIfJudgedPairwiseComparison(
            left_candidate_id="legal_hold_internal",
            right_candidate_id="narrow_external_status",
            preferred_candidate_id="legal_hold_internal",
            confidence=0.7,
            rationale="The draft stays inside Enron.",
        ),
        WhatIfJudgedPairwiseComparison(
            left_candidate_id="legal_hold_internal",
            right_candidate_id="broad_external_send",
            preferred_candidate_id="legal_hold_internal",
            confidence=0.7,
            rationale="Broad circulation raises external spread.",
        ),
        WhatIfJudgedPairwiseComparison(
            left_candidate_id="narrow_external_status",
            right_candidate_id="broad_external_send",
            preferred_candidate_id="narrow_external_status",
            confidence=0.65,
            rationale="A status note is narrower than a full draft send.",
        ),
    ]
    judge_result = WhatIfBenchmarkJudgeResult(
        build_root=root,
        judge_model="gpt-4.1-mini",
        judgments=[
            WhatIfJudgedRanking(
                case_id="case_master_agreement",
                objective_pack_id="minimize_enterprise_risk",
                judge_id="judge-1",
                judge_model="gpt-4.1-mini",
                ordered_candidate_ids=candidate_ids,
                pairwise_comparisons=pairwise,
                confidence=0.72,
            )
        ],
        audit_queue=[
            WhatIfAuditRecord(
                case_id="case_master_agreement",
                objective_pack_id="minimize_enterprise_risk",
                status="pending",
            )
        ],
        artifacts=WhatIfBenchmarkJudgeArtifacts(
            root=root,
            result_path=root / "judge_result.json",
            audit_queue_path=root / "audit_queue.json",
        ),
    )
    judge_result.artifacts.result_path.write_text(
        judge_result.model_dump_json(indent=2),
        encoding="utf-8",
    )
    judge_result.artifacts.audit_queue_path.write_text(
        json.dumps(
            [item.model_dump(mode="json") for item in judge_result.audit_queue],
            indent=2,
        ),
        encoding="utf-8",
    )


def test_ui_api_serves_workspace_and_run_details(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    manifest = launch_workspace_run(root, runner="workflow")

    client = TestClient(ui_api.create_ui_app(root))

    workspace_response = client.get("/api/workspace")
    assert workspace_response.status_code == 200
    assert workspace_response.json()["manifest"]["name"]

    runs_response = client.get("/api/runs")
    assert runs_response.status_code == 200
    assert runs_response.json()[0]["run_id"] == manifest.run_id

    timeline_response = client.get(f"/api/runs/{manifest.run_id}/timeline")
    assert timeline_response.status_code == 200
    assert any(item["kind"] == "workflow_step" for item in timeline_response.json())

    snapshots_response = client.get(f"/api/runs/{manifest.run_id}/snapshots")
    assert snapshots_response.status_code == 200
    snapshots = snapshots_response.json()
    assert len(snapshots) >= 2

    diff_response = client.get(
        f"/api/runs/{manifest.run_id}/diff",
        params={
            "snapshot_from": snapshots[0]["snapshot_id"],
            "snapshot_to": snapshots[-1]["snapshot_id"],
        },
    )
    assert diff_response.status_code == 200
    assert isinstance(diff_response.json()["changed"], dict)

    contract_response = client.get(f"/api/runs/{manifest.run_id}/contract")
    assert contract_response.status_code == 200
    assert contract_response.json()["ok"] is True

    receipts_response = client.get(f"/api/runs/{manifest.run_id}/receipts")
    assert receipts_response.status_code == 200
    assert isinstance(receipts_response.json(), list)

    orientation_response = client.get(f"/api/runs/{manifest.run_id}/orientation")
    assert orientation_response.status_code == 200
    assert orientation_response.json()["organization_name"] == "MacroCompute"

    timeline_path = root / "runs" / manifest.run_id / "timeline.json"
    timeline_path.unlink()
    with client.stream("GET", f"/api/runs/{manifest.run_id}/stream") as response:
        body = "".join(response.iter_text())
    assert response.status_code == 200
    assert "workflow_step" in body


def test_ui_api_start_run_returns_generated_run_id(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )

    monkeypatch.setattr(ui_api, "Thread", _ImmediateThread)
    client = TestClient(ui_api.create_ui_app(root))

    response = client.post("/api/runs", json={"runner": "workflow"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["run_id"].startswith("run_")

    run_response = client.get(f"/api/runs/{payload['run_id']}")
    assert run_response.status_code == 200
    assert run_response.json()["status"] == "ok"


def test_ui_api_whatif_search_and_open_routes(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(rosetta_dir))

    client = TestClient(ui_api.create_ui_app(root))

    status_response = client.get("/api/workspace/whatif")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["available"] is True
    assert {pack["pack_id"] for pack in status_payload["objective_packs"]} == {
        "contain_exposure",
        "reduce_delay",
        "protect_relationship",
    }

    search_response = client.post(
        "/api/workspace/whatif/search",
        json={"source": "enron", "query": "Jeff Skilling draft term sheet"},
    )
    assert search_response.status_code == 200
    payload = search_response.json()
    assert payload["match_count"] == 1
    assert payload["matches"][0]["event"]["event_id"] == "evt-001"
    assert (
        payload["matches"][0]["event"]["snippet"]
        == "External draft attached for review."
    )

    open_response = client.post(
        "/api/workspace/whatif/open",
        json={"source": "enron", "event_id": "evt-001", "label": "term-sheet"},
    )
    assert open_response.status_code == 200
    open_payload = open_response.json()
    assert open_payload["materialization"]["branch_event_id"] == "evt-001"
    assert open_payload["materialization"]["future_event_count"] == 2


def test_ui_api_whatif_routes_support_generic_mail_archive(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    archive_path = _write_mail_archive_fixture(tmp_path / "mail_archive")
    monkeypatch.setenv("VEI_WHATIF_SOURCE_DIR", str(archive_path))
    monkeypatch.setenv("VEI_WHATIF_SOURCE", "mail_archive")

    client = TestClient(ui_api.create_ui_app(root))

    status_response = client.get("/api/workspace/whatif")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["available"] is True
    assert status_payload["source"] == "mail_archive"
    assert status_payload["source_dir"] == str(archive_path.resolve())

    search_response = client.post(
        "/api/workspace/whatif/search",
        json={"source": "auto", "query": "Redwood draft"},
    )
    assert search_response.status_code == 200
    search_payload = search_response.json()
    assert search_payload["source"] == "mail_archive"
    assert search_payload["match_count"] >= 1
    assert search_payload["matches"][0]["event"]["thread_id"] == "py-legal-001"

    open_response = client.post(
        "/api/workspace/whatif/open",
        json={"source": "auto", "thread_id": "py-legal-001", "label": "py-legal"},
    )
    assert open_response.status_code == 200
    open_payload = open_response.json()
    assert open_payload["source"] == "mail_archive"
    assert open_payload["materialization"]["organization_name"] == "Py Corp"
    assert open_payload["materialization"]["branch_event_id"] == "py-msg-002"


def test_ui_api_whatif_routes_support_company_history_bundle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    snapshot_path = _write_company_history_fixture(tmp_path / "company_history")
    monkeypatch.setenv("VEI_WHATIF_SOURCE_DIR", str(snapshot_path))

    client = TestClient(ui_api.create_ui_app(root))

    status_response = client.get("/api/workspace/whatif")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["available"] is True
    assert status_payload["source"] == "company_history"
    assert status_payload["source_dir"] == str(snapshot_path.resolve())

    search_response = client.post(
        "/api/workspace/whatif/search",
        json={"source": "auto", "query": "legal signs off"},
    )
    assert search_response.status_code == 200
    search_payload = search_response.json()
    assert search_payload["source"] == "company_history"
    assert search_payload["matches"][0]["event"]["surface"] == "slack"
    slack_thread_id = search_payload["matches"][0]["event"]["thread_id"]

    scene_response = client.post(
        "/api/workspace/whatif/scene",
        json={"source": "auto", "thread_id": slack_thread_id},
    )
    assert scene_response.status_code == 200
    scene_payload = scene_response.json()
    assert scene_payload["source"] == "company_history"
    assert scene_payload["surface"] == "slack"
    assert scene_payload["branch_event"]["surface"] == "slack"
    assert scene_payload["case_id"] == "case:LEGAL-7"
    assert scene_payload["case_context"]["related_history"]
    assert {item["surface"] for item in scene_payload["case_context"]["records"]} >= {
        "docs",
        "crm",
    }
    assert scene_payload["situation_context"]["related_threads"]
    assert {
        item["surface"]
        for item in scene_payload["situation_context"]["related_threads"]
    } >= {"docs", "crm"}

    auto_scene_response = client.post(
        "/api/workspace/whatif/scene",
        json={"source": "auto"},
    )
    assert auto_scene_response.status_code == 200
    auto_scene_payload = auto_scene_response.json()
    assert auto_scene_payload["thread_id"] == slack_thread_id


def test_ui_api_whatif_timeline_supports_company_history_filters(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    snapshot_path = _write_company_history_fixture(tmp_path / "company_history")
    monkeypatch.setenv("VEI_WHATIF_SOURCE_DIR", str(snapshot_path))

    client = TestClient(ui_api.create_ui_app(root))

    response = client.get(
        "/api/workspace/whatif/timeline",
        params={
            "source": "auto",
            "surface": "tickets",
            "case_id": "case:LEGAL-7",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["available"] is True
    assert payload["source"] == "company_history"
    assert payload["matching_event_count"] >= 1
    assert all(row["surface"] == "tickets" for row in payload["rows"])
    assert all(row["case_id"] == "case:LEGAL-7" for row in payload["rows"])

    status_payload = client.get("/api/workspace/whatif").json()
    assert status_payload["timeline_available"] is True
    assert status_payload["timeline_readiness"]["available"] is True
    assert status_payload["timeline_readiness"]["surface_count"] >= 4


def test_ui_api_whatif_scene_route_returns_playable_enron_decision(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(rosetta_dir))

    client = TestClient(ui_api.create_ui_app(root))
    response = client.post(
        "/api/workspace/whatif/scene",
        json={"source": "enron", "event_id": "evt-001"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "enron"
    assert payload["thread_id"] == "thr-external"
    assert payload["branch_event_id"] == "evt-001"
    assert payload["branch_summary"].startswith("Jeff Skilling is about to send")
    assert payload["historical_action_summary"].startswith(
        "Historically, Jeff Skilling"
    )
    assert payload["future_event_count"] == 2
    assert len(payload["candidate_options"]) == 3
    assert payload["candidate_options"][0]["label"] == "Hold for internal review"
    assert payload["historical_business_state"]["summary"]
    assert payload["public_context"]["financial_snapshots"] == []
    assert payload["public_context"]["public_news_events"] == []


def test_ui_api_whatif_scene_route_returns_branch_filtered_public_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    rosetta_dir = tmp_path / "rosetta"
    _write_public_context_rosetta_fixture(rosetta_dir)
    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(rosetta_dir))

    client = TestClient(ui_api.create_ui_app(root))
    response = client.post(
        "/api/workspace/whatif/scene",
        json={"source": "enron", "event_id": "evt-201"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["thread_id"] == "thr-public-context"
    assert [
        item["snapshot_id"] for item in payload["public_context"]["financial_snapshots"]
    ] == ["q1_2001_earnings_release"]
    assert [
        item["event_id"] for item in payload["public_context"]["public_news_events"]
    ] == ["cliff_baxter_resignation"]
    assert payload["historical_business_state"]["summary"]


def test_ui_api_whatif_scene_route_supports_generic_mail_archive(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    archive_path = _write_mail_archive_fixture(tmp_path / "mail_archive")
    monkeypatch.setenv("VEI_WHATIF_SOURCE_DIR", str(archive_path))
    monkeypatch.setenv("VEI_WHATIF_SOURCE", "mail_archive")

    client = TestClient(ui_api.create_ui_app(root))
    response = client.post(
        "/api/workspace/whatif/scene",
        json={"source": "auto", "thread_id": "py-legal-001"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "mail_archive"
    assert payload["organization_name"] == "Py Corp"
    assert payload["branch_event_id"] == "py-msg-002"
    assert payload["history_message_count"] == 1
    assert payload["candidate_options"][0]["label"] == "Keep the loop tight"
    assert payload["candidate_options"][0]["prompt"].startswith(
        'Keep "Pricing addendum" in a tight internal loop'
    )
    assert payload["decision_question"] == (
        'What should the company do at this point in "Pricing addendum"?'
    )


def test_ui_api_historical_workspace_prefers_saved_mail_archive(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive_path = _write_mail_archive_fixture(tmp_path / "mail_archive")
    world = load_world(source="auto", source_dir=archive_path)
    workspace_root = tmp_path / "historical_workspace"
    materialize_episode(world, root=workspace_root, thread_id="py-legal-001")

    other_archive = tmp_path / "other_mail_archive" / "context_snapshot.json"
    other_archive.parent.mkdir(parents=True, exist_ok=True)
    other_archive.write_text(
        json.dumps(
            {
                "organization_name": "Other Corp",
                "organization_domain": "other.example.com",
                "threads": [
                    {
                        "thread_id": "other-001",
                        "subject": "Other thread",
                        "messages": [
                            {
                                "message_id": "other-msg-001",
                                "from": "ceo@other.example.com",
                                "to": "board@other.example.com",
                                "subject": "Other thread",
                                "body_text": "Different archive entirely.",
                                "timestamp": "2026-04-01T10:00:00Z",
                            }
                        ],
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("VEI_WHATIF_SOURCE_DIR", str(other_archive))
    monkeypatch.setenv("VEI_WHATIF_SOURCE", "enron")

    client = TestClient(ui_api.create_ui_app(workspace_root))

    status_response = client.get("/api/workspace/whatif")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["source"] == "mail_archive"
    assert status_payload["source_dir"].endswith("context_snapshot.json")

    search_response = client.post(
        "/api/workspace/whatif/search",
        json={"source": "auto", "query": "pricing"},
    )
    assert search_response.status_code == 200
    search_payload = search_response.json()
    assert search_payload["match_count"] == 3
    assert search_payload["matches"][0]["event"]["timestamp"].startswith("2026-03-01")

    scene_response = client.post(
        "/api/workspace/whatif/scene",
        json={
            "source": "auto",
            "event_id": "py-msg-002",
            "thread_id": "py-legal-001",
        },
    )
    assert scene_response.status_code == 200
    scene_payload = scene_response.json()
    assert scene_payload["organization_name"] == "Py Corp"
    assert scene_payload["branch_event_id"] == "py-msg-002"
    assert scene_payload["history_preview"][0]["actor_id"] == "emma@pycorp.example.com"
    assert scene_payload["historical_future_preview"][0]["event_id"] == "py-msg-002"


def test_ui_api_historical_workspace_prefers_manifest_rosetta_dir(
    tmp_path: Path,
    monkeypatch,
) -> None:
    primary_rosetta = tmp_path / "primary_rosetta"
    fallback_rosetta = tmp_path / "fallback_rosetta"
    _write_rosetta_fixture(primary_rosetta)
    _write_rosetta_fixture(fallback_rosetta)
    workspace_root = tmp_path / "historical_enron_workspace"
    create_workspace_from_template(
        root=workspace_root,
        source_kind="vertical",
        source_ref="b2b_saas",
    )
    episode = WhatIfEpisodeManifest(
        source="enron",
        source_dir=primary_rosetta,
        workspace_root="workspace",
        organization_name="Enron Corporation",
        organization_domain="enron.com",
        thread_id="thr-external",
        thread_subject="Draft term sheet",
        branch_event_id="evt-001",
        branch_timestamp="2001-05-01T10:00:00Z",
        branch_event=WhatIfEventReference(
            event_id="evt-001",
            timestamp="2001-05-01T10:00:00Z",
            actor_id="jeff.skilling@enron.com",
            target_id="outside@lawfirm.com",
            event_type="message",
            thread_id="thr-external",
            subject="Draft term sheet",
            snippet="External draft attached for review.",
        ),
        history_message_count=0,
        future_event_count=2,
        baseline_dataset_path="whatif_baseline_dataset.json",
        content_notice="Historical email bodies are grounded in archive excerpts and metadata.",
        forecast=WhatIfHistoricalScore(backend="historical", risk_score=1.0),
    )
    (workspace_root / "episode_manifest.json").write_text(
        episode.model_dump_json(indent=2),
        encoding="utf-8",
    )
    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(fallback_rosetta))
    monkeypatch.setenv("VEI_WHATIF_SOURCE", "mail_archive")

    client = TestClient(ui_api.create_ui_app(workspace_root))
    status_response = client.get("/api/workspace/whatif")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["source"] == "enron"
    assert status_payload["source_dir"] == str(primary_rosetta.resolve())


def test_ui_api_saved_enron_workspace_without_rosetta_uses_saved_context_snapshot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("HOME", str((tmp_path / "home").resolve()))
    monkeypatch.delenv("VEI_WHATIF_ROSETTA_DIR", raising=False)
    monkeypatch.delenv("VEI_WHATIF_SOURCE", raising=False)
    monkeypatch.delenv("VEI_WHATIF_SOURCE_DIR", raising=False)
    workspace_root = tmp_path / "saved_enron_workspace"
    create_workspace_from_template(
        root=workspace_root,
        source_kind="vertical",
        source_ref="b2b_saas",
    )
    snapshot_path = _write_saved_context_snapshot(workspace_root)
    episode = WhatIfEpisodeManifest(
        source="enron",
        source_dir="/missing/rosetta",
        workspace_root="workspace",
        organization_name="Enron Corporation",
        organization_domain="enron.com",
        thread_id="thr-master-agreement",
        thread_subject="Master Agreement",
        branch_event_id="enron_bcda1b925800af8c",
        branch_timestamp="2000-09-27T13:42:00Z",
        branch_event=WhatIfEventReference(
            event_id="enron_bcda1b925800af8c",
            timestamp="2000-09-27T13:42:00Z",
            actor_id="debra.perlingiere@enron.com",
            target_id="kathy_gerken@cargill.com",
            event_type="assignment",
            thread_id="thr-master-agreement",
            subject="Master Agreement",
            snippet="Historical branch point.",
            to_recipients=["kathy_gerken@cargill.com"],
        ),
        history_message_count=1,
        future_event_count=84,
        baseline_dataset_path="whatif_baseline_dataset.json",
        content_notice="Historical email bodies are grounded in archive excerpts and metadata.",
        forecast=WhatIfHistoricalScore(backend="historical", risk_score=1.0),
        public_context=WhatIfPublicContext(
            pack_name="enron_public_context",
            organization_name="Enron Corporation",
            organization_domain="enron.com",
            window_start="2000-09-26T15:00:00Z",
            window_end="2000-09-27T13:42:00Z",
            branch_timestamp="2000-09-27T13:42:00Z",
            financial_snapshots=[
                WhatIfPublicFinancialSnapshot(
                    snapshot_id="fy_1999",
                    as_of="1999-12-31T00:00:00Z",
                    kind="annual",
                    label="1999 Annual",
                    summary="Revenue and net income improved ahead of the branch date.",
                )
            ],
        ),
    )
    (workspace_root / "episode_manifest.json").write_text(
        episode.model_dump_json(indent=2),
        encoding="utf-8",
    )

    client = TestClient(ui_api.create_ui_app(workspace_root))
    status_response = client.get("/api/workspace/whatif")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["available"] is True
    assert status_payload["source"] == "mail_archive"
    assert status_payload["source_dir"] == str(snapshot_path.resolve())

    scene_response = client.post(
        "/api/workspace/whatif/scene",
        json={
            "source": status_payload["source"],
            "event_id": "enron_bcda1b925800af8c",
            "thread_id": "thr-master-agreement",
        },
    )
    assert scene_response.status_code == 200
    scene_payload = scene_response.json()
    assert scene_payload["organization_name"] == "Enron Corporation"
    assert scene_payload["branch_event_id"] == "enron_bcda1b925800af8c"
    assert scene_payload["public_context"]["financial_snapshots"][0]["snapshot_id"] == (
        "fy_1999"
    )


def test_ui_api_saved_bundle_routes_recheck_bundle_after_app_start(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("HOME", str((tmp_path / "home").resolve()))
    monkeypatch.delenv("VEI_WHATIF_ROSETTA_DIR", raising=False)
    monkeypatch.delenv("VEI_WHATIF_SOURCE", raising=False)
    monkeypatch.delenv("VEI_WHATIF_SOURCE_DIR", raising=False)
    workspace_root = tmp_path / "saved_bundle" / "workspace"
    create_workspace_from_template(
        root=workspace_root,
        source_kind="vertical",
        source_ref="b2b_saas",
    )
    snapshot_path = _write_saved_context_snapshot(workspace_root)
    episode = WhatIfEpisodeManifest(
        source="enron",
        source_dir="/missing/rosetta",
        workspace_root="workspace",
        organization_name="Enron Corporation",
        organization_domain="enron.com",
        thread_id="thr-master-agreement",
        thread_subject="Master Agreement",
        branch_event_id="enron_bcda1b925800af8c",
        branch_timestamp="2000-09-27T13:42:00Z",
        branch_event=WhatIfEventReference(
            event_id="enron_bcda1b925800af8c",
            timestamp="2000-09-27T13:42:00Z",
            actor_id="debra.perlingiere@enron.com",
            target_id="kathy_gerken@cargill.com",
            event_type="assignment",
            thread_id="thr-master-agreement",
            subject="Master Agreement",
            snippet="Historical branch point.",
            to_recipients=["kathy_gerken@cargill.com"],
        ),
        history_message_count=1,
        future_event_count=84,
        baseline_dataset_path="whatif_baseline_dataset.json",
        content_notice="Historical email bodies are grounded in archive excerpts and metadata.",
        forecast=WhatIfHistoricalScore(backend="historical", risk_score=1.0),
    )
    (workspace_root / "episode_manifest.json").write_text(
        episode.model_dump_json(indent=2),
        encoding="utf-8",
    )

    client = TestClient(ui_api.create_ui_app(workspace_root))
    status_response = client.get("/api/workspace/whatif")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["available"] is True
    assert status_payload["source"] == "mail_archive"
    assert status_payload["source_dir"] == str(snapshot_path.resolve())

    bundle_root = workspace_root.parent
    (bundle_root / "whatif_experiment_result.json").write_text(
        json.dumps(
            {
                "label": "saved_bundle_run",
                "selection": {},
                "baseline": {},
                "materialization": {
                    "branch_event_id": "enron_bcda1b925800af8c",
                    "future_event_count": 84,
                },
                "forecast_result": {
                    "business_state_change": {
                        "summary": "Saved business-state summary.",
                    }
                },
                "artifacts": {
                    "result_json_path": "whatif_experiment_result.json",
                    "overview_markdown_path": "whatif_experiment_overview.md",
                    "llm_json_path": "whatif_llm_result.json",
                    "forecast_json_path": "whatif_ejepa_result.json",
                },
            }
        ),
        encoding="utf-8",
    )
    (bundle_root / "whatif_experiment_overview.md").write_text(
        "# Saved bundle\n",
        encoding="utf-8",
    )
    (bundle_root / "whatif_llm_result.json").write_text("{}", encoding="utf-8")
    (bundle_root / "whatif_ejepa_result.json").write_text(
        json.dumps({"cache_root": "not-included-in-repo-example"}),
        encoding="utf-8",
    )
    (bundle_root / "whatif_business_state_comparison.json").write_text(
        json.dumps(
            {
                "label": "saved_ranked",
                "candidates": [
                    {
                        "label": "Hold internal",
                        "prompt": "Keep the draft inside Enron.",
                        "rank": 1,
                        "business_state_change": {
                            "summary": "Fewer outside sends.",
                            "net_effect_score": 0.42,
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (bundle_root / "whatif_business_state_comparison.md").write_text(
        "# Saved ranked comparison\n",
        encoding="utf-8",
    )

    open_response = client.post(
        "/api/workspace/whatif/open",
        json={
            "source": status_payload["source"],
            "event_id": "enron_bcda1b925800af8c",
            "thread_id": "thr-master-agreement",
            "label": "ignored-after-start",
        },
    )
    assert open_response.status_code == 200
    open_payload = open_response.json()
    assert (
        open_payload["materialization"]["branch_event_id"] == "enron_bcda1b925800af8c"
    )

    run_response = client.post(
        "/api/workspace/whatif/run",
        json={
            "source": status_payload["source"],
            "event_id": "enron_bcda1b925800af8c",
            "thread_id": "thr-master-agreement",
            "label": "ignored-after-start",
            "prompt": "Keep the draft inside Enron.",
        },
    )
    assert run_response.status_code == 200
    run_payload = run_response.json()
    assert run_payload["label"] == "saved_bundle_run"
    assert run_payload["source_dir"] == str(snapshot_path.resolve())

    rank_response = client.post(
        "/api/workspace/whatif/rank",
        json={
            "source": status_payload["source"],
            "event_id": "enron_bcda1b925800af8c",
            "thread_id": "thr-master-agreement",
            "label": "ignored-after-start",
            "objective_pack_id": "contain_exposure",
            "candidates": [
                {
                    "label": "Hold internal",
                    "prompt": "Keep the draft inside Enron.",
                }
            ],
        },
    )
    assert rank_response.status_code == 200
    rank_payload = rank_response.json()
    assert rank_payload["recommended_candidate_label"] == "Hold internal"
    assert rank_payload["candidates"][0]["saved_result"] is True


def test_ui_api_saved_bundle_routes_support_non_enron_saved_branches(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("VEI_WHATIF_ROSETTA_DIR", raising=False)
    monkeypatch.delenv("VEI_WHATIF_SOURCE", raising=False)
    monkeypatch.delenv("VEI_WHATIF_SOURCE_DIR", raising=False)

    workspace_root = tmp_path / "saved_mail_bundle" / "workspace"
    create_workspace_from_template(
        root=workspace_root,
        source_kind="vertical",
        source_ref="b2b_saas",
    )
    episode = WhatIfEpisodeManifest(
        source="mail_archive",
        source_dir="/missing/mail_archive.json",
        workspace_root="workspace",
        organization_name="Py Corp",
        organization_domain="pycorp.example.com",
        thread_id="py-legal-001",
        thread_subject="Pricing addendum",
        branch_event_id="py-msg-002",
        branch_timestamp="2026-03-01T09:05:00Z",
        branch_event=WhatIfEventReference(
            event_id="py-msg-002",
            timestamp="2026-03-01T09:05:00Z",
            actor_id="legal@pycorp.example.com",
            target_id="emma@pycorp.example.com",
            event_type="reply",
            thread_id="py-legal-001",
            subject="Pricing addendum",
            snippet="Hold for one markup round.",
            to_recipients=["emma@pycorp.example.com"],
        ),
        history_message_count=1,
        future_event_count=2,
        baseline_dataset_path="whatif_baseline_dataset.json",
        content_notice="Historical email bodies are grounded in archive excerpts and metadata.",
        forecast=WhatIfHistoricalScore(backend="historical", risk_score=0.82),
    )
    (workspace_root / "episode_manifest.json").write_text(
        episode.model_dump_json(indent=2),
        encoding="utf-8",
    )

    bundle_root = workspace_root.parent
    (bundle_root / "whatif_experiment_result.json").write_text(
        json.dumps(
            {
                "label": "saved_mail_bundle_run",
                "selection": {},
                "baseline": {},
                "materialization": {
                    "branch_event_id": "py-msg-002",
                    "future_event_count": 2,
                },
                "forecast_result": {
                    "business_state_change": {
                        "summary": "Outside exposure drops with one internal hold.",
                    }
                },
                "artifacts": {
                    "result_json_path": "whatif_experiment_result.json",
                    "overview_markdown_path": "whatif_experiment_overview.md",
                    "llm_json_path": "whatif_llm_result.json",
                    "forecast_json_path": "whatif_heuristic_baseline_result.json",
                },
            }
        ),
        encoding="utf-8",
    )
    (bundle_root / "whatif_experiment_overview.md").write_text(
        "# Saved mail bundle\n",
        encoding="utf-8",
    )
    (bundle_root / "whatif_llm_result.json").write_text("{}", encoding="utf-8")
    (bundle_root / "whatif_heuristic_baseline_result.json").write_text(
        json.dumps({"cache_root": "not-included-in-repo-example"}),
        encoding="utf-8",
    )
    (bundle_root / "whatif_business_state_comparison.json").write_text(
        json.dumps(
            {
                "label": "saved_mail_ranked",
                "objective_pack": {"pack_id": "contain_exposure"},
                "candidates": [
                    {
                        "label": "Hold for internal review",
                        "prompt": "Keep the draft internal for one more review pass.",
                        "rank": 1,
                        "business_state_change": {
                            "summary": "Reduced outside spread risk.",
                            "net_effect_score": 0.31,
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (bundle_root / "whatif_business_state_comparison.md").write_text(
        "# Saved mail ranked comparison\n",
        encoding="utf-8",
    )

    client = TestClient(ui_api.create_ui_app(workspace_root))

    status_response = client.get("/api/workspace/whatif")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["available"] is True
    assert status_payload["source"] == "mail_archive"
    assert status_payload["source_dir"] == str(workspace_root.resolve())

    open_response = client.post(
        "/api/workspace/whatif/open",
        json={
            "source": "auto",
            "event_id": "py-msg-002",
            "thread_id": "py-legal-001",
            "label": "ignored-for-saved-mail-bundle",
        },
    )
    assert open_response.status_code == 200
    open_payload = open_response.json()
    assert open_payload["source"] == "mail_archive"
    assert open_payload["materialization"]["branch_event_id"] == "py-msg-002"

    run_response = client.post(
        "/api/workspace/whatif/run",
        json={
            "source": "auto",
            "event_id": "py-msg-002",
            "thread_id": "py-legal-001",
            "label": "ignored-for-saved-mail-bundle",
            "prompt": "Keep this inside while legal reviews.",
        },
    )
    assert run_response.status_code == 200
    run_payload = run_response.json()
    assert run_payload["label"] == "saved_mail_bundle_run"

    rank_response = client.post(
        "/api/workspace/whatif/rank",
        json={
            "source": "auto",
            "event_id": "py-msg-002",
            "thread_id": "py-legal-001",
            "label": "ignored-for-saved-mail-bundle",
            "objective_pack_id": "reduce_delay",
            "candidates": [
                {
                    "label": "Hold for internal review",
                    "prompt": "Keep the draft internal for one more review pass.",
                }
            ],
        },
    )
    assert rank_response.status_code == 200
    rank_payload = rank_response.json()
    assert rank_payload["recommended_candidate_label"] == "Hold for internal review"
    assert rank_payload["objective_pack"]["pack_id"] == "contain_exposure"
    assert rank_payload["candidates"][0]["outcome_score"]["objective_pack_id"] == (
        "contain_exposure"
    )
    assert rank_payload["candidates"][0]["saved_result"] is True


def test_ui_api_saved_enron_workspace_prefers_live_rosetta_for_auto_actions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    workspace_root = tmp_path / "saved_enron_workspace"
    create_workspace_from_template(
        root=workspace_root,
        source_kind="vertical",
        source_ref="b2b_saas",
    )
    _write_saved_context_snapshot(workspace_root)
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    episode = WhatIfEpisodeManifest(
        source="enron",
        source_dir="not-included-in-repo-example",
        workspace_root="workspace",
        organization_name="Enron Corporation",
        organization_domain="enron.com",
        thread_id="thr-master-agreement",
        thread_subject="Master Agreement",
        branch_event_id="enron_bcda1b925800af8c",
        branch_timestamp="2000-09-27T13:42:00Z",
        branch_event=WhatIfEventReference(
            event_id="enron_bcda1b925800af8c",
            timestamp="2000-09-27T13:42:00Z",
            actor_id="debra.perlingiere@enron.com",
            target_id="kathy_gerken@cargill.com",
            event_type="assignment",
            thread_id="thr-master-agreement",
            subject="Master Agreement",
            snippet="Historical branch point.",
            to_recipients=["kathy_gerken@cargill.com"],
        ),
        history_message_count=1,
        future_event_count=84,
        baseline_dataset_path="whatif_baseline_dataset.json",
        content_notice="Historical email bodies are grounded in archive excerpts and metadata.",
        forecast=WhatIfHistoricalScore(backend="historical", risk_score=1.0),
    )
    (workspace_root / "episode_manifest.json").write_text(
        episode.model_dump_json(indent=2),
        encoding="utf-8",
    )
    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(rosetta_dir))
    monkeypatch.setenv("OPENAI_API_KEY", "openai-test-key")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)

    def fake_run_ranked_counterfactual_experiment(world, *args, **kwargs):
        assert world.source == "enron"
        return SimpleNamespace(
            model_dump=lambda mode="json": {
                "label": kwargs["label"],
                "objective_pack": {
                    "pack_id": "contain_exposure",
                    "title": "Contain Exposure",
                },
                "recommended_candidate_label": "Hold internal",
                "candidates": [],
                "artifacts": {
                    "result_json_path": "ranked-result.json",
                    "overview_markdown_path": "ranked-overview.md",
                },
            }
        )

    monkeypatch.setattr(
        workspace_routes,
        "run_ranked_counterfactual_experiment",
        fake_run_ranked_counterfactual_experiment,
    )

    client = TestClient(ui_api.create_ui_app(workspace_root))

    status_response = client.get("/api/workspace/whatif")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["available"] is True
    assert status_payload["source"] == "enron"
    assert status_payload["source_dir"] == str(rosetta_dir.resolve())

    scene_response = client.post(
        "/api/workspace/whatif/scene",
        json={
            "source": "auto",
            "event_id": "enron_bcda1b925800af8c",
            "thread_id": "thr-master-agreement",
        },
    )
    assert scene_response.status_code == 200
    scene_payload = scene_response.json()
    assert scene_payload["branch_event_id"] == "enron_bcda1b925800af8c"

    rank_response = client.post(
        "/api/workspace/whatif/rank",
        json={
            "source": "auto",
            "event_id": "evt-001",
            "thread_id": "thr-external",
            "label": "ranked term-sheet options",
            "objective_pack_id": "contain_exposure",
            "candidates": [
                {
                    "label": "Hold internal",
                    "prompt": "Keep this internal.",
                }
            ],
        },
    )
    assert rank_response.status_code == 200
    rank_payload = rank_response.json()
    assert rank_payload["recommended_candidate_label"] == "Hold internal"


def test_ui_api_saved_bundle_respects_explicit_company_history_source(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bundle_root = tmp_path / "saved_bundle"
    workspace_root = bundle_root / "workspace"
    create_workspace_from_template(
        root=workspace_root,
        source_kind="vertical",
        source_ref="b2b_saas",
    )
    _write_saved_context_snapshot(workspace_root)
    snapshot_path = _write_company_history_fixture(tmp_path / "company_history")
    episode = WhatIfEpisodeManifest(
        source="enron",
        source_dir="not-included-in-repo-example",
        workspace_root="workspace",
        organization_name="Enron Corporation",
        organization_domain="enron.com",
        thread_id="thr-master-agreement",
        thread_subject="Master Agreement",
        branch_event_id="enron_bcda1b925800af8c",
        branch_timestamp="2000-09-27T13:42:00Z",
        branch_event=WhatIfEventReference(
            event_id="enron_bcda1b925800af8c",
            timestamp="2000-09-27T13:42:00Z",
            actor_id="debra.perlingiere@enron.com",
            target_id="kathy_gerken@cargill.com",
            event_type="assignment",
            thread_id="thr-master-agreement",
            subject="Master Agreement",
            snippet="Historical branch point.",
            to_recipients=["kathy_gerken@cargill.com"],
        ),
        history_message_count=1,
        future_event_count=84,
        baseline_dataset_path="whatif_baseline_dataset.json",
        content_notice="Historical email bodies are grounded in archive excerpts and metadata.",
        forecast=WhatIfHistoricalScore(backend="historical", risk_score=1.0),
    )
    (workspace_root / "episode_manifest.json").write_text(
        episode.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (bundle_root / "whatif_experiment_result.json").write_text(
        json.dumps(
            {
                "label": "saved_enron_bundle_run",
                "materialization": {
                    "branch_event_id": "enron_bcda1b925800af8c",
                    "thread_id": "thr-master-agreement",
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("VEI_WHATIF_SOURCE_DIR", str(snapshot_path))

    def fake_materialize_episode(world, *args, **kwargs):
        assert world.source == "company_history"
        return SimpleNamespace(
            model_dump=lambda mode="json": {
                "branch_event_id": kwargs.get("event_id") or "evt-1",
                "thread_id": kwargs.get("thread_id") or "thr-1",
            }
        )

    monkeypatch.setattr(
        workspace_routes,
        "materialize_episode",
        fake_materialize_episode,
    )

    client = TestClient(ui_api.create_ui_app(workspace_root))
    response = client.post(
        "/api/workspace/whatif/open",
        json={
            "source": "company_history",
            "event_id": "enron_bcda1b925800af8c",
            "thread_id": "thr-master-agreement",
            "label": "explicit-company-history",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "company_history"
    assert payload["source_dir"] == str(snapshot_path.resolve())


def test_ui_api_whatif_run_route_returns_experiment_payload(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(rosetta_dir))

    def fake_run_counterfactual_experiment(*args, **kwargs):
        assert "forecast_backend" not in kwargs
        assert "allow_proxy_fallback" not in kwargs
        return SimpleNamespace(
            model_dump=lambda mode="json": {
                "label": kwargs["label"],
                "baseline": {
                    "delivered_event_count": 1,
                    "forecast": {"future_external_event_count": 1},
                },
                "llm_result": {
                    "status": "ok",
                    "summary": "Internal review replaces the outside send.",
                    "delivered_event_count": 2,
                },
                "forecast_result": {
                    "backend": "e_jepa",
                    "summary": "Risk drops and outside sends fall.",
                    "baseline": {"risk_score": 1.0},
                    "predicted": {"risk_score": 0.8},
                },
                "materialization": {
                    "branch_event": {
                        "event_id": "evt-001",
                        "subject": "Draft term sheet",
                        "actor_id": "jeff.skilling@enron.com",
                        "target_id": "outside@lawfirm.com",
                    }
                },
                "artifacts": {
                    "result_json_path": "result.json",
                    "overview_markdown_path": "overview.md",
                },
            }
        )

    monkeypatch.setattr(
        workspace_routes,
        "run_counterfactual_experiment",
        fake_run_counterfactual_experiment,
    )

    client = TestClient(ui_api.create_ui_app(root))
    response = client.post(
        "/api/workspace/whatif/run",
        json={
            "source": "enron",
            "event_id": "evt-001",
            "label": "term-sheet alternate path",
            "prompt": "What if Jeff had kept the term sheet internal?",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["label"] == "term-sheet alternate path"
    assert payload["llm_result"]["status"] == "ok"
    assert payload["forecast_result"]["backend"] == "e_jepa"


def test_ui_api_whatif_run_route_respects_anthropic_key(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(rosetta_dir))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "anthropic-test-key")

    def fake_run_counterfactual_experiment(*args, **kwargs):
        assert kwargs["mode"] == "llm"
        assert kwargs["provider"] == "anthropic"
        return SimpleNamespace(
            model_dump=lambda mode="json": {
                "label": kwargs["label"],
                "llm_result": {"status": "ok"},
                "forecast_result": {"backend": "heuristic_baseline"},
            }
        )

    monkeypatch.setattr(
        workspace_routes,
        "run_counterfactual_experiment",
        fake_run_counterfactual_experiment,
    )

    client = TestClient(ui_api.create_ui_app(root))

    status_response = client.get("/api/workspace/whatif")
    assert status_response.status_code == 200
    assert status_response.json()["llm_available"] is True

    response = client.post(
        "/api/workspace/whatif/run",
        json={
            "source": "enron",
            "event_id": "evt-001",
            "label": "anthropic alternate path",
            "prompt": "What if Jeff had kept the term sheet internal?",
            "mode": "llm",
            "provider": "anthropic",
        },
    )

    assert response.status_code == 200
    assert response.json()["label"] == "anthropic alternate path"


def test_ui_api_whatif_status_lists_available_providers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(rosetta_dir))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "anthropic-test-key")

    client = TestClient(ui_api.create_ui_app(root))
    payload = client.get("/api/workspace/whatif").json()

    assert payload["llm_available"] is True
    assert payload["available_providers"] == ["anthropic"]
    assert payload["default_provider"] == "anthropic"
    assert payload["default_model"]


def test_ui_api_whatif_run_route_falls_back_to_available_provider(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Default provider is openai, but only anthropic key is set.

    The server must transparently swap to anthropic instead of silently
    downgrading to heuristic_baseline.
    """

    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(rosetta_dir))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "anthropic-test-key")

    captured: dict[str, object] = {}

    def fake_run_counterfactual_experiment(*args, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            model_dump=lambda mode="json": {
                "label": kwargs["label"],
                "llm_result": {"status": "ok"},
                "forecast_result": {"backend": "heuristic_baseline"},
            }
        )

    monkeypatch.setattr(
        workspace_routes,
        "run_counterfactual_experiment",
        fake_run_counterfactual_experiment,
    )

    client = TestClient(ui_api.create_ui_app(root))

    response = client.post(
        "/api/workspace/whatif/run",
        json={
            "source": "enron",
            "event_id": "evt-001",
            "label": "fallback path",
            "prompt": "What if Jeff had kept the term sheet internal?",
            "mode": "both",
            # Note: no provider override, so default is "openai"
        },
    )

    assert response.status_code == 200
    assert captured["mode"] == "both"
    assert captured["provider"] == "anthropic"
    # Model must be the anthropic default, not the openai default
    assert "claude" in str(captured["model"]).lower()


def test_ui_api_whatif_run_route_downgrades_to_heuristic_when_no_key(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(rosetta_dir))
    for env_name in (
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "GOOGLE_API_KEY",
        "GEMINI_API_KEY",
        "OPENROUTER_API_KEY",
    ):
        monkeypatch.delenv(env_name, raising=False)

    captured: dict[str, object] = {}

    def fake_run_counterfactual_experiment(*args, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            model_dump=lambda mode="json": {
                "label": kwargs["label"],
                "llm_result": None,
                "forecast_result": {"backend": "heuristic_baseline"},
            }
        )

    monkeypatch.setattr(
        workspace_routes,
        "run_counterfactual_experiment",
        fake_run_counterfactual_experiment,
    )

    client = TestClient(ui_api.create_ui_app(root))
    status = client.get("/api/workspace/whatif").json()
    assert status["llm_available"] is False
    assert status["available_providers"] == []
    assert status["default_provider"] is None

    response = client.post(
        "/api/workspace/whatif/run",
        json={
            "source": "enron",
            "event_id": "evt-001",
            "label": "no key path",
            "prompt": "What if Jeff had kept the term sheet internal?",
            "mode": "both",
        },
    )

    assert response.status_code == 200
    assert captured["mode"] == "heuristic_baseline"


def test_ui_api_whatif_rank_route_returns_ranked_payload(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(rosetta_dir))
    monkeypatch.setenv("OPENAI_API_KEY", "openai-test-key")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)

    def fake_run_ranked_counterfactual_experiment(*args, **kwargs):
        assert kwargs["objective_pack_id"] == "contain_exposure"
        assert kwargs["rollout_count"] == 4
        assert kwargs["shadow_forecast_backend"] == "heuristic_baseline"
        assert [item.label for item in kwargs["candidate_interventions"]] == [
            "Hold internal",
            "Send outside",
        ]
        return SimpleNamespace(
            model_dump=lambda mode="json": {
                "label": kwargs["label"],
                "objective_pack": {
                    "pack_id": "contain_exposure",
                    "title": "Contain Exposure",
                },
                "recommended_candidate_label": "Hold internal",
                "candidates": [
                    {
                        "rank": 1,
                        "intervention": {
                            "label": "Hold internal",
                            "prompt": "Keep this internal.",
                        },
                        "rollout_count": 4,
                        "reason": "Best for contain exposure because it keeps the thread internal.",
                        "outcome_score": {
                            "objective_pack_id": "contain_exposure",
                            "overall_score": 0.91,
                        },
                        "shadow": {
                            "backend": "heuristic_baseline",
                            "outcome_score": {
                                "objective_pack_id": "contain_exposure",
                                "overall_score": 0.62,
                            },
                        },
                    },
                    {
                        "rank": 2,
                        "intervention": {
                            "label": "Send outside",
                            "prompt": "Send it now.",
                        },
                        "rollout_count": 4,
                        "reason": "Lower-ranked because it leaves more exposure in the simulated branches.",
                        "outcome_score": {
                            "objective_pack_id": "contain_exposure",
                            "overall_score": 0.34,
                        },
                        "shadow": {
                            "backend": "heuristic_baseline",
                            "outcome_score": {
                                "objective_pack_id": "contain_exposure",
                                "overall_score": 0.81,
                            },
                        },
                    },
                ],
                "artifacts": {
                    "result_json_path": "ranked-result.json",
                    "overview_markdown_path": "ranked-overview.md",
                },
            }
        )

    monkeypatch.setattr(
        workspace_routes,
        "run_ranked_counterfactual_experiment",
        fake_run_ranked_counterfactual_experiment,
    )

    client = TestClient(ui_api.create_ui_app(root))
    response = client.post(
        "/api/workspace/whatif/rank",
        json={
            "source": "enron",
            "event_id": "evt-001",
            "label": "ranked term-sheet options",
            "objective_pack_id": "contain_exposure",
            "shadow_forecast_backend": "heuristic_baseline",
            "candidates": [
                {
                    "label": "Hold internal",
                    "prompt": "Keep this internal.",
                },
                {
                    "label": "Send outside",
                    "prompt": "Send it now.",
                },
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["recommended_candidate_label"] == "Hold internal"
    assert payload["candidates"][0]["rank"] == 1
    assert payload["candidates"][0]["shadow"]["backend"] == "heuristic_baseline"


def test_ui_api_whatif_rank_route_requires_llm_key(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    monkeypatch.setenv("VEI_WHATIF_ROSETTA_DIR", str(rosetta_dir))
    for env_name in (
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "GOOGLE_API_KEY",
        "GEMINI_API_KEY",
        "OPENROUTER_API_KEY",
    ):
        monkeypatch.delenv(env_name, raising=False)

    client = TestClient(ui_api.create_ui_app(root))
    response = client.post(
        "/api/workspace/whatif/rank",
        json={
            "source": "enron",
            "event_id": "evt-001",
            "label": "ranked term-sheet options",
            "objective_pack_id": "contain_exposure",
            "candidates": [
                {
                    "label": "Hold internal",
                    "prompt": "Keep this internal.",
                }
            ],
        },
    )

    assert response.status_code == 400
    assert "needs an LLM provider key" in response.json()["detail"]
    assert "GOOGLE_API_KEY" in response.json()["detail"]


def test_ui_api_quickstart_service_ops_payloads_keep_one_company_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "service_ops_quickstart"
    alive_pids: set[int] = set()

    def fake_spawn(command: list[str], *, log_path: Path) -> int:
        pid = 5100 + len(alive_pids)
        alive_pids.add(pid)
        return pid

    def fake_stop(pid: int) -> None:
        alive_pids.discard(pid)

    def fake_service_alive(service) -> bool:
        return service.pid in alive_pids

    def fake_wait(_: str, *, timeout_s: float = 20.0) -> None:
        return None

    def fake_fetch(url: str):
        if url.endswith("/healthz"):
            return {"ok": True}
        if url.endswith("/api/workspace"):
            return {"manifest": {"name": "service_ops"}}
        if url.endswith("/api/twin"):
            return {
                "runtime": {
                    "run_id": "external_service_ops_run",
                    "status": "running",
                    "request_count": 1,
                },
                "manifest": {
                    "contract": {
                        "ok": True,
                        "issue_count": 0,
                    }
                },
            }
        if url.endswith("/api/twin/history"):
            return []
        if url.endswith("/api/twin/surfaces"):
            return {"current_tension": "Dispatch is under pressure.", "panels": []}
        return None

    monkeypatch.setattr(pilot_api, "_spawn_service", fake_spawn)
    monkeypatch.setattr(pilot_api, "_stop_pid", fake_stop)
    monkeypatch.setattr(pilot_api, "_service_alive", fake_service_alive)
    monkeypatch.setattr(pilot_api, "_wait_for_ready", fake_wait)
    monkeypatch.setattr(pilot_api, "_fetch_json", fake_fetch)

    state = prepare_playable_workspace(
        root,
        world="service_ops",
        mission="service_day_collision",
    )
    pilot_api.start_pilot(
        root,
        organization_name=state.world_name,
        archetype="service_ops",
        gateway_port=3320,
        studio_port=3311,
    )

    client = TestClient(ui_api.create_ui_app(root))

    workspace_response = client.get("/api/workspace")
    playable_response = client.get("/api/playable")
    governor_response = client.get("/api/workspace/governor")

    assert workspace_response.status_code == 200
    assert playable_response.status_code == 200
    assert governor_response.status_code == 200
    assert workspace_response.json()["manifest"]["title"] == "Clearwater Field Services"
    assert playable_response.json()["world_name"] == "Clearwater Field Services"
    assert (
        governor_response.json()["manifest"]["organization_name"]
        == "Clearwater Field Services"
    )


def test_ui_api_serves_cross_run_diff_over_http(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    run_a = launch_workspace_run(root, runner="workflow", run_id="cross-a")
    run_b = launch_workspace_run(root, runner="workflow", run_id="cross-b")

    client = TestClient(ui_api.create_ui_app(root))
    snapshots_a = client.get(f"/api/runs/{run_a.run_id}/snapshots").json()
    snapshots_b = client.get(f"/api/runs/{run_b.run_id}/snapshots").json()

    response = client.get(
        "/api/runs/diff-cross",
        params={
            "run_a": run_a.run_id,
            "snap_a": snapshots_a[-1]["snapshot_id"],
            "run_b": run_b.run_id,
            "snap_b": snapshots_b[-1]["snapshot_id"],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_a"] == run_a.run_id
    assert payload["run_b"] == run_b.run_id
    assert isinstance(payload["added"], dict)
    assert isinstance(payload["removed"], dict)
    assert isinstance(payload["changed"], dict)


def test_ui_api_returns_400_for_invalid_single_run_snapshot_diff(
    tmp_path: Path,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    manifest = launch_workspace_run(root, runner="workflow")

    client = TestClient(ui_api.create_ui_app(root))
    response = client.get(
        f"/api/runs/{manifest.run_id}/diff",
        params={"snapshot_from": 999999, "snapshot_to": 1},
    )

    assert response.status_code == 400
    assert "snapshot not found" in response.json()["detail"]


def test_ui_api_returns_400_for_invalid_cross_run_snapshot_diff(
    tmp_path: Path,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    run_a = launch_workspace_run(root, runner="workflow", run_id="cross-a")
    run_b = launch_workspace_run(root, runner="workflow", run_id="cross-b")

    client = TestClient(ui_api.create_ui_app(root))
    response = client.get(
        "/api/runs/diff-cross",
        params={
            "run_a": run_a.run_id,
            "snap_a": 999999,
            "run_b": run_b.run_id,
            "snap_b": 1,
        },
    )

    assert response.status_code == 400
    assert "snapshot not found" in response.json()["detail"]


def test_ui_api_serves_living_company_surfaces_for_vertical_runs(
    tmp_path: Path,
) -> None:
    for vertical_name in (
        "real_estate_management",
        "digital_marketing_agency",
        "storage_solutions",
        "service_ops",
    ):
        root = tmp_path / vertical_name
        create_workspace_from_template(
            root=root,
            source_kind="vertical",
            source_ref=vertical_name,
        )
        manifest = launch_workspace_run(root, runner="workflow")
        client = TestClient(ui_api.create_ui_app(root))

        response = client.get(f"/api/runs/{manifest.run_id}/surfaces")

        assert response.status_code == 200
        payload = response.json()
        assert payload["company_name"]
        assert payload["current_tension"]
        panel_map = {panel["surface"]: panel for panel in payload["panels"]}
        assert set(panel_map) == {
            "slack",
            "mail",
            "tickets",
            "docs",
            "approvals",
            "vertical_heartbeat",
        }
        assert panel_map["mail"]["items"]
        if vertical_name == "service_ops":
            assert panel_map["vertical_heartbeat"]["policy"] == {
                "approval_threshold_usd": 1000.0,
                "vip_priority_override": True,
                "billing_hold_on_dispute": True,
                "max_auto_reschedules": 2,
            }


def test_ui_api_serves_exercise_and_dataset_sidecar_payloads(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="real_estate_management",
    )
    client = TestClient(ui_api.create_ui_app(root))

    monkeypatch.setattr(
        ui_api,
        "build_workspace_governor_status",
        lambda *_args, **_kwargs: WorkspaceGovernorStatus(
            exercise={
                "manifest": ExerciseManifest(
                    workspace_root=root,
                    workspace_name="workspace",
                    company_name="Harbor Point Management",
                    archetype="real_estate_management",
                    crisis_name="Tenant Opening Conflict",
                    scenario_variant="tenant_opening_conflict",
                    contract_variant="opening_readiness",
                    success_criteria=["Protect the opening date."],
                    catalog=[
                        ExerciseCatalogItem(
                            scenario_variant="tenant_opening_conflict",
                            crisis_name="Tenant Opening Conflict",
                            summary="Opening is blocked.",
                            contract_variant="opening_readiness",
                            objective_summary="Keep the opening valid.",
                            active=True,
                        )
                    ],
                ).model_dump(mode="json"),
                "comparison": [
                    ExerciseComparisonRow(
                        runner="workflow",
                        label="Workflow baseline",
                        run_id="run_workflow",
                        status="ok",
                        summary="healthy",
                    ).model_dump(mode="json")
                ],
            }
        ),
    )
    monkeypatch.setattr(
        ui_api,
        "load_workspace_dataset_bundle",
        lambda *_args, **_kwargs: DatasetBundle(
            spec=DatasetBuildSpec(output_root=root / "dataset"),
            environment_count=1,
            run_count=3,
            splits=[
                DatasetSplitManifest(
                    split="train",
                    run_count=2,
                    example_count=10,
                    run_ids=["run_a", "run_b"],
                )
            ],
            reward_summary={"success_rate": 1.0},
            generated_at="2026-03-25T18:00:00+00:00",
        ),
    )

    exercise_response = client.get("/api/workspace/governor")
    assert exercise_response.status_code == 200
    assert (
        exercise_response.json()["exercise"]["manifest"]["company_name"]
        == "Harbor Point Management"
    )

    dataset_response = client.get("/api/dataset")
    assert dataset_response.status_code == 200
    assert dataset_response.json()["run_count"] == 3


def test_ui_api_rejects_invalid_runner_before_worker_starts(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    client = TestClient(ui_api.create_ui_app(root))

    response = client.post("/api/runs", json={"runner": "invalid-runner"})
    assert response.status_code == 400
    assert response.json()["detail"] == "runner must be workflow, scripted, bc, or llm"
    runs_response = client.get("/api/runs")
    assert runs_response.json() == []


def test_ui_api_rejects_bc_runner_without_model(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    client = TestClient(ui_api.create_ui_app(root))

    response = client.post("/api/runs", json={"runner": "bc"})
    assert response.status_code == 400
    assert response.json()["detail"] == "bc runner requires bc_model"


def test_ui_api_serves_import_diagnostics_and_provenance(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    import_workspace(
        root=root,
        package_path=get_import_package_example_path("macrocompute_identity_export"),
    )
    generate_workspace_scenarios_from_import(root)
    manifest = launch_workspace_run(
        root,
        runner="workflow",
        scenario_name="oversharing_remediation",
    )

    client = TestClient(ui_api.create_ui_app(root))

    summary_response = client.get("/api/imports/summary")
    assert summary_response.status_code == 200
    assert summary_response.json()["package_name"] == "macrocompute_identity_export"

    identity_flow_response = client.get("/api/identity/flow")
    assert identity_flow_response.status_code == 200
    assert identity_flow_response.json()["active_scenario"] == "default"

    normalization_response = client.get("/api/imports/normalization")
    assert normalization_response.status_code == 200
    assert normalization_response.json()["normalized_counts"]["identity_users"] == 2
    assert (
        normalization_response.json()["identity_reconciliation"]["resolved_count"] >= 2
    )

    review_response = client.get("/api/imports/review")
    assert review_response.status_code == 200
    assert review_response.json()["package"]["name"] == "macrocompute_identity_export"
    assert (
        review_response.json()["normalization_report"]["identity_reconciliation"][
            "subject_count"
        ]
        >= 1
    )

    scenarios_response = client.get("/api/imports/scenarios")
    assert scenarios_response.status_code == 200
    assert any(
        item["name"] == "oversharing_remediation" for item in scenarios_response.json()
    )

    activate_response = client.post(
        "/api/scenarios/activate",
        json={"scenario_name": "oversharing_remediation", "bootstrap_contract": True},
    )
    assert activate_response.status_code == 200
    assert activate_response.json()["name"] == "oversharing_remediation"

    provenance_response = client.get(
        "/api/imports/provenance", params={"object_ref": "drive_share:GDRIVE-2201"}
    )
    assert provenance_response.status_code == 200
    assert provenance_response.json()[0]["origin"] == "imported"

    timeline_response = client.get(f"/api/runs/{manifest.run_id}/timeline")
    assert timeline_response.status_code == 200
    assert any(
        "drive_share:GDRIVE-2201" in item.get("object_refs", [])
        for item in timeline_response.json()
    )
    assert any(
        item.get("graph_intent") == "doc_graph.restrict_drive_share"
        for item in timeline_response.json()
        if item.get("kind") == "workflow_step"
    )


def test_ui_api_serves_event_alias_and_import_sources(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    package_source = get_import_package_example_path("macrocompute_identity_export")
    config_path = tmp_path / "okta.json"
    config_path.write_text(
        '{"base_url":"https://macrocompute.okta.com","token":"test"}',
        encoding="utf-8",
    )

    def fake_sync(sync_root, config, *, source_prefix="okta_live"):
        package_root = Path(sync_root)
        import shutil
        from vei.imports.api import load_import_package

        shutil.copytree(package_source, package_root, dirs_exist_ok=True)
        package = load_import_package(package_root)
        for source in package.sources:
            source.source_kind = "connector_snapshot"
            source.connector_id = source_prefix
        (package_root / "package.json").write_text(
            package.model_dump_json(indent=2), encoding="utf-8"
        )
        return SimpleNamespace(
            connector="okta",
            package_root=package_root,
            package=package,
            record_counts={"users": 2, "groups": 2, "applications": 2},
            metadata={"source_prefix": source_prefix},
        )

    monkeypatch.setattr("vei.workspace.api.sync_okta_import_package", fake_sync)
    sync_workspace_source(
        root,
        connector="okta",
        config_path=config_path,
        source_id="macro_okta",
    )
    manifest = launch_workspace_run(root, runner="workflow")

    client = TestClient(ui_api.create_ui_app(root))

    events_response = client.get(f"/api/runs/{manifest.run_id}/events")
    assert events_response.status_code == 200
    assert events_response.json()[0]["kind"] == "run_started"


def test_ui_api_exposes_vertical_variant_browser(tmp_path: Path) -> None:
    root = tmp_path / "vertical-ui"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="real_estate_management",
    )
    client = TestClient(ui_api.create_ui_app(root))

    scenario_variants = client.get("/api/scenario-variants")
    contract_variants = client.get("/api/contract-variants")
    assert scenario_variants.status_code == 200
    assert contract_variants.status_code == 200
    assert len(scenario_variants.json()) == 4
    assert len(contract_variants.json()) == 3

    activate_scenario = client.post(
        "/api/scenarios/activate",
        json={"variant": "vendor_no_show", "bootstrap_contract": True},
    )
    assert activate_scenario.status_code == 200
    assert activate_scenario.json()["workflow_variant"] == "vendor_no_show"

    activate_contract = client.post(
        "/api/contract-variants/activate",
        json={"variant": "safety_over_speed"},
    )
    assert activate_contract.status_code == 200
    assert activate_contract.json()["metadata"]["vertical_contract_variant"] == (
        "safety_over_speed"
    )

    preview = client.get("/api/scenarios/default/preview")
    assert preview.status_code == 200
    assert preview.json()["active_scenario_variant"] == "vendor_no_show"
    assert preview.json()["active_contract_variant"] == "safety_over_speed"

    sources_response = client.get("/api/imports/sources")
    assert sources_response.status_code == 200
    payload = sources_response.json()
    assert payload["sources"] == []
    assert payload["syncs"] == []
    assert (
        preview.json()["compiled_blueprint"]["asset"]["capability_graphs"]["metadata"][
            "active_scenario_variant"
        ]
        == "vendor_no_show"
    )


def test_ui_api_exposes_story_bundle_and_export_preview(tmp_path: Path) -> None:
    root = tmp_path / "story-ui"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="digital_marketing_agency",
    )
    client = TestClient(ui_api.create_ui_app(root))

    story_response = client.get("/api/story")
    assert story_response.status_code == 200
    story_payload = story_response.json()
    assert story_payload["manifest"]["company_name"] == "Northstar Growth"
    assert story_payload["scenario_variant"] == "campaign_launch_guardrail"
    assert story_payload["contract_variant"] == "launch_safely"
    assert story_payload["presentation"]["beats"][0]["studio_view"] == "presentation"

    presentation_response = client.get("/api/presentation")
    assert presentation_response.status_code == 200
    presentation_payload = presentation_response.json()
    assert presentation_payload["opening_hook"]
    assert len(presentation_payload["primitives"]) == 6

    launch_workspace_run(root, runner="workflow")
    launch_workspace_run(root, runner="scripted")

    story_response = client.get("/api/story")
    assert story_response.status_code == 200
    story_payload = story_response.json()
    assert story_payload["outcome"]["baseline_branch"]
    assert story_payload["kernel_proof"]["baseline"]["events"] > 0

    exports_response = client.get("/api/exports-preview")
    assert exports_response.status_code == 200
    exports_payload = exports_response.json()
    assert [item["name"] for item in exports_payload] == [
        "rl_episode_export",
        "continuous_eval_export",
        "agent_ops_export",
    ]


def test_ui_api_exposes_historical_workspace_without_vertical_story(
    tmp_path: Path,
) -> None:
    root = tmp_path / "historical-ui"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="b2b_saas",
    )
    manifest = load_workspace(root)
    manifest.title = "Enron Corporation"
    manifest.description = "Historical Enron replay workspace"
    write_workspace(root, manifest)
    episode = WhatIfEpisodeManifest(
        source="enron",
        source_dir=tmp_path / "rosetta",
        workspace_root=root,
        organization_name="Enron Corporation",
        organization_domain="enron.com",
        thread_id="thr_master_agreement",
        thread_subject="Master Agreement",
        branch_event_id="enron_branch_001",
        branch_timestamp="2000-09-27T13:42:00Z",
        branch_event=WhatIfEventReference(
            event_id="enron_branch_001",
            timestamp="2000-09-27T13:42:00Z",
            actor_id="debra.perlingiere@enron.com",
            target_id="kathy_gerken@cargill.com",
            event_type="assignment",
            thread_id="thr_master_agreement",
            subject="Master Agreement",
            snippet="Attached for your review is a draft Master Agreement.",
        ),
        history_message_count=6,
        future_event_count=84,
        baseline_dataset_path="whatif_baseline_dataset.json",
        content_notice="Historical email bodies are grounded in archive excerpts and metadata.",
        forecast=WhatIfHistoricalScore(backend="historical", risk_score=1.0),
    )
    (root / "episode_manifest.json").write_text(
        episode.model_dump_json(indent=2),
        encoding="utf-8",
    )

    client = TestClient(ui_api.create_ui_app(root))

    story_response = client.get("/api/story")
    assert story_response.status_code == 200
    assert story_response.json() == {}

    historical_response = client.get("/api/workspace/historical")
    assert historical_response.status_code == 200
    payload = historical_response.json()
    assert payload["organization_name"] == "Enron Corporation"
    assert payload["thread_subject"] == "Master Agreement"
    assert payload["branch_event"]["actor_id"] == "debra.perlingiere@enron.com"

    fidelity_response = client.get("/api/fidelity")
    assert fidelity_response.status_code == 200
    assert fidelity_response.json() == {}


def test_ui_api_returns_clean_400_for_malformed_saved_bundle_manifest(
    tmp_path: Path,
) -> None:
    bundle_root = tmp_path / "bundle"
    workspace_root = bundle_root / "workspace"
    workspace_root.mkdir(parents=True)
    (bundle_root / "whatif_experiment_result.json").write_text(
        json.dumps({}),
        encoding="utf-8",
    )
    (workspace_root / "episode_manifest.json").write_text(
        json.dumps({"workspace_root": "workspace"}),
        encoding="utf-8",
    )

    client = TestClient(ui_api.create_ui_app(workspace_root))

    historical_response = client.get("/api/workspace/historical")
    assert historical_response.status_code == 400
    assert "invalid saved workspace manifest" in historical_response.json()["detail"]

    whatif_response = client.get("/api/workspace/whatif")
    assert whatif_response.status_code == 400
    assert "invalid saved workspace manifest" in whatif_response.json()["detail"]

    scene_response = client.post(
        "/api/workspace/whatif/scene",
        json={"source": "auto"},
    )
    assert scene_response.status_code == 400
    assert "invalid saved workspace manifest" in scene_response.json()["detail"]


def test_ui_api_live_source_ignores_malformed_manifest_without_saved_bundle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    workspace_root = tmp_path / "workspace"
    create_workspace_from_template(
        root=workspace_root,
        source_kind="vertical",
        source_ref="b2b_saas",
    )
    snapshot_path = _write_company_history_fixture(tmp_path / "company_history")
    (workspace_root / "episode_manifest.json").write_text(
        json.dumps({"workspace_root": "workspace"}),
        encoding="utf-8",
    )
    monkeypatch.setenv("VEI_WHATIF_SOURCE_DIR", str(snapshot_path))

    client = TestClient(ui_api.create_ui_app(workspace_root))
    response = client.get("/api/workspace/whatif")

    assert response.status_code == 200
    payload = response.json()
    assert payload["available"] is True
    assert payload["source"] == "company_history"
    assert payload["source_dir"] == str(snapshot_path.resolve())
    assert payload["saved_bundle_active"] is False
    assert payload["timeline_available"] is True
    assert payload["timeline_readiness"]["available"] is True


def test_ui_api_supports_benchmark_audit_root(tmp_path: Path) -> None:
    root = tmp_path / "benchmark-audit-ui"
    _write_benchmark_audit_fixture(root)

    client = TestClient(ui_api.create_ui_app(root))

    workspace_response = client.get("/api/workspace")
    assert workspace_response.status_code == 200
    workspace_payload = workspace_response.json()
    assert workspace_payload["manifest"]["metadata"]["ui_mode"] == "benchmark_audit"

    scenarios_response = client.get("/api/scenarios")
    assert scenarios_response.status_code == 200
    assert scenarios_response.json() == []

    runs_response = client.get("/api/runs")
    assert runs_response.status_code == 200
    assert runs_response.json() == []

    audit_response = client.get("/api/workspace/whatif/audit")
    assert audit_response.status_code == 200
    audit_payload = audit_response.json()
    assert audit_payload["total"] == 1
    assert audit_payload["items"][0]["case_id"] == "case_master_agreement"
    assert "Master Agreement" in audit_payload["items"][0]["dossier_text"]
    assert "Public Company Context" in audit_payload["items"][0]["dossier_text"]


def test_ui_api_returns_empty_audit_queue_until_judge_results_exist(
    tmp_path: Path,
) -> None:
    root = tmp_path / "benchmark-audit-build-only"
    _write_benchmark_audit_fixture(root, include_judge=False)

    client = TestClient(ui_api.create_ui_app(root))

    audit_response = client.get("/api/workspace/whatif/audit")
    assert audit_response.status_code == 200
    assert audit_response.json() == {"items": [], "total": 0}


def test_ui_api_audit_submission_preserves_all_completed_records(
    tmp_path: Path,
) -> None:
    root = tmp_path / "benchmark-audit-submit"
    _write_benchmark_audit_fixture(root)

    client = TestClient(ui_api.create_ui_app(root))
    route = (
        "/api/workspace/whatif/audit/" "case_master_agreement/minimize_enterprise_risk"
    )
    first_response = client.post(
        route,
        json={
            "reviewer_id": "auditor-1",
            "ordered_candidate_ids": [
                "legal_hold_internal",
                "narrow_external_status",
                "broad_external_send",
            ],
            "pairwise_comparisons": [
                {
                    "left_candidate_id": "legal_hold_internal",
                    "right_candidate_id": "narrow_external_status",
                    "preferred_candidate_id": "legal_hold_internal",
                    "confidence": 0.8,
                    "rationale": "Keep the draft inside Enron.",
                },
                {
                    "left_candidate_id": "legal_hold_internal",
                    "right_candidate_id": "broad_external_send",
                    "preferred_candidate_id": "legal_hold_internal",
                    "confidence": 0.8,
                    "rationale": "Outside spread rises with the broad send.",
                },
                {
                    "left_candidate_id": "narrow_external_status",
                    "right_candidate_id": "broad_external_send",
                    "preferred_candidate_id": "narrow_external_status",
                    "confidence": 0.7,
                    "rationale": "A status note stays narrower.",
                },
            ],
            "confidence": 0.8,
            "notes": "First review",
        },
    )
    assert first_response.status_code == 200
    assert first_response.json()["agreement_with_judge"] is True

    second_response = client.post(
        route,
        json={
            "reviewer_id": "auditor-2",
            "ordered_candidate_ids": [
                "narrow_external_status",
                "legal_hold_internal",
                "broad_external_send",
            ],
            "pairwise_comparisons": [
                {
                    "left_candidate_id": "legal_hold_internal",
                    "right_candidate_id": "narrow_external_status",
                    "preferred_candidate_id": "narrow_external_status",
                    "confidence": 0.6,
                    "rationale": "A quick note keeps the thread moving.",
                },
                {
                    "left_candidate_id": "legal_hold_internal",
                    "right_candidate_id": "broad_external_send",
                    "preferred_candidate_id": "legal_hold_internal",
                    "confidence": 0.7,
                    "rationale": "The broad send still looks riskiest.",
                },
                {
                    "left_candidate_id": "narrow_external_status",
                    "right_candidate_id": "broad_external_send",
                    "preferred_candidate_id": "narrow_external_status",
                    "confidence": 0.7,
                    "rationale": "The note stays narrower than the draft.",
                },
            ],
            "confidence": 0.65,
            "notes": "Second review",
        },
    )
    assert second_response.status_code == 200
    assert second_response.json()["agreement_with_judge"] is False

    completed_records = json.loads(
        (root / "completed_audit_records.json").read_text(encoding="utf-8")
    )
    assert len(completed_records) == 2
    assert [item["reviewer_id"] for item in completed_records] == [
        "auditor-1",
        "auditor-2",
    ]
    assert all(item["submission_id"] for item in completed_records)
    assert all(item["submitted_at"] for item in completed_records)

    audit_response = client.get("/api/workspace/whatif/audit")
    assert audit_response.status_code == 200
    assert audit_response.json()["items"][0]["status"] == "completed"


def test_ui_api_audit_submission_rejects_duplicate_rankings(tmp_path: Path) -> None:
    root = tmp_path / "benchmark-audit-invalid-submit"
    _write_benchmark_audit_fixture(root)

    client = TestClient(ui_api.create_ui_app(root))
    response = client.post(
        "/api/workspace/whatif/audit/case_master_agreement/minimize_enterprise_risk",
        json={
            "reviewer_id": "auditor-1",
            "ordered_candidate_ids": [
                "legal_hold_internal",
                "legal_hold_internal",
                "broad_external_send",
            ],
            "pairwise_comparisons": [
                {
                    "left_candidate_id": "legal_hold_internal",
                    "right_candidate_id": "narrow_external_status",
                    "preferred_candidate_id": "legal_hold_internal",
                },
                {
                    "left_candidate_id": "legal_hold_internal",
                    "right_candidate_id": "broad_external_send",
                    "preferred_candidate_id": "legal_hold_internal",
                },
                {
                    "left_candidate_id": "narrow_external_status",
                    "right_candidate_id": "broad_external_send",
                    "preferred_candidate_id": "narrow_external_status",
                },
            ],
        },
    )
    assert response.status_code == 400
    assert (
        response.json()["detail"]
        == "ordered_candidate_ids must contain each candidate exactly once"
    )


def test_ui_api_exposes_playable_mission_mode(tmp_path: Path) -> None:
    root = tmp_path / "playable-ui"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="real_estate_management",
    )
    client = TestClient(ui_api.create_ui_app(root))

    missions_response = client.get("/api/missions")
    assert missions_response.status_code == 200
    missions_payload = missions_response.json()
    assert len(missions_payload) == 5
    assert missions_payload[0]["vertical_name"] == "real_estate_management"

    fidelity_response = client.get("/api/fidelity")
    assert fidelity_response.status_code == 200
    fidelity_payload = fidelity_response.json()
    assert fidelity_payload["company_name"] == "Harbor Point Management"
    assert len(fidelity_payload["cases"]) == 5

    start_response = client.post(
        "/api/missions/start",
        json={"mission_name": "tenant_opening_conflict"},
    )
    assert start_response.status_code == 200
    mission_state = start_response.json()
    assert mission_state["run_id"].startswith("human_play")
    assert mission_state["scorecard"]["move_count"] == 0
    assert mission_state["available_moves"]

    move_id = mission_state["available_moves"][0]["move_id"]
    move_response = client.post(
        f"/api/missions/{mission_state['run_id']}/moves/{move_id}"
    )
    assert move_response.status_code == 200
    moved_state = move_response.json()
    assert moved_state["turn_index"] >= 1
    assert len(moved_state["executed_moves"]) == 1

    exports_response = client.get(f"/api/missions/{mission_state['run_id']}/exports")
    assert exports_response.status_code == 200
    assert [item["name"] for item in exports_response.json()] == [
        "rl",
        "eval",
        "agent_ops",
    ]

    branch_response = client.post(
        f"/api/missions/{mission_state['run_id']}/branch", json={}
    )
    assert branch_response.status_code == 200
    branch_payload = branch_response.json()
    assert branch_payload["run_id"].startswith("human_branch")

    activate_response = client.post(
        "/api/missions/activate",
        json={
            "mission_name": "vendor_no_show",
            "objective_variant": "safety_over_speed",
        },
    )
    assert activate_response.status_code == 200

    playable_response = client.get("/api/playable")
    assert playable_response.status_code == 200
    assert playable_response.json()["mission"]["mission_name"] == "vendor_no_show"
    assert playable_response.json()["run_id"] is None

    ready_state_response = client.get("/api/missions/state")
    assert ready_state_response.status_code == 200
    assert ready_state_response.json() == {}


def test_ui_api_supports_service_ops_policy_replay(tmp_path: Path) -> None:
    root = tmp_path / "service-ops-replay-ui"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="service_ops",
    )
    client = TestClient(ui_api.create_ui_app(root))

    start_response = client.post(
        "/api/missions/start",
        json={"mission_name": "service_day_collision"},
    )
    assert start_response.status_code == 200
    mission_state = start_response.json()

    knobs_response = client.get(f"/api/runs/{mission_state['run_id']}/policy-knobs")
    assert knobs_response.status_code == 200
    knob_fields = {item["field"] for item in knobs_response.json()["knobs"]}
    assert knob_fields == {
        "approval_threshold_usd",
        "vip_priority_override",
        "billing_hold_on_dispute",
        "max_auto_reschedules",
    }

    replay_response = client.post(
        f"/api/runs/{mission_state['run_id']}/replay-with-policy",
        json={
            "policy_delta": {
                "billing_hold_on_dispute": False,
                "approval_threshold_usd": 2500,
            }
        },
    )
    assert replay_response.status_code == 200
    replay_payload = replay_response.json()
    assert replay_payload["replay_run_id"] != mission_state["run_id"]

    surfaces_response = client.get(
        f"/api/runs/{replay_payload['replay_run_id']}/surfaces"
    )
    assert surfaces_response.status_code == 200
    panel_map = {
        panel["surface"]: panel for panel in surfaces_response.json()["panels"]
    }
    assert panel_map["vertical_heartbeat"]["policy"]["billing_hold_on_dispute"] is False
    assert panel_map["vertical_heartbeat"]["policy"]["approval_threshold_usd"] == 2500.0


def test_ui_api_serves_governor_workspace_controls(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "pilot-ui"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="digital_marketing_agency",
    )
    payload = _sample_pilot_status(root)

    monkeypatch.setattr(
        ui_api,
        "build_workspace_governor_status",
        lambda *_args, **_kwargs: _sample_workspace_governor_status(root),
    )
    monkeypatch.setattr(
        ui_api,
        "reset_twin",
        lambda _: payload.model_copy(update={"request_count": 0}),
    )
    monkeypatch.setattr(
        ui_api,
        "finalize_twin",
        lambda _: payload.model_copy(update={"twin_status": "completed"}),
    )
    monkeypatch.setattr(ui_api, "sync_twin", lambda _: payload)
    monkeypatch.setattr(
        ui_api,
        "pause_twin_orchestrator_agent",
        lambda _root, _agent_id: payload,
    )
    monkeypatch.setattr(
        ui_api,
        "resume_twin_orchestrator_agent",
        lambda _root, _agent_id: payload,
    )
    monkeypatch.setattr(
        ui_api,
        "comment_on_twin_orchestrator_task",
        lambda _root, _task_id, body: payload,
    )
    monkeypatch.setattr(
        ui_api,
        "approve_twin_orchestrator_approval",
        lambda _root, _approval_id, decision_note=None: payload,
    )
    monkeypatch.setattr(
        ui_api,
        "reject_twin_orchestrator_approval",
        lambda _root, _approval_id, decision_note=None: payload,
    )
    monkeypatch.setattr(
        ui_api,
        "request_twin_orchestrator_revision",
        lambda _root, _approval_id, decision_note=None: payload,
    )

    client = TestClient(ui_api.create_ui_app(root))

    page_response = client.get("/pilot")
    assert page_response.status_code == 404

    status_response = client.get("/api/workspace/governor")
    assert status_response.status_code == 200
    assert (
        status_response.json()["manifest"]["organization_name"] == "Pinnacle Analytics"
    )

    reset_response = client.post("/api/workspace/governor/reset")
    assert reset_response.status_code == 200
    assert reset_response.json()["request_count"] == 0

    finalize_response = client.post("/api/workspace/governor/finalize")
    assert finalize_response.status_code == 200
    assert finalize_response.json()["twin_status"] == "completed"

    sync_response = client.post("/api/workspace/governor/sync")
    assert sync_response.status_code == 200
    assert sync_response.json()["manifest"]["organization_name"] == "Pinnacle Analytics"

    pause_response = client.post(
        "/api/workspace/governor/orchestrator/agents/paperclip%3Aeng-1/pause"
    )
    assert pause_response.status_code == 200

    resume_response = client.post(
        "/api/workspace/governor/orchestrator/agents/paperclip%3Aeng-1/resume"
    )
    assert resume_response.status_code == 200

    comment_response = client.post(
        "/api/workspace/governor/orchestrator/tasks/paperclip%3Aissue-1/comment",
        json={"body": "Ask for a safer rollout plan."},
    )
    assert comment_response.status_code == 200

    approve_response = client.post(
        "/api/workspace/governor/orchestrator/approvals/paperclip%3Aapproval-1/approve",
        json={"decision_note": "Approved for the first engineering hire."},
    )
    assert approve_response.status_code == 200

    revision_response = client.post(
        "/api/workspace/governor/orchestrator/approvals/paperclip%3Aapproval-1/request-revision",
        json={"decision_note": "Tighten the budget case first."},
    )
    assert revision_response.status_code == 200

    reject_response = client.post(
        "/api/workspace/governor/orchestrator/approvals/paperclip%3Aapproval-1/reject",
        json={"decision_note": "Not aligned with current plan."},
    )
    assert reject_response.status_code == 200


def test_ui_api_serves_workforce_payload_from_gateway_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workforce-ui"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="service_ops",
    )
    expected = {
        "summary": {
            "provider": "paperclip",
            "observed_agent_count": 2,
            "task_count": 3,
        }
    }

    def fake_gateway(*_args, **_kwargs):
        raise HTTPException(status_code=503, detail="gateway unavailable")

    monkeypatch.setattr(workspace_routes, "gateway_json_request", fake_gateway)
    monkeypatch.setattr(
        workspace_routes,
        "load_workspace_workforce_payload",
        lambda _root: expected,
    )

    client = TestClient(ui_api.create_ui_app(root))

    response = client.get("/api/workforce")

    assert response.status_code == 200
    assert response.json() == expected


def _sample_pilot_status(root: Path) -> TwinLaunchStatus:
    return TwinLaunchStatus(
        manifest=TwinLaunchManifest(
            workspace_root=root,
            workspace_name="pinnacle",
            organization_name="Pinnacle Analytics",
            organization_domain="pinnacle.example.com",
            archetype="b2b_saas",
            crisis_name="Renewal save",
            studio_url="http://127.0.0.1:3011",
            control_room_url="http://127.0.0.1:3011/",
            gateway_url="http://127.0.0.1:3020",
            gateway_status_url="http://127.0.0.1:3020/api/twin",
            bearer_token="pilot-token",
            supported_surfaces=[
                CompatibilitySurfaceSpec(
                    name="slack",
                    title="Slack",
                    base_path="/slack/api",
                ),
                CompatibilitySurfaceSpec(
                    name="jira",
                    title="Jira",
                    base_path="/jira/rest/api/3",
                ),
            ],
            recommended_first_move="Read Slack and Jira, then send one customer-safe update.",
            sample_client_path="/tmp/governor_client.py",
        ),
        runtime=TwinLaunchRuntime(
            workspace_root=root,
            services=[
                TwinServiceRecord(
                    name="gateway",
                    host="127.0.0.1",
                    port=3020,
                    url="http://127.0.0.1:3020",
                    pid=4101,
                    state="running",
                ),
                TwinServiceRecord(
                    name="studio",
                    host="127.0.0.1",
                    port=3011,
                    url="http://127.0.0.1:3011",
                    pid=4102,
                    state="running",
                ),
            ],
            started_at="2026-03-25T18:00:00+00:00",
            updated_at="2026-03-25T18:05:00+00:00",
        ),
        active_run="external_renewal_run",
        twin_status="running",
        request_count=4,
        services_ready=True,
        outcome=TwinOutcomeSummary(
            status="running",
            contract_ok=False,
            issue_count=2,
            summary="The renewal is still at risk and needs another action.",
            latest_tool="slack.send_message",
            current_tension="Customer trust is slipping.",
            affected_surfaces=["Email", "Slack"],
        ),
    )


def _sample_workspace_governor_status(root: Path) -> WorkspaceGovernorStatus:
    pilot = _sample_pilot_status(root)
    return WorkspaceGovernorStatus(
        governor={"config": {"connector_mode": "sim", "demo_mode": False}},
        manifest=pilot.manifest.model_dump(mode="json"),
        runtime=pilot.runtime.model_dump(mode="json"),
        active_run=pilot.active_run,
        twin_status=pilot.twin_status,
        request_count=pilot.request_count,
        services_ready=pilot.services_ready,
        outcome=pilot.outcome.model_dump(mode="json"),
    )
