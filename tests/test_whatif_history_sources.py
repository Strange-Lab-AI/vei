from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from vei.cli.vei import app
from vei.whatif import load_world, search_events
from vei.whatif.benchmark import choose_branch_event as benchmark_choose_branch_event
from vei.whatif.corpus import choose_branch_event as corpus_choose_branch_event


def test_load_company_history_world_supports_docs_only_snapshot(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_snapshot(
        tmp_path / "docs_only",
        sources=[
            {
                "provider": "google",
                "captured_at": "2026-03-01T10:00:00Z",
                "status": "ok",
                "record_counts": {"documents": 1, "drive_shares": 1},
                "data": {
                    "documents": [
                        {
                            "doc_id": "doc-legal-1",
                            "title": "Legal review tracker",
                            "body": "Procurement hold until counsel signs off.",
                            "owner": "maya@acme.example.com",
                            "modified_time": "2026-03-01T09:00:00Z",
                            "comments": [
                                {
                                    "id": "comment-1",
                                    "author": "legal@acme.example.com",
                                    "body": "Need one more legal pass.",
                                    "created": "2026-03-01T09:05:00Z",
                                }
                            ],
                            "permissions": [
                                {
                                    "id": "perm-1",
                                    "shared_with": ["legal@acme.example.com"],
                                    "granted_by": "maya@acme.example.com",
                                    "created": "2026-03-01T09:06:00Z",
                                }
                            ],
                        }
                    ],
                    "drive_shares": [
                        {
                            "doc_id": "doc-legal-1",
                            "shared_with": ["legal@acme.example.com"],
                        }
                    ],
                },
            }
        ],
    )

    world = load_world(source="company_history", source_dir=snapshot_path)

    assert world.summary.event_count >= 3
    assert {event.surface for event in world.events} == {"docs"}
    assert any(event.event_type == "reply" for event in world.events)
    result = search_events(world, query="legal pass")
    assert result.match_count >= 1


def test_load_company_history_world_supports_crm_and_salesforce_only_snapshots(
    tmp_path: Path,
) -> None:
    for provider in ("crm", "salesforce"):
        snapshot_path = _write_snapshot(
            tmp_path / provider,
            sources=[
                {
                    "provider": provider,
                    "captured_at": "2026-03-01T10:00:00Z",
                    "status": "ok",
                    "record_counts": {"companies": 1, "contacts": 1, "deals": 1},
                    "data": {
                        "companies": [
                            {
                                "id": "acct-1",
                                "name": "Acme Buyer",
                                "created_ms": 1_772_329_200_000,
                            }
                        ],
                        "contacts": [
                            {
                                "id": "contact-1",
                                "email": "buyer@acmebuyer.example.com",
                                "first_name": "Buyer",
                                "last_name": "One",
                                "company_id": "acct-1",
                                "created_ms": 1_772_329_260_000,
                            }
                        ],
                        "deals": [
                            {
                                "id": "deal-1",
                                "name": "Acme expansion",
                                "stage": "closed_won",
                                "owner": "maya@acme.example.com",
                                "company_id": "acct-1",
                                "contact_id": "contact-1",
                                "created_ms": 1_772_329_200_000,
                                "updated_ms": 1_772_329_560_000,
                                "closed_ms": 1_772_329_860_000,
                                "history": [
                                    {
                                        "id": "h-1",
                                        "field": "owner",
                                        "from": "maya@acme.example.com",
                                        "to": "owner2@acme.example.com",
                                        "changed_by": "vp.sales@acme.example.com",
                                        "timestamp": "2026-03-01T09:06:00Z",
                                    }
                                ],
                            }
                        ],
                    },
                }
            ],
        )

        world = load_world(source="company_history", source_dir=snapshot_path)

        assert world.summary.event_count >= 4
        assert {event.surface for event in world.events} == {"crm"}
        assert any(event.event_type == "assignment" for event in world.events)
        assert any("Closed won" in event.snippet for event in world.events)


def test_load_company_history_world_supports_mixed_docs_and_crm_snapshot(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_snapshot(
        tmp_path / "mixed",
        sources=[
            {
                "provider": "google",
                "captured_at": "2026-03-01T10:00:00Z",
                "status": "ok",
                "record_counts": {"documents": 1},
                "data": {
                    "documents": [
                        {
                            "doc_id": "doc-1",
                            "title": "Renewal plan",
                            "body": "Internal review only.",
                            "owner": "maya@acme.example.com",
                            "modified_time": "2026-03-01T09:00:00Z",
                        }
                    ]
                },
            },
            {
                "provider": "crm",
                "captured_at": "2026-03-01T10:00:00Z",
                "status": "ok",
                "record_counts": {"deals": 1},
                "data": {
                    "deals": [
                        {
                            "id": "deal-1",
                            "name": "Acme renewal",
                            "stage": "legal_review",
                            "owner": "maya@acme.example.com",
                            "created_ms": 1_772_329_200_000,
                            "updated_ms": 1_772_329_560_000,
                        }
                    ]
                },
            },
        ],
    )

    world = load_world(source="company_history", source_dir=snapshot_path)

    assert {"crm", "docs"} <= {event.surface for event in world.events}
    assert world.situation_graph is not None
    assert len(world.situation_graph.clusters) == 1
    cluster = world.situation_graph.clusters[0]
    assert set(cluster.surfaces) == {"crm", "docs"}


def test_load_company_history_world_keeps_unrelated_same_week_threads_apart(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_snapshot(
        tmp_path / "unrelated",
        sources=[
            {
                "provider": "google",
                "captured_at": "2026-03-01T10:00:00Z",
                "status": "ok",
                "record_counts": {"documents": 1},
                "data": {
                    "documents": [
                        {
                            "doc_id": "doc-1",
                            "title": "Procurement checklist",
                            "body": "Internal checklist only.",
                            "owner": "docs@acme.example.com",
                            "modified_time": "2026-03-01T09:00:00Z",
                        }
                    ]
                },
            },
            {
                "provider": "crm",
                "captured_at": "2026-03-01T10:00:00Z",
                "status": "ok",
                "record_counts": {"deals": 1},
                "data": {
                    "deals": [
                        {
                            "id": "deal-1",
                            "name": "Northwind expansion",
                            "stage": "legal_review",
                            "owner": "sales@acme.example.com",
                            "created_ms": 1_772_329_200_000,
                            "updated_ms": 1_772_329_560_000,
                        }
                    ]
                },
            },
        ],
    )

    world = load_world(source="company_history", source_dir=snapshot_path)

    assert world.situation_graph is not None
    assert world.situation_graph.clusters == []


def test_benchmark_module_uses_shared_branch_event_chooser() -> None:
    assert benchmark_choose_branch_event is corpus_choose_branch_event


def test_smoke_and_visualize_cli_help_commands_load() -> None:
    runner = CliRunner()

    smoke_result = runner.invoke(app, ["smoke", "run", "--help"])
    assert smoke_result.exit_code == 0, smoke_result.output
    assert "--transport" in smoke_result.output

    visualize_result = runner.invoke(app, ["visualize", "flow", "--help"])
    assert visualize_result.exit_code == 0, visualize_result.output
    assert "--out" in visualize_result.output


def _write_snapshot(root: Path, *, sources: list[dict[str, object]]) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    snapshot_path = root / "context_snapshot.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "version": "1",
                "organization_name": "Acme Cloud",
                "organization_domain": "acme.example.com",
                "captured_at": "2026-03-01T10:00:00Z",
                "sources": sources,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return snapshot_path
