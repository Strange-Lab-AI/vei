from __future__ import annotations

import json
import shutil
from pathlib import Path

from typer.testing import CliRunner

from vei.cli.vei import app
from vei.context.models import ContextSnapshot, ContextSourceResult
from vei.context import normalize_raw_exports, verify_context_snapshot
from vei.imports.api import get_import_package_example_path
from vei.whatif.models import WhatIfPublicContext


def test_normalize_raw_exports_merges_mixed_sources(tmp_path: Path) -> None:
    source_dir = tmp_path / "exports"
    _write_slack_export(source_dir)
    (source_dir / "google.json").write_text(
        json.dumps(
            {
                "users": [
                    {
                        "id": "g-1",
                        "email": "maya@acme.example.com",
                        "name": "Maya Ops",
                    }
                ],
                "documents": [
                    {
                        "doc_id": "doc-1",
                        "title": "Renewal plan",
                        "body": "Internal legal review stays open.",
                        "owner": "maya@acme.example.com",
                        "modified_time": "2026-03-01T10:00:00Z",
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (source_dir / "crm.json").write_text(
        json.dumps(
            {
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
                        "stage": "legal_review",
                        "owner": "maya@acme.example.com",
                        "company_id": "acct-1",
                        "contact_id": "contact-1",
                        "created_ms": 1_772_329_200_000,
                        "updated_ms": 1_772_329_560_000,
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    snapshot = normalize_raw_exports(
        source_dir,
        organization_name="Acme Cloud",
        organization_domain="acme.example.com",
    )

    assert snapshot.organization_name == "Acme Cloud"
    assert snapshot.organization_domain == "acme.example.com"
    assert {source.provider for source in snapshot.sources} == {
        "crm",
        "google",
        "slack",
    }
    assert snapshot.source_for("slack").status == "ok"  # type: ignore[union-attr]
    assert snapshot.source_for("google").record_counts["documents"] == 1  # type: ignore[union-attr]
    assert snapshot.source_for("crm").record_counts["deals"] == 1  # type: ignore[union-attr]

    verification = verify_context_snapshot(snapshot)
    assert verification.ok is True
    assert verification.error_count == 0


def test_context_cli_normalize_verify_and_public(tmp_path: Path) -> None:
    runner = CliRunner()
    source_dir = tmp_path / "exports"
    _write_slack_export(source_dir)
    (source_dir / "google.json").write_text(
        json.dumps(
            {
                "documents": [
                    {
                        "doc_id": "doc-2",
                        "title": "Recovery plan",
                        "body": "Share only with the named team.",
                        "owner": "maya@acme.example.com",
                        "modified_time": "2026-03-02T11:00:00Z",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    snapshot_path = tmp_path / "context_snapshot.json"
    normalize_result = runner.invoke(
        app,
        [
            "context",
            "normalize",
            "--source-dir",
            str(source_dir),
            "--org",
            "Acme Cloud",
            "--domain",
            "acme.example.com",
            "--output",
            str(snapshot_path),
        ],
    )
    assert normalize_result.exit_code == 0, normalize_result.output
    assert snapshot_path.exists()

    verify_result = runner.invoke(
        app,
        [
            "context",
            "verify",
            "--snapshot",
            str(snapshot_path),
        ],
    )
    assert verify_result.exit_code == 0, verify_result.output
    verify_payload = json.loads(verify_result.output)
    assert verify_payload["ok"] is True
    assert verify_payload["snapshot_path"].endswith("context_snapshot.json")

    public_path = tmp_path / "whatif_public_context.json"
    public_result = runner.invoke(
        app,
        [
            "context",
            "public",
            "--company",
            "Acme Cloud",
            "--domain",
            "acme.example.com",
            "--output",
            str(public_path),
        ],
    )
    assert public_result.exit_code == 0, public_result.output
    template = WhatIfPublicContext.model_validate_json(
        public_path.read_text(encoding="utf-8")
    )
    assert template.organization_name == "Acme Cloud"
    assert template.organization_domain == "acme.example.com"


def test_normalize_raw_exports_wraps_legacy_archive_json(tmp_path: Path) -> None:
    legacy_path = tmp_path / "legacy_archive.json"
    legacy_path.write_text(
        json.dumps(
            {
                "organization_name": "Legacy Co",
                "organization_domain": "legacy.example.com",
                "captured_at": "2026-03-01T10:00:00Z",
                "threads": [
                    {
                        "thread_id": "legacy-1",
                        "subject": "Draft note",
                        "messages": [
                            {
                                "message_id": "msg-1",
                                "from": "alex@legacy.example.com",
                                "to": "legal@legacy.example.com",
                                "subject": "Draft note",
                                "body_text": "Please review.",
                                "timestamp": "2026-03-01T09:00:00Z",
                            }
                        ],
                    }
                ],
                "actors": [{"actor_id": "alex@legacy.example.com"}],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    snapshot = normalize_raw_exports(
        legacy_path,
        organization_name="Legacy Co",
        organization_domain="legacy.example.com",
    )

    source = snapshot.source_for("mail_archive")

    assert source is not None
    assert source.status == "ok"
    assert source.record_counts["threads"] == 1
    assert source.record_counts["actors"] == 1


def test_normalize_raw_exports_supports_import_package_fixture(tmp_path: Path) -> None:
    source = get_import_package_example_path("macrocompute_identity_export")
    package_path = tmp_path / "macrocompute_identity_export"
    shutil.copytree(source, package_path)

    snapshot = normalize_raw_exports(
        package_path,
        organization_name="",
        organization_domain="",
    )

    assert snapshot.organization_name == "MacroCompute"
    assert snapshot.organization_domain == "macrocompute.example"
    assert {"google", "jira", "crm"} <= {source.provider for source in snapshot.sources}


def test_normalize_raw_exports_merges_existing_snapshot_with_raw_exports(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "exports"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "context_snapshot.json").write_text(
        json.dumps(
            {
                "version": "1",
                "organization_name": "Acme Cloud",
                "organization_domain": "acme.example.com",
                "captured_at": "2026-03-01T10:00:00Z",
                "sources": [
                    {
                        "provider": "google",
                        "captured_at": "2026-03-01T09:00:00Z",
                        "status": "ok",
                        "record_counts": {"documents": 1},
                        "data": {
                            "documents": [
                                {
                                    "doc_id": "doc-existing",
                                    "title": "Existing plan",
                                    "modified_time": "2026-03-01T08:00:00Z",
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
    (source_dir / "google.json").write_text(
        json.dumps(
            {
                "documents": [
                    {
                        "doc_id": "doc-new",
                        "title": "New plan",
                        "modified_time": "2026-03-02T08:00:00Z",
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    snapshot = normalize_raw_exports(
        source_dir,
        organization_name="Acme Cloud",
        organization_domain="acme.example.com",
    )

    google = snapshot.source_for("google")

    assert google is not None
    assert google.record_counts["documents"] == 2
    assert {doc["doc_id"] for doc in google.data["documents"]} == {
        "doc-existing",
        "doc-new",
    }


def test_normalize_raw_exports_reports_partial_exports_and_errors(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "exports"
    _write_slack_export(source_dir)
    (source_dir / "jira.json").write_text("{bad json", encoding="utf-8")
    (source_dir / "google.json").write_text(
        json.dumps(
            {
                "documents": [
                    {
                        "doc_id": "doc-1",
                        "title": "Recovery plan",
                        "modified_time": "2026-03-01T11:00:00Z",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    snapshot = normalize_raw_exports(
        source_dir,
        organization_name="Acme Cloud",
        organization_domain="acme.example.com",
    )
    verification = verify_context_snapshot(snapshot)

    jira = snapshot.source_for("jira")
    google = snapshot.source_for("google")

    assert jira is not None
    assert jira.status == "error"
    assert google is not None
    assert google.status == "ok"
    assert verification.ok is False
    assert any(
        check.code == "source.status"
        and check.provider == "jira"
        and check.passed is False
        for check in verification.checks
    )


def test_normalize_raw_exports_flags_display_name_emails(tmp_path: Path) -> None:
    source_dir = tmp_path / "exports"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "google.json").write_text(
        json.dumps(
            {
                "users": [
                    {
                        "id": "g-1",
                        "email": "Maya Ops",
                        "name": "Maya Ops",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    snapshot = normalize_raw_exports(
        source_dir,
        organization_name="Acme Cloud",
        organization_domain="acme.example.com",
    )
    verification = verify_context_snapshot(snapshot)

    assert any(
        check.code == "source.actor_normalization"
        and check.provider == "google"
        and check.passed is False
        for check in verification.checks
    )


def test_verify_context_snapshot_flags_bundle_identity_and_time_range_issues() -> None:
    snapshot = ContextSnapshot(
        organization_name="Acme Cloud",
        organization_domain="acme.example.com",
        captured_at="2026-03-01T10:00:00Z",
        sources=[
            ContextSourceResult(
                provider="slack",
                captured_at="2026-03-01T10:00:00Z",
                status="ok",
                record_counts={"channels": 1, "users": 1},
                data={
                    "users": [
                        {
                            "id": "U1",
                            "email": "maya@acme.example.com",
                            "name": "maya",
                            "real_name": "Maya Ops",
                        }
                    ],
                    "channels": [
                        {
                            "channel_id": "C1",
                            "messages": [
                                {"ts": "1772329200.000100", "text": "Hold the draft."}
                            ],
                        }
                    ],
                },
            ),
            ContextSourceResult(
                provider="google",
                captured_at="2026-03-01T10:00:00Z",
                status="ok",
                record_counts={"users": 1, "documents": 1},
                data={
                    "users": [
                        {
                            "id": "g-1",
                            "email": "maya@acme.example.com",
                            "name": "Maya Operations",
                        }
                    ],
                    "documents": [
                        {
                            "doc_id": "doc-1",
                            "title": "Renewal plan",
                            "modified_time": "2026-03-01T10:10:00Z",
                        }
                    ],
                },
            ),
            ContextSourceResult(
                provider="crm",
                captured_at="2026-03-01T10:00:00Z",
                status="ok",
                record_counts={"contacts": 1, "deals": 1},
                data={
                    "contacts": [
                        {
                            "id": "contact-1",
                            "email": "ops@acme.example.com",
                            "first_name": "Maya",
                            "last_name": "Ops",
                        }
                    ],
                    "deals": [
                        {
                            "id": "deal-1",
                            "name": "Acme renewal",
                            "stage": "legal_review",
                            "owner": "maya@acme.example.com",
                            "created_ms": 1_772_329_200_000,
                            "updated_ms": 1_772_329_560_000,
                        }
                    ],
                },
            ),
        ],
    )

    verification = verify_context_snapshot(snapshot)

    assert any(
        check.code == "bundle.timestamp_span" and check.passed is False
        for check in verification.checks
    )
    assert any(
        check.code == "bundle.identity_email_names" and check.passed is False
        for check in verification.checks
    )
    assert any(
        check.code == "bundle.identity_name_emails" and check.passed is False
        for check in verification.checks
    )


def _write_slack_export(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "users.json").write_text(
        json.dumps(
            [
                {
                    "id": "U001",
                    "name": "maya",
                    "real_name": "Maya Ops",
                    "profile": {"email": "maya@acme.example.com"},
                }
            ]
        ),
        encoding="utf-8",
    )
    (root / "channels.json").write_text(
        json.dumps([{"id": "C001", "name": "deal-desk"}]),
        encoding="utf-8",
    )
    channel_dir = root / "deal-desk"
    channel_dir.mkdir(parents=True, exist_ok=True)
    (channel_dir / "2026-03-01.json").write_text(
        json.dumps(
            [
                {
                    "ts": "1772329200.000100",
                    "user": "U001",
                    "text": "Keep the renewal thread internal until legal signs off.",
                }
            ]
        ),
        encoding="utf-8",
    )
