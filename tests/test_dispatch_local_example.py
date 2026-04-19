from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


def _build_mbox_content() -> str:
    return "\n".join(
        [
            "From founder@dispatch.ai Mon Mar 10 10:00:00 2025",
            "From: founder@dispatch.ai",
            "To: ops@dispatch.ai",
            "Subject: Weekly sync",
            "Date: Mon, 10 Mar 2025 10:00:00 +0000",
            "Message-ID: <dispatch-1@dispatch.ai>",
            "",
            "Weekly notes.",
            "",
            "From ops@dispatch.ai Mon Mar 10 11:00:00 2025",
            "From: ops@dispatch.ai",
            "To: founder@dispatch.ai",
            "Subject: Re: Weekly sync",
            "Date: Mon, 10 Mar 2025 11:00:00 +0000",
            "Message-ID: <dispatch-2@dispatch.ai>",
            "In-Reply-To: <dispatch-1@dispatch.ai>",
            "References: <dispatch-1@dispatch.ai>",
            "",
            "Follow-up actions.",
            "",
        ]
    )


def _write_dispatch_gmail_zip(path: Path) -> None:
    export_root = path.parent / "gmail_export" / "Takeout" / "Mail"
    export_root.mkdir(parents=True)
    mbox_path = export_root / "All mail Including Spam and Trash.mbox"
    mbox_path.write_text(_build_mbox_content(), encoding="utf-8")
    with zipfile.ZipFile(path, "w") as archive:
        archive.write(
            mbox_path,
            arcname="Takeout/Mail/All mail Including Spam and Trash.mbox",
        )


def _write_dispatch_notion_zip(path: Path) -> None:
    notion_root = (
        path.parent / "notion_export" / "Private & Shared" / "Central Dispatch"
    )
    notion_root.mkdir(parents=True)
    (notion_root / "Weekly priorities 74fdd6b1c536473aa670c3373f5e7f89.md").write_text(
        "\n".join(
            [
                "# Weekly priorities - 2024-05-06T16:30:00Z",
                "",
                "Owner: Zapier",
                "Last edited time: May 6, 2024 9:57 AM",
                "Created time: May 6, 2024 9:57 AM",
                "",
                "Transcript starts here.",
            ]
        ),
        encoding="utf-8",
    )
    (
        notion_root / "Internal Ops Tasks 3d140d68e2a842b582878efb0c8be893.csv"
    ).write_text(
        "\n".join(
            [
                "Name,Assign,Status",
                "Weekly cron for LLM reports based on GH,Robb Chen-Ware,Done",
                "New Demo Video,,Not started",
            ]
        ),
        encoding="utf-8",
    )

    inner_zip = path.parent / "dispatch_inner.zip"
    with zipfile.ZipFile(inner_zip, "w") as archive:
        for file_path in sorted((path.parent / "notion_export").rglob("*")):
            if not file_path.is_file():
                continue
            archive.write(
                file_path,
                arcname=str(file_path.relative_to(path.parent / "notion_export")),
            )

    with zipfile.ZipFile(path, "w") as archive:
        archive.write(inner_zip, arcname="Dispatch-Export.zip")


def test_build_dispatch_local_example_script(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    gmail_zip = tmp_path / "dispatch-gmail.zip"
    notion_zip = tmp_path / "dispatch-notion.zip"
    workspace_root = tmp_path / "dispatch-real-example"

    _write_dispatch_gmail_zip(gmail_zip)
    _write_dispatch_notion_zip(notion_zip)

    result = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "build_dispatch_local_example.py"),
            "--root",
            str(workspace_root),
            "--gmail-export",
            str(gmail_zip),
            "--notion-export",
            str(notion_zip),
            "--gmail-limit",
            "50",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["readiness"]["available"] is True
    assert payload["readiness"]["event_count"] >= 3
    assert payload["timeline_preview"]["available"] is True
    assert (workspace_root / "context_snapshot.json").exists()
    assert (workspace_root / "canonical_events.jsonl").exists()
    assert (workspace_root / "canonical_event_index.json").exists()
    assert (workspace_root / "dispatch_example_summary.json").exists()
