from __future__ import annotations

import json
from pathlib import Path

import typer.testing
from vei.cli.vei_score import app as score_cmd


def run_score(artifacts_dir: Path) -> dict:
    runner = typer.testing.CliRunner()
    result = runner.invoke(score_cmd, ["--artifacts-dir", str(artifacts_dir)])
    assert result.exit_code == 0, result.output
    return json.loads(result.stdout)


def write_trace(dirpath: Path, records: list[dict]) -> None:
    dirpath.mkdir(parents=True, exist_ok=True)
    (dirpath / "trace.jsonl").write_text(
        "\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8"
    )


def test_score_citations_triggered_by_browser_read(tmp_path: Path):
    art = tmp_path / "citations"
    records = [
        {
            "type": "call",
            "tool": "browser.read",
            "args": {},
            "response": {"url": "u", "title": "t"},
        },
    ]
    write_trace(art, records)
    data = run_score(art)
    assert data["subgoals"]["citations"] == 1
    # No email parsed in this trace, so success should be False
    assert data["success"] is False


def test_score_approval_detected_via_emoji(tmp_path: Path):
    art = tmp_path / "approval_emoji"
    records = [
        {
            "type": "event",
            "target": "slack",
            "payload": {"text": ":white_check_mark: Approved"},
        },
    ]
    write_trace(art, records)
    data = run_score(art)
    assert data["subgoals"]["approval"] == 1
    assert data["subgoals"]["approval_with_amount"] == 0
    # Email parsing still not present
    assert data["success"] is False


def test_score_approval_detected_via_text_only(tmp_path: Path):
    art = tmp_path / "approval_text"
    records = [
        {"type": "event", "target": "slack", "payload": {"text": "Approved, proceed."}},
    ]
    write_trace(art, records)
    data = run_score(art)
    assert data["subgoals"]["approval"] == 1
    assert data["subgoals"]["approval_with_amount"] == 0
    assert data["success"] is False


def test_score_no_approval_for_irrelevant_slack(tmp_path: Path):
    art = tmp_path / "no_approval"
    records = [
        {
            "type": "event",
            "target": "slack",
            "payload": {"text": "What is the budget amount?"},
        },
    ]
    write_trace(art, records)
    data = run_score(art)
    assert data["subgoals"]["approval"] == 0
    assert data["subgoals"]["approval_with_amount"] == 0
    assert data["success"] is False


def test_score_policy_findings_detect_missing_budget(tmp_path: Path) -> None:
    art = tmp_path / "policy_budget"
    records = [
        {
            "type": "call",
            "time_ms": 5000,
            "tool": "slack.send_message",
            "args": {
                "channel": "#procurement",
                "text": "Please approve the laptop purchase",
            },
            "response": {"ts": "2"},
        }
    ]
    write_trace(art, records)
    data = run_score(art)
    policy = data.get("policy", {})
    assert policy.get("warning_count", 0) >= 1
    codes = {f["code"] for f in policy.get("findings", [])}
    assert "slack.approval_missing_amount" in codes
    usage = data.get("usage", {})
    assert usage.get("slack.send_message") == 1
    assert data["subgoals"].get("approval_with_amount", 0) == 0


def test_score_enterprise_subgoals_complete(tmp_path: Path) -> None:
    art = tmp_path / "enterprise_full"
    records = [
        {
            "type": "call",
            "time_ms": 1000,
            "tool": "browser.read",
            "args": {},
            "response": {"url": "u", "title": "t"},
        },
        {
            "type": "call",
            "time_ms": 1500,
            "tool": "mail.compose",
            "args": {
                "to": "vendor@example",
                "subj": "Quote request",
                "body_text": "Need MacroBook quote",
            },
            "response": {"id": "m1"},
        },
        {
            "type": "call",
            "time_ms": 2000,
            "tool": "slack.send_message",
            "args": {
                "channel": "#procurement",
                "text": ":white_check_mark: Approved at $3199",
            },
            "response": {"ts": "2"},
        },
        {
            "type": "event",
            "time_ms": 2100,
            "target": "slack",
            "payload": {"text": ":white_check_mark: Approved at $3199"},
        },
        {
            "type": "event",
            "time_ms": 5000,
            "target": "mail",
            "payload": {
                "from": "vendor@example",
                "subj": "Re: Quote",
                "body_text": "Formal quote — Total: $3,199. ETA: 5 business days.",
            },
        },
        {
            "type": "call",
            "time_ms": 6000,
            "tool": "docs.create",
            "args": {
                "title": "MacroBook Quote",
                "body": "Quote $3199, ETA 5 business days",
            },
            "response": {"doc_id": "DOC-1"},
        },
        {
            "type": "call",
            "time_ms": 6500,
            "tool": "tickets.update",
            "args": {
                "ticket_id": "TCK-42",
                "description": "Logged vendor quote $3199 ETA 5 days",
            },
            "response": {"ok": True},
        },
        {
            "type": "call",
            "time_ms": 7000,
            "tool": "crm.log_activity",
            "args": {
                "kind": "note",
                "note": "Logged MacroBook quote $3199 with ETA 5 business days",
            },
            "response": {"id": "ACT-1"},
        },
    ]
    write_trace(art, records)
    data = run_score(art)
    sg = data["subgoals"]
    assert sg["citations"] == 1
    assert sg["email_sent"] == 1
    assert sg["email_parsed"] == 1
    assert sg["approval"] == 1
    assert sg["approval_with_amount"] == 1
    assert sg["doc_logged"] == 1
    assert sg["ticket_updated"] == 1
    assert sg["crm_logged"] == 1
    assert data["success_full_flow"] is True
    assert data["policy"]["warning_count"] == 0
    assert data["policy"]["error_count"] == 0
