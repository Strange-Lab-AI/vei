from __future__ import annotations

import json
from pathlib import Path

from vei.cli.vei_score import app as score_app
import typer.testing


def run_score(artifacts_dir: Path) -> dict:
    runner = typer.testing.CliRunner()
    result = runner.invoke(score_app, ["--artifacts-dir", str(artifacts_dir)])
    assert result.exit_code == 0, result.output
    return json.loads(result.stdout)


def write_trace(dirpath: Path, records: list[dict]) -> None:
    dirpath.mkdir(parents=True, exist_ok=True)
    trace = dirpath / "trace.jsonl"
    trace.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")


def test_score_parses_all_vendor_variants(tmp_path: Path):
    variants = [
        "Thanks — Price: $3199, ETA: 5-7 business days.",
        "> On Mon, we received your request\nPRICE: USD 3,199\nEta: within 5-7 business days\n--\nBest, MacroCompute",
        "quote attached (inline): total: $3,199.00, ETA: 5 business days. Regards, Sales",
        "PRICE - $3199; eta: approx. 1 week\n\n\nJohn Doe\nSales Representative\nMacroCompute",
    ]

    for i, body in enumerate(variants):
        art = tmp_path / f"case_{i}"
        records = [
            {
                "type": "event",
                "target": "mail",
                "payload": {
                    "from": "vendor@example",
                    "subj": "Re: Quote",
                    "body_text": body,
                },
            },
        ]
        write_trace(art, records)
        data = run_score(art)
        assert data["subgoals"]["email_parsed"] == 1, body
        assert data["success"] is True


def test_score_requires_both_price_and_eta(tmp_path: Path):
    # Missing ETA -> should not count as parsed, success False
    art = tmp_path / "missing_eta"
    records = [
        {
            "type": "event",
            "target": "mail",
            "payload": {
                "from": "vendor@example",
                "subj": "Re: Quote",
                "body_text": "Price: $3199",
            },
        },
    ]
    write_trace(art, records)
    data = run_score(art)
    assert data["subgoals"]["email_parsed"] == 0
    assert data["success"] is False


def test_score_other_subgoals_independent_of_success(tmp_path: Path):
    # Include typical calls so citations and email_sent get marked, but success
    # still hinges on email_parsed which will be False here due to missing ETA
    art = tmp_path / "subgoals"
    records = [
        {
            "type": "call",
            "tool": "browser.read",
            "args": {},
            "response": {"url": "u", "title": "t"},
        },
        {
            "type": "call",
            "tool": "mail.compose",
            "args": {"to": "vendor@example", "subj": "Q", "body_text": "x"},
            "response": {"id": "m1"},
        },
        {
            "type": "event",
            "target": "mail",
            "payload": {
                "from": "vendor@example",
                "subj": "Re: Q",
                "body_text": "total: $3,199",
            },
        },
    ]
    write_trace(art, records)
    data = run_score(art)
    assert data["subgoals"]["citations"] == 1
    assert data["subgoals"]["email_sent"] == 1
    assert data["subgoals"]["email_parsed"] == 0
    assert data["success"] is False


def test_score_parses_vendor_quote_from_mail_open_call_response(tmp_path: Path):
    art = tmp_path / "mail_open_response"
    records = [
        {
            "type": "call",
            "time_ms": 8000,
            "tool": "mail.open",
            "args": {"id": "m2"},
            "response": {
                "headers": {"Subject": "Re: Quote request"},
                "body_text": "Quote $3199, ETA 5 business days",
            },
        }
    ]
    write_trace(art, records)
    data = run_score(art)
    assert data["subgoals"]["email_parsed"] == 1
    assert data["success"] is True
