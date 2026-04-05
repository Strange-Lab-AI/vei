from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import typer.testing

from vei.cli.vei_llm_test import (
    _full_flow_progress,
    _normalize_result,
    _select_progress_action,
    _should_bypass_strict_planning,
    _strict_full_flow_action,
    _strict_full_flow_complete,
    app as llm_app,
    run_episode,
)


class _FakeResult:
    def model_dump(self) -> dict:
        return {
            "structuredContent": None,
            "content": [
                {"type": "text", "text": '{"result": {"id": "D-1"}, "ok": true}'}
            ],
            "isError": False,
        }


def test_normalize_result_parses_text_json_payload() -> None:
    normalized = _normalize_result(_FakeResult())
    assert normalized == {"result": {"id": "D-1"}, "ok": True}


def test_select_progress_action_prefers_non_observe_with_args() -> None:
    action_menu = [
        {"tool": "vei.observe", "args": {}},
        {"tool": "browser.click", "args": {"node_id": "CLICK:open_pdp#0"}},
        {"tool": "mail.list", "args": {}},
    ]
    tool, args = _select_progress_action(action_menu) or ("", {})
    assert tool == "browser.click"
    assert args == {"node_id": "CLICK:open_pdp#0"}


def test_strict_full_flow_action_advances_missing_enterprise_steps() -> None:
    transcript = [
        {"action": {"tool": "browser.read", "args": {}, "result": {"url": "u"}}},
        {
            "action": {
                "tool": "slack.send_message",
                "args": {"text": "Budget $2999 approved"},
                "result": {"ts": "1"},
            }
        },
        {
            "action": {
                "tool": "mail.compose",
                "args": {"to": "sales@macrocompute.example"},
                "result": {"id": "m1"},
            }
        },
        {
            "action": {
                "tool": "mail.open",
                "args": {"id": "m2"},
                "result": {
                    "body_text": "Quote is $2999 and ETA is 5 business days.",
                },
            }
        },
    ]
    progress = _full_flow_progress(transcript)
    action = _strict_full_flow_action(progress)
    assert action is not None
    assert action[0] == "docs.create"


def test_strict_full_flow_action_uses_discovered_ids_for_ticket_and_crm() -> None:
    transcript = [
        {"action": {"tool": "browser.read", "args": {}, "result": {"url": "u"}}},
        {
            "action": {
                "tool": "slack.send_message",
                "args": {"text": "Budget $2999 approved"},
                "result": {"ts": "1"},
            }
        },
        {
            "action": {
                "tool": "mail.compose",
                "args": {"to": "sales@macrocompute.example"},
                "result": {"id": "m1"},
            }
        },
        {
            "action": {
                "tool": "mail.open",
                "args": {"id": "m2"},
                "result": {"body_text": "Price $2999 ETA 5 days"},
            }
        },
        {"action": {"tool": "docs.create", "args": {}, "result": {"doc_id": "D-1"}}},
        {
            "action": {
                "tool": "tickets.list",
                "args": {},
                "result": {"tickets": [{"ticket_id": "TCK-77"}]},
            }
        },
    ]
    progress = _full_flow_progress(transcript)
    ticket_action = _strict_full_flow_action(progress)
    assert ticket_action is not None
    assert ticket_action[0] == "tickets.update"
    assert ticket_action[1]["ticket_id"] == "TCK-77"

    transcript.append(
        {
            "action": {
                "tool": "tickets.update",
                "args": {"ticket_id": "TCK-77"},
                "result": {"ok": True},
            }
        }
    )
    transcript.append(
        {
            "action": {
                "tool": "crm.list_deals",
                "args": {},
                "result": {"deals": [{"id": "D-301"}]},
            }
        }
    )
    progress = _full_flow_progress(transcript)
    crm_action = _strict_full_flow_action(progress)
    assert crm_action is not None
    assert crm_action[0] == "crm.log_activity"
    assert crm_action[1]["deal_id"] == "D-301"


def test_strict_full_flow_action_uses_default_ids_when_missing() -> None:
    transcript = [
        {"action": {"tool": "browser.read", "args": {}, "result": {"url": "u"}}},
        {
            "action": {
                "tool": "slack.send_message",
                "args": {"text": "Budget $2999 approved"},
                "result": {"ts": "1"},
            }
        },
        {
            "action": {
                "tool": "mail.compose",
                "args": {"to": "sales@macrocompute.example"},
                "result": {"id": "m1"},
            }
        },
        {
            "action": {
                "tool": "mail.open",
                "args": {"id": "m2"},
                "result": {"body_text": "Price $2999 ETA 5 days"},
            }
        },
        {"action": {"tool": "docs.create", "args": {}, "result": {"doc_id": "D-1"}}},
    ]
    progress = _full_flow_progress(transcript)
    ticket_action = _strict_full_flow_action(progress)
    assert ticket_action is not None
    assert ticket_action[0] == "tickets.create"

    transcript.append(
        {
            "action": {
                "tool": "tickets.create",
                "args": {"title": "Quote follow-up"},
                "result": {"ticket_id": "TCK-88"},
            }
        }
    )
    progress = _full_flow_progress(transcript)
    ticket_update_action = _strict_full_flow_action(progress)
    assert ticket_update_action is not None
    assert ticket_update_action[0] == "tickets.update"
    assert ticket_update_action[1]["ticket_id"] == "TCK-88"

    progress = _full_flow_progress(transcript)
    crm_action = _strict_full_flow_action(progress)
    assert crm_action is not None
    assert crm_action[0] == "tickets.update"

    transcript.append(
        {
            "action": {
                "tool": "tickets.update",
                "args": {"ticket_id": "TCK-88"},
                "result": {"ok": True},
            }
        }
    )
    progress = _full_flow_progress(transcript)
    crm_create_action = _strict_full_flow_action(progress)
    assert crm_create_action is not None
    assert crm_create_action[0] == "crm.create_deal"

    transcript.append(
        {
            "action": {
                "tool": "crm.create_deal",
                "args": {"name": "Deal"},
                "result": {"id": "D-902"},
            }
        }
    )
    progress = _full_flow_progress(transcript)
    crm_log_action = _strict_full_flow_action(progress)
    assert crm_log_action is not None
    assert crm_log_action[0] == "crm.log_activity"
    assert crm_log_action[1]["deal_id"] == "D-902"


def test_strict_full_flow_complete_requires_all_full_subgoals() -> None:
    progress = {
        "citations": True,
        "approval_with_amount": True,
        "email_sent": True,
        "email_parsed": True,
        "doc_logged": True,
        "ticket_updated": True,
        "crm_logged": True,
    }
    assert _strict_full_flow_complete(progress) is True
    progress["crm_logged"] = False
    assert _strict_full_flow_complete(progress) is False


def test_should_bypass_strict_planning_only_after_quote_is_parsed() -> None:
    early_progress = {
        "citations": True,
        "approval_with_amount": True,
        "email_sent": True,
        "email_parsed": False,
        "doc_logged": False,
        "ticket_updated": False,
        "crm_logged": False,
    }
    late_progress = {
        **early_progress,
        "email_parsed": True,
    }

    assert (
        _should_bypass_strict_planning(
            early_progress,
            (
                "docs.create",
                {"title": "Vendor quote summary"},
            ),
        )
        is False
    )
    assert (
        _should_bypass_strict_planning(
            late_progress,
            (
                "docs.create",
                {"title": "Vendor quote summary"},
            ),
        )
        is True
    )
    assert (
        _should_bypass_strict_planning(
            late_progress,
            (
                "mail.list",
                {},
            ),
        )
        is False
    )


def test_llm_cli_writes_summary_artifact(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    async def _fake_run_episode(**kwargs):
        artifacts_dir = Path(kwargs["artifacts_dir"])
        (artifacts_dir / "trace.jsonl").write_text(
            json.dumps({"type": "call", "time_ms": 10}) + "\n",
            encoding="utf-8",
        )
        metrics_path = Path(kwargs["metrics_path"])
        metrics_path.write_text(
            json.dumps(
                {
                    "calls": 1,
                    "prompt_tokens": 11,
                    "completion_tokens": 7,
                    "total_tokens": 18,
                    "estimated_cost_usd": 0.12,
                    "latency_p95_ms": 25,
                }
            ),
            encoding="utf-8",
        )
        return [
            {"action": {"tool": "browser.read", "args": {}, "result": {"url": "u"}}}
        ]

    monkeypatch.setattr("vei.cli.vei_llm_test.run_episode", _fake_run_episode)
    monkeypatch.setattr(
        "vei.cli.vei_llm_test.compute_score",
        lambda artifacts_dir, success_mode: {"success": True, "costs": {"actions": 1}},
    )
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    runner = typer.testing.CliRunner()
    artifacts = tmp_path / "artifacts"
    result = runner.invoke(
        llm_app,
        [
            "--provider",
            "openai",
            "--model",
            "gpt-5",
            "--artifacts",
            str(artifacts),
            "--no-print-transcript",
        ],
    )

    assert result.exit_code == 0, result.output
    summary_payload = json.loads(
        (artifacts / "summary.json").read_text(encoding="utf-8")
    )
    assert summary_payload["summary"]["success"] is True
    assert summary_payload["summary"]["llm_calls"] == 1
    assert summary_payload["summary"]["total_tokens"] == 18


def test_llm_cli_writes_summary_artifact_on_require_success_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    async def _fake_run_episode(**kwargs):
        artifacts_dir = Path(kwargs["artifacts_dir"])
        (artifacts_dir / "trace.jsonl").write_text(
            json.dumps({"type": "call", "time_ms": 10}) + "\n",
            encoding="utf-8",
        )
        metrics_path = Path(kwargs["metrics_path"])
        metrics_path.write_text(
            json.dumps(
                {
                    "calls": 1,
                    "prompt_tokens": 3,
                    "completion_tokens": 2,
                    "total_tokens": 5,
                    "estimated_cost_usd": None,
                    "latency_p95_ms": 10,
                }
            ),
            encoding="utf-8",
        )
        return [
            {"action": {"tool": "browser.read", "args": {}, "result": {"url": "u"}}}
        ]

    monkeypatch.setattr("vei.cli.vei_llm_test.run_episode", _fake_run_episode)
    monkeypatch.setattr(
        "vei.cli.vei_llm_test.compute_score",
        lambda artifacts_dir, success_mode: {"success": False, "costs": {"actions": 1}},
    )
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    runner = typer.testing.CliRunner()
    artifacts = tmp_path / "artifacts"
    result = runner.invoke(
        llm_app,
        [
            "--provider",
            "openai",
            "--model",
            "gpt-5",
            "--artifacts",
            str(artifacts),
            "--require-success",
            "--no-print-transcript",
        ],
    )

    assert result.exit_code == 1, result.output
    summary_payload = json.loads(
        (artifacts / "summary.json").read_text(encoding="utf-8")
    )
    assert summary_payload["summary"]["success"] is False
    assert summary_payload["summary"]["total_tokens"] == 5


@pytest.mark.anyio("asyncio")
async def test_run_episode_defaults_stdio_log_level_to_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    @asynccontextmanager
    async def _fake_stdio_client(params):
        captured.update(params.env or {})
        yield (object(), object())

    class _FakeSession:
        def __init__(self, read, write):
            del read, write

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

        async def initialize(self):
            return None

        async def list_tools(self):
            class _Tools:
                tools = []

            return _Tools()

    monkeypatch.delenv("FASTMCP_LOG_LEVEL", raising=False)
    monkeypatch.delenv("FASTMCP_DEBUG", raising=False)
    monkeypatch.setattr("vei.cli.vei_llm_test.stdio_client", _fake_stdio_client)
    monkeypatch.setattr("vei.cli.vei_llm_test.ClientSession", _FakeSession)

    transcript = await run_episode(
        model="gpt-5",
        sse_url="",
        max_steps=0,
        provider="openai",
    )

    assert transcript == []
    assert captured["FASTMCP_LOG_LEVEL"] == "ERROR"
    assert captured["FASTMCP_DEBUG"] == "0"
