from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

from vei.llm import codex_cli
from vei.llm import providers


def test_build_usage_uses_builtin_openai_pricing_when_env_is_absent(
    monkeypatch,
) -> None:
    monkeypatch.delenv("VEI_OPENAI_GPT_5_MINI_INPUT_USD_PER_1M", raising=False)
    monkeypatch.delenv("VEI_OPENAI_GPT_5_MINI_OUTPUT_USD_PER_1M", raising=False)
    monkeypatch.delenv("VEI_OPENAI_INPUT_USD_PER_1M", raising=False)
    monkeypatch.delenv("VEI_OPENAI_OUTPUT_USD_PER_1M", raising=False)
    monkeypatch.delenv("VEI_LLM_INPUT_USD_PER_1M", raising=False)
    monkeypatch.delenv("VEI_LLM_OUTPUT_USD_PER_1M", raising=False)

    usage = providers._build_usage(
        provider="openai",
        model="gpt-5-mini",
        prompt_tokens=20_166,
        completion_tokens=3_240,
    )

    assert usage.estimated_cost_usd == 0.0115215


def test_plan_once_with_usage_supports_codex_provider(monkeypatch) -> None:
    recorded: dict[str, object] = {}

    def fake_run_codex_json(**kwargs: object) -> dict[str, object]:
        recorded.update(kwargs)
        return {"tool": "tickets.list", "args": {"limit": 3}}

    monkeypatch.setattr(providers, "run_codex_json", fake_run_codex_json)

    result = asyncio.run(
        providers.plan_once_with_usage(
            provider="codex",
            model="gpt-5.4",
            system="system prompt",
            user="user prompt",
            plan_schema={
                "type": "object",
                "properties": {
                    "tool": {"type": "string"},
                    "args": {"type": "object"},
                },
                "required": ["tool", "args"],
            },
        )
    )

    assert result.plan == {"tool": "tickets.list", "args": {"limit": 3}}
    assert result.usage.provider == "codex"
    assert result.usage.model == "gpt-5.4"
    assert recorded["model"] == "gpt-5.4"
    assert recorded["timeout_s"] == 240


def test_codex_schema_normalizer_closes_objects_and_requires_all_keys() -> None:
    normalized = codex_cli._normalize_output_schema(
        {
            "type": "object",
            "properties": {
                "tool_name": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                "args": {
                    "type": "object",
                    "properties": {
                        "doc_id": {"anyOf": [{"type": "string"}, {"type": "null"}]}
                    },
                },
            },
        }
    )

    assert normalized["additionalProperties"] is False
    assert normalized["required"] == ["tool_name", "args"]
    assert normalized["properties"]["args"]["additionalProperties"] is False
    assert normalized["properties"]["args"]["required"] == ["doc_id"]


def test_codex_exec_skips_ignore_rules_when_cli_does_not_support_it(
    monkeypatch,
) -> None:
    monkeypatch.setattr(codex_cli, "_codex_exec_supports", lambda flag: False)
    monkeypatch.setenv("VEI_CODEX_MODEL_REASONING_EFFORT", "medium")

    def fake_run(command: list[str], **_: object) -> SimpleNamespace:
        output_index = command.index("-o") + 1
        Path(command[output_index]).write_text(
            '{"tool":"tickets.list","args":{"limit":1}}',
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(codex_cli.subprocess, "run", fake_run)

    result = codex_cli.run_codex_exec(
        model="gpt-5.4",
        prompt="Return a tool call.",
        output_schema={
            "type": "object",
            "properties": {
                "tool": {"type": "string"},
                "args": {"type": "object"},
            },
        },
    )

    assert "--ignore-rules" not in result.command
    assert result.command[3:5] == ["-c", 'model_reasoning_effort="medium"']
