from __future__ import annotations

import asyncio
from dataclasses import dataclass
import inspect
import json
import os
import re
from typing import Any, Dict, Optional

from .codex_cli import run_codex_json

try:
    from openai import AsyncOpenAI, BadRequestError
except Exception:  # pragma: no cover
    AsyncOpenAI = None  # type: ignore
    BadRequestError = None  # type: ignore

try:
    from anthropic import AsyncAnthropic
except Exception:  # pragma: no cover
    AsyncAnthropic = None  # type: ignore

try:
    from google import genai
except Exception:  # pragma: no cover
    genai = None  # type: ignore

_JSON_FENCE_RE = re.compile(r"^```(?:json)?\s*(.*?)\s*```$", re.DOTALL)
_OPENAI_DEFAULT_PRICING_USD_PER_1M: dict[str, dict[str, float]] = {
    "gpt-5": {"input": 1.25, "output": 10.0},
    "gpt-5-mini": {"input": 0.25, "output": 2.0},
    "gpt-5-nano": {"input": 0.05, "output": 0.4},
    "gpt-5.4": {"input": 2.5, "output": 15.0},
    "gpt-5.4-mini": {"input": 0.75, "output": 4.5},
    "gpt-5.4-nano": {"input": 0.2, "output": 1.25},
}


def _is_codex_session_model(model: str) -> bool:
    return "codex" in (model or "").strip().lower()


@dataclass
class PlanUsage:
    provider: str
    model: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    estimated_cost_usd: float | None = None


@dataclass
class PlanResult:
    plan: Dict[str, Any]
    usage: PlanUsage


def _parse_plan_text(raw: str) -> Dict[str, Any]:
    text = raw.strip()
    match = _JSON_FENCE_RE.match(text)
    if match:
        text = match.group(1).strip()
    decoder = json.JSONDecoder()
    try:
        obj, _ = decoder.raw_decode(text)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        pass
    brace_index = text.find("{")
    if brace_index != -1:
        try:
            obj, _ = decoder.raw_decode(text[brace_index:])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass
    raise json.JSONDecodeError("Could not parse plan JSON", raw, 0)


def auto_provider_for_model(model: str, explicit: Optional[str] = None) -> str:
    if explicit:
        return explicit.strip().lower()
    m = (model or "").strip().lower()
    if _is_codex_session_model(m):
        return "codex"
    if m.startswith("claude-"):
        return "anthropic"
    if m.startswith("gemini-") or m.startswith("models/gemini"):
        return "google"
    if m.startswith("grok-") or "grok" in m:
        return "openrouter"
    return "openai"


def _rate_from_env(provider: str, model: str, side: str) -> float | None:
    model_key = re.sub(r"[^A-Z0-9]+", "_", model.upper()).strip("_")
    keys = [
        f"VEI_{provider.upper()}_{model_key}_{side}_USD_PER_1M",
        f"VEI_{provider.upper()}_{side}_USD_PER_1M",
        f"VEI_LLM_{side}_USD_PER_1M",
    ]
    for key in keys:
        value = os.environ.get(key)
        if value is None or not value.strip():
            continue
        try:
            return float(value)
        except ValueError:
            continue
    return None


def _rate_from_defaults(provider: str, model: str, side: str) -> float | None:
    if provider.strip().lower() != "openai":
        return None
    normalized_model = model.strip().lower()
    normalized_side = side.strip().lower()
    known_models = sorted(
        _OPENAI_DEFAULT_PRICING_USD_PER_1M.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    )
    for known_model, rates in known_models:
        if normalized_model == known_model or normalized_model.startswith(
            f"{known_model}-"
        ):
            return rates.get(normalized_side)
    return None


def _build_usage(
    *,
    provider: str,
    model: str,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    total_tokens: int | None = None,
) -> PlanUsage:
    total = (
        int(total_tokens)
        if total_tokens is not None
        else int(prompt_tokens) + int(completion_tokens)
    )
    input_rate = _rate_from_env(provider, model, "INPUT")
    output_rate = _rate_from_env(provider, model, "OUTPUT")
    if input_rate is None:
        input_rate = _rate_from_defaults(provider, model, "input")
    if output_rate is None:
        output_rate = _rate_from_defaults(provider, model, "output")
    estimated_cost_usd: float | None = None
    if input_rate is not None and output_rate is not None:
        estimated_cost_usd = round(
            ((int(prompt_tokens) / 1_000_000) * input_rate)
            + ((int(completion_tokens) / 1_000_000) * output_rate),
            8,
        )
    return PlanUsage(
        provider=provider,
        model=model,
        prompt_tokens=int(prompt_tokens),
        completion_tokens=int(completion_tokens),
        total_tokens=total,
        estimated_cost_usd=estimated_cost_usd,
    )


async def _openai_plan(
    *,
    model: str,
    system: str,
    user: str,
    plan_schema: Optional[dict] = None,
    timeout_s: int = 240,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> PlanResult:
    """OpenAI provider. Uses Responses API for gpt-5, Chat Completions for others."""
    if _is_codex_session_model(model):
        raise RuntimeError(
            f"{model} is a Codex-session model. Use provider='codex' so the "
            "request runs through Codex instead of an OpenAI API key."
        )
    if AsyncOpenAI is None:
        raise RuntimeError("openai SDK not installed; install with extras [llm]")

    client = AsyncOpenAI(
        base_url=base_url or os.environ.get("OPENAI_BASE_URL"),
        api_key=api_key or os.environ.get("OPENAI_API_KEY"),
    )

    # Per user feedback, gpt-5 requires the Responses API and specific params.
    if model.startswith("gpt-5"):
        prompt = (
            f"[system] {system}\n[user] {user}\n"
            "Reply strictly as JSON with keys 'tool' (string) and 'args' (object)."
        )
        kwargs: dict[str, Any] = {
            "model": model,
            "input": prompt,
            "reasoning": {"effort": "high"},
        }
        try:
            resp = await asyncio.wait_for(
                client.responses.create(**kwargs), timeout=timeout_s
            )
            usage = _build_usage(
                provider="openai",
                model=model,
                prompt_tokens=int(
                    getattr(getattr(resp, "usage", None), "input_tokens", 0) or 0
                ),
                completion_tokens=int(
                    getattr(getattr(resp, "usage", None), "output_tokens", 0) or 0
                ),
                total_tokens=int(
                    getattr(getattr(resp, "usage", None), "total_tokens", 0) or 0
                )
                or None,
            )
            raw = getattr(resp, "output_text", None)
            if not raw and resp.status == "incomplete":
                detail = getattr(resp, "incomplete_details", None)
                if detail and getattr(detail, "reason", "") == "max_output_tokens":
                    # Fall back to safetensors tool output if present
                    try:
                        for item in getattr(resp, "output", []) or []:
                            if getattr(item, "type", None) == "tool_call" and getattr(
                                item, "content", None
                            ):
                                content = getattr(item, "content", None)
                                if isinstance(content, dict):
                                    return PlanResult(plan=content, usage=usage)
                                raw = json.dumps(content)
                                break
                    except Exception:
                        raw = None
                if not raw:
                    raise RuntimeError(f"Response incomplete: {detail}")
            if raw:
                return PlanResult(plan=_parse_plan_text(raw), usage=usage)
        except Exception:
            raise
    else:
        # Fallback to standard Chat Completions for other OpenAI models
        try:
            resp = await asyncio.wait_for(
                client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {
                            "role": "user",
                            "content": user
                            + "\nReply strictly as JSON with keys 'tool' (string) and 'args' (object).",
                        },
                    ],
                    temperature=0.0,
                    top_p=1,
                    response_format={"type": "json_object"},
                ),
                timeout=timeout_s,
            )
            usage = _build_usage(
                provider="openai",
                model=model,
                prompt_tokens=int(
                    getattr(getattr(resp, "usage", None), "prompt_tokens", 0) or 0
                ),
                completion_tokens=int(
                    getattr(getattr(resp, "usage", None), "completion_tokens", 0) or 0
                ),
                total_tokens=int(
                    getattr(getattr(resp, "usage", None), "total_tokens", 0) or 0
                )
                or None,
            )
            choice = resp.choices[0]
            if choice.finish_reason == "length":
                raise RuntimeError("OpenAI response truncated due to max_tokens.")
            if choice.message.content:
                return PlanResult(
                    plan=_parse_plan_text(choice.message.content),
                    usage=usage,
                )
        except BadRequestError as e:
            # Handle specific error for gpt-5 if it was routed here by mistake
            if "max_completion_tokens" in str(e):
                raise RuntimeError(
                    f"Model {model} may require the Responses API. Rerun with a more specific model name if this is gpt-5."
                ) from e
            raise
        except Exception:
            raise

    # Fallback if no content extracted
    return PlanResult(
        plan={"tool": "vei.observe", "args": {}},
        usage=_build_usage(provider="openai", model=model),
    )


async def _anthropic_plan(
    *,
    model: str,
    system: str,
    user: str,
    timeout_s: int = 240,
    api_key: Optional[str] = None,
    tool_schemas: Optional[list[Dict[str, Any]]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> PlanResult:
    """Anthropic provider using Messages API."""
    if AsyncAnthropic is None:
        raise RuntimeError("anthropic SDK not installed; install with extras [llm]")

    headers: Dict[str, str] = {}
    version = os.environ.get("ANTHROPIC_VERSION")
    beta = os.environ.get("ANTHROPIC_BETA")
    if version:
        headers["anthropic-version"] = version.strip()
    if beta:
        headers["anthropic-beta"] = beta.strip()
    client_kwargs: Dict[str, Any] = {
        "api_key": api_key or os.environ.get("ANTHROPIC_API_KEY")
    }
    if headers:
        client_kwargs["default_headers"] = headers
    client = AsyncAnthropic(**client_kwargs)

    use_beta_api = (
        os.environ.get("ANTHROPIC_USE_BETA", "").strip().lower() in {"1", "true", "yes"}
    ) or model.startswith("claude-4.5")
    messages_api = client.beta.messages if use_beta_api else client.messages

    try:
        bridge_mode = bool(
            tool_schemas
            and len(tool_schemas) == 1
            and tool_schemas[0]["name"] == "vei_call"
        )
        if tool_schemas:
            msg = await asyncio.wait_for(
                messages_api.create(
                    model=model,
                    system=system,
                    max_tokens=2048,
                    temperature=0,
                    tools=tool_schemas,
                    messages=[
                        {
                            "role": "user",
                            "content": user,
                        }
                    ],
                ),
                timeout=timeout_s,
            )
        else:
            msg = await asyncio.wait_for(
                messages_api.create(
                    model=model,
                    system=system
                    + "\nYou MUST respond ONLY with valid JSON. No explanations, no prose, ONLY JSON.",
                    max_tokens=2048,
                    temperature=0,
                    messages=[
                        {
                            "role": "user",
                            "content": user
                            + '\n\nIMPORTANT: Reply with ONLY a JSON object. No other text. Format: {"tool": "<name>", "args": {...}}',
                        }
                    ],
                ),
                timeout=timeout_s,
            )
        usage = _build_usage(
            provider="anthropic",
            model=model,
            prompt_tokens=int(
                getattr(getattr(msg, "usage", None), "input_tokens", 0) or 0
            ),
            completion_tokens=int(
                getattr(getattr(msg, "usage", None), "output_tokens", 0) or 0
            ),
        )

        if msg.stop_reason == "max_tokens":
            raise RuntimeError(
                f"Anthropic response truncated due to max_tokens ({msg.usage.output_tokens})."
            )

        if tool_schemas:
            for block in getattr(msg, "content", []) or []:
                if getattr(block, "type", None) == "tool_use":
                    alias = getattr(block, "name", "")
                    tool_input = getattr(block, "input", {})
                    if hasattr(tool_input, "model_dump"):
                        tool_input = tool_input.model_dump()
                    if bridge_mode and alias == "vei_call":
                        if isinstance(tool_input, dict):
                            actual_tool = tool_input.get("tool")
                            args = tool_input.get("args", {})
                            if not isinstance(args, dict):
                                args = {}
                            if actual_tool:
                                return PlanResult(
                                    plan={"tool": actual_tool, "args": args},
                                    usage=usage,
                                )
                        raise RuntimeError(
                            "Claude returned vei_call without valid tool/args"
                        )
                    tool_name = alias_map.get(alias, alias) if alias_map else alias
                    return PlanResult(
                        plan={
                            "tool": tool_name,
                            "args": tool_input if isinstance(tool_input, dict) else {},
                        },
                        usage=usage,
                    )

        for block in msg.content:
            if hasattr(block, "text") and block.text:
                text = block.text.strip()
                if text:
                    parsed = json.loads(text)
                    if bridge_mode and parsed.get("tool") == "vei_call":
                        actual_tool = (
                            parsed.get("args", {}).get("tool")
                            if isinstance(parsed.get("args"), dict)
                            else parsed.get("tool_name")
                        )
                        args = (
                            parsed.get("args", {})
                            if isinstance(parsed.get("args"), dict)
                            else parsed.get("tool_args", {})
                        )
                        if not isinstance(args, dict):
                            args = {}
                        if actual_tool:
                            return PlanResult(
                                plan={"tool": actual_tool, "args": args},
                                usage=usage,
                            )
                    if alias_map:
                        alias = parsed.get("tool")
                        if alias in alias_map:
                            parsed["tool"] = alias_map[alias]
                    return PlanResult(plan=parsed, usage=usage)

        raise RuntimeError(f"No text content in Claude response: {msg}")

    except Exception:
        raise


async def _google_plan(
    *,
    model: str,
    system: str,
    user: str,
    timeout_s: int = 30,
    api_key: Optional[str] = None,
) -> PlanResult:
    """Google Gemini provider using genai library with JSON mode."""
    if genai is None:
        raise RuntimeError("google-genai not installed; install with extras [llm]")

    key = (
        api_key or os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    )
    if not key:
        raise RuntimeError("Google API key missing (GOOGLE_API_KEY/GEMINI_API_KEY)")

    client = genai.Client(api_key=key)

    prompt = (
        f"[system] {system}\n[user] {user}\n"
        "Reply strictly as JSON with keys 'tool' (string) and 'args' (object)."
    )

    config = genai.types.GenerateContentConfig(
        temperature=0.0,
        top_p=1,
        response_mime_type="application/json",
    )

    try:
        resp = await asyncio.wait_for(
            client.aio.models.generate_content(
                model=model,
                contents=prompt,
                config=config,
            ),
            timeout=timeout_s,
        )
        usage = _build_usage(
            provider="google",
            model=model,
            prompt_tokens=int(
                getattr(getattr(resp, "usage_metadata", None), "prompt_token_count", 0)
                or 0
            ),
            completion_tokens=int(
                getattr(
                    getattr(resp, "usage_metadata", None),
                    "candidates_token_count",
                    0,
                )
                or 0
            ),
            total_tokens=int(
                getattr(getattr(resp, "usage_metadata", None), "total_token_count", 0)
                or 0
            )
            or None,
        )

        candidates = getattr(resp, "candidates", None) or []
        if candidates:
            finish_reason = getattr(candidates[0], "finish_reason", None)
            finish_name = getattr(finish_reason, "name", str(finish_reason or ""))
            if "MAX" in finish_name.upper():
                raise RuntimeError("Google response truncated due to max_tokens.")

        if hasattr(resp, "text") and resp.text:
            return PlanResult(plan=_parse_plan_text(resp.text), usage=usage)

        for cand in candidates:
            content = getattr(cand, "content", None)
            parts = getattr(content, "parts", None) if content is not None else None
            if not parts:
                continue
            for part in parts:
                text = getattr(part, "text", None)
                if text:
                    return PlanResult(plan=_parse_plan_text(text), usage=usage)

    except Exception:
        raise
    finally:
        # Release http resources in long-lived runners.
        close = getattr(client, "close", None)
        if callable(close):
            maybe_awaitable = close()
            if inspect.isawaitable(maybe_awaitable):
                await maybe_awaitable

    return PlanResult(
        plan={"tool": "vei.observe", "args": {}},
        usage=_build_usage(provider="google", model=model),
    )


async def _openrouter_plan(
    *,
    model: str,
    system: str,
    user: str,
    timeout_s: int = 90,
    api_key: Optional[str] = None,
) -> PlanResult:
    """OpenRouter provider using OpenAI-compatible API."""
    if AsyncOpenAI is None:
        raise RuntimeError("openai SDK not installed; install with extras [llm]")

    headers: Dict[str, str] = {}
    referer = os.environ.get("OPENROUTER_HTTP_REFERER")
    app_title = os.environ.get("OPENROUTER_APP_TITLE")
    if referer:
        headers["HTTP-Referer"] = referer
    if app_title:
        headers["X-Title"] = app_title

    client = AsyncOpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key or os.environ.get("OPENROUTER_API_KEY"),
        default_headers=headers or None,
    )

    try:
        resp = await asyncio.wait_for(
            client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {
                        "role": "user",
                        "content": user
                        + "\nReply strictly as JSON with keys 'tool' (string) and 'args' (object).",
                    },
                ],
                max_tokens=2048,
                temperature=0,
                top_p=1,
                response_format={"type": "json_object"},
            ),
            timeout=timeout_s,
        )
        usage = _build_usage(
            provider="openrouter",
            model=model,
            prompt_tokens=int(
                getattr(getattr(resp, "usage", None), "prompt_tokens", 0) or 0
            ),
            completion_tokens=int(
                getattr(getattr(resp, "usage", None), "completion_tokens", 0) or 0
            ),
            total_tokens=int(
                getattr(getattr(resp, "usage", None), "total_tokens", 0) or 0
            )
            or None,
        )

        choice = resp.choices[0]
        if choice.finish_reason == "length":
            raise RuntimeError("OpenRouter response truncated due to max_tokens.")

        if choice.message.content:
            try:
                return PlanResult(
                    plan=json.loads(choice.message.content),
                    usage=usage,
                )
            except json.JSONDecodeError as exc:
                snippet = choice.message.content.strip()
                if len(snippet) > 200:
                    snippet = snippet[:200] + "…"
                raise RuntimeError(
                    f"OpenRouter returned non-JSON payload: {snippet}"
                ) from exc

    except Exception:
        raise

    return PlanResult(
        plan={"tool": "vei.observe", "args": {}},
        usage=_build_usage(provider="openrouter", model=model),
    )


async def _codex_plan(
    *,
    model: str,
    system: str,
    user: str,
    plan_schema: Optional[dict] = None,
    timeout_s: int = 240,
) -> PlanResult:
    schema = plan_schema or {
        "type": "object",
        "additionalProperties": False,
    }
    prompt = (
        "You are planning one VEI action.\n\n"
        "System instructions:\n"
        f"{system.strip()}\n\n"
        "User instructions:\n"
        f"{user.strip()}\n\n"
        "Return one JSON object that matches the provided schema. "
        "Do not call tools. Do not explain your answer."
    )
    plan = await asyncio.to_thread(
        run_codex_json,
        model=model,
        prompt=prompt,
        output_schema=schema,
        timeout_s=timeout_s,
    )
    return PlanResult(
        plan=plan,
        usage=_build_usage(provider="codex", model=model),
    )


async def plan_once_with_usage(
    *,
    provider: str,
    model: str,
    system: str,
    user: str,
    plan_schema: Optional[dict] = None,
    timeout_s: int = 240,
    openai_base_url: Optional[str] = None,
    openai_api_key: Optional[str] = None,
    anthropic_api_key: Optional[str] = None,
    google_api_key: Optional[str] = None,
    openrouter_api_key: Optional[str] = None,
    tool_schemas: Optional[list[Dict[str, Any]]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> PlanResult:
    p = (provider or "openai").strip().lower()
    if p == "auto":
        p = auto_provider_for_model(model)

    if p == "openai":
        return await _openai_plan(
            model=model,
            system=system,
            user=user,
            plan_schema=plan_schema,  # Pass it through for gpt-5
            timeout_s=timeout_s,
            base_url=openai_base_url,
            api_key=openai_api_key,
        )
    if p == "anthropic":
        return await _anthropic_plan(
            model=model,
            system=system,
            user=user,
            timeout_s=timeout_s,
            api_key=anthropic_api_key,
            tool_schemas=tool_schemas,
            alias_map=alias_map,
        )
    if p == "google":
        return await _google_plan(
            model=model,
            system=system,
            user=user,
            timeout_s=timeout_s,
            api_key=google_api_key,
        )
    if p == "openrouter":
        return await _openrouter_plan(
            model=model,
            system=system,
            user=user,
            timeout_s=timeout_s,
            api_key=openrouter_api_key,
        )
    if p == "codex":
        return await _codex_plan(
            model=model,
            system=system,
            user=user,
            plan_schema=plan_schema,
            timeout_s=timeout_s,
        )
    raise ValueError(f"Unknown provider: {provider}")


async def plan_once(
    *,
    provider: str,
    model: str,
    system: str,
    user: str,
    plan_schema: Optional[dict] = None,
    timeout_s: int = 240,
    openai_base_url: Optional[str] = None,
    openai_api_key: Optional[str] = None,
    anthropic_api_key: Optional[str] = None,
    google_api_key: Optional[str] = None,
    openrouter_api_key: Optional[str] = None,
    tool_schemas: Optional[list[Dict[str, Any]]] = None,
    alias_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    result = await plan_once_with_usage(
        provider=provider,
        model=model,
        system=system,
        user=user,
        plan_schema=plan_schema,
        timeout_s=timeout_s,
        openai_base_url=openai_base_url,
        openai_api_key=openai_api_key,
        anthropic_api_key=anthropic_api_key,
        google_api_key=google_api_key,
        openrouter_api_key=openrouter_api_key,
        tool_schemas=tool_schemas,
        alias_map=alias_map,
    )
    return result.plan
