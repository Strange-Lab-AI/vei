from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Optional

import typer

try:  # optional dependency; present when installing extras [llm]
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - fallback when not installed

    def load_dotenv(*args: object, **kwargs: object) -> None:
        return None


import sys
from mcp.client.session import ClientSession
from mcp.client.sse import sse_client
from mcp.client.stdio import StdioServerParameters, stdio_client
from ._llm_loop import extract_plan  # robust JSON extraction (handles ```json blocks)

app = typer.Typer(add_completion=False)


async def _call(session: ClientSession, tool: str, args: dict) -> dict:
    return await session.call_tool(tool, args)


def _ensure_sse_available(sse_url: str, autostart: bool) -> bool:
    def _port_open(host: str, port: int) -> bool:
        import socket

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.2)
            try:
                return s.connect_ex((host, port)) == 0
            except Exception:
                return False

    from urllib.parse import urlparse

    parsed = urlparse(sse_url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 3001
    if _port_open(host, port):
        return True
    if not autostart:
        return False
    env = os.environ.copy()
    env.setdefault("VEI_HOST", host)
    env.setdefault("VEI_PORT", str(port))
    # Propagate artifacts/scenario to the spawned server so traces are written
    for key in (
        "VEI_ARTIFACTS_DIR",
        "VEI_SCENARIO",
        "VEI_SCENARIO_CONFIG",
        "VEI_SCENARIO_RANDOM",
    ):
        val = os.environ.get(key)
        if val:
            env[key] = val
    import sys as _sys

    subprocess.Popen([_sys.executable or "python3", "-m", "vei.router.sse"], env=env)
    # Wait up to ~8 seconds for server to bind
    for _ in range(80):
        if _port_open(host, port):
            return True
        time.sleep(0.1)
    return False


@app.command()
def run(
    sse_url: str = typer.Option(
        os.environ.get("VEI_SSE_URL", "http://127.0.0.1:3001/sse"),
        help="MCP SSE endpoint",
    ),
    artifacts_dir: Path | None = typer.Option(
        None, help="Artifacts directory for server trace"
    ),
    autostart: bool = typer.Option(
        True, help="Auto-start local SSE server if not reachable"
    ),
    mode: str = typer.Option(
        "scripted",
        help="'scripted' (no API key) or 'llm' (requires OPENAI_API_KEY)",
        show_default=True,
    ),
    model: str = typer.Option("gpt-5", help="Model for LLM mode"),
    task: str | None = typer.Option(
        "Research specs, Slack approval < $3200, email vendor.", help="LLM task"
    ),
    max_steps: int = typer.Option(12, help="Max steps for LLM mode"),
    openai_base_url: str | None = typer.Option(None, help="Override OPENAI_BASE_URL"),
    openai_api_key: str | None = typer.Option(None, help="Override OPENAI_API_KEY"),
    score: bool = typer.Option(False, help="Print score summary after transcript"),
    verbose: bool = typer.Option(False, help="Print step-by-step progress to stdout"),
    transcript_out: Optional[Path] = typer.Option(
        None, help="Optional path to write final transcript.json"
    ),
    timeout_s: int = typer.Option(30, help="Per-LLM-call timeout in seconds"),
    transport: str = typer.Option(
        "stdio", help="MCP transport for inner loop: 'stdio' (default) or 'sse'"
    ),
) -> None:
    load_dotenv(override=True)
    # Fallback: if key not loaded, try explicit project .env path
    if not os.getenv("OPENAI_API_KEY"):
        try:
            proj_env = Path.cwd() / ".env"
            if proj_env.exists():
                load_dotenv(dotenv_path=str(proj_env), override=True)
        except Exception:
            ...
    if artifacts_dir:
        os.environ["VEI_ARTIFACTS_DIR"] = str(artifacts_dir)

    # Only require SSE when explicitly requested
    if transport.strip().lower() == "sse":
        ok = _ensure_sse_available(sse_url, autostart)
        if not ok:
            print(
                f"Error: SSE server not reachable at {sse_url}. Use --autostart or start it manually (python -m vei.router.sse).",
                flush=True,
            )
            raise SystemExit(1)

    if mode.strip().lower() == "llm":
        # Ensure API key is available for LLM runs
        if not (openai_api_key or os.getenv("OPENAI_API_KEY")):
            raise typer.BadParameter(
                "OPENAI_API_KEY not set (put it in .env or pass --openai-api-key)"
            )
        from openai import AsyncOpenAI

        transcript: list[dict[str, Any]] = []

        async def _llm() -> list[dict[str, Any]]:
            # Open MCP session via selected transport
            transport_mode = transport.strip().lower()
            if transport_mode == "stdio":
                env = os.environ.copy()
                # Prevent background SSE autostart in the child to keep stdio clean
                env["VEI_DISABLE_AUTOSTART"] = "1"
                # Ensure unbuffered IO for prompt server handshake
                env.setdefault("PYTHONUNBUFFERED", "1")
                py = sys.executable or "python3"
                params = StdioServerParameters(
                    command=py, args=["-m", "vei.router"], env=env
                )
                ctx = stdio_client(params, errlog=sys.stderr)
            else:
                ctx = sse_client(sse_url)
            async with ctx as (read, write):
                if verbose:
                    typer.echo(f"MCP connect via {transport_mode}…")
                async with ClientSession(read, write) as s:
                    # If initialize hangs, fail fast with a clear message
                    try:
                        await asyncio.wait_for(s.initialize(), timeout=20)
                    except Exception as e:
                        typer.echo(
                            f"Error: MCP {transport_mode} session.initialize timed out or failed ({e}). "
                            + (
                                "Try running 'python -m vei.router' manually."
                                if transport_mode == "stdio"
                                else "Confirm the SSE server is running at the given URL."
                            ),
                            err=True,
                        )
                        raise
                    if verbose:
                        typer.echo("MCP session initialized.")

                    # Resolve base URL with an explicit override; allow "default" to force the SDK default
                    effective_base_url: str | None
                    if (
                        openai_base_url is not None
                        and openai_base_url.strip().lower() == "default"
                    ):
                        effective_base_url = None
                    elif openai_base_url:
                        effective_base_url = openai_base_url
                    else:
                        effective_base_url = os.environ.get("OPENAI_BASE_URL")
                    client = AsyncOpenAI(
                        base_url=effective_base_url,
                        api_key=openai_api_key or os.environ.get("OPENAI_API_KEY"),
                    )
                    if verbose:
                        typer.echo(
                            f"LLM base_url={'default' if not effective_base_url else effective_base_url}"
                        )
                    # Define a JSON schema for plan outputs to increase reliability while preserving freedom
                    allowed_tools = [
                        "vei.observe",
                        "vei.tick",
                        "vei.help",
                        "browser.read",
                        "browser.find",
                        "browser.open",
                        "browser.click",
                        "browser.back",
                        "slack.send_message",
                        "mail.compose",
                        "mail.list",
                        "mail.open",
                        "mail.reply",
                    ]
                    plan_schema = {
                        "name": "vei.plan.schema",
                        "schema": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["tool", "args"],
                            "properties": {
                                "tool": {"type": "string", "enum": allowed_tools},
                                "args": {"type": "object"},
                            },
                        },
                    }

                    # Helper: normalize MCP CallToolResult into JSON-safe dicts
                    def _normalize_result(res: Any) -> dict:
                        try:
                            if getattr(res, "isError", False):
                                return {
                                    "error": True,
                                    "content": getattr(res, "content", None),
                                }
                            sc = getattr(res, "structuredContent", None)
                            if sc is not None:
                                return sc
                            content = getattr(res, "content", None)
                            if (
                                content
                                and isinstance(content, list)
                                and getattr(content[0], "text", None)
                            ):
                                import json as _json

                                txt = content[0].text
                                try:
                                    return _json.loads(txt)
                                except Exception:
                                    return {"text": txt}
                            return {"result": res}
                        except Exception:
                            return {"result": str(res)}

                    messages: list[dict] = [
                        {
                            "role": "system",
                            "content": (
                                'You are a planner that MUST return a JSON object with keys "tool" and "args". '
                                "Use MCP tools to achieve the task. You may call any of: vei.* (observe,tick), browser.*, slack.*, mail.*. "
                                "The action_menu suggests common actions for the current focus but is not exhaustive. Reply with JSON only."
                            ),
                        }
                    ]
                    if task:
                        messages.append({"role": "user", "content": f"Task: {task}"})
                    # Simple 1-tool-per-step loop
                    for i in range(max_steps):
                        obs_raw = await _call(s, "vei.observe", {})
                        obs = _normalize_result(obs_raw)
                        transcript.append({"observation": obs})
                        _append_transcript_line(
                            {"observation": obs}, os.environ.get("VEI_ARTIFACTS_DIR")
                        )
                        _print_observation(verbose, obs)
                        _append_transcript_line(
                            {"observation": obs}, os.environ.get("VEI_ARTIFACTS_DIR")
                        )
                        pend = obs.get("pending_events", {})
                        # Only early-stop after at least one step to allow the model to act
                        if (
                            i > 0
                            and pend.get("mail", 0) == 0
                            and pend.get("slack", 0) == 0
                        ):
                            break
                        menu = obs.get("action_menu", [])
                        menu_text = "\n".join(
                            f"- {m.get('tool')} {json.dumps(m.get('args', m.get('args_schema', {})))}"
                            for m in menu
                        )
                        user = (
                            f"Time: {obs.get('time_ms')}\nFocus: {obs.get('focus')}\nSummary: {obs.get('summary')}\n"
                            f"Pending: {json.dumps(obs.get('pending_events'))}\nAction menu (choose ONE):\n{menu_text}\n"
                            'Reply strictly as JSON {"tool": str, "args": object}.'
                        )
                        messages.append({"role": "user", "content": user})
                        # Responses API with reasoning for gpt-5; hard timeout wrapper
                        prompt = (
                            "\n".join(f"[{m['role']}] {m['content']}" for m in messages)
                            + '\nReply strictly as JSON {"tool": str, "args": object}.'
                        )
                        try:
                            if verbose:
                                typer.echo("LLM: requesting plan via Responses API…")
                            # Try with JSON schema enforcement first; fall back if unsupported
                            try:
                                resp = await asyncio.wait_for(
                                    client.responses.create(
                                        model=model,
                                        input=prompt,
                                        response_format={
                                            "type": "json_schema",
                                            "json_schema": plan_schema,
                                        },
                                        max_output_tokens=256,
                                        reasoning={"effort": "high"},
                                    ),
                                    timeout=timeout_s,
                                )
                            except Exception:
                                resp = await asyncio.wait_for(
                                    client.responses.create(
                                        model=model,
                                        input=prompt,
                                        max_output_tokens=256,
                                        reasoning={"effort": "high"},
                                    ),
                                    timeout=timeout_s,
                                )
                            raw = getattr(resp, "output_text", None)
                            if not raw:
                                out = getattr(resp, "output", None)
                                if out and isinstance(out, list):
                                    try:
                                        raw = out[0].content[0].text
                                    except Exception:
                                        raw = "{}"
                                else:
                                    raw = "{}"
                            if verbose:
                                typer.echo(
                                    f"LLM: received plan text (len={len(raw or '')})"
                                )
                        except asyncio.TimeoutError:
                            if verbose:
                                typer.echo("LLM: timeout waiting for Responses API")
                            transcript.append(
                                {"error": {"step": i, "message": "llm_timeout"}}
                            )
                            _append_transcript_line(
                                {"error": {"step": i, "message": "llm_timeout"}},
                                os.environ.get("VEI_ARTIFACTS_DIR"),
                            )
                            break
                        except Exception as e:
                            if verbose:
                                typer.echo(f"LLM: error {e}")
                            transcript.append({"error": {"step": i, "message": str(e)}})
                            _append_transcript_line(
                                {"error": {"step": i, "message": str(e)}},
                                os.environ.get("VEI_ARTIFACTS_DIR"),
                            )
                            break
                        plan = extract_plan(raw, default_tool="vei.observe")
                        # Enforce non-empty JSON plan shape; default and log when empty/invalid
                        if not isinstance(plan, dict) or not plan.get("tool"):
                            # One-shot retry with terse corrective if model returned invalid content
                            if verbose:
                                typer.echo(
                                    "LLM: invalid/empty plan; retrying once with corrective hint"
                                )
                            try:
                                retry_prompt = (
                                    prompt
                                    + "\nReturn a strict JSON object with keys tool (enum) and args (object)."
                                )
                                resp2 = await asyncio.wait_for(
                                    client.responses.create(
                                        model=model,
                                        input=retry_prompt,
                                        response_format={
                                            "type": "json_schema",
                                            "json_schema": plan_schema,
                                        },
                                        max_output_tokens=192,
                                    ),
                                    timeout=max(8, timeout_s // 2),
                                )
                                raw2 = getattr(resp2, "output_text", None) or "{}"
                                plan = extract_plan(raw2, default_tool="vei.observe")
                            except Exception:
                                plan = {"tool": "vei.observe", "args": {}}
                        tool = str(plan.get("tool"))
                        args = (
                            plan.get("args", {})
                            if isinstance(plan.get("args"), dict)
                            else {}
                        )
                        if tool not in allowed_tools:
                            if verbose:
                                typer.echo(
                                    f"LLM: unsupported tool '{tool}', falling back to vei.observe"
                                )
                            tool, args = "vei.observe", {}
                        if tool == "vei.observe":
                            try:
                                result_raw = await _call(s, tool, args)
                                result = _normalize_result(result_raw)
                            except Exception as e:
                                result = {"error": str(e)}
                            transcript.append(
                                {
                                    "action": {
                                        "tool": tool,
                                        "args": args,
                                        "result": result,
                                    }
                                }
                            )
                            _append_transcript_line(
                                {
                                    "action": {
                                        "tool": tool,
                                        "args": args,
                                        "result": result,
                                    }
                                },
                                os.environ.get("VEI_ARTIFACTS_DIR"),
                            )
                            _print_action(verbose, tool, args, result)
                        else:
                            # Execute and observe in one deterministic step
                            try:
                                ao_raw = await _call(
                                    s,
                                    "vei.act_and_observe",
                                    {"tool": tool, "args": args},
                                )
                                ao = _normalize_result(ao_raw)
                                result = ao.get("result", ao)
                                obs2 = ao.get("observation")
                            except Exception as e:
                                result = {"error": str(e)}
                                obs2 = None
                            transcript.append(
                                {
                                    "action": {
                                        "tool": tool,
                                        "args": args,
                                        "result": result,
                                    }
                                }
                            )
                            _append_transcript_line(
                                {
                                    "action": {
                                        "tool": tool,
                                        "args": args,
                                        "result": result,
                                    }
                                },
                                os.environ.get("VEI_ARTIFACTS_DIR"),
                            )
                            _print_action(verbose, tool, args, result)
                            if isinstance(obs2, dict):
                                transcript.append({"observation": obs2})
                                _append_transcript_line(
                                    {"observation": obs2},
                                    os.environ.get("VEI_ARTIFACTS_DIR"),
                                )
                                _print_observation(verbose, obs2)

                            # Auto-drain pending events after key actions to ensure bounded runs
                            if tool in {"mail.compose", "slack.send_message"}:
                                try:
                                    await _call(s, "vei.tick", {"dt_ms": 20000})
                                    obs3_raw = await _call(s, "vei.observe", {})
                                    obs3 = _normalize_result(obs3_raw)
                                    transcript.append({"observation": obs3})
                                    _append_transcript_line(
                                        {"observation": obs3},
                                        os.environ.get("VEI_ARTIFACTS_DIR"),
                                    )
                                    _print_observation(verbose, obs3)
                                    p3 = obs3.get("pending_events", {})
                                    if (
                                        p3.get("mail", 0) == 0
                                        and p3.get("slack", 0) == 0
                                    ):
                                        break
                                except Exception:
                                    ...
                    return transcript

        try:
            out = asyncio.run(_llm())
        except Exception as e:
            typer.echo(f"Error running LLM demo: {e}", err=True)
            raise typer.Exit(code=1)
    else:
        try:
            # Scripted episode supports both transports as well
            async def _scripted() -> list[dict[str, Any]]:
                transport_mode = transport.strip().lower()
                if transport_mode == "stdio":
                    env = os.environ.copy()
                    env["VEI_DISABLE_AUTOSTART"] = "1"
                    env.setdefault("PYTHONUNBUFFERED", "1")
                    py = sys.executable or "python3"
                    params = StdioServerParameters(
                        command=py, args=["-m", "vei.router"], env=env
                    )
                    ctx = stdio_client(params, errlog=sys.stderr)
                else:
                    ctx = sse_client(sse_url)
                transcript: list[dict[str, Any]] = []
                async with ctx as (read, write):
                    if verbose:
                        typer.echo(f"MCP connect via {transport_mode}…")
                    async with ClientSession(read, write) as s:
                        try:
                            await asyncio.wait_for(s.initialize(), timeout=20)
                        except Exception as e:
                            typer.echo(
                                f"Error: MCP {transport_mode} session.initialize timed out or failed ({e}). "
                                + (
                                    "Try running 'python -m vei.router' manually."
                                    if transport_mode == "stdio"
                                    else "Confirm the SSE server is running at the given URL."
                                ),
                                err=True,
                            )
                            raise
                        if verbose:
                            typer.echo("MCP session initialized.")

                        # Local normalizer mirrors LLM branch
                        def _normalize_result(res: Any) -> dict:
                            try:
                                if getattr(res, "isError", False):
                                    return {
                                        "error": True,
                                        "content": getattr(res, "content", None),
                                    }
                                sc = getattr(res, "structuredContent", None)
                                if sc is not None:
                                    return sc
                                content = getattr(res, "content", None)
                                if (
                                    content
                                    and isinstance(content, list)
                                    and getattr(content[0], "text", None)
                                ):
                                    import json as _json

                                    txt = content[0].text
                                    try:
                                        return _json.loads(txt)
                                    except Exception:
                                        return {"text": txt}
                                return {"result": res}
                            except Exception:
                                return {"result": str(res)}

                        obs_raw = await _call(s, "vei.observe", {})
                        obs = _normalize_result(obs_raw)
                        transcript.append({"observation": obs})
                        _append_transcript_line(
                            {"observation": obs}, os.environ.get("VEI_ARTIFACTS_DIR")
                        )
                        res_raw = await _call(s, "browser.read", {})
                        res = _normalize_result(res_raw)
                        transcript.append(
                            {
                                "action": {
                                    "tool": "browser.read",
                                    "args": {},
                                    "result": res,
                                }
                            }
                        )
                        _append_transcript_line(
                            {
                                "action": {
                                    "tool": "browser.read",
                                    "args": {},
                                    "result": res,
                                }
                            },
                            os.environ.get("VEI_ARTIFACTS_DIR"),
                        )
                        res = await _call(
                            s,
                            "slack.send_message",
                            {
                                "channel": "#procurement",
                                "text": "Summary: budget $3200, citations included.",
                            },
                        )
                        res_n = _normalize_result(res)
                        transcript.append(
                            {
                                "action": {
                                    "tool": "slack.send_message",
                                    "args": {"channel": "#procurement"},
                                    "result": res_n,
                                }
                            }
                        )
                        _append_transcript_line(
                            {
                                "action": {
                                    "tool": "slack.send_message",
                                    "args": {"channel": "#procurement"},
                                    "result": res_n,
                                }
                            },
                            os.environ.get("VEI_ARTIFACTS_DIR"),
                        )
                        res = await _call(
                            s,
                            "mail.compose",
                            {
                                "to": "sales@macrocompute.example",
                                "subj": "Quote request",
                                "body_text": "Please send latest price and ETA.",
                            },
                        )
                        res_n = _normalize_result(res)
                        transcript.append(
                            {
                                "action": {
                                    "tool": "mail.compose",
                                    "args": {"to": "sales@macrocompute.example"},
                                    "result": res_n,
                                }
                            }
                        )
                        _append_transcript_line(
                            {
                                "action": {
                                    "tool": "mail.compose",
                                    "args": {"to": "sales@macrocompute.example"},
                                    "result": res_n,
                                }
                            },
                            os.environ.get("VEI_ARTIFACTS_DIR"),
                        )
                        for _ in range(24):
                            obs_raw = await _call(s, "vei.observe", {"focus": "mail"})
                            obs = _normalize_result(obs_raw)
                            transcript.append({"observation": obs})
                            _append_transcript_line(
                                {"observation": obs},
                                os.environ.get("VEI_ARTIFACTS_DIR"),
                            )
                            pend = obs.get("pending_events", {})
                            if pend.get("mail", 0) == 0 and pend.get("slack", 0) == 0:
                                break
                        return transcript

            out = asyncio.run(_scripted())
            # Also print scripted progress if requested
            if verbose:
                for entry in out:
                    if "observation" in entry:
                        _print_observation(True, entry["observation"])  # always print
                    if "action" in entry:
                        a = entry["action"]
                        _print_action(
                            True, a.get("tool"), a.get("args", {}), a.get("result")
                        )
        except Exception as e:
            typer.echo(f"Error running scripted demo: {e}", err=True)
            raise typer.Exit(code=1)

    # Emit final transcript
    final_json = json.dumps(out, indent=2)
    typer.echo(final_json)
    if transcript_out:
        try:
            Path(transcript_out).write_text(final_json, encoding="utf-8")
        except Exception:
            ...

    # If we captured artifacts, try to score
    if score:
        try:
            from vei.cli.vei_score import score as score_cmd
            from typer.testing import CliRunner as _CliRunner

            if os.environ.get("VEI_ARTIFACTS_DIR"):
                runner = _CliRunner()
                result = runner.invoke(
                    score_cmd, ["--artifacts-dir", os.environ["VEI_ARTIFACTS_DIR"]]
                )
                if result.exit_code == 0:
                    typer.echo("\nScore:")
                    typer.echo(result.stdout)
        except Exception:
            ...


if __name__ == "__main__":
    app()


def _append_transcript_line(
    entry: dict[str, Any], artifacts_dir: Optional[str]
) -> None:
    if not artifacts_dir:
        return
    try:
        outp = Path(artifacts_dir) / "transcript.jsonl"
        with open(outp, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, separators=(",", ":")) + "\n")
    except Exception:
        ...


def _print_observation(verbose: bool, obs: dict[str, Any]) -> None:
    if not verbose:
        return
    summary = obs.get("summary")
    pending = obs.get("pending_events")
    focus = obs.get("focus")
    typer.echo(f"OBS focus={focus} pending={pending} summary={summary}")


def _print_action(
    verbose: bool, tool: Optional[str], args: dict[str, Any], result: Any
) -> None:
    if not verbose:
        return
    try:
        args_s = json.dumps(args, separators=(",", ":"))
    except Exception:
        args_s = str(args)
    compact = result
    if isinstance(result, dict):
        compact = {k: result[k] for k in list(result.keys())[:3]}
    typer.echo(f"ACT {tool} args={args_s} result~={compact}")
