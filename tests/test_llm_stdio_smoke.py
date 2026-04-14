from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
from datetime import datetime

import pytest

pytestmark = pytest.mark.integration


@pytest.mark.anyio("asyncio")
@pytest.mark.timeout(120)
@pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY") or os.getenv("VEI_RUN_LLM_SMOKE") != "1",
    reason="Requires OPENAI_API_KEY and VEI_RUN_LLM_SMOKE=1",
)
async def test_llm_stdio_smoke(tmp_path: Path) -> None:
    """Live LLM + stdio MCP smoke: one plan + one tool call.

    This test requires OPENAI_API_KEY. It intentionally avoids SSE/network server
    setup and uses stdio to keep the footprint small and deterministic on the VEI side.
    """
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(override=True)
    except Exception:
        ...

    # Prepare artifacts dir for the server trace.
    # Honor pre-set VEI_ARTIFACTS_DIR (so logs can persist outside pytest tmp),
    # otherwise default to the test's tmp_path.
    preset_art = os.environ.get("VEI_ARTIFACTS_DIR")
    if preset_art:
        base = Path(preset_art)
        # Append timestamped subdir to avoid overwrites when a fixed path is used
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        art = base / f"run_{ts}"
        # Ensure server writes to the subdir as well
        os.environ["VEI_ARTIFACTS_DIR"] = str(art)
    else:
        art = tmp_path / "artifacts"
        os.environ["VEI_ARTIFACTS_DIR"] = str(art)

    # Spawn stdio MCP server (vei.router)
    from mcp.client.stdio import StdioServerParameters, stdio_client
    from mcp.client.session import ClientSession
    import sys as _sys

    params = StdioServerParameters(
        command=_sys.executable or "python3",
        args=["-m", "vei.router"],
        env={
            **os.environ,
            "VEI_DISABLE_AUTOSTART": "1",
            "PYTHONUNBUFFERED": "1",
        },
    )

    from openai import AsyncOpenAI

    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Construct a deterministic LLM prompt to select a simple tool
            client = AsyncOpenAI(
                api_key=os.environ.get("OPENAI_API_KEY"),
                base_url=os.environ.get("OPENAI_BASE_URL"),
            )
            # Hard-pin smoke test to gpt-5-mini by default for speed/predictability.
            # Respect VEI_MODEL if explicitly set.
            model = os.environ.get("VEI_MODEL", "gpt-5-mini")
            system = os.environ.get(
                "LLM_SMOKE_PROMPT",
                "You are a planner that MUST return a JSON object with keys 'tool' and 'args'. "
                "Choose 'browser.read' with {} as args. Reply with JSON only.",
            )
            # Responses API for gpt-5 family (no temperature). Enforce JSON output.

            # One observation to seed context
            obs = await session.call_tool("vei.observe", {})
            assert obs is not None

            resp = await client.responses.create(
                model=model, input=system + "\nReturn a JSON object only."
            )
            content = getattr(resp, "output_text", None)
            if not content:
                try:
                    out = getattr(resp, "output", [])
                    if out and hasattr(out[0], "content"):
                        cnt = out[0].content
                        if cnt and hasattr(cnt[0], "text"):
                            content = cnt[0].text
                except Exception:
                    content = None
            if not content:
                content = "{}"
            try:
                plan = json.loads(content)
            except Exception:
                plan = {"tool": "browser.read", "args": {}}
            tool = plan.get("tool", "browser.read")
            args = plan.get("args", {})

            res = await session.call_tool(tool, args)
            # Validate a minimal shape for browser.read
            # Depending on MCP client lib, result may be structuredContent or text JSON.
            sc = getattr(res, "structuredContent", None)
            if sc is not None:
                assert "url" in sc and "title" in sc
            else:
                content_list = getattr(res, "content", [])
                assert content_list, "expected content from tool result"
    # Ensure artifacts were created
    trace = Path(art) / "trace.jsonl"
    assert trace.exists(), "trace.jsonl should be written by the server"

    # Persist a copy under repo-local .artifacts and echo a brief tail to console
    repo_root = Path(__file__).resolve().parents[1]
    try:
        stash_dir = repo_root / ".artifacts"
        stash_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        stash_path = stash_dir / f"llm_stdio_smoke_trace.{ts}.jsonl"
        shutil.copyfile(trace, stash_path)
        # Print last few lines to stdout for quick inspection in CI/local runs
        try:
            tail_n = 20
            with trace.open("r", encoding="utf-8") as f:
                lines = f.readlines()
            print(f"[LLM smoke] trace path: {trace}")
            print(f"[LLM smoke] copied to: {stash_path}")
            print("[LLM smoke] last lines:")
            for line in lines[-tail_n:]:
                print(line.rstrip())
        except Exception:
            ...
    except Exception:
        # Non-fatal: logging persistence is best-effort
        ...
