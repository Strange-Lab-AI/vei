from __future__ import annotations

import json
from pathlib import Path

from vei.router.core import Router


def test_trace_entries_have_version_and_time(tmp_path: Path, monkeypatch):
    out = tmp_path / "artifacts"
    monkeypatch.setenv("VEI_ARTIFACTS_DIR", str(out))
    r = Router(seed=1, artifacts_dir=str(out))

    # Perform a call and an observation to produce both call and event entries
    r.call_and_step("browser.read", {})
    r.observe()

    trace_path = out / "trace.jsonl"
    assert trace_path.exists()
    lines = [
        json.loads(s)
        for s in trace_path.read_text(encoding="utf-8").splitlines()
        if s.strip()
    ]
    assert lines, "trace should not be empty"
    for rec in lines:
        assert rec.get("trace_version") == 1
        assert isinstance(rec.get("time_ms"), int)
