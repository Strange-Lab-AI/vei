from __future__ import annotations

import json
import logging
import queue
from pathlib import Path

import pytest

from vei.router.core import Router
from vei.router._trace import TraceLogger


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


def test_trace_logger_warns_when_stream_queue_is_full(caplog):
    logger = TraceLogger(out_dir=None)
    logger._q = queue.Queue(maxsize=1)
    logger._q.put_nowait({"type": "call"})

    with caplog.at_level(logging.WARNING):
        logger._try_stream({"type": "event"})

    assert "trace stream queue is full" in caplog.text


def test_trace_logger_warns_when_post_fails(monkeypatch, caplog):
    logger = TraceLogger(out_dir=None)
    logger.post_url = "http://localhost:9999"

    class _FakeQueue:
        def __init__(self):
            self._calls = 0

        def get(self, timeout: float):
            if self._calls == 0:
                self._calls += 1
                return {"type": "call"}
            raise SystemExit

    def _boom(*args, **kwargs):
        raise OSError("post failed")

    logger._q = _FakeQueue()
    monkeypatch.setattr("urllib.request.urlopen", _boom)

    with caplog.at_level(logging.WARNING):
        with pytest.raises(SystemExit):
            logger._poster_loop()

    assert "trace stream post failed" in caplog.text
