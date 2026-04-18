"""Trace logger: in-memory trace entries with optional JSONL flush and HTTP streaming."""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class TraceLogger:
    def __init__(self, out_dir: Optional[str]):
        self.out_dir = out_dir
        self.entries: List[Dict[str, Any]] = []
        self.post_url: Optional[str] = self._validated_post_url(
            os.environ.get("VEI_TRACE_POST_URL")
        )
        self._flush_idx = 0
        self.append_mode = os.environ.get("VEI_TRACE_APPEND", "1") == "1"
        self._q: queue.Queue[Dict[str, Any]] | None = None
        self._poster_thread: threading.Thread | None = None
        if self.post_url:
            self._q = queue.Queue(maxsize=256)
            self._poster_thread = threading.Thread(
                target=self._poster_loop, name="vei-trace-poster", daemon=True
            )
            self._poster_thread.start()

    @staticmethod
    def _validated_post_url(raw: Optional[str]) -> Optional[str]:
        """Allow https endpoints by default and localhost http for dev."""
        if not raw:
            return None
        try:
            parsed = urlparse(raw)
        except Exception:
            return None
        host = (parsed.hostname or "").lower()
        if parsed.scheme == "https":
            return raw
        if parsed.scheme == "http" and host in {"127.0.0.1", "localhost", "::1"}:
            return raw
        return None

    def _try_stream(self, entry: Dict[str, Any]) -> None:
        if not self._q:
            return
        try:
            self._q.put_nowait(entry)
        except queue.Full:
            logger.warning("trace stream queue is full; dropping trace entry")

    def _poster_loop(self) -> None:
        import urllib.request

        while True:
            try:
                item = self._q.get(timeout=1.0) if self._q else None
            except queue.Empty:
                continue
            if not item:
                continue
            try:
                data = json.dumps(item).encode("utf-8")
                req = urllib.request.Request(
                    self.post_url or "",
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=1.0) as _:  # nosec B310
                    pass
            except Exception:
                logger.warning(
                    "trace stream post failed for %s",
                    self.post_url,
                    exc_info=True,
                )

    def record_call(
        self, tool: str, args: Dict[str, Any], response: Any, time_ms: int
    ) -> None:
        entry = {
            "trace_version": 1,
            "type": "call",
            "tool": tool,
            "args": args,
            "response": response,
            "time_ms": time_ms,
        }
        self.entries.append(entry)
        self._try_stream(entry)

    def record_event(
        self, target: str, payload: Dict[str, Any], emitted: Any, time_ms: int
    ) -> None:
        entry = {
            "trace_version": 1,
            "type": "event",
            "target": target,
            "payload": payload,
            "emitted": emitted,
            "time_ms": time_ms,
        }
        self.entries.append(entry)
        self._try_stream(entry)

    def flush(self) -> None:
        if not self.out_dir:
            return
        os.makedirs(self.out_dir, exist_ok=True)
        path = os.path.join(self.out_dir, "trace.jsonl")
        if self.append_mode and os.path.exists(path):
            mode = "a"
        else:
            mode = "w"
            self._flush_idx = 0
        with open(path, mode, encoding="utf-8") as f:
            for entry in self.entries[self._flush_idx :]:
                f.write(json.dumps(entry, separators=(",", ":")) + "\n")
        self._flush_idx = len(self.entries)
