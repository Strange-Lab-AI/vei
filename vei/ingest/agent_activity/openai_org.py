"""OpenAI organization usage/audit ingest adapter.

OpenAI org usage is aggregate evidence, not a per-call trace. This adapter
therefore emits ``llm.usage.observed`` unless the caller supplies already
expanded records through tests or future API responses.
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from typing import Any, Iterable

from vei.events.api import CanonicalEvent, build_llm_usage_observed, stable_event_id

from .api import RawAgentActivity


class OpenAIOrgAdapter:
    source_name = "openai_org"

    def __init__(
        self,
        *,
        token_env: str = "OPENAI_ADMIN_KEY",
        tenant_id: str = "",
        base_url: str = "https://api.openai.com/v1",
        records: list[dict[str, Any]] | None = None,
    ) -> None:
        self.token_env = token_env
        self.tenant_id = tenant_id
        self.base_url = base_url.rstrip("/")
        self._records = records

    def fetch(self, window: str = "") -> Iterable[RawAgentActivity]:
        if self._records is not None:
            for idx, record in enumerate(self._records):
                yield self._raw_from_record(record, idx)
            return
        token = os.environ.get(self.token_env, "").strip()
        if not token:
            raise RuntimeError(f"missing OpenAI admin token env var: {self.token_env}")
        params = urllib.parse.urlencode({"limit": 31})
        url = f"{self.base_url}/organization/costs?{params}"
        req = urllib.request.Request(
            url,
            headers={"Authorization": f"Bearer {token}"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:  # nosec B310
            payload = json.loads(resp.read().decode("utf-8"))
        for idx, bucket in enumerate(payload.get("data", [])):
            yield self._raw_from_record(bucket, idx)

    def _raw_from_record(self, record: dict[str, Any], idx: int) -> RawAgentActivity:
        record_id = str(record.get("id") or record.get("start_time") or idx)
        return RawAgentActivity(
            source=self.source_name,
            source_record_id=record_id,
            ts_ms=int(record.get("start_time", 0) or 0) * 1000,
            kind="llm.usage.observed",
            provider="openai",
            model=str(record.get("model", "")),
            source_granularity="aggregate",
            payload=record,
        )

    def to_canonical_events(self, raw: RawAgentActivity) -> Iterable[CanonicalEvent]:
        source_id = f"{self.source_name}:{raw.source_record_id}"
        yield build_llm_usage_observed(
            event_id=stable_event_id(source_id, "llm.usage.observed"),
            tenant_id=self.tenant_id,
            ts_ms=raw.ts_ms,
            provider="openai",
            model=raw.model,
            source_id=source_id,
            bucket_start=str(raw.payload.get("start_time", "")),
            bucket_end=str(raw.payload.get("end_time", "")),
            usage=raw.payload,
        )
