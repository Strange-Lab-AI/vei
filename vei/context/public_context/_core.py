from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pydantic import ValidationError
from vei.whatif_filenames import PUBLIC_CONTEXT_FILE

from .models import (
    WhatIfPublicContext,
    WhatIfPublicFinancialSnapshot,
    WhatIfPublicNewsEvent,
)
from ._fetchers import (
    _DEFAULT_NEWS_LIMIT,
    _build_news_events,
    _build_sec_public_context,
    _resolve_sec_company_match,
)

_DEFAULT_PUBLIC_CONTEXT_FILE_NAMES = (PUBLIC_CONTEXT_FILE,)
_PUBLIC_CONTEXT_METADATA_KEYS = ("whatif_public_context_path",)

logger = logging.getLogger(__name__)


def empty_public_context(
    *,
    organization_name: str = "",
    organization_domain: str = "",
    pack_name: str = "",
    window_start: str = "",
    window_end: str = "",
    branch_timestamp: str = "",
) -> WhatIfPublicContext:
    normalized_domain = organization_domain.strip().lower()
    resolved_pack_name = pack_name.strip() or _default_pack_name(normalized_domain)
    return WhatIfPublicContext(
        pack_name=resolved_pack_name,
        organization_name=organization_name,
        organization_domain=normalized_domain,
        window_start=window_start,
        window_end=window_end,
        branch_timestamp=branch_timestamp,
    )


def empty_enron_public_context(
    *,
    window_start: str = "",
    window_end: str = "",
    branch_timestamp: str = "",
) -> WhatIfPublicContext:
    return empty_public_context(
        organization_name="Enron Corporation",
        organization_domain="enron.com",
        pack_name="enron_public_context",
        window_start=window_start,
        window_end=window_end,
        branch_timestamp=branch_timestamp,
    )


def build_public_context(
    *,
    organization_name: str,
    organization_domain: str,
    live: bool = True,
    news_limit: int = _DEFAULT_NEWS_LIMIT,
) -> WhatIfPublicContext:
    normalized_domain = organization_domain.strip().lower()
    context = empty_public_context(
        organization_name=organization_name,
        organization_domain=normalized_domain,
    )
    prepared_at = _iso_now()
    if not live:
        return context.model_copy(
            update={
                "prepared_at": prepared_at,
                "integration_hint": (
                    "Template only. Fill in financial snapshots and public news events for this company."
                ),
            }
        )

    financial_snapshots: list[WhatIfPublicFinancialSnapshot] = []
    public_news_events: list[WhatIfPublicNewsEvent] = []
    notes: list[str] = []
    sec_match = _resolve_sec_company_match(
        organization_name=organization_name,
        organization_domain=normalized_domain,
    )
    if sec_match is None:
        notes.append("No SEC filing match was found.")
    else:
        sec_snapshots, sec_events = _build_sec_public_context(sec_match)
        financial_snapshots.extend(sec_snapshots)
        public_news_events.extend(sec_events)

    news_events = _build_news_events(
        organization_name=organization_name,
        organization_domain=normalized_domain,
        limit=news_limit,
    )
    if news_events:
        public_news_events.extend(news_events)
    else:
        notes.append("No recent public news items were found.")

    financial_snapshots = _sort_financial_snapshots(
        _unique_financial_snapshots(financial_snapshots)
    )
    public_news_events = _sort_public_news_events(
        _unique_public_news_events(public_news_events)
    )
    return context.model_copy(
        update={
            "prepared_at": prepared_at,
            "integration_hint": _public_context_integration_hint(
                financial_snapshots=financial_snapshots,
                public_news_events=public_news_events,
                notes=notes,
            ),
            "financial_snapshots": financial_snapshots,
            "public_news_events": public_news_events,
        }
    )


def discover_public_context_path(
    *,
    source_dir: str | Path,
    metadata: Mapping[str, Any] | None = None,
) -> Path | None:
    env_path = os.environ.get("VEI_WHATIF_PUBLIC_CONTEXT_PATH", "").strip()
    if env_path:
        return Path(env_path).expanduser().resolve()

    resolved_source_dir = Path(source_dir).expanduser().resolve()
    metadata_path = _metadata_public_context_path(
        resolved_source_dir,
        metadata=metadata,
    )
    if metadata_path is not None:
        return metadata_path

    candidate_paths = _sidecar_public_context_paths(resolved_source_dir)
    for candidate in candidate_paths:
        if candidate.exists():
            return candidate
    return None


def load_public_context(
    *,
    path: str | Path,
    organization_name: str = "",
    organization_domain: str = "",
    pack_name: str = "",
    window_start: str = "",
    window_end: str = "",
) -> WhatIfPublicContext:
    empty = empty_public_context(
        organization_name=organization_name,
        organization_domain=organization_domain,
        pack_name=pack_name,
        window_start=window_start,
        window_end=window_end,
    )
    try:
        payload = Path(path).expanduser().resolve().read_text(encoding="utf-8")
        context = WhatIfPublicContext.model_validate(json.loads(payload))
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        logger.warning(
            "whatif public context load failed for %s (%s)",
            path,
            type(exc).__name__,
            extra={
                "source": "public_context",
                "provider": "file",
                "file_path": str(path),
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return empty
    context = _fill_public_context_identity(
        context,
        organization_name=organization_name,
        organization_domain=organization_domain,
        pack_name=pack_name,
    )
    sliced = slice_public_context_to_window(
        context,
        window_start=window_start,
        window_end=window_end,
    )
    return sliced if sliced is not None else empty


def load_enron_public_context(
    *,
    window_start: str = "",
    window_end: str = "",
) -> WhatIfPublicContext:
    empty = empty_enron_public_context(
        window_start=window_start,
        window_end=window_end,
    )
    try:
        fixture = (
            Path(__file__).resolve().parents[2]
            / "whatif"
            / "fixtures"
            / "enron_public_context"
            / "enron_public_context_v1.json"
        )
        context = load_public_context(
            path=fixture,
            organization_name="Enron Corporation",
            organization_domain="enron.com",
            pack_name="enron_public_context",
            window_start=window_start,
            window_end=window_end,
        )
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        logger.warning(
            "whatif enron public context load failed (%s)",
            type(exc).__name__,
            extra={
                "source": "public_context",
                "provider": "enron_fixture",
                "file_path": "fixtures/enron_public_context/enron_public_context_v1.json",
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return empty
    return context


def resolve_world_public_context(
    *,
    source: str,
    source_dir: str | Path,
    organization_name: str,
    organization_domain: str,
    window_start: str = "",
    window_end: str = "",
    metadata: Mapping[str, Any] | None = None,
) -> WhatIfPublicContext | None:
    context_path = discover_public_context_path(
        source_dir=source_dir,
        metadata=metadata,
    )
    if context_path is not None:
        return load_public_context(
            path=context_path,
            organization_name=organization_name,
            organization_domain=organization_domain,
            window_start=window_start,
            window_end=window_end,
        )
    if source.strip().lower() == "enron":
        return load_enron_public_context(
            window_start=window_start,
            window_end=window_end,
        )
    return None


def slice_public_context_to_window(
    context: WhatIfPublicContext | None,
    *,
    window_start: str = "",
    window_end: str = "",
) -> WhatIfPublicContext | None:
    if context is None:
        return None

    start_day = _date_value(window_start)
    end_day = _date_value(window_end)

    financial_snapshots = [
        snapshot
        for snapshot in context.financial_snapshots
        if _within_bounds(
            _date_value(snapshot.as_of), start_day=start_day, end_day=end_day
        )
    ]
    financial_snapshots = _sort_financial_snapshots(financial_snapshots)
    public_news_events = [
        event
        for event in context.public_news_events
        if _within_bounds(
            _date_value(event.timestamp),
            start_day=start_day,
            end_day=end_day,
        )
    ]
    public_news_events = _sort_public_news_events(public_news_events)
    return context.model_copy(
        update={
            "window_start": window_start,
            "window_end": window_end,
            "branch_timestamp": "",
            "financial_snapshots": financial_snapshots,
            "public_news_events": public_news_events,
        }
    )


def slice_public_context_to_branch(
    context: WhatIfPublicContext | None,
    *,
    branch_timestamp: str = "",
) -> WhatIfPublicContext | None:
    if context is None:
        return None

    branch_day = _date_value(branch_timestamp)
    if branch_day is None:
        return context.model_copy(
            update={
                "branch_timestamp": branch_timestamp,
                "financial_snapshots": _sort_financial_snapshots(
                    context.financial_snapshots
                ),
                "public_news_events": _sort_public_news_events(
                    context.public_news_events
                ),
            }
        )

    financial_snapshots = [
        snapshot
        for snapshot in context.financial_snapshots
        if (snapshot_day := _date_value(snapshot.as_of)) is not None
        and snapshot_day <= branch_day
    ]
    financial_snapshots = _sort_financial_snapshots(financial_snapshots)
    public_news_events = [
        event
        for event in context.public_news_events
        if (event_day := _date_value(event.timestamp)) is not None
        and event_day <= branch_day
    ]
    public_news_events = _sort_public_news_events(public_news_events)
    return context.model_copy(
        update={
            "branch_timestamp": branch_timestamp,
            "financial_snapshots": financial_snapshots,
            "public_news_events": public_news_events,
        }
    )


def public_context_has_items(context: WhatIfPublicContext | None) -> bool:
    if context is None:
        return False
    return bool(context.financial_snapshots or context.public_news_events)


def public_context_prompt_lines(
    context: WhatIfPublicContext | None,
    *,
    max_financial: int = 4,
    max_news: int = 4,
) -> list[str]:
    if not public_context_has_items(context):
        return []
    assert context is not None

    lines = ["Public company context known by this date:"]

    financial_snapshots = list(context.financial_snapshots[-max(1, max_financial) :])
    if financial_snapshots:
        lines.append("Financial checkpoints:")
        for snapshot in financial_snapshots:
            lines.append(
                f"- {snapshot.as_of[:10]} {snapshot.label}: {snapshot.summary}"
            )

    public_news_events = list(context.public_news_events[-max(1, max_news) :])
    if public_news_events:
        lines.append("Public news checkpoints:")
        for event in public_news_events:
            lines.append(f"- {event.timestamp[:10]} {event.headline}: {event.summary}")

    return lines


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _within_bounds(
    date_value: int | None,
    *,
    start_day: int | None,
    end_day: int | None,
) -> bool:
    if date_value is None:
        return False
    if start_day is not None and date_value < start_day:
        return False
    if end_day is not None and date_value > end_day:
        return False
    return True


def _sort_financial_snapshots(
    snapshots: list[WhatIfPublicFinancialSnapshot],
) -> list[WhatIfPublicFinancialSnapshot]:
    return sorted(
        snapshots,
        key=lambda snapshot: _date_sort_key(
            snapshot.as_of,
            tie_breaker=snapshot.snapshot_id,
        ),
    )


def _sort_public_news_events(
    events: list[WhatIfPublicNewsEvent],
) -> list[WhatIfPublicNewsEvent]:
    return sorted(
        events,
        key=lambda event: _date_sort_key(
            event.timestamp,
            tie_breaker=event.event_id,
        ),
    )


def _date_sort_key(value: str, *, tie_breaker: str) -> tuple[int, int, str]:
    dv = _date_value(value)
    if dv is None:
        return (1, 0, tie_breaker)
    return (0, dv, tie_breaker)


def _date_value(value: str) -> int | None:
    timestamp_ms = _timestamp_ms(value)
    if timestamp_ms is None:
        return None
    return int(
        datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).date().toordinal()
    )


def _timestamp_ms(value: str) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(
            datetime.fromisoformat(text.replace("Z", "+00:00"))
            .astimezone(timezone.utc)
            .timestamp()
            * 1000
        )
    except ValueError:
        return None


def _public_context_integration_hint(
    *,
    financial_snapshots: list[WhatIfPublicFinancialSnapshot],
    public_news_events: list[WhatIfPublicNewsEvent],
    notes: list[str],
) -> str:
    if financial_snapshots or public_news_events:
        source_bits: list[str] = []
        if financial_snapshots:
            source_bits.append(f"{len(financial_snapshots)} SEC checkpoints")
        if public_news_events:
            source_bits.append(f"{len(public_news_events)} public events")
        detail = ", ".join(source_bits)
        return f"Built from live public data: {detail}. Review before relying on it."
    if notes:
        return " ".join(notes)
    return "No live public context was found. Fill in financial snapshots and public news events manually."


def _default_pack_name(organization_domain: str) -> str:
    if not organization_domain:
        return "public_context"
    return f"{organization_domain.replace('.', '_')}_public_context"


def _unique_financial_snapshots(
    snapshots: list[WhatIfPublicFinancialSnapshot],
) -> list[WhatIfPublicFinancialSnapshot]:
    unique: dict[str, WhatIfPublicFinancialSnapshot] = {}
    for snapshot in snapshots:
        unique[snapshot.snapshot_id] = snapshot
    return list(unique.values())


def _unique_public_news_events(
    events: list[WhatIfPublicNewsEvent],
) -> list[WhatIfPublicNewsEvent]:
    unique: dict[str, WhatIfPublicNewsEvent] = {}
    for event in events:
        unique[event.event_id] = event
    return list(unique.values())


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _fill_public_context_identity(
    context: WhatIfPublicContext,
    *,
    organization_name: str,
    organization_domain: str,
    pack_name: str,
) -> WhatIfPublicContext:
    update: dict[str, str] = {}
    if not context.organization_name and organization_name:
        update["organization_name"] = organization_name
    if not context.organization_domain and organization_domain:
        update["organization_domain"] = organization_domain.strip().lower()
    if not context.pack_name:
        update["pack_name"] = pack_name.strip() or _default_pack_name(
            organization_domain.strip().lower()
        )
    if not update:
        return context
    return context.model_copy(update=update)


def _metadata_public_context_path(
    source_dir: Path,
    *,
    metadata: Mapping[str, Any] | None,
) -> Path | None:
    if metadata is None:
        return None
    for key in _PUBLIC_CONTEXT_METADATA_KEYS:
        value = str(metadata.get(key, "") or "").strip()
        if not value:
            continue
        candidate = Path(value).expanduser()
        if not candidate.is_absolute():
            candidate = _source_root(source_dir) / candidate
        return candidate.resolve()
    return None


def _sidecar_public_context_paths(source_dir: Path) -> list[Path]:
    root = _source_root(source_dir)
    candidates = [root / filename for filename in _DEFAULT_PUBLIC_CONTEXT_FILE_NAMES]
    if source_dir.is_file():
        candidates.insert(
            0,
            root / f"{source_dir.stem}_public_context.json",
        )
    return candidates


def _source_root(source_dir: Path) -> Path:
    if source_dir.is_file():
        return source_dir.parent
    return source_dir
