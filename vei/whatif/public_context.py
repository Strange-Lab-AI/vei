from __future__ import annotations

import html
import json
import os
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timezone
from email.utils import parsedate_to_datetime
from importlib import resources
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from defusedxml import ElementTree as SafeElementTree

from .models import (
    WhatIfPublicContext,
    WhatIfPublicFinancialSnapshot,
    WhatIfPublicNewsEvent,
)

_DEFAULT_PUBLIC_CONTEXT_FILE_NAMES = ("whatif_public_context.json",)
_PUBLIC_CONTEXT_METADATA_KEYS = ("whatif_public_context_path",)
_SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
_SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
_SEC_COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
_GOOGLE_NEWS_RSS_URL = "https://news.google.com/rss/search"
_DEFAULT_FETCH_TIMEOUT_S = 20
_DEFAULT_NEWS_LIMIT = 8
_SEC_METRIC_CONCEPTS = {
    "revenue": (
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
        "Revenues",
    ),
    "net_income": ("NetIncomeLoss",),
    "assets": ("Assets",),
    "liabilities": ("Liabilities",),
    "equity": (
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ),
}
_SEC_SNAPSHOT_FORMS = {"10-K", "10-Q", "20-F", "6-K"}
_SEC_NEWS_FORMS = {"8-K", "10-K", "10-Q"}
_COMMON_COMPANY_SUFFIXES = {
    "co",
    "company",
    "corp",
    "corporation",
    "holdings",
    "inc",
    "incorporated",
    "limited",
    "llc",
    "ltd",
    "plc",
}

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _SecCompanyMatch:
    cik: str
    ticker: str
    title: str


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
    except Exception as exc:  # noqa: BLE001
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
    return slice_public_context_to_window(
        context,
        window_start=window_start,
        window_end=window_end,
    )


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
        fixture = resources.files("vei.whatif").joinpath(
            "fixtures/enron_public_context/enron_public_context_v1.json"
        )
        context = load_public_context(
            path=Path(str(fixture)),
            organization_name="Enron Corporation",
            organization_domain="enron.com",
            pack_name="enron_public_context",
            window_start=window_start,
            window_end=window_end,
        )
    except Exception as exc:  # noqa: BLE001
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
        if _date_value(snapshot.as_of) is not None
        and _date_value(snapshot.as_of) <= branch_day
    ]
    financial_snapshots = _sort_financial_snapshots(financial_snapshots)
    public_news_events = [
        event
        for event in context.public_news_events
        if _date_value(event.timestamp) is not None
        and _date_value(event.timestamp) <= branch_day
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


def _build_sec_public_context(
    match: _SecCompanyMatch,
) -> tuple[list[WhatIfPublicFinancialSnapshot], list[WhatIfPublicNewsEvent]]:
    try:
        submissions = _fetch_json(_SEC_SUBMISSIONS_URL.format(cik=match.cik))
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "whatif public context SEC submissions fetch failed for %s (%s)",
            match.title,
            type(exc).__name__,
            extra={
                "source": "public_context",
                "provider": "sec",
                "file_path": _SEC_SUBMISSIONS_URL.format(cik=match.cik),
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return [], []

    recent_filings = _sec_recent_filings(submissions, cik=match.cik)
    filing_lookup = {filing["accession"]: filing for filing in recent_filings}
    snapshots: list[WhatIfPublicFinancialSnapshot] = []
    filing_events = _sec_filing_news_events(match, recent_filings)
    try:
        companyfacts = _fetch_json(_SEC_COMPANYFACTS_URL.format(cik=match.cik))
        snapshots = _sec_financial_snapshots(
            match=match,
            companyfacts=companyfacts,
            filing_lookup=filing_lookup,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "whatif public context SEC companyfacts fetch failed for %s (%s)",
            match.title,
            type(exc).__name__,
            extra={
                "source": "public_context",
                "provider": "sec",
                "file_path": _SEC_COMPANYFACTS_URL.format(cik=match.cik),
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
    if not snapshots:
        snapshots = _sec_filing_snapshots(match, recent_filings)
    return snapshots, filing_events


def _build_news_events(
    *,
    organization_name: str,
    organization_domain: str,
    limit: int,
) -> list[WhatIfPublicNewsEvent]:
    query_tokens = [organization_name.strip()]
    if organization_domain.strip():
        query_tokens.append(organization_domain.strip().lower())
    query = " OR ".join(f'"{token}"' for token in query_tokens if token)
    if not query:
        return []
    url = f"{_GOOGLE_NEWS_RSS_URL}?" + urlencode(
        {
            "q": query,
            "hl": "en-US",
            "gl": "US",
            "ceid": "US:en",
        }
    )
    try:
        payload = _fetch_text(url)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "whatif public context news fetch failed for %s (%s)",
            organization_name or organization_domain,
            type(exc).__name__,
            extra={
                "source": "public_context",
                "provider": "news",
                "file_path": url,
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return []
    return _google_news_events(payload, limit=limit)


def public_context_prompt_lines(
    context: WhatIfPublicContext | None,
    *,
    max_financial: int = 3,
    max_news: int = 3,
) -> list[str]:
    if not public_context_has_items(context):
        return []

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


def _resolve_sec_company_match(
    *,
    organization_name: str,
    organization_domain: str,
) -> _SecCompanyMatch | None:
    try:
        payload = _fetch_json(_SEC_COMPANY_TICKERS_URL)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "whatif public context SEC directory fetch failed (%s)",
            type(exc).__name__,
            extra={
                "source": "public_context",
                "provider": "sec",
                "file_path": _SEC_COMPANY_TICKERS_URL,
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return None

    candidates = payload.values() if isinstance(payload, dict) else payload
    if not isinstance(candidates, list) and not hasattr(candidates, "__iter__"):
        return None

    best_match: _SecCompanyMatch | None = None
    best_score = 0
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        title = str(candidate.get("title") or "").strip()
        ticker = str(candidate.get("ticker") or "").strip().upper()
        cik = str(candidate.get("cik_str") or "").strip()
        if not title or not cik:
            continue
        score = _sec_company_match_score(
            title=title,
            ticker=ticker,
            organization_name=organization_name,
            organization_domain=organization_domain,
        )
        if score <= best_score:
            continue
        best_score = score
        best_match = _SecCompanyMatch(
            cik=cik.zfill(10),
            ticker=ticker,
            title=title,
        )
    if best_score < 3:
        return None
    return best_match


def _sec_company_match_score(
    *,
    title: str,
    ticker: str,
    organization_name: str,
    organization_domain: str,
) -> int:
    normalized_title = _normalized_company_text(title)
    normalized_name = _normalized_company_text(organization_name)
    domain_stem = _domain_stem(organization_domain)
    if not normalized_title or not normalized_name:
        return 0
    if normalized_title == normalized_name:
        return 10
    title_tokens = set(normalized_title.split())
    name_tokens = set(normalized_name.split())
    overlap = len(title_tokens & name_tokens)
    score = overlap * 2
    if normalized_name in normalized_title or normalized_title in normalized_name:
        score += 3
    if domain_stem:
        if domain_stem == ticker.lower():
            score += 3
        if domain_stem in normalized_title.replace(" ", ""):
            score += 2
    return score


def _sec_recent_filings(
    submissions: Mapping[str, Any],
    *,
    cik: str,
) -> list[dict[str, str]]:
    recent = submissions.get("filings", {}).get("recent", {})
    if not isinstance(recent, dict):
        return []
    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    primary_documents = recent.get("primaryDocument", [])
    descriptions = recent.get("primaryDocDescription", [])
    filings: list[dict[str, str]] = []
    count = max(
        len(forms),
        len(filing_dates),
        len(accessions),
        len(primary_documents),
        len(descriptions),
    )
    for index in range(count):
        form = _list_value(forms, index)
        if not form:
            continue
        filing_date = _list_value(filing_dates, index)
        accession = _list_value(accessions, index)
        primary_document = _list_value(primary_documents, index)
        description = _list_value(descriptions, index)
        filings.append(
            {
                "cik": cik,
                "form": form,
                "filing_date": filing_date,
                "accession": accession,
                "primary_document": primary_document,
                "description": description,
                "filing_url": _sec_filing_url(
                    cik=cik,
                    accession=accession,
                    primary_document=primary_document,
                ),
            }
        )
    return filings


def _sec_financial_snapshots(
    *,
    match: _SecCompanyMatch,
    companyfacts: Mapping[str, Any],
    filing_lookup: Mapping[str, dict[str, str]],
) -> list[WhatIfPublicFinancialSnapshot]:
    snapshot_rows: dict[str, dict[str, Any]] = {}
    for metric_name, concept_names in _SEC_METRIC_CONCEPTS.items():
        for row in _first_companyfacts_rows(companyfacts, concept_names):
            form = str(row.get("form") or "").strip()
            if form not in _SEC_SNAPSHOT_FORMS:
                continue
            end_date = str(row.get("end") or "").strip()
            filed_at = str(row.get("filed") or "").strip()
            if not end_date and not filed_at:
                continue
            metric_value = _coerce_float(row.get("val"))
            if metric_value is None:
                continue
            snapshot_key = end_date or filed_at
            snapshot_row = snapshot_rows.setdefault(
                snapshot_key,
                {
                    "snapshot_key": snapshot_key,
                    "end_date": end_date,
                    "filed_at": filed_at,
                    "form": form,
                    "metrics": {},
                    "accession": str(row.get("accn") or "").strip(),
                },
            )
            snapshot_row["metrics"][metric_name] = metric_value
            if filed_at and filed_at > str(snapshot_row.get("filed_at") or ""):
                snapshot_row["filed_at"] = filed_at
                snapshot_row["form"] = form
                snapshot_row["accession"] = str(row.get("accn") or "").strip()

    ordered_rows = sorted(
        snapshot_rows.values(),
        key=lambda row: str(row.get("filed_at") or row.get("end_date") or ""),
        reverse=True,
    )[:4]
    snapshots: list[WhatIfPublicFinancialSnapshot] = []
    for row in ordered_rows:
        accession = str(row.get("accession") or "")
        filing = filing_lookup.get(accession, {})
        as_of = str(row.get("end_date") or row.get("filed_at") or "")
        if not as_of:
            continue
        snapshots.append(
            WhatIfPublicFinancialSnapshot(
                snapshot_id=_public_snapshot_id(
                    match=match,
                    label_seed=f"{row.get('form')}-{as_of}",
                ),
                as_of=f"{as_of}T00:00:00Z" if "T" not in as_of else as_of,
                kind="sec_filing",
                label=f"{match.title} {row.get('form')} checkpoint",
                source_ids=[
                    item
                    for item in (
                        str(filing.get("filing_url") or "").strip(),
                        _SEC_COMPANYFACTS_URL.format(cik=match.cik),
                    )
                    if item
                ],
                summary=_sec_snapshot_summary(row),
                metrics={
                    key: round(float(value), 2)
                    for key, value in dict(row.get("metrics") or {}).items()
                },
            )
        )
    return snapshots


def _sec_filing_snapshots(
    match: _SecCompanyMatch,
    filings: list[dict[str, str]],
) -> list[WhatIfPublicFinancialSnapshot]:
    snapshots: list[WhatIfPublicFinancialSnapshot] = []
    for filing in filings:
        form = filing.get("form", "")
        if form not in _SEC_SNAPSHOT_FORMS:
            continue
        filing_date = filing.get("filing_date", "")
        if not filing_date:
            continue
        description = filing.get("description") or f"{form} filed"
        snapshots.append(
            WhatIfPublicFinancialSnapshot(
                snapshot_id=_public_snapshot_id(
                    match=match,
                    label_seed=f"{form}-{filing_date}",
                ),
                as_of=f"{filing_date}T00:00:00Z",
                kind="sec_filing",
                label=f"{match.title} {form} filing",
                source_ids=[filing.get("filing_url", "")],
                summary=description.strip(),
            )
        )
        if len(snapshots) >= 4:
            break
    return snapshots


def _sec_filing_news_events(
    match: _SecCompanyMatch,
    filings: list[dict[str, str]],
) -> list[WhatIfPublicNewsEvent]:
    events: list[WhatIfPublicNewsEvent] = []
    for filing in filings:
        form = filing.get("form", "")
        filing_date = filing.get("filing_date", "")
        if form not in _SEC_NEWS_FORMS or not filing_date:
            continue
        headline = filing.get("description") or f"{match.title} filed {form}"
        events.append(
            WhatIfPublicNewsEvent(
                event_id=_public_event_id(
                    prefix="sec",
                    seed=f"{form}-{filing_date}-{filing.get('accession', '')}",
                ),
                timestamp=f"{filing_date}T00:00:00Z",
                category="filing",
                headline=headline.strip(),
                summary=f"{match.title} filed {form}.",
                source_ids=[filing.get("filing_url", "")],
            )
        )
        if len(events) >= 4:
            break
    return events


def _google_news_events(
    payload: str,
    *,
    limit: int,
) -> list[WhatIfPublicNewsEvent]:
    try:
        root = SafeElementTree.fromstring(payload)
    except SafeElementTree.ParseError:
        return []
    events: list[WhatIfPublicNewsEvent] = []
    for item in root.findall(".//item"):
        title = _xml_text(item, "title")
        pub_date = _xml_text(item, "pubDate")
        link = _xml_text(item, "link")
        description = _clean_html(_xml_text(item, "description"))
        timestamp = _rss_timestamp(pub_date)
        if not title or not timestamp:
            continue
        events.append(
            WhatIfPublicNewsEvent(
                event_id=_public_event_id(prefix="news", seed=f"{title}|{timestamp}"),
                timestamp=timestamp,
                category="news",
                headline=title.strip(),
                summary=description
                or "Public news coverage identified for this company.",
                source_ids=[link] if link else [],
            )
        )
        if len(events) >= max(1, limit):
            break
    return events


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
    date_value = _date_value(value)
    if date_value is None:
        return (1, 0, tie_breaker)
    return (0, date_value, tie_breaker)


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


def _fetch_json(url: str) -> Any:
    request = Request(url, headers=_public_fetch_headers(), method="GET")
    with urlopen(request, timeout=_DEFAULT_FETCH_TIMEOUT_S) as response:  # nosec B310
        return json.loads(response.read().decode("utf-8"))


def _fetch_text(url: str) -> str:
    request = Request(url, headers=_public_fetch_headers(), method="GET")
    with urlopen(request, timeout=_DEFAULT_FETCH_TIMEOUT_S) as response:  # nosec B310
        return response.read().decode("utf-8")


def _public_fetch_headers() -> dict[str, str]:
    user_agent = os.environ.get(
        "VEI_SEC_USER_AGENT",
        "digital-enterprise-twin/1.0 support@local.invalid",
    )
    return {
        "Accept": "application/json,text/xml,application/xml,text/plain,*/*",
        "User-Agent": user_agent,
    }


def _normalized_company_text(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", value.lower())
    tokens = [
        token
        for token in cleaned.split()
        if token and token not in _COMMON_COMPANY_SUFFIXES
    ]
    return " ".join(tokens)


def _domain_stem(organization_domain: str) -> str:
    text = organization_domain.strip().lower()
    if not text:
        return ""
    return text.split(".", 1)[0]


def _list_value(values: object, index: int) -> str:
    if not isinstance(values, list) or index >= len(values):
        return ""
    return str(values[index] or "").strip()


def _first_companyfacts_rows(
    companyfacts: Mapping[str, Any],
    concept_names: tuple[str, ...],
) -> list[dict[str, Any]]:
    facts = companyfacts.get("facts", {})
    if not isinstance(facts, dict):
        return []
    for taxonomy in ("us-gaap", "dei"):
        taxonomy_facts = facts.get(taxonomy, {})
        if not isinstance(taxonomy_facts, dict):
            continue
        for concept_name in concept_names:
            concept = taxonomy_facts.get(concept_name, {})
            if not isinstance(concept, dict):
                continue
            units = concept.get("units", {})
            if not isinstance(units, dict):
                continue
            for unit_name in ("USD", "USD/shares", "shares"):
                rows = units.get(unit_name)
                if isinstance(rows, list) and rows:
                    return [row for row in rows if isinstance(row, dict)]
    return []


def _coerce_float(value: object) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sec_snapshot_summary(row: Mapping[str, Any]) -> str:
    metrics = dict(row.get("metrics") or {})
    fragments: list[str] = []
    if "revenue" in metrics:
        fragments.append(f"revenue {_format_compact_usd(metrics['revenue'])}")
    if "net_income" in metrics:
        fragments.append(f"net income {_format_compact_usd(metrics['net_income'])}")
    if "assets" in metrics:
        fragments.append(f"assets {_format_compact_usd(metrics['assets'])}")
    form = str(row.get("form") or "").strip()
    end_date = str(row.get("end_date") or row.get("filed_at") or "").strip()
    if fragments:
        return f"{form} checkpoint for {end_date}: " + ", ".join(fragments) + "."
    return f"{form} checkpoint for {end_date}."


def _format_compact_usd(value: float) -> str:
    amount = abs(value)
    if amount >= 1_000_000_000:
        return f"${value / 1_000_000_000:.1f}B"
    if amount >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if amount >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:.0f}"


def _sec_filing_url(*, cik: str, accession: str, primary_document: str) -> str:
    if not accession or not primary_document:
        return ""
    accession_dir = accession.replace("-", "")
    cik_number = str(int(cik))
    return (
        "https://www.sec.gov/Archives/edgar/data/"
        f"{cik_number}/{accession_dir}/{primary_document}"
    )


def _public_snapshot_id(*, match: _SecCompanyMatch, label_seed: str) -> str:
    normalized_seed = re.sub(r"[^a-z0-9]+", "_", label_seed.lower()).strip("_")
    return f"{match.ticker.lower() or _normalized_company_text(match.title).replace(' ', '_')}_{normalized_seed}"


def _public_event_id(*, prefix: str, seed: str) -> str:
    normalized_seed = re.sub(r"[^a-z0-9]+", "_", seed.lower()).strip("_")
    return f"{prefix}_{normalized_seed[:80]}"


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


def _xml_text(element: Any, tag: str) -> str:
    child = element.find(tag)
    if child is None or child.text is None:
        return ""
    return child.text.strip()


def _clean_html(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", value)
    cleaned = html.unescape(cleaned)
    return " ".join(cleaned.split())


def _rss_timestamp(value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError):
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return (
        parsed.astimezone(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )


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
