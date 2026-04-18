from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import re
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from defusedxml import ElementTree as SafeElementTree  # type: ignore[import-untyped]

from .models import (
    WhatIfPublicFinancialSnapshot,
    WhatIfPublicNewsEvent,
)

_SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
_SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
_SEC_COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
_GOOGLE_NEWS_RSS_URL = "https://news.google.com/rss/search"
_DEFAULT_FETCH_TIMEOUT_S = 20
_DEFAULT_CACHE_TTL_HOURS = 24
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


def _resolve_sec_company_match(
    *,
    organization_name: str,
    organization_domain: str,
) -> _SecCompanyMatch | None:
    try:
        key = _cache_key(organization_name, organization_domain, "sec_tickers")
        payload = _cached_fetch(key, lambda: _fetch_json(_SEC_COMPANY_TICKERS_URL))
    except (OSError, json.JSONDecodeError, ValueError) as exc:
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


def _build_sec_public_context(
    match: _SecCompanyMatch,
) -> tuple[list[WhatIfPublicFinancialSnapshot], list[WhatIfPublicNewsEvent]]:
    try:
        submissions = _fetch_json(_SEC_SUBMISSIONS_URL.format(cik=match.cik))
    except (OSError, json.JSONDecodeError, ValueError) as exc:
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
    except (OSError, json.JSONDecodeError, ValueError) as exc:
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
        key = _cache_key(organization_name, organization_domain, "google_news")
        payload = _cached_fetch(key, lambda: _fetch_text(url))
    except (OSError, json.JSONDecodeError, ValueError) as exc:
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


# ---------------------------------------------------------------------------
# SEC helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def _cache_key(
    organization_name: str,
    organization_domain: str,
    function_name: str,
) -> str:
    raw = f"{organization_name}|{organization_domain}|{function_name}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _cached_fetch(
    cache_key: str,
    fetcher: Callable[[], Any],
    *,
    ttl_hours: int = _DEFAULT_CACHE_TTL_HOURS,
) -> Any:
    if os.environ.get("VEI_PUBLIC_CONTEXT_CACHE_DISABLE") == "1":
        return fetcher()
    env_ttl = os.environ.get("VEI_PUBLIC_CONTEXT_CACHE_TTL_HOURS")
    if env_ttl is not None:
        ttl_hours = int(env_ttl)
    cache_dir = (
        Path(os.environ.get("VEI_ARTIFACTS_DIR", "_vei_out")) / "public_context_cache"
    )
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{cache_key}.json"
    if cache_file.exists():
        age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
        if age_hours < ttl_hours:
            cached = _read_cached_payload(cache_file)
            if cached is not None:
                return cached
    result = fetcher()
    _write_cached_payload(cache_file, result)
    return result


def _read_cached_payload(cache_file: Path) -> Any | None:
    try:
        return json.loads(cache_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        logger.warning(
            "whatif public context cache read failed for %s (%s); refetching",
            cache_file,
            type(exc).__name__,
            extra={
                "source": "public_context",
                "provider": "cache",
                "file_path": str(cache_file),
                "exception_type": type(exc).__name__,
            },
        )
        with suppress(OSError):
            cache_file.unlink()
        return None


def _write_cached_payload(cache_file: Path, result: Any) -> None:
    payload = json.dumps(result, default=str)
    temp_path = cache_file.with_suffix(
        f"{cache_file.suffix}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temp_path.write_text(payload, encoding="utf-8")
        temp_path.replace(cache_file)
    except OSError:
        logger.warning(
            "whatif public context cache write failed for %s",
            cache_file,
            extra={
                "source": "public_context",
                "provider": "cache",
                "file_path": str(cache_file),
            },
            exc_info=True,
        )
    finally:
        with suppress(OSError):
            temp_path.unlink()


# ---------------------------------------------------------------------------
# Text / parsing helpers
# ---------------------------------------------------------------------------


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
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
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
