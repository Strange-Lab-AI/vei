#!/usr/bin/env python3
"""Build a VEI company-history-style snapshot from a bounded news corpus sample."""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import re
import shutil
import subprocess
import tarfile
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from vei.context.api import (
    ContextSnapshot,
    ContextSourceResult,
    write_canonical_history_sidecars,
)

_PLEIAS_REPO = "PleIAs/US-PD-Newspapers"
_AMERICANSTORIES_REPO = "dell-research-harvard/AmericanStories"
_DEFAULT_PLEIAS_SHARDS = (
    "az_educatedfella_ver02.parquet",
    "dlc_divebomb_ver01.parquet",
    "ct_arnold_ver02.parquet",
    "fu_estero_ver02.parquet",
)
_TOPIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "war_foreign_affairs": (
        "texas",
        "mexico",
        "santa anna",
        "seminole",
        "cherokee",
        "creek",
        "hitler",
        "nazi",
        "germany",
        "mussolini",
        "italy",
        "spain",
        "war",
        "league of nations",
        "ethiopia",
    ),
    "government_policy": (
        "president",
        "jackson",
        "van buren",
        "roosevelt",
        "new deal",
        "wpa",
        "relief",
        "congress",
        "supreme court",
        "social security",
        "administration",
        "cabinet",
        "senate",
        "post office",
    ),
    "banking_markets": (
        "specie",
        "currency",
        "deposit",
        "panic",
        "credit",
        "stock",
        "market",
        "bank",
        "bond",
        "business",
        "trade",
        "industry",
        "dollar",
        "prices",
    ),
    "labor_work": (
        "labor",
        "strike",
        "union",
        "wage",
        "workers",
        "factory",
        "employment",
    ),
    "agriculture_weather": (
        "farm",
        "crop",
        "drought",
        "agriculture",
        "livestock",
        "cotton",
        "wheat",
        "soil",
    ),
    "slavery_abolition": (
        "slavery",
        "abolition",
        "abolitionist",
        "slave",
        "slaves",
        "anti-slavery",
        "emancipation",
    ),
    "public_health_disaster": (
        "flood",
        "storm",
        "fire",
        "disease",
        "hospital",
        "death",
        "relief",
        "emergency",
    ),
    "crime_courts": (
        "court",
        "trial",
        "police",
        "crime",
        "arrest",
        "jury",
        "judge",
        "murder",
    ),
    "transport_infrastructure": (
        "aviation",
        "airplane",
        "railroad",
        "ship",
        "highway",
        "automobile",
        "transport",
    ),
}
_ENGLISH_COMMON_WORDS = (
    "the",
    "and",
    "that",
    "for",
    "with",
    "from",
    "this",
    "have",
    "will",
    "were",
    "government",
    "president",
    "court",
    "market",
    "state",
    "city",
    "county",
    "people",
)
_NON_NEWS_PATTERNS = (
    "auction",
    "cash store",
    "estate of",
    "for rent",
    "for sale",
    "great bargains",
    "new goods",
    "notice is hereby given",
    "proposals will be received",
    "public sale",
    "runaway",
    "sheriff's sale",
    "strayed",
    "taken up",
    "to rent",
    "wanted",
)


@dataclass(frozen=True)
class NewsPage:
    dataset_repo: str
    source_domain: str
    source_id: str
    shard: str
    page_id: str
    page_date: str
    page_no: str
    file_name: str
    word_count: int
    title: str
    text: str
    topic: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a bounded news context snapshot for VEI world-model benchmarks.",
    )
    parser.add_argument(
        "--dataset",
        choices=["pleias", "americanstories"],
        default="pleias",
        help="News dataset adapter to use.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("_vei_out/news_world_model/pleias_1935_1939_sample"),
    )
    parser.add_argument("--start-date", default="1935-01-01")
    parser.add_argument("--end-date", default="1939-12-31")
    parser.add_argument("--max-pages-per-day", type=int, default=20)
    parser.add_argument("--max-pages-per-source-per-day", type=int, default=2)
    parser.add_argument("--body-excerpt-chars", type=int, default=1600)
    parser.add_argument("--min-ascii-alpha-ratio", type=float, default=0.85)
    parser.add_argument("--min-english-common-word-hits", type=int, default=10)
    parser.add_argument(
        "--allow-non-english",
        action="store_true",
        help="Disable the default English/OCR-quality filter.",
    )
    parser.add_argument(
        "--include-notices-and-ads",
        action="store_true",
        help="Keep likely ads, classifieds, auctions, and legal notices.",
    )
    parser.add_argument(
        "--shard",
        action="append",
        default=[],
        help="PleIAs parquet shard path. Repeat to override the default bounded shard set.",
    )
    parser.add_argument(
        "--year",
        action="append",
        type=int,
        default=[],
        help="AmericanStories year archive. Repeat to override date-range years.",
    )
    parser.add_argument(
        "--keep-parquet",
        action="store_true",
        help="Keep downloaded parquet shards after writing canonical sidecars.",
    )
    return parser.parse_args()


def _download_shard(shard: str, cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / Path(shard).name
    if path.exists() and path.stat().st_size > 0:
        return path
    url = f"https://huggingface.co/datasets/{_PLEIAS_REPO}/resolve/main/{shard}"
    subprocess.run(
        ["curl", "-L", "--fail", "-o", str(path), url],
        check=True,
    )
    return path


def _download_americanstories_year(year: int, cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    name = f"faro_{year}.tar.gz"
    path = cache_dir / name
    if path.exists() and path.stat().st_size > 0:
        return path
    url = (
        f"https://huggingface.co/datasets/{_AMERICANSTORIES_REPO}/"
        f"resolve/main/{name}"
    )
    subprocess.run(
        ["curl", "-L", "--fail", "-o", str(path), url],
        check=True,
    )
    return path


def _clean_text(value: Any, *, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[:limit].rsplit(" ", 1)[0].strip()


def _title_from_text(text: str, *, fallback: str) -> str:
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    for line in lines[:6]:
        cleaned = re.sub(r"\s+", " ", line).strip(" .:-")
        if 12 <= len(cleaned) <= 96:
            return cleaned
    return fallback


def _topic_for(text: str) -> str:
    lowered = str(text or "").lower()
    scores: dict[str, int] = {}
    for topic, keywords in _TOPIC_KEYWORDS.items():
        scores[topic] = sum(1 for keyword in keywords if keyword in lowered)
    topic, score = max(scores.items(), key=lambda item: item[1])
    if score > 0:
        return topic
    return "local_civic"


def _looks_like_usable_english(
    text: str,
    *,
    min_ascii_alpha_ratio: float,
    min_common_word_hits: int,
) -> bool:
    sample = re.sub(r"\s+", " ", str(text or "")[:6000]).lower()
    alpha_chars = [char for char in sample if char.isalpha()]
    if len(alpha_chars) < 250:
        return False
    ascii_alpha = sum(1 for char in alpha_chars if "a" <= char <= "z")
    ascii_alpha_ratio = ascii_alpha / max(1, len(alpha_chars))
    padded = f" {sample} "
    common_word_hits = sum(
        padded.count(f" {word} ") for word in _ENGLISH_COMMON_WORDS
    )
    return (
        ascii_alpha_ratio >= min_ascii_alpha_ratio
        and common_word_hits >= min_common_word_hits
    )


def _is_likely_notice_or_ad(title: str, text: str) -> bool:
    candidate = f"{title} {text[:800]}".lower()
    return any(pattern in candidate for pattern in _NON_NEWS_PATTERNS)


def _load_pages(
    *,
    shard_paths: list[Path],
    start_date: str,
    end_date: str,
    body_excerpt_chars: int,
    allow_non_english: bool,
    min_ascii_alpha_ratio: float,
    min_english_common_word_hits: int,
    include_notices_and_ads: bool,
) -> list[NewsPage]:
    pages: list[NewsPage] = []
    for path in shard_paths:
        table = pq.read_table(
            path,
            columns=["id", "date", "page", "file_name", "word_count", "text"],
        )
        for row in table.to_pylist():
            page_date = str(row.get("date") or "")
            if not (start_date <= page_date <= end_date):
                continue
            text = str(row.get("text") or "")
            word_count = int(row.get("word_count") or 0)
            if word_count < 100 or len(text.strip()) < 500:
                continue
            source_id = str(row.get("id") or Path(path).stem)
            file_name = str(row.get("file_name") or "")
            page_no = str(row.get("page") or "")
            fallback = f"{source_id} page {page_no} on {page_date}"
            title = _title_from_text(text, fallback=fallback)
            if not include_notices_and_ads and _is_likely_notice_or_ad(title, text):
                continue
            if not allow_non_english and not _looks_like_usable_english(
                text,
                min_ascii_alpha_ratio=min_ascii_alpha_ratio,
                min_common_word_hits=min_english_common_word_hits,
            ):
                continue
            pages.append(
                NewsPage(
                    dataset_repo=_PLEIAS_REPO,
                    source_domain="pleias.public",
                    source_id=source_id,
                    shard=path.name,
                    page_id=f"{source_id}:{file_name or page_no}:{page_date}",
                    page_date=page_date,
                    page_no=page_no,
                    file_name=file_name,
                    word_count=word_count,
                    title=title,
                    text=_clean_text(text, limit=body_excerpt_chars),
                    topic=_topic_for(text),
                )
            )
    pages.sort(key=lambda item: (item.page_date, item.source_id, item.page_no))
    return pages


def _load_americanstories_articles(
    *,
    archive_paths: list[Path],
    start_date: str,
    end_date: str,
    body_excerpt_chars: int,
    max_pages_per_day: int,
    max_pages_per_source_per_day: int,
    allow_non_english: bool,
    min_ascii_alpha_ratio: float,
    min_english_common_word_hits: int,
    include_notices_and_ads: bool,
) -> list[NewsPage]:
    selected: list[NewsPage] = []
    per_day: dict[str, int] = defaultdict(int)
    per_day_source: dict[tuple[str, str], int] = defaultdict(int)
    seen_ids: set[str] = set()
    for path in archive_paths:
        with tarfile.open(path, mode="r:gz") as archive:
            for member in archive:
                if not member.isfile() or not member.name.endswith(".json"):
                    continue
                page_date = member.name.split("/", 1)[-1].split("_", 1)[0]
                if not (start_date <= page_date <= end_date):
                    continue
                if per_day[page_date] >= max_pages_per_day:
                    continue
                handle = archive.extractfile(member)
                if handle is None:
                    continue
                page = json.loads(handle.read().decode("utf-8"))
                articles = _americanstories_articles_from_page(
                    page=page,
                    member_name=member.name,
                    archive_name=path.name,
                    page_date=page_date,
                    body_excerpt_chars=body_excerpt_chars,
                )
                for article in articles:
                    if article.page_id in seen_ids:
                        continue
                    source_key = (article.page_date, article.source_id)
                    if per_day[article.page_date] >= max_pages_per_day:
                        break
                    if per_day_source[source_key] >= max_pages_per_source_per_day:
                        continue
                    if article.word_count < 80 or len(article.text) < 300:
                        continue
                    if not include_notices_and_ads and _is_likely_notice_or_ad(
                        article.title,
                        article.text,
                    ):
                        continue
                    if not allow_non_english and not _looks_like_usable_english(
                        f"{article.title} {article.text}",
                        min_ascii_alpha_ratio=min_ascii_alpha_ratio,
                        min_common_word_hits=min_english_common_word_hits,
                    ):
                        continue
                    selected.append(article)
                    seen_ids.add(article.page_id)
                    per_day[article.page_date] += 1
                    per_day_source[source_key] += 1
    selected.sort(key=lambda item: (item.page_date, item.source_id, item.page_id))
    return selected


def _americanstories_articles_from_page(
    *,
    page: dict[str, Any],
    member_name: str,
    archive_name: str,
    page_date: str,
    body_excerpt_chars: int,
) -> list[NewsPage]:
    lccn = page.get("lccn") or {}
    edition = page.get("edition") or {}
    source_id = str(lccn.get("lccn") or edition.get("lccn") or "unknown")
    page_no = str(page.get("page_number") or "na")
    newspaper_title = _clean_text(lccn.get("title") or source_id, limit=120)
    grouped: dict[str, dict[str, list[tuple[int, str]]]] = defaultdict(
        lambda: {"headline": [], "article": []}
    )
    for box in page.get("bboxes") or []:
        if str(box.get("legibility") or "").lower() != "legible":
            continue
        box_class = str(box.get("class") or "").lower()
        if box_class not in {"headline", "article"}:
            continue
        article_id = str(box.get("full_article_id") or "")
        if not article_id:
            continue
        text = _clean_text(box.get("raw_text") or "", limit=4000)
        if not text:
            continue
        reading_order = int(box.get("reading_order_id") or 0)
        grouped[article_id][box_class].append((reading_order, text))

    articles: list[NewsPage] = []
    for article_id, parts in grouped.items():
        body_parts = [
            text for _order, text in sorted(parts["article"], key=lambda item: item[0])
        ]
        if not body_parts:
            continue
        headline_parts = [
            text
            for _order, text in sorted(parts["headline"], key=lambda item: item[0])
        ]
        body = _clean_text(" ".join(body_parts), limit=body_excerpt_chars)
        fallback_title = _title_from_text(body, fallback=newspaper_title)
        title = _clean_text(" ".join(headline_parts), limit=120) or fallback_title
        articles.append(
            NewsPage(
                dataset_repo=_AMERICANSTORIES_REPO,
                source_domain="americanstories.public",
                source_id=source_id,
                shard=archive_name,
                page_id=f"{source_id}:{member_name}:{article_id}",
                page_date=page_date,
                page_no=page_no,
                file_name=member_name,
                word_count=len(body.split()),
                title=title,
                text=body,
                topic=_topic_for(f"{title} {body}"),
            )
        )
    return articles


def _sample_pages(
    pages: list[NewsPage],
    *,
    max_pages_per_day: int,
    max_pages_per_source_per_day: int,
) -> list[NewsPage]:
    selected: list[NewsPage] = []
    per_day: dict[str, int] = defaultdict(int)
    per_day_source: dict[tuple[str, str], int] = defaultdict(int)
    seen_ids: set[str] = set()
    for page in pages:
        if page.page_id in seen_ids:
            continue
        day_key = page.page_date
        source_key = (page.page_date, page.source_id)
        if per_day[day_key] >= max_pages_per_day:
            continue
        if per_day_source[source_key] >= max_pages_per_source_per_day:
            continue
        selected.append(page)
        seen_ids.add(page.page_id)
        per_day[day_key] += 1
        per_day_source[source_key] += 1
    return selected


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _mail_payload(pages: list[NewsPage]) -> dict[str, Any]:
    by_topic: dict[str, list[NewsPage]] = defaultdict(list)
    for page in pages:
        by_topic[page.topic].append(page)
    threads: list[dict[str, Any]] = []
    for topic_index, topic in enumerate(sorted(by_topic)):
        if topic_index % 2:
            continue
        messages = []
        for page in sorted(by_topic[topic], key=lambda item: item.page_date):
            messages.append(
                {
                    "message_id": f"{page.page_id}:mail",
                    "timestamp": page.page_date,
                    "from": f"{page.source_id}@{page.source_domain}",
                    "to": ["public@outside.local"],
                    "subject": f"{topic.replace('_', ' ').title()}: {page.title}",
                    "body_text": page.text,
                    "thread_id": f"news:{topic}",
                    "metadata": {
                        "dataset": page.dataset_repo,
                        "shard": page.shard,
                        "file_name": page.file_name,
                        "word_count": page.word_count,
                    },
                }
            )
        if messages:
            threads.append(
                {
                    "thread_id": f"news:{topic}",
                    "subject": topic.replace("_", " ").title(),
                    "messages": messages,
                }
            )
    return {"threads": threads, "actors": [], "profile": {}}


def _notion_payload(pages: list[NewsPage]) -> dict[str, Any]:
    by_topic: dict[str, list[NewsPage]] = defaultdict(list)
    for page in pages:
        by_topic[page.topic].append(page)
    notion_pages: list[dict[str, Any]] = []
    for topic_index, topic in enumerate(sorted(by_topic)):
        if not topic_index % 2:
            continue
        topic_pages = sorted(by_topic[topic], key=lambda item: item.page_date)
        if not topic_pages:
            continue
        first = topic_pages[0]
        comments = [
            {
                "id": f"{page.page_id}:comment",
                "created_at": page.page_date,
                "author": f"{page.source_id}@{page.source_domain}",
                "body": f"{page.title}. {page.text}",
            }
            for page in topic_pages[1:]
        ]
        notion_pages.append(
            {
                "page_id": f"news:{topic}",
                "title": topic.replace("_", " ").title(),
                "created_at": first.page_date,
                "owner": f"{first.source_id}@{first.source_domain}",
                "body": f"{first.title}. {first.text}",
                "comments": comments,
            }
        )
    return {"pages": notion_pages, "databases": [], "blocks": []}


def _write_snapshot(
    *,
    output_root: Path,
    pages: list[NewsPage],
    shards: list[str],
    args: argparse.Namespace,
) -> Path:
    captured_at = _iso_now()
    mail_payload = _mail_payload(pages)
    notion_payload = _notion_payload(pages)
    dataset_repo = pages[0].dataset_repo if pages else _PLEIAS_REPO
    is_americanstories = dataset_repo == _AMERICANSTORIES_REPO
    organization_name = (
        "AmericanStories Historical News Sample"
        if is_americanstories
        else "PleIAs Historical News Sample"
    )
    source_kind = (
        "historical_news_articles"
        if is_americanstories
        else "historical_news_pages"
    )
    license_name = "cc-by-4.0" if is_americanstories else "cc0-1.0"
    snapshot = ContextSnapshot(
        organization_name=organization_name,
        organization_domain="historical-news.local",
        captured_at=captured_at,
        sources=[
            ContextSourceResult(
                provider="mail_archive",
                captured_at=captured_at,
                status="ok",
                record_counts={
                    "threads": len(mail_payload["threads"]),
                    "messages": sum(
                        len(thread.get("messages") or [])
                        for thread in mail_payload["threads"]
                    ),
                },
                data=mail_payload,
            ),
            ContextSourceResult(
                provider="notion",
                captured_at=captured_at,
                status="ok",
                record_counts={
                    "pages": len(notion_payload["pages"]),
                    "comments": sum(
                        len(page.get("comments") or []) for page in notion_payload["pages"]
                    ),
                },
                data=notion_payload,
            ),
        ],
        metadata={
            "snapshot_role": "company_history_bundle",
            "dataset": dataset_repo,
            "dataset_url": f"https://huggingface.co/datasets/{dataset_repo}",
            "license": license_name,
            "source_kind": source_kind,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "selected_record_count": len(pages),
            "selected_source_count": len({page.source_id for page in pages}),
            "selected_topic_count": len({page.topic for page in pages}),
            "source_shards": shards,
            "max_pages_per_day": args.max_pages_per_day,
            "max_pages_per_source_per_day": args.max_pages_per_source_per_day,
            "body_excerpt_chars": args.body_excerpt_chars,
            "allow_non_english": bool(args.allow_non_english),
            "include_notices_and_ads": bool(args.include_notices_and_ads),
            "min_ascii_alpha_ratio": args.min_ascii_alpha_ratio,
            "min_english_common_word_hits": args.min_english_common_word_hits,
            "notes": [
                (
                    "AmericanStories rows are article-level extractions from Chronicling America."
                    if is_americanstories
                    else "PleIAs rows are OCR newspaper pages, not clean article units."
                ),
                "This bounded sample is shaped as a VEI company-history bundle for world-model testing.",
                "Mail and docs surfaces are synthetic wrappers over the same public news corpus to preserve VEI temporal-thread semantics.",
            ],
        },
    )
    output_root.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_root / "context_snapshot.json"
    snapshot_path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
    write_canonical_history_sidecars(snapshot, snapshot_path)
    return snapshot_path


def main() -> None:
    args = _parse_args()
    output_root = args.output_root.expanduser().resolve()
    cache_dir = output_root / "_download_cache"
    if args.dataset == "americanstories":
        start_year = int(args.start_date[:4])
        end_year = int(args.end_date[:4])
        years = [int(year) for year in (args.year or range(start_year, end_year + 1))]
        shards = [f"faro_{year}.tar.gz" for year in years]
        archive_paths = [
            _download_americanstories_year(year, cache_dir) for year in years
        ]
        selected = _load_americanstories_articles(
            archive_paths=archive_paths,
            start_date=args.start_date,
            end_date=args.end_date,
            body_excerpt_chars=args.body_excerpt_chars,
            max_pages_per_day=args.max_pages_per_day,
            max_pages_per_source_per_day=args.max_pages_per_source_per_day,
            allow_non_english=bool(args.allow_non_english),
            min_ascii_alpha_ratio=args.min_ascii_alpha_ratio,
            min_english_common_word_hits=args.min_english_common_word_hits,
            include_notices_and_ads=bool(args.include_notices_and_ads),
        )
    else:
        shards = list(args.shard or _DEFAULT_PLEIAS_SHARDS)
        shard_paths = [_download_shard(shard, cache_dir) for shard in shards]
        pages = _load_pages(
            shard_paths=shard_paths,
            start_date=args.start_date,
            end_date=args.end_date,
            body_excerpt_chars=args.body_excerpt_chars,
            allow_non_english=bool(args.allow_non_english),
            min_ascii_alpha_ratio=args.min_ascii_alpha_ratio,
            min_english_common_word_hits=args.min_english_common_word_hits,
            include_notices_and_ads=bool(args.include_notices_and_ads),
        )
        selected = _sample_pages(
            pages,
            max_pages_per_day=args.max_pages_per_day,
            max_pages_per_source_per_day=args.max_pages_per_source_per_day,
        )
    snapshot_path = _write_snapshot(
        output_root=output_root,
        pages=selected,
        shards=shards,
        args=args,
    )
    summary = {
        "context_snapshot": str(snapshot_path),
        "canonical_events": str(output_root / "canonical_events.jsonl"),
        "canonical_event_index": str(output_root / "canonical_event_index.json"),
        "selected_records": len(selected),
        "dataset": selected[0].dataset_repo if selected else args.dataset,
        "date_range": [args.start_date, args.end_date],
        "topics": sorted({page.topic for page in selected}),
        "sources": sorted({page.source_id for page in selected}),
        "parquet_cache_kept": bool(args.keep_parquet),
    }
    (output_root / "news_ingest_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if not args.keep_parquet:
        shutil.rmtree(cache_dir, ignore_errors=True)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
