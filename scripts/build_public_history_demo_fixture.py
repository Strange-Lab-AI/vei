#!/usr/bin/env python3
"""Build the checked-in Public History demo fixture from a news world bundle."""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Iterable

from vei.whatif.api import WhatIfEvent, load_world

DEFAULT_INPUT = Path("_vei_out/datasets/news_americanstories_1836_1838")
DEFAULT_WORKSPACE = Path("docs/examples/news-public-history-demo/workspace")
DEFAULT_AS_OF = "1837-09-06"
SOURCE_ID = "news_americanstories_public_world"

TOPIC_LABELS = {
    "news:banking_markets": "Banking Markets",
    "news:government_policy": "Government Policy",
    "news:war_foreign_affairs": "War Foreign Affairs",
    "news:local_civic": "Local Civic",
    "news:slavery_abolition": "Slavery Abolition",
    "public@outside.local": "Transport Infrastructure",
}

TOPIC_ORDER = (
    "Banking Markets",
    "Government Policy",
    "War Foreign Affairs",
    "Local Civic",
    "Slavery Abolition",
    "Transport Infrastructure",
)

GENERIC_HEADLINES = {label.lower() for label in TOPIC_ORDER}
NOISY_AD_TERMS = (
    "for sale",
    "subscribers",
    "respectfully inform",
    "medicine",
    "pills",
    "sarsapari",
    "goods",
    "warehouse",
    "store",
    "valuable property",
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    parser.add_argument("--max-events", type=int, default=480)
    parser.add_argument("--per-month-topic", type=int, default=2)
    args = parser.parse_args()

    world = load_world(
        source="company_history",
        source_dir=args.input,
        include_situation_graph=False,
    )
    selected = select_events(
        world.events,
        max_events=args.max_events,
        per_month_topic=args.per_month_topic,
    )
    args.workspace.mkdir(parents=True, exist_ok=True)
    write_context_snapshot(
        selected,
        output_path=args.workspace / "context_snapshot.json",
        source_event_count=len(world.events),
    )
    write_manifest(args.workspace / "public_demo_manifest.json")


def select_events(
    events: Iterable[WhatIfEvent],
    *,
    max_events: int,
    per_month_topic: int,
) -> list[WhatIfEvent]:
    buckets: dict[tuple[str, str], list[WhatIfEvent]] = defaultdict(list)
    all_events: list[WhatIfEvent] = []
    for event in events:
        topic = _topic_label(event)
        if topic not in TOPIC_ORDER:
            continue
        all_events.append(event)
        buckets[(event.timestamp[:7], topic)].append(event)

    for bucket in buckets.values():
        bucket.sort(key=lambda event: (-_quality_score(event), event.event_id))

    selected_by_id: dict[str, WhatIfEvent] = {}
    months = sorted({event.timestamp[:7] for event in all_events})
    for month in months:
        for topic in TOPIC_ORDER:
            for event in buckets.get((month, topic), [])[:per_month_topic]:
                selected_by_id.setdefault(event.event_id, event)

    fill = sorted(
        all_events, key=lambda event: (-_quality_score(event), event.event_id)
    )
    for event in fill:
        if len(selected_by_id) >= max_events:
            break
        selected_by_id.setdefault(event.event_id, event)

    selected = list(selected_by_id.values())[:max_events]
    return sorted(selected, key=lambda event: (event.timestamp_ms, event.event_id))


def write_context_snapshot(
    events: list[WhatIfEvent],
    *,
    output_path: Path,
    source_event_count: int,
) -> None:
    docs = []
    for index, event in enumerate(events, start=1):
        topic = _topic_label(event)
        date = event.timestamp[:10]
        clean_snippet = _clean_text(event.snippet, max_chars=720)
        headline = _display_headline(event, topic=topic)
        topic_slug = _slug(topic)
        docs.append(
            {
                "doc_id": f"NEWS-{date}-{topic_slug}-{index:04d}",
                "title": headline,
                "body": (
                    f"Topic: {topic}. Date: {date}. "
                    f"Source: {event.actor_id or 'AmericanStories public record'}. "
                    f"Public record excerpt: {clean_snippet}"
                ),
                "created_time": event.timestamp,
                "modified_time": event.timestamp,
                "owner": event.actor_id or "americanstories@historical-news.local",
                "url": "",
            }
        )

    payload = {
        "version": "1",
        "organization_name": "AmericanStories Historical News Demo",
        "organization_domain": "historical-news.local",
        "captured_at": "1838-12-31T00:00:00Z",
        "sources": [
            {
                "provider": "google",
                "captured_at": "1838-12-31T00:00:00Z",
                "status": "ok",
                "record_counts": {"documents": len(docs)},
                "data": {
                    "documents": docs,
                    "users": [],
                    "drive_shares": [],
                },
                "error": None,
            }
        ],
        "metadata": {
            "snapshot_role": "public_history_demo",
            "dataset": "dell-research-harvard/AmericanStories",
            "source_kind": "historical_news_articles",
            "source_event_count": source_event_count,
            "selected_event_count": len(docs),
            "start_date": events[0].timestamp[:10] if events else "",
            "end_date": events[-1].timestamp[:10] if events else "",
            "selection_method": "stratified_by_month_and_topic",
            "notes": (
                "Compact public demo fixture derived from the local AmericanStories "
                "1836-1838 news world bundle."
            ),
        },
    }
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_manifest(path: Path) -> None:
    payload = {
        "source_id": SOURCE_ID,
        "title": "Public History: AmericanStories News World",
        "summary": (
            "Choose a point from a compact 1836-1838 public-news record, inspect "
            "what was visible by then, and test a scenario from that state."
        ),
        "source_path": "context_snapshot.json",
        "saved_result_path": "public_demo_saved_result.json",
        "default_topic": "all_public_record",
        "default_as_of": DEFAULT_AS_OF,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _topic_label(event: WhatIfEvent) -> str:
    return TOPIC_LABELS.get(event.target_id, event.subject or "Public Record")


def _quality_score(event: WhatIfEvent) -> float:
    topic = _topic_label(event)
    headline = _display_headline(event, topic=topic)
    text = _clean_text(" ".join([event.subject, event.snippet]), max_chars=900)
    words = re.findall(r"[A-Za-z]{3,}", text)
    useful_hits = sum(
        1
        for token in (
            "bank",
            "congress",
            "president",
            "treasury",
            "war",
            "foreign",
            "public",
            "market",
            "slavery",
            "abolition",
            "rail",
            "canal",
            "credit",
            "policy",
            "trade",
        )
        if token in text.lower()
    )
    specificity_bonus = 120 if _specific_headline(headline, topic=topic) else 0
    noise_penalty = 90 if _looks_like_ad_or_noise(text) else 0
    return (
        len(words)
        + (useful_hits * 20)
        + min(len(text), 400) / 20
        + specificity_bonus
        - noise_penalty
    )


def _display_headline(event: WhatIfEvent, *, topic: str) -> str:
    subject = _clean_headline_text(event.subject)
    if _specific_headline(subject, topic=topic):
        return subject

    snippet = _clean_text(event.snippet, max_chars=900)
    for candidate in _headline_candidates(snippet):
        cleaned = _clean_headline_text(candidate)
        if _specific_headline(cleaned, topic=topic):
            return cleaned

    return f"{topic} report"


def _headline_candidates(snippet: str) -> list[str]:
    normalized = snippet.replace("[volume]", "volume")
    parts = re.split(r"(?:\.\.|\.\s+|--|;|\n)", normalized)
    candidates: list[str] = []
    for part in parts[:8]:
        cleaned = _clean_text(part, max_chars=150)
        if not cleaned:
            continue
        candidates.append(cleaned)
        colon_tail = cleaned.rsplit(":", maxsplit=1)[-1].strip()
        if colon_tail and colon_tail != cleaned:
            candidates.append(colon_tail)
    return candidates


def _specific_headline(value: str, *, topic: str) -> bool:
    lowered = value.lower().strip(" .:-")
    if not lowered or lowered in GENERIC_HEADLINES or lowered == topic.lower():
        return False
    if lowered.endswith(" report") and lowered.startswith(topic.lower()):
        return False
    if "volume" in lowered and re.search(r"\b18\d{2}-18", lowered):
        return False
    if len(re.findall(r"[a-z]{3,}", lowered)) < 2:
        return False
    if _looks_like_ad_or_noise(lowered) and len(lowered) > 80:
        return False
    weird_tokens = re.findall(r"\b[a-z]*[A-Z][a-z]+[A-Z][A-Za-z]*\b", value)
    return len(weird_tokens) <= 2


def _clean_headline_text(value: str) -> str:
    cleaned = _clean_text(value, max_chars=120)
    cleaned = re.sub(r"\bOF\b", "of", cleaned)
    cleaned = re.sub(r"\bIN\b", "in", cleaned)
    cleaned = re.sub(r"\bTO\b", "to", cleaned)
    cleaned = cleaned.replace(",S ", "'s ")
    cleaned = cleaned.replace("President,S", "President's")
    cleaned = cleaned.replace("PRESIDENT,S", "President's")
    if cleaned.isupper() or sum(ch.isupper() for ch in cleaned) > len(cleaned) * 0.55:
        cleaned = cleaned.title()
        cleaned = cleaned.replace("'S", "'s")
    return cleaned.strip(" .:-")


def _looks_like_ad_or_noise(value: str) -> bool:
    lowered = value.lower()
    return any(term in lowered for term in NOISY_AD_TERMS)


def _clean_text(value: str, *, max_chars: int) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = re.sub(r"\s+", " ", ascii_text).strip()
    ascii_text = re.sub(r"[^A-Za-z0-9.,;:!?()'\"%/$&@# -]+", "", ascii_text)
    if len(ascii_text) <= max_chars:
        return ascii_text
    return ascii_text[: max_chars - 1].rstrip() + "."


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


if __name__ == "__main__":
    main()
