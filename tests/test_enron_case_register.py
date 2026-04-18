from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq

from vei.whatif._benchmark_case_packs import (
    BENCHMARK_CASE_PACKS,
    DEFAULT_BENCHMARK_PACK_ID,
)


def test_enron_case_event_register_entries_resolve_in_repo_rosetta() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    register_path = repo_root / "data" / "enron" / "enron_case_event_register.json"
    rosetta_path = repo_root / "data" / "enron" / "rosetta"

    register = json.loads(register_path.read_text(encoding="utf-8"))
    entries = register["events"]
    assert len(entries) == 8

    event_ids = [entry["event_id"] for entry in entries]
    metadata_rows = pq.read_table(
        rosetta_path / "enron_rosetta_events_metadata.parquet",
        filters=[[("event_id", "in", event_ids)]],
        columns=["event_id"],
    ).to_pylist()
    resolved_ids = {row["event_id"] for row in metadata_rows}
    assert resolved_ids == set(event_ids)


def test_enron_benchmark_pack_includes_macro_families() -> None:
    cases = BENCHMARK_CASE_PACKS[DEFAULT_BENCHMARK_PACK_ID]
    family_names = {case.family for case in cases}
    case_ids = {case.case_id for case in cases}

    assert len(cases) == 31
    assert {
        "whistleblower",
        "market_manipulation",
        "crisis_communication",
        "accounting_disclosure",
    }.issubset(family_names)
    assert {
        "watkins_followup_questions",
        "california_crisis_order",
        "baxter_press_release",
        "q3_disclosure_review",
        "ees_preholiday_update",
        "braveheart_forward",
        "skilling_resignation_materials",
    }.issubset(case_ids)
