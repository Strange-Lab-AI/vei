from __future__ import annotations

import json
from pathlib import Path


def test_enron_public_context_fixture_is_present_and_cross_referenced() -> None:
    fixture_root = (
        Path(__file__).resolve().parents[1]
        / "vei"
        / "whatif"
        / "fixtures"
        / "enron_public_context"
    )
    package_payload = json.loads(
        (fixture_root / "package.json").read_text(encoding="utf-8")
    )
    dataset_payload = json.loads(
        (fixture_root / "enron_public_context_v2.json").read_text(encoding="utf-8")
    )

    source_ids = {source["source_id"] for source in package_payload["sources"]}
    assert package_payload["name"] == "enron_public_context"
    assert dataset_payload["pack_name"] == "enron_public_context"
    assert dataset_payload["version"] == "2"
    assert len(package_payload["sources"]) == 24
    assert len(dataset_payload["financial_snapshots"]) == 11
    assert len(dataset_payload["public_news_events"]) == 21
    assert dataset_payload["financial_snapshots"][0]["as_of"] == "1998-12-31T00:00:00Z"
    assert (
        dataset_payload["public_news_events"][-1]["timestamp"] == "2002-03-14T00:00:00Z"
    )
    event_ids = {event["event_id"] for event in dataset_payload["public_news_events"]}
    assert "pge_chapter_11" in event_ids
    assert "ferc_western_refund_order" in event_ids
    assert "skilling_resignation" in event_ids
    assert "watkins_memo_public_release" in event_ids
    assert "arthur_andersen_indictment" in event_ids

    for source in package_payload["sources"]:
        path = fixture_root / source["relative_path"]
        assert path.exists(), f"missing source file: {path}"
        assert path.stat().st_size > 0

    for snapshot in dataset_payload["financial_snapshots"]:
        assert snapshot["source_ids"]
        assert set(snapshot["source_ids"]).issubset(source_ids)

    for event in dataset_payload["public_news_events"]:
        assert event["source_ids"]
        assert set(event["source_ids"]).issubset(source_ids)

    watkins_event = next(
        event
        for event in dataset_payload["public_news_events"]
        if event["event_id"] == "watkins_memo_public_release"
    )
    assert watkins_event["timestamp"] == "2002-01-15T00:00:00Z"
    assert watkins_event["internally_known_date"] == "2001-08-22T00:00:00Z"


def test_enron_macro_sidecar_fixtures_are_present() -> None:
    fixtures_root = Path(__file__).resolve().parents[1] / "vei" / "whatif" / "fixtures"

    stock_payload = json.loads(
        (
            fixtures_root / "enron_stock_history" / "enron_stock_history_v1.json"
        ).read_text(encoding="utf-8")
    )
    credit_payload = json.loads(
        (
            fixtures_root / "enron_credit_history" / "enron_credit_history_v1.json"
        ).read_text(encoding="utf-8")
    )
    ferc_payload = json.loads(
        (fixtures_root / "enron_ferc_history" / "enron_ferc_history_v1.json").read_text(
            encoding="utf-8"
        )
    )

    assert stock_payload["pack_name"] == "enron_stock_history"
    assert len(stock_payload["stock_history"]) == 986
    assert stock_payload["stock_history"][0]["as_of"] == "1998-01-02T00:00:00Z"
    assert stock_payload["stock_history"][-1]["as_of"] == "2001-12-31T00:00:00Z"
    assert stock_payload["stock_history"][0]["close"] == 20.38
    assert stock_payload["stock_history"][-1]["close"] == 0.6

    assert credit_payload["pack_name"] == "enron_credit_history"
    assert [event["event_id"] for event in credit_payload["credit_history"]] == [
        "moodys_baseline_investment_grade",
        "sp_baseline_investment_grade",
        "fitch_baseline_investment_grade",
        "moodys_baa2_review",
        "sp_bbb_creditwatch_negative",
        "fitch_bbb_minus_watch_negative",
        "moodys_sp_cut_to_junk",
    ]

    assert ferc_payload["pack_name"] == "enron_ferc_history"
    assert [event["event_id"] for event in ferc_payload["ferc_history"]] == [
        "ferc_june_2001_mitigation_and_refund_path"
    ]
