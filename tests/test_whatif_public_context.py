from __future__ import annotations

import json
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from vei.context.public_context import _core as public_context_core
from vei.context.public_context import _fetchers as public_context_fetchers
from vei.whatif import (
    build_branch_point_benchmark,
    build_saved_decision_scene,
    load_episode_manifest,
    load_world,
    materialize_episode,
)
from vei.whatif.counterfactual import (
    _allowed_thread_participants,
    _llm_counterfactual_prompt,
)
from vei.whatif.models import (
    WhatIfPublicContext,
    WhatIfPublicFinancialSnapshot,
    WhatIfPublicNewsEvent,
    WhatIfResearchCandidate,
    WhatIfResearchCase,
    WhatIfResearchPack,
)
from vei.context.api import (
    build_public_context,
    load_public_context,
    load_enron_public_context,
    public_context_prompt_lines,
    slice_public_context_to_branch,
    WhatIfPublicCreditEvent,
    WhatIfPublicRegulatoryEvent,
    WhatIfPublicStockHistoryRow,
)


def test_cached_fetch_recovers_from_corrupt_cache_file(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("VEI_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    cache_root = tmp_path / "artifacts" / "public_context_cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    cache_key = "cache-recovery"
    cache_file = cache_root / f"{cache_key}.json"
    cache_file.write_text('{"stale": true}{"extra": true}', encoding="utf-8")
    fetch_calls = 0

    def fetcher() -> dict[str, str]:
        nonlocal fetch_calls
        fetch_calls += 1
        return {"status": "fresh"}

    recovered = public_context_fetchers._cached_fetch(cache_key, fetcher, ttl_hours=24)
    cached = public_context_fetchers._cached_fetch(cache_key, fetcher, ttl_hours=24)

    assert recovered == {"status": "fresh"}
    assert cached == {"status": "fresh"}
    assert fetch_calls == 1
    assert json.loads(cache_file.read_text(encoding="utf-8")) == {"status": "fresh"}


def _write_public_context_rosetta_fixture(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    metadata_rows = [
        {
            "event_id": "evt-200",
            "timestamp": "2001-04-17T14:00:00Z",
            "actor_id": "vince.kaminski@enron.com",
            "target_id": "sara.shackleton@enron.com",
            "event_type": "message",
            "thread_task_id": "thr-public-context",
            "artifacts": json.dumps(
                {
                    "subject": "Q1 numbers follow-up",
                    "to_recipients": ["sara.shackleton@enron.com"],
                    "to_count": 1,
                }
            ),
        },
        {
            "event_id": "evt-201",
            "timestamp": "2001-05-03T09:00:00Z",
            "actor_id": "jeff.skilling@enron.com",
            "target_id": "outside@lawfirm.com",
            "event_type": "message",
            "thread_task_id": "thr-public-context",
            "artifacts": json.dumps(
                {
                    "subject": "Draft term sheet",
                    "to_recipients": ["outside@lawfirm.com"],
                    "to_count": 1,
                    "has_attachment_reference": True,
                }
            ),
        },
        {
            "event_id": "evt-202",
            "timestamp": "2001-05-03T11:00:00Z",
            "actor_id": "sara.shackleton@enron.com",
            "target_id": "jeff.skilling@enron.com",
            "event_type": "reply",
            "thread_task_id": "thr-public-context",
            "artifacts": json.dumps(
                {
                    "subject": "Draft term sheet",
                    "to_recipients": ["jeff.skilling@enron.com"],
                    "to_count": 1,
                    "is_reply": True,
                }
            ),
        },
    ]
    content_rows = [
        {"event_id": "evt-200", "content": "Flagging the quarter numbers for review."},
        {"event_id": "evt-201", "content": "Sending the outside draft today."},
        {"event_id": "evt-202", "content": "Replying with legal concerns."},
    ]
    pq.write_table(
        pa.Table.from_pylist(metadata_rows),
        root / "enron_rosetta_events_metadata.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(content_rows),
        root / "enron_rosetta_events_content.parquet",
    )


def _public_context_research_pack() -> WhatIfResearchPack:
    return WhatIfResearchPack(
        pack_id="fixture_public_context_pack",
        title="Fixture Public Context Pack",
        summary="Held-out case that exercises Enron public context slicing.",
        objective_pack_ids=[
            "contain_exposure",
            "reduce_delay",
            "protect_relationship",
        ],
        rollout_seeds=[42042],
        cases=[
            WhatIfResearchCase(
                case_id="public_context_case",
                title="Public Context Case",
                event_id="evt-201",
                thread_id="thr-public-context",
                summary="A branch point with dated public company context.",
                candidates=[
                    WhatIfResearchCandidate(
                        candidate_id="hold_internal",
                        label="Hold internal",
                        prompt="Keep the draft inside Enron and route it through legal review.",
                    ),
                    WhatIfResearchCandidate(
                        candidate_id="narrow_status",
                        label="Narrow status",
                        prompt="Send a short status update outside without the draft.",
                    ),
                    WhatIfResearchCandidate(
                        candidate_id="broad_send",
                        label="Broad send",
                        prompt="Send the draft now and widen the outside loop.",
                    ),
                ],
            )
        ],
    )


def _write_generic_public_context_fixture(root: Path) -> Path:
    path = root / "whatif_public_context.json"
    path.write_text(
        json.dumps(
            {
                "pack_name": "pycorp_public_context",
                "organization_name": "Py Corp",
                "organization_domain": "pycorp.example.com",
                "financial_snapshots": [
                    {
                        "snapshot_id": "py_q1_outlook",
                        "as_of": "2026-03-01T00:00:00Z",
                        "kind": "guidance",
                        "label": "Q1 outlook note",
                        "summary": "Public guidance held steady entering March.",
                    },
                    {
                        "snapshot_id": "py_q2_outlook",
                        "as_of": "2026-03-03T00:00:00Z",
                        "kind": "guidance",
                        "label": "Q2 outlook note",
                        "summary": "Public guidance changed after the branch.",
                    },
                ],
                "public_news_events": [
                    {
                        "event_id": "py_launch_note",
                        "timestamp": "2026-03-01T08:00:00Z",
                        "category": "press",
                        "headline": "Product launch note",
                        "summary": "The company announced its launch that morning.",
                    },
                    {
                        "event_id": "py_board_change",
                        "timestamp": "2026-03-04T08:00:00Z",
                        "category": "filing",
                        "headline": "Board change note",
                        "summary": "A later public filing changed the board slate.",
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def test_load_enron_public_context_slices_world_window() -> None:
    context = load_enron_public_context(
        window_start="2001-04-01T00:00:00Z",
        window_end="2001-05-31T23:59:59Z",
    )

    assert [snapshot.snapshot_id for snapshot in context.financial_snapshots] == [
        "q1_2001_earnings_release"
    ]
    assert [event.event_id for event in context.public_news_events] == [
        "pge_chapter_11",
        "cliff_baxter_resignation",
    ]
    assert len(context.stock_history) == 42
    assert context.stock_history[0].as_of == "2001-04-02T00:00:00Z"
    assert context.stock_history[-1].as_of == "2001-05-31T00:00:00Z"
    assert context.credit_history == []
    assert context.ferc_history == []


def test_load_enron_public_context_soft_fails_when_fixture_is_unavailable(
    monkeypatch,
    caplog,
) -> None:
    monkeypatch.setattr(
        public_context_core,
        "load_public_context",
        lambda **_kwargs: (_ for _ in ()).throw(FileNotFoundError("missing")),
    )

    with caplog.at_level(logging.WARNING):
        context = load_enron_public_context(
            window_start="2001-04-01T00:00:00Z",
            window_end="2001-05-31T23:59:59Z",
        )

    assert context.pack_name == "enron_public_context"
    assert context.financial_snapshots == []
    assert context.public_news_events == []
    assert context.stock_history == []
    assert context.credit_history == []
    assert context.ferc_history == []
    record = next(
        record
        for record in caplog.records
        if "whatif enron public context load failed" in record.getMessage()
    )
    assert getattr(record, "source") == "public_context"
    assert getattr(record, "provider") == "enron_fixture"


def test_load_public_context_soft_fails_for_generic_pack(
    tmp_path: Path,
    caplog,
) -> None:
    with caplog.at_level(logging.WARNING):
        context = load_public_context(
            path=tmp_path / "missing_public_context.json",
            organization_name="Py Corp",
            organization_domain="pycorp.example.com",
            window_start="2026-03-01T00:00:00Z",
            window_end="2026-03-01T23:59:59Z",
        )

    assert context.organization_name == "Py Corp"
    assert context.organization_domain == "pycorp.example.com"
    assert context.financial_snapshots == []
    assert context.public_news_events == []
    record = next(
        record
        for record in caplog.records
        if "whatif public context load failed" in record.getMessage()
    )
    assert getattr(record, "source") == "public_context"
    assert getattr(record, "provider") == "file"


def test_build_public_context_live_aggregates_sec_and_news(monkeypatch) -> None:
    def fake_fetch_json(url: str) -> object:
        if url.endswith("company_tickers.json"):
            return {
                "0": {
                    "cik_str": 320193,
                    "ticker": "AAPL",
                    "title": "Apple Inc.",
                }
            }
        if "submissions/CIK0000320193.json" in url:
            return {
                "filings": {
                    "recent": {
                        "form": ["10-Q", "8-K"],
                        "filingDate": ["2026-01-31", "2026-02-15"],
                        "accessionNumber": [
                            "0000320193-26-000001",
                            "0000320193-26-000002",
                        ],
                        "primaryDocument": ["q1.htm", "8k.htm"],
                        "primaryDocDescription": [
                            "Quarterly report",
                            "Current report",
                        ],
                    }
                }
            }
        if "companyfacts/CIK0000320193.json" in url:
            return {
                "facts": {
                    "us-gaap": {
                        "Revenues": {
                            "units": {
                                "USD": [
                                    {
                                        "form": "10-Q",
                                        "end": "2025-12-31",
                                        "filed": "2026-01-31",
                                        "val": 123456789,
                                        "accn": "0000320193-26-000001",
                                    }
                                ]
                            }
                        },
                        "NetIncomeLoss": {
                            "units": {
                                "USD": [
                                    {
                                        "form": "10-Q",
                                        "end": "2025-12-31",
                                        "filed": "2026-01-31",
                                        "val": 4567890,
                                        "accn": "0000320193-26-000001",
                                    }
                                ]
                            }
                        },
                    }
                }
            }
        raise AssertionError(f"unexpected url: {url}")

    def fake_fetch_text(url: str) -> str:
        assert "news.google.com" in url
        return """
        <rss>
          <channel>
            <item>
              <title>Apple launches new enterprise program</title>
              <link>https://example.com/apple-launch</link>
              <pubDate>Mon, 16 Feb 2026 14:00:00 GMT</pubDate>
              <description><![CDATA[Apple expanded its enterprise rollout.]]></description>
            </item>
          </channel>
        </rss>
        """

    monkeypatch.setattr(
        "vei.context.public_context._fetchers._fetch_json", fake_fetch_json
    )
    monkeypatch.setattr(
        "vei.context.public_context._fetchers._fetch_text", fake_fetch_text
    )

    context = build_public_context(
        organization_name="Apple",
        organization_domain="apple.com",
        live=True,
        news_limit=3,
    )

    assert context.organization_name == "Apple"
    assert context.organization_domain == "apple.com"
    assert len(context.financial_snapshots) == 1
    assert context.financial_snapshots[0].label == "Apple Inc. 10-Q checkpoint"
    assert "revenue $123.5M" in context.financial_snapshots[0].summary
    assert len(context.public_news_events) == 3
    assert {event.category for event in context.public_news_events} == {
        "filing",
        "news",
    }
    assert any(
        event.headline == "Apple launches new enterprise program"
        for event in context.public_news_events
    )


def test_public_context_branch_slice_sorts_items_before_prompt_truncation() -> None:
    context = WhatIfPublicContext(
        pack_name="enron_public_context",
        financial_snapshots=[
            WhatIfPublicFinancialSnapshot(
                snapshot_id="later_financial",
                as_of="2001-05-01T00:00:00Z",
                kind="quarterly",
                label="Later financial checkpoint",
                summary="Later financial summary.",
            ),
            WhatIfPublicFinancialSnapshot(
                snapshot_id="earlier_financial",
                as_of="2001-04-01T00:00:00Z",
                kind="quarterly",
                label="Earlier financial checkpoint",
                summary="Earlier financial summary.",
            ),
        ],
        public_news_events=[
            WhatIfPublicNewsEvent(
                event_id="later_news",
                timestamp="2001-05-03T00:00:00Z",
                category="press",
                headline="Later news checkpoint",
                summary="Later news summary.",
            ),
            WhatIfPublicNewsEvent(
                event_id="earlier_news",
                timestamp="2001-04-17T00:00:00Z",
                category="press",
                headline="Earlier news checkpoint",
                summary="Earlier news summary.",
            ),
        ],
        stock_history=[
            WhatIfPublicStockHistoryRow(
                as_of="2001-05-03T00:00:00Z",
                label="Same-day market checkpoint",
                close=49.12,
                volume=100.0,
                summary="Same-day market summary.",
            ),
            WhatIfPublicStockHistoryRow(
                as_of="2001-05-02T00:00:00Z",
                label="Later market checkpoint",
                close=52.91,
                volume=100.0,
                summary="Later market summary.",
            ),
            WhatIfPublicStockHistoryRow(
                as_of="2001-04-30T00:00:00Z",
                label="Earlier market checkpoint",
                close=62.72,
                volume=100.0,
                summary="Earlier market summary.",
            ),
        ],
        credit_history=[
            WhatIfPublicCreditEvent(
                event_id="later_credit",
                as_of="2001-05-03T00:00:00Z",
                agency="S&P",
                to_rating="BBB+",
                headline="Later credit checkpoint",
                summary="Later credit summary.",
            ),
            WhatIfPublicCreditEvent(
                event_id="earlier_credit",
                as_of="2001-04-17T00:00:00Z",
                agency="S&P",
                to_rating="A-",
                headline="Earlier credit checkpoint",
                summary="Earlier credit summary.",
            ),
        ],
        ferc_history=[
            WhatIfPublicRegulatoryEvent(
                event_id="later_ferc",
                timestamp="2001-05-03T00:00:00Z",
                category="ferc",
                headline="Later regulatory checkpoint",
                summary="Later regulatory summary.",
            ),
            WhatIfPublicRegulatoryEvent(
                event_id="earlier_ferc",
                timestamp="2001-04-17T00:00:00Z",
                category="ferc",
                headline="Earlier regulatory checkpoint",
                summary="Earlier regulatory summary.",
            ),
        ],
    )

    sliced = slice_public_context_to_branch(
        context,
        branch_timestamp="2001-05-03T09:00:00Z",
    )
    assert sliced is not None
    assert [snapshot.snapshot_id for snapshot in sliced.financial_snapshots] == [
        "earlier_financial",
        "later_financial",
    ]
    assert [event.event_id for event in sliced.public_news_events] == [
        "earlier_news",
        "later_news",
    ]
    assert [row.as_of for row in sliced.stock_history] == [
        "2001-04-30T00:00:00Z",
        "2001-05-02T00:00:00Z",
    ]
    assert [event.event_id for event in sliced.credit_history] == [
        "earlier_credit",
        "later_credit",
    ]
    assert [event.event_id for event in sliced.ferc_history] == [
        "earlier_ferc",
        "later_ferc",
    ]

    prompt_lines = public_context_prompt_lines(
        sliced,
        max_financial=1,
        max_news=1,
    )

    assert any("Later financial checkpoint" in line for line in prompt_lines)
    assert not any("Earlier financial checkpoint" in line for line in prompt_lines)
    assert any("Later news checkpoint" in line for line in prompt_lines)
    assert not any("Earlier news checkpoint" in line for line in prompt_lines)
    assert any("Market checkpoints:" == line for line in prompt_lines)
    assert any("2001-05-02 close 52.91" in line for line in prompt_lines)
    assert not any("2001-04-30 close 62.72" in line for line in prompt_lines)
    assert not any("2001-05-03 close 49.12" in line for line in prompt_lines)
    assert any("Credit checkpoints:" == line for line in prompt_lines)
    assert any("Later credit checkpoint" in line for line in prompt_lines)
    assert not any("Earlier credit checkpoint" in line for line in prompt_lines)
    assert any("Regulatory checkpoints:" == line for line in prompt_lines)
    assert any("Later regulatory checkpoint" in line for line in prompt_lines)
    assert not any("Earlier regulatory checkpoint" in line for line in prompt_lines)


def test_public_context_branch_slice_includes_same_day_stock_after_market_close() -> (
    None
):
    context = WhatIfPublicContext(
        pack_name="enron_public_context",
        stock_history=[
            WhatIfPublicStockHistoryRow(
                as_of="2001-05-03T00:00:00Z",
                label="Same-day market checkpoint",
                close=49.12,
                volume=100.0,
                summary="Same-day market summary.",
            ),
            WhatIfPublicStockHistoryRow(
                as_of="2001-05-02T00:00:00Z",
                label="Earlier market checkpoint",
                close=52.91,
                volume=100.0,
                summary="Earlier market summary.",
            ),
        ],
    )

    sliced = slice_public_context_to_branch(
        context,
        branch_timestamp="2001-05-03T21:30:00Z",
    )

    assert sliced is not None
    assert [row.as_of for row in sliced.stock_history] == [
        "2001-05-02T00:00:00Z",
        "2001-05-03T00:00:00Z",
    ]


def test_load_world_materialize_episode_and_saved_scene_round_trip_public_context(
    tmp_path: Path,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_public_context_rosetta_fixture(rosetta_dir)

    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    materialization = materialize_episode(
        world,
        root=tmp_path / "episode",
        event_id="evt-201",
    )
    manifest = load_episode_manifest(materialization.workspace_root)
    scene = build_saved_decision_scene(materialization.workspace_root)
    snapshot_payload = json.loads(
        materialization.context_snapshot_path.read_text(encoding="utf-8")
    )

    assert world.public_context is not None
    assert [
        snapshot.snapshot_id for snapshot in world.public_context.financial_snapshots
    ] == ["q1_2001_earnings_release"]
    assert [event.event_id for event in world.public_context.public_news_events] == [
        "cliff_baxter_resignation"
    ]
    assert len(world.public_context.stock_history) == 13
    assert world.public_context.stock_history[0].as_of == "2001-04-17T00:00:00Z"
    assert world.public_context.stock_history[-1].as_of == "2001-05-03T00:00:00Z"
    assert manifest.public_context is not None
    assert [
        snapshot.snapshot_id for snapshot in manifest.public_context.financial_snapshots
    ] == ["q1_2001_earnings_release"]
    assert [event.event_id for event in manifest.public_context.public_news_events] == [
        "cliff_baxter_resignation"
    ]
    assert len(manifest.public_context.stock_history) == 12
    assert manifest.public_context.stock_history[0].as_of == "2001-04-17T00:00:00Z"
    assert manifest.public_context.stock_history[-1].as_of == "2001-05-02T00:00:00Z"
    assert scene.public_context is not None
    assert [event.event_id for event in scene.public_context.public_news_events] == [
        "cliff_baxter_resignation"
    ]
    assert len(scene.public_context.stock_history) == 12
    assert scene.public_context.stock_history[0].as_of == "2001-04-17T00:00:00Z"
    assert scene.public_context.stock_history[-1].as_of == "2001-05-02T00:00:00Z"
    assert manifest.historical_business_state is not None
    assert scene.historical_business_state is not None
    assert scene.historical_business_state.summary
    assert (
        snapshot_payload["metadata"]["whatif"]["public_context"]["stock_history"][0][
            "as_of"
        ]
        == "2001-04-17T00:00:00Z"
    )
    assert (
        snapshot_payload["metadata"]["whatif"]["public_context"]["public_news_events"][
            0
        ]["event_id"]
        == "cliff_baxter_resignation"
    )
    assert snapshot_payload["metadata"]["whatif"]["historical_business_state"][
        "summary"
    ]


def test_llm_prompt_only_includes_public_facts_known_by_branch_date(
    tmp_path: Path,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_public_context_rosetta_fixture(rosetta_dir)

    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    materialization = materialize_episode(
        world,
        root=tmp_path / "episode",
        event_id="evt-201",
    )
    manifest = load_episode_manifest(materialization.workspace_root)
    context = json.loads(
        materialization.context_snapshot_path.read_text(encoding="utf-8")
    )
    allowed_actors, allowed_recipients = _allowed_thread_participants(
        context=context,
        manifest=manifest,
    )

    prompt = _llm_counterfactual_prompt(
        context=context,
        manifest=manifest,
        prompt="Keep the draft inside Enron.",
        allowed_actors=allowed_actors,
        allowed_recipients=allowed_recipients,
    )

    assert "Q1 2001 earnings release" in prompt
    assert "Vice chairman Cliff Baxter resigned" in prompt
    assert "2001-04-30 close 62.72" in prompt
    assert "Q2 2001 earnings release" not in prompt
    assert "third-quarter loss" not in prompt
    assert "PG&E entered Chapter 11" not in prompt


def test_benchmark_dossier_includes_branch_filtered_public_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_public_context_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _public_context_research_pack()

    monkeypatch.setattr(
        "vei.whatif.benchmark.get_research_pack",
        lambda _pack_id: pack,
    )

    result = build_branch_point_benchmark(
        world,
        artifacts_root=tmp_path / "benchmark_artifacts",
        label="public_context_benchmark",
        heldout_pack_id="fixture_public_context_pack",
    )
    dossier = (
        result.artifacts.dossier_root
        / "public_context_case"
        / "minimize_enterprise_risk.md"
    ).read_text(encoding="utf-8")

    assert result.cases[0].public_context is not None
    assert "## Public Company Context" in dossier
    assert "Q1 2001 earnings release" in dossier
    assert "Vice chairman Cliff Baxter resigned" in dossier
    assert "2001-04-30 close 62.72" in dossier
    assert "Q2 2001 earnings release" not in dossier


def test_non_enron_world_has_no_public_context(tmp_path: Path) -> None:
    archive_path = tmp_path / "context_snapshot.json"
    archive_path.write_text(
        json.dumps(
            {
                "organization_name": "Py Corp",
                "organization_domain": "pycorp.example.com",
                "threads": [
                    {
                        "thread_id": "py-thread",
                        "subject": "Draft note",
                        "messages": [
                            {
                                "message_id": "py-msg-001",
                                "from": "emma@pycorp.example.com",
                                "to": "legal@pycorp.example.com",
                                "subject": "Draft note",
                                "body_text": "Please review.",
                                "timestamp": "2026-03-01T09:00:00Z",
                            }
                        ],
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    world = load_world(source="mail_archive", source_dir=archive_path)

    assert world.public_context is None


def test_mail_archive_world_loads_sidecar_public_context(tmp_path: Path) -> None:
    archive_path = tmp_path / "context_snapshot.json"
    archive_path.write_text(
        json.dumps(
            {
                "organization_name": "Py Corp",
                "organization_domain": "pycorp.example.com",
                "threads": [
                    {
                        "thread_id": "py-thread",
                        "subject": "Draft note",
                        "messages": [
                            {
                                "message_id": "py-msg-001",
                                "from": "emma@pycorp.example.com",
                                "to": "legal@pycorp.example.com",
                                "subject": "Draft note",
                                "body_text": "Please review.",
                                "timestamp": "2026-03-01T09:00:00Z",
                            },
                            {
                                "message_id": "py-msg-002",
                                "from": "legal@pycorp.example.com",
                                "to": "emma@pycorp.example.com",
                                "subject": "Re: Draft note",
                                "body_text": "Keep this internal.",
                                "timestamp": "2026-03-01T09:05:00Z",
                            },
                        ],
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    _write_generic_public_context_fixture(tmp_path)

    world = load_world(source="mail_archive", source_dir=archive_path)
    materialization = materialize_episode(
        world,
        root=tmp_path / "generic_episode",
        event_id="py-msg-002",
    )

    assert world.public_context is not None
    assert [item.snapshot_id for item in world.public_context.financial_snapshots] == [
        "py_q1_outlook"
    ]
    assert [item.event_id for item in world.public_context.public_news_events] == [
        "py_launch_note"
    ]
    assert materialization.public_context is not None
    assert [
        item.snapshot_id for item in materialization.public_context.financial_snapshots
    ] == ["py_q1_outlook"]
    assert [
        item.event_id for item in materialization.public_context.public_news_events
    ] == ["py_launch_note"]
