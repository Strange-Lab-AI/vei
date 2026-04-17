from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from vei.knowledge.api import (
    asset_status_at,
    compose_artifact,
    empty_store,
    latest_composed_asset_payload,
    register_asset,
    resolve_knowledge_now_ms,
    run_compaction,
    supersede,
)
from vei.knowledge.models import (
    KnowledgeAsset,
    KnowledgeComposeRequest,
    KnowledgeProvenance,
)


def _note(
    asset_id: str, *, captured_at: str, shelf_life_ms: int | None = None
) -> KnowledgeAsset:
    return KnowledgeAsset(
        asset_id=asset_id,
        kind="note",
        title=asset_id,
        body=f"Body for {asset_id}",
        summary=f"Summary for {asset_id}",
        provenance=KnowledgeProvenance(
            source="test",
            source_id=asset_id,
            captured_at=captured_at,
            shelf_life_ms=shelf_life_ms,
        ),
    )


def test_compose_llm_without_key_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "GOOGLE_API_KEY",
        "GEMINI_API_KEY",
        "OPENROUTER_API_KEY",
    ):
        monkeypatch.delenv(name, raising=False)

    result = compose_artifact(
        empty_store(),
        KnowledgeComposeRequest(
            subject_object_ref="crm_deal:CRM-1",
            mode="llm",
            provider="openai",
        ),
        now_ms=1_713_312_000_000,
    )

    assert result.artifact.composition is not None
    assert result.artifact.composition.mode == "heuristic_baseline"
    assert result.notes == [
        "llm compose fell back to heuristic baseline: missing API key"
    ]


def test_compose_llm_runs_inside_active_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_plan_once_with_usage(**_: object) -> SimpleNamespace:
        return SimpleNamespace(
            plan={
                "title": "Grounded Proposal",
                "summary": "Summary",
                "body": "## Executive summary\nGrounded claim [KA-1]",
                "sections": ["Executive summary"],
                "claims": [
                    {
                        "text": "Grounded claim",
                        "section": "Executive summary",
                        "citation_asset_ids": ["KA-1"],
                    }
                ],
            },
            usage=SimpleNamespace(
                provider="openai",
                model="test-model",
                prompt_tokens=10,
                completion_tokens=20,
                total_tokens=30,
                estimated_cost_usd=0.01,
            ),
        )

    monkeypatch.setattr(
        "vei.knowledge.api.plan_once_with_usage", fake_plan_once_with_usage
    )
    monkeypatch.setattr("vei.knowledge.api._llm_available", lambda provider: True)

    store = empty_store()
    register_asset(
        store,
        _note("KA-1", captured_at="2024-04-10T00:00:00+00:00"),
        now_ms=1_713_312_000_000,
    )

    async def runner() -> tuple[str, dict[str, object]]:
        result = compose_artifact(
            store,
            KnowledgeComposeRequest(
                subject_object_ref="crm_deal:CRM-1",
                prompt="Use the best source",
                mode="llm",
                provider="openai",
                model="test-model",
            ),
            now_ms=1_713_312_000_000,
        )
        assert result.artifact.composition is not None
        return result.artifact.title, result.usage

    title, usage = asyncio.run(runner())

    assert title == "Grounded Proposal"
    assert usage["model"] == "test-model"


def test_compose_without_sources_keeps_fallback_section_text() -> None:
    result = compose_artifact(
        empty_store(),
        KnowledgeComposeRequest(
            subject_object_ref="crm_deal:CRM-1",
            prompt="Need a draft",
            target="proposal",
            template_id="proposal_v1",
        ),
        now_ms=1_713_312_000_000,
    )

    assert (
        "No fresh source matched this section yet. Need a draft" in result.artifact.body
    )


def test_supersede_records_newer_asset_replacing_older_one() -> None:
    store = empty_store()
    register_asset(
        store,
        _note("KA-OLD", captured_at="2024-04-01T00:00:00+00:00"),
        now_ms=1_712_000_000_000,
    )
    register_asset(
        store,
        _note("KA-NEW", captured_at="2024-04-05T00:00:00+00:00"),
        now_ms=1_712_400_000_000,
    )

    supersede(
        store,
        asset_id="KA-OLD",
        replacement_asset_id="KA-NEW",
        now_ms=1_712_400_000_000,
    )

    assert store.assets["KA-OLD"].status == "superseded"
    assert store.assets["KA-NEW"].supersedes == ["KA-OLD"]
    assert [(edge.kind, edge.from_asset_id, edge.to_ref) for edge in store.edges] == [
        ("supersedes", "KA-NEW", "KA-OLD")
    ]


def test_run_compaction_uses_same_supersession_direction() -> None:
    store = empty_store()
    store.metadata["reference_now_ms"] = 1_712_000_000_000
    older = _note(
        "KA-OLD",
        captured_at="2024-04-01T00:00:00+00:00",
        shelf_life_ms=90 * 86_400_000,
    )
    older.linked_object_refs = ["crm_deal:CRM-1"]
    newer = _note(
        "KA-NEW",
        captured_at="2024-04-05T00:00:00+00:00",
        shelf_life_ms=90 * 86_400_000,
    )
    newer.linked_object_refs = ["crm_deal:CRM-1"]
    register_asset(store, older, now_ms=1_712_000_000_000)
    register_asset(store, newer, now_ms=1_712_400_000_000)

    changes = run_compaction(store, clock_ms=10)

    assert {"kind": "superseded", "asset_id": "KA-OLD"} in changes
    assert store.assets["KA-OLD"].status == "superseded"
    assert store.assets["KA-NEW"].supersedes == ["KA-OLD"]
    assert any(
        edge.kind == "supersedes"
        and edge.from_asset_id == "KA-NEW"
        and edge.to_ref == "KA-OLD"
        for edge in store.edges
    )


def test_resolve_knowledge_now_ms_uses_reference_clock_and_latest_sorting() -> None:
    store = empty_store()
    store.metadata["reference_now_ms"] = 1_712_000_000_000
    older = _note(
        "ART-001",
        captured_at="2024-04-01T00:00:00+00:00",
        shelf_life_ms=86_400_000,
    )
    older.metadata["composed_at_ms"] = 1_712_000_000_000
    older.composition = compose_artifact(
        empty_store(),
        KnowledgeComposeRequest(subject_object_ref="crm_deal:CRM-1"),
        now_ms=1_712_000_000_000,
    ).artifact.composition
    newer = older.model_copy(deep=True)
    newer.asset_id = "ART-999"
    newer.metadata["composed_at_ms"] = 1_712_000_000_500
    store.assets = {older.asset_id: older, newer.asset_id: newer}

    now_ms = resolve_knowledge_now_ms(store, clock_ms=500)
    latest = latest_composed_asset_payload(
        {
            asset_id: asset.model_dump(mode="json")
            for asset_id, asset in store.assets.items()
        }
    )

    assert now_ms == 1_712_000_000_500
    assert latest is not None
    assert latest["asset_id"] == "ART-999"


def test_reference_clock_drives_freshness_status() -> None:
    store = empty_store()
    store.metadata["reference_now_ms"] = 1_712_000_000_000
    asset = _note(
        "KA-STALE",
        captured_at="2024-04-01T00:00:00+00:00",
        shelf_life_ms=86_400_000,
    )
    register_asset(store, asset, now_ms=1_712_000_000_000)

    now_ms = resolve_knowledge_now_ms(store, clock_ms=2 * 86_400_000)

    assert asset_status_at(store.assets["KA-STALE"], now_ms=now_ms) == "expired"
