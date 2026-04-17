from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import json
import math
import os
import re
import threading
from typing import Any, Dict, List, Sequence

from vei.events.api import (
    EventDomain,
    EventProvenance,
    ObjectRef,
    build_event,
    emit_event,
)
from vei.llm.providers import plan_once_with_usage
from vei.project_settings import default_model_for_provider

from .compaction import apply_compaction, classify_freshness
from .models import (
    KnowledgeAsset,
    KnowledgeClaim,
    KnowledgeCitationSpan,
    KnowledgeComposeRequest,
    KnowledgeComposeResult,
    KnowledgeCompositionDetails,
    KnowledgeCompositionValidation,
    KnowledgeEdge,
    KnowledgeMetricBinding,
    KnowledgeProvenance,
    KnowledgeRetrieveHit,
    KnowledgeRetrieveRequest,
    KnowledgeStoreSnapshot,
)

_ASSET_ID_RE = re.compile(r"[^A-Za-z0-9_-]+")
_ABSOLUTE_TIME_FLOOR_MS = 10_000_000_000


def empty_store() -> KnowledgeStoreSnapshot:
    return KnowledgeStoreSnapshot()


def store_from_payload(payload: Any) -> KnowledgeStoreSnapshot:
    if isinstance(payload, KnowledgeStoreSnapshot):
        return payload.model_copy(deep=True)
    if not isinstance(payload, dict):
        return empty_store()

    raw_assets = payload.get("assets") or {}
    raw_edges = payload.get("edges") or []
    metadata = dict(payload.get("metadata") or {})

    if isinstance(raw_assets, list):
        assets = {}
        for item in raw_assets:
            if not isinstance(item, dict):
                continue
            asset = KnowledgeAsset.model_validate(item)
            assets[asset.asset_id] = asset
        raw_assets = assets

    if isinstance(raw_assets, dict):
        assets = {
            str(asset_id): (
                value
                if isinstance(value, KnowledgeAsset)
                else KnowledgeAsset.model_validate(value)
            )
            for asset_id, value in raw_assets.items()
            if isinstance(value, (KnowledgeAsset, dict))
        }
    else:
        assets = {}

    edges = [
        edge if isinstance(edge, KnowledgeEdge) else KnowledgeEdge.model_validate(edge)
        for edge in raw_edges
        if isinstance(edge, (KnowledgeEdge, dict))
    ]
    reference_now_ms = _default_reference_now_ms(metadata=metadata, assets=assets)
    if reference_now_ms > 0:
        metadata.setdefault("reference_now_ms", reference_now_ms)
    return KnowledgeStoreSnapshot(
        assets=assets,
        edges=edges,
        events=[
            dict(event)
            for event in (payload.get("events") or [])
            if isinstance(event, dict)
        ],
        asset_seq=int(payload.get("asset_seq", 1) or 1),
        edge_seq=int(payload.get("edge_seq", 1) or 1),
        metadata=metadata,
    )


def parse_iso_to_ms(value: str | None) -> int:
    if not value:
        return 0
    text = str(value).strip()
    if not text:
        return 0
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def utc_now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def iso_from_ms(value: int) -> str:
    if int(value or 0) <= 0:
        return ""
    return (
        datetime.fromtimestamp(int(value) / 1000, UTC)
        .replace(microsecond=0)
        .isoformat()
    )


def _asset_timestamp_ms(asset: KnowledgeAsset) -> int:
    composed_at_ms = int(asset.metadata.get("composed_at_ms", 0) or 0)
    if composed_at_ms > 0:
        return composed_at_ms
    captured_at_ms = int(asset.metadata.get("captured_at_ms", 0) or 0)
    if captured_at_ms > 0:
        return captured_at_ms
    return parse_iso_to_ms(asset.provenance.captured_at)


def _payload_timestamp_ms(payload: dict[str, Any]) -> int:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        composed_at_ms = int(metadata.get("composed_at_ms", 0) or 0)
        if composed_at_ms > 0:
            return composed_at_ms
        captured_at_ms = int(metadata.get("captured_at_ms", 0) or 0)
        if captured_at_ms > 0:
            return captured_at_ms
    provenance = payload.get("provenance")
    if isinstance(provenance, dict):
        return parse_iso_to_ms(str(provenance.get("captured_at", "")))
    return 0


def _latest_asset_timestamp_ms(assets: Sequence[KnowledgeAsset]) -> int:
    return max((_asset_timestamp_ms(asset) for asset in assets), default=0)


def _default_reference_now_ms(
    *,
    metadata: dict[str, Any],
    assets: dict[str, KnowledgeAsset],
) -> int:
    reference_now_ms = int(metadata.get("reference_now_ms", 0) or 0)
    if reference_now_ms > 0:
        return reference_now_ms
    captured_at_ms = parse_iso_to_ms(str(metadata.get("captured_at", "")))
    if captured_at_ms > 0:
        return captured_at_ms
    return _latest_asset_timestamp_ms(list(assets.values()))


def resolve_knowledge_now_ms(
    source: KnowledgeStoreSnapshot | dict[str, Any],
    *,
    clock_ms: int = 0,
    now_ms: int | None = None,
) -> int:
    explicit_now_ms = int(now_ms or 0)
    if explicit_now_ms > 0:
        return explicit_now_ms

    relative_clock_ms = max(0, int(clock_ms or 0))
    if relative_clock_ms >= _ABSOLUTE_TIME_FLOOR_MS:
        return relative_clock_ms

    if isinstance(source, KnowledgeStoreSnapshot):
        metadata = dict(source.metadata)
        assets = dict(source.assets)
    else:
        metadata = dict(source.get("metadata") or {})
        raw_assets = source.get("assets") or {}
        assets = {
            str(asset_id): KnowledgeAsset.model_validate(payload)
            for asset_id, payload in raw_assets.items()
            if isinstance(payload, dict)
        }

    reference_now_ms = _default_reference_now_ms(metadata=metadata, assets=assets)
    if reference_now_ms > 0:
        return reference_now_ms + relative_clock_ms
    return relative_clock_ms


def _remember_reference_now_ms(
    store: KnowledgeStoreSnapshot,
    *,
    clock_ms: int,
    now_ms: int,
) -> None:
    if int(store.metadata.get("reference_now_ms", 0) or 0) > 0:
        return
    if now_ms <= 0:
        return
    store.metadata["reference_now_ms"] = max(
        0, int(now_ms) - max(0, int(clock_ms or 0))
    )


def latest_composed_asset_payload(
    assets: dict[str, Any],
) -> dict[str, Any] | None:
    composed = [
        payload
        for payload in assets.values()
        if isinstance(payload, dict) and isinstance(payload.get("composition"), dict)
    ]
    if not composed:
        return None
    composed.sort(
        key=lambda payload: (
            _payload_timestamp_ms(payload),
            str(payload.get("asset_id", "")),
        )
    )
    return composed[-1]


def asset_status_at(asset: KnowledgeAsset, *, now_ms: int) -> str:
    return classify_freshness(asset, now_ms=now_ms)


def normalize_asset_id(text: str) -> str:
    normalized = _ASSET_ID_RE.sub("-", text.strip()).strip("-")
    if not normalized:
        return "knowledge-asset"
    return normalized.lower()


def next_asset_id(store: KnowledgeStoreSnapshot, *, prefix: str = "KA") -> str:
    asset_id = f"{prefix}-{store.asset_seq:04d}"
    store.asset_seq += 1
    return asset_id


def next_edge_id(store: KnowledgeStoreSnapshot) -> str:
    edge_id = f"knowledge-edge-{store.edge_seq:04d}"
    store.edge_seq += 1
    return edge_id


def _store_assets(
    store: KnowledgeStoreSnapshot,
) -> dict[str, KnowledgeAsset]:
    return store.assets


def _store_edges(
    store: KnowledgeStoreSnapshot,
) -> list[KnowledgeEdge]:
    return store.edges


def register_asset(
    store: KnowledgeStoreSnapshot,
    asset: KnowledgeAsset,
    *,
    clock_ms: int = 0,
    now_ms: int | None = None,
    source_kind: str = "knowledge.ingested",
) -> KnowledgeAsset:
    resolved_now_ms = resolve_knowledge_now_ms(
        store,
        clock_ms=clock_ms,
        now_ms=now_ms,
    )
    captured_at_ms = _asset_timestamp_ms(asset)
    if captured_at_ms <= 0 and resolved_now_ms > 0:
        captured_at_ms = resolved_now_ms
    if captured_at_ms > 0:
        asset.metadata.setdefault("captured_at_ms", captured_at_ms)
        if not asset.provenance.captured_at:
            asset.provenance.captured_at = iso_from_ms(captured_at_ms)
    asset.status = asset_status_at(asset, now_ms=resolved_now_ms or captured_at_ms or 0)
    _remember_reference_now_ms(
        store,
        clock_ms=clock_ms,
        now_ms=resolved_now_ms,
    )
    _store_assets(store)[asset.asset_id] = asset
    event = build_event(
        domain=EventDomain("knowledge_graph"),
        kind=source_kind,
        ts_ms=int(clock_ms or captured_at_ms or resolved_now_ms or 0),
        provenance_origin=EventProvenance.DERIVED,
        provenance_source_id=asset.provenance.source_id,
        object_refs=[
            ObjectRef(
                object_id=asset.asset_id,
                domain="knowledge_graph",
                kind=asset.kind,
                label=asset.title,
            )
        ],
        delta_data={
            "asset_id": asset.asset_id,
            "kind": asset.kind,
            "status": asset.status,
            "linked_object_refs": list(asset.linked_object_refs),
        },
    )
    hashed = emit_event(event)
    store.events.append(hashed.model_dump(mode="json"))
    return asset


def link_asset(
    store: KnowledgeStoreSnapshot,
    *,
    from_asset_id: str,
    kind: str,
    to_ref: str,
    metadata: Dict[str, Any] | None = None,
) -> KnowledgeEdge:
    edge = KnowledgeEdge(
        edge_id=next_edge_id(store),
        kind=kind,  # type: ignore[arg-type]
        from_asset_id=from_asset_id,
        to_ref=to_ref,
        metadata=dict(metadata or {}),
    )
    _store_edges(store).append(edge)
    return edge


def supersede(
    store: KnowledgeStoreSnapshot,
    *,
    asset_id: str,
    replacement_asset_id: str,
    clock_ms: int = 0,
    now_ms: int | None = None,
) -> KnowledgeAsset:
    asset = _store_assets(store)[asset_id]
    replacement = _store_assets(store)[replacement_asset_id]
    asset.status = "superseded"
    if asset_id not in replacement.supersedes:
        replacement.supersedes.append(asset_id)
    if not any(
        edge.kind == "supersedes"
        and edge.from_asset_id == replacement_asset_id
        and edge.to_ref == asset_id
        for edge in _store_edges(store)
    ):
        link_asset(
            store,
            from_asset_id=replacement_asset_id,
            kind="supersedes",
            to_ref=asset_id,
        )
    register_asset(
        store,
        asset,
        clock_ms=clock_ms,
        now_ms=now_ms,
        source_kind="knowledge.superseded",
    )
    return asset


def expire(
    store: KnowledgeStoreSnapshot,
    *,
    asset_id: str,
    clock_ms: int,
    now_ms: int | None = None,
) -> KnowledgeAsset:
    asset = _store_assets(store)[asset_id]
    asset.status = "expired"
    register_asset(
        store,
        asset,
        clock_ms=clock_ms,
        now_ms=now_ms,
        source_kind="knowledge.expired",
    )
    return asset


def retrieve(
    store: KnowledgeStoreSnapshot,
    request: KnowledgeRetrieveRequest,
) -> List[KnowledgeRetrieveHit]:
    now_ms = int(request.now_ms or 0)
    query_tokens = {
        token
        for token in re.findall(r"[a-z0-9_]+", request.query.lower())
        if token.strip()
    }
    scope_refs = {item for item in request.scope_refs if item}
    kinds = {item for item in request.kinds if item}
    tags = {item.lower() for item in request.tags if item}

    hits: List[KnowledgeRetrieveHit] = []
    for asset in _store_assets(store).values():
        asset_status = asset_status_at(asset, now_ms=now_ms) if now_ms else asset.status
        if asset_status == "expired":
            continue
        if kinds and asset.kind not in kinds:
            continue
        if scope_refs and not set(asset.linked_object_refs).intersection(scope_refs):
            continue
        if tags and not tags.intersection({tag.lower() for tag in asset.tags}):
            continue
        score = float(asset.provenance.authority or 1.0)
        reasons: List[str] = []
        if scope_refs and set(asset.linked_object_refs).intersection(scope_refs):
            score += 3.0
            reasons.append("subject")
        if tags and tags.intersection({tag.lower() for tag in asset.tags}):
            score += 1.0
            reasons.append("tag")
        asset_text = " ".join(
            [
                asset.title.lower(),
                asset.summary.lower(),
                asset.body.lower()[:400],
                " ".join(tag.lower() for tag in asset.tags),
            ]
        )
        if query_tokens:
            match_count = sum(1 for token in query_tokens if token in asset_text)
            if match_count == 0:
                continue
            score += float(match_count) * 1.5
            reasons.append("query")
        captured_at_ms = int(asset.metadata.get("captured_at_ms", 0) or 0)
        if now_ms and captured_at_ms:
            age_days = max(0.0, (now_ms - captured_at_ms) / 86_400_000)
            score += max(0.0, 2.0 - min(age_days / 30.0, 2.0))
        if asset_status == "stale":
            score -= 1.5
            reasons.append("stale")
        if asset.kind in {"pricing", "metric_snapshot", "proposal"}:
            score += 0.75
        hits.append(
            KnowledgeRetrieveHit(
                asset=asset.model_copy(deep=True),
                score=round(score, 4),
                reasons=reasons,
            )
        )

    hits.sort(key=lambda item: (item.score, item.asset.asset_id), reverse=True)
    return hits[: max(1, int(request.limit))]


def _template_sections(
    target: str, template_id: str, seed_outline: Sequence[str]
) -> list[str]:
    if seed_outline:
        return [str(item) for item in seed_outline if str(item).strip()]
    if target == "brief":
        return ["What changed", "Grounding", "Risks", "Decision"]
    if target == "weekly_review":
        return ["Wins", "Drift", "Metrics", "Next week"]
    if "proposal" in template_id.lower():
        return [
            "Executive summary",
            "Current state",
            "Recommended plan",
            "Proof points",
            "Risks and approvals",
        ]
    return [
        "Executive summary",
        "Current state",
        "Recommended plan",
        "Proof points",
        "Risks and approvals",
    ]


def _truncate_summary(text: str, limit: int = 220) -> str:
    cleaned = " ".join(text.split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


def _best_metrics(assets: Sequence[KnowledgeAsset]) -> list[KnowledgeMetricBinding]:
    bindings: list[KnowledgeMetricBinding] = []
    for asset in assets:
        for key, value in asset.metrics.items():
            if isinstance(value, (str, int, float)) and str(value) != "":
                bindings.append(
                    KnowledgeMetricBinding(
                        metric_key=key,
                        expected_value=value,
                        cited_asset_id=asset.asset_id,
                        source_field=f"metrics.{key}",
                    )
                )
    return bindings


def _heuristic_compose(
    request: KnowledgeComposeRequest,
    retrieved_assets: Sequence[KnowledgeAsset],
) -> tuple[
    str,
    str,
    list[str],
    list[str],
    list[KnowledgeClaim],
    list[KnowledgeCitationSpan],
]:
    sections = _template_sections(
        request.target,
        request.template_id,
        request.seed_outline,
    )
    subject = request.subject_object_ref
    title = f"{subject} {request.target.replace('_', ' ').title()}"
    summary = " ".join(
        _truncate_summary(asset.summary or asset.title, 90)
        for asset in list(retrieved_assets)[:2]
    ).strip()
    if not summary:
        summary = f"{request.target.replace('_', ' ').title()} grounded in current company knowledge."

    section_bodies: list[str] = []
    claims: list[KnowledgeClaim] = []
    citation_spans: list[KnowledgeCitationSpan] = []
    for index, section in enumerate(sections, start=1):
        asset = (
            retrieved_assets[(index - 1) % max(len(retrieved_assets), 1)]
            if retrieved_assets
            else None
        )
        marker = f"[{asset.asset_id}]" if asset is not None else ""
        if asset is None:
            section_text = (
                f"## {section}\n"
                f"No fresh source matched this section yet. {request.prompt}".strip()
            )
            section_bodies.append(section_text)
            continue
        sentence = _truncate_summary(asset.summary or asset.body or asset.title, 220)
        section_text = f"## {section}\n{sentence} {marker}".strip()
        section_bodies.append(section_text)
        claim_id = f"claim-{index:02d}"
        claims.append(
            KnowledgeClaim(
                claim_id=claim_id,
                text=sentence,
                citation_asset_ids=[asset.asset_id],
                section=section,
            )
        )
        citation_spans.append(
            KnowledgeCitationSpan(
                asset_id=asset.asset_id,
                marker=marker,
                section=section,
                quote=_truncate_summary(asset.body, 140),
            )
        )
    return title, summary, sections, section_bodies, claims, citation_spans


def _llm_available(provider: str) -> bool:
    provider_key_map = {
        "openai": ("OPENAI_API_KEY",),
        "anthropic": ("ANTHROPIC_API_KEY",),
        "google": ("GOOGLE_API_KEY", "GEMINI_API_KEY"),
        "openrouter": ("OPENROUTER_API_KEY",),
    }
    return any(
        os.environ.get(name, "").strip() for name in provider_key_map.get(provider, ())
    )


def _run_async(awaitable: Any) -> Any:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)
    if not loop.is_running():
        return asyncio.run(awaitable)
    return _run_async_in_thread(awaitable)


def _run_async_in_thread(awaitable: Any) -> Any:
    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}

    def runner() -> None:
        try:
            result["value"] = asyncio.run(awaitable)
        except BaseException as exc:  # pragma: no cover - surfaced in caller
            error["value"] = exc

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    thread.join()
    if "value" in error:
        raise error["value"]
    if "value" not in result:
        raise RuntimeError("knowledge runner returned no result")
    return result["value"]


def _llm_compose(
    request: KnowledgeComposeRequest,
    retrieved_assets: Sequence[KnowledgeAsset],
) -> tuple[dict[str, Any], dict[str, Any]]:
    sections = _template_sections(
        request.target,
        request.template_id,
        request.seed_outline,
    )
    source_map = {
        asset.asset_id: {
            "title": asset.title,
            "summary": asset.summary,
            "body": asset.body[:800],
            "kind": asset.kind,
            "metrics": asset.metrics,
            "linked_object_refs": asset.linked_object_refs,
        }
        for asset in retrieved_assets
    }
    system = (
        "You write grounded business artifacts. "
        "Return strict JSON with keys title, summary, body, sections, claims. "
        "Every claim must cite one or more allowed asset ids."
    )
    user = json.dumps(
        {
            "target": request.target,
            "template_id": request.template_id,
            "subject_object_ref": request.subject_object_ref,
            "required_sections": sections,
            "prompt": request.prompt,
            "allowed_asset_ids": list(source_map.keys()),
            "sources": source_map,
        },
        indent=2,
    )
    schema = {
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "summary": {"type": "string"},
            "body": {"type": "string"},
            "sections": {"type": "array", "items": {"type": "string"}},
            "claims": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "text": {"type": "string"},
                        "section": {"type": "string"},
                        "citation_asset_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                    "required": ["text", "citation_asset_ids"],
                },
            },
        },
        "required": ["title", "summary", "body", "sections", "claims"],
    }
    result = _run_async(
        plan_once_with_usage(
            provider=request.provider,
            model=request.model or default_model_for_provider(request.provider),
            system=system,
            user=user,
            plan_schema=schema,
            timeout_s=180,
        )
    )
    return result.plan, {
        "provider": result.usage.provider,
        "model": result.usage.model,
        "prompt_tokens": result.usage.prompt_tokens,
        "completion_tokens": result.usage.completion_tokens,
        "total_tokens": result.usage.total_tokens,
        "estimated_cost_usd": result.usage.estimated_cost_usd,
    }


def validate_composed_asset(
    asset: KnowledgeAsset,
    *,
    assets: Dict[str, KnowledgeAsset],
    now_ms: int,
    tolerance: float = 0.0,
) -> KnowledgeCompositionValidation:
    details = asset.composition
    validation = KnowledgeCompositionValidation()
    if details is None:
        validation.issues.append("asset has no composition details")
        validation.citations_present = False
        validation.citations_resolve = False
        validation.sources_within_shelf_life = False
        validation.numbers_reconcile = False
        validation.format_matches_template = False
        return validation

    if not details.claims or any(
        not claim.citation_asset_ids for claim in details.claims
    ):
        validation.citations_present = False
        validation.issues.append("one or more claims are missing citations")

    for claim in details.claims:
        for asset_id in claim.citation_asset_ids:
            source = assets.get(asset_id)
            if source is None:
                validation.citations_resolve = False
                validation.issues.append(f"missing cited asset {asset_id}")
                continue
            source_status = asset_status_at(source, now_ms=now_ms)
            if source_status == "expired":
                validation.sources_within_shelf_life = False
                validation.issues.append(f"expired cited asset {asset_id}")

    for binding in details.metric_bindings:
        source = assets.get(binding.cited_asset_id)
        if source is None:
            validation.numbers_reconcile = False
            validation.issues.append(f"missing metric source {binding.cited_asset_id}")
            continue
        source_value = source.metrics.get(binding.metric_key)
        if source_value is None:
            validation.numbers_reconcile = False
            validation.issues.append(
                f"missing metric {binding.metric_key} on {binding.cited_asset_id}"
            )
            continue
        if _numeric_mismatch(source_value, binding.expected_value, tolerance=tolerance):
            validation.numbers_reconcile = False
            validation.issues.append(
                f"metric mismatch for {binding.metric_key} on {binding.cited_asset_id}"
            )

    actual_sections = list(details.sections)
    if details.required_sections and actual_sections != details.required_sections:
        validation.format_matches_template = False
        validation.issues.append("section order does not match the template")

    return validation


def _numeric_mismatch(
    actual: Any,
    expected: Any,
    *,
    tolerance: float,
) -> bool:
    if actual == expected:
        return False
    try:
        actual_num = float(actual)
        expected_num = float(expected)
    except (TypeError, ValueError):
        return str(actual) != str(expected)
    if math.isnan(actual_num) or math.isnan(expected_num):
        return True
    return abs(actual_num - expected_num) > float(tolerance)


def compose_artifact(
    store: KnowledgeStoreSnapshot,
    request: KnowledgeComposeRequest,
    *,
    clock_ms: int = 0,
    now_ms: int | None = None,
) -> KnowledgeComposeResult:
    resolved_now_ms = resolve_knowledge_now_ms(
        store,
        clock_ms=clock_ms,
        now_ms=now_ms,
    )
    required_sections = _template_sections(
        request.target,
        request.template_id,
        request.seed_outline,
    )
    retrieval = retrieve(
        store,
        KnowledgeRetrieveRequest(
            query=request.prompt,
            scope_refs=list({request.subject_object_ref, *request.scope_refs}),
            kinds=request.kinds,
            tags=request.tags,
            limit=request.limit,
            now_ms=resolved_now_ms,
        ),
    )
    retrieved_assets = [hit.asset for hit in retrieval]
    notes: list[str] = []
    usage: dict[str, Any] = {}

    title: str
    summary: str
    sections: list[str]
    section_bodies: list[str]
    claims: list[KnowledgeClaim]
    citation_spans: list[KnowledgeCitationSpan]
    body: str

    mode = request.mode
    if mode == "llm" and not _llm_available(request.provider):
        notes.append("llm compose fell back to heuristic baseline: missing API key")
        mode = "heuristic_baseline"
    if mode == "llm":
        try:
            llm_payload, usage = _llm_compose(request, retrieved_assets)
            title = str(llm_payload.get("title") or "")
            summary = str(llm_payload.get("summary") or "")
            body = str(llm_payload.get("body") or "")
            sections = [str(item) for item in llm_payload.get("sections") or []]
            section_bodies = []
            claims = [
                KnowledgeClaim(
                    claim_id=f"claim-{index:02d}",
                    text=str(item.get("text") or ""),
                    section=str(item.get("section") or "") or None,
                    citation_asset_ids=[
                        str(asset_id)
                        for asset_id in item.get("citation_asset_ids") or []
                    ],
                )
                for index, item in enumerate(llm_payload.get("claims") or [], start=1)
                if isinstance(item, dict)
            ]
            citation_spans = [
                KnowledgeCitationSpan(
                    asset_id=str(asset_id),
                    marker=f"[{asset_id}]",
                )
                for claim in claims
                for asset_id in claim.citation_asset_ids
            ]
            if not title or not sections:
                raise ValueError("LLM composition returned an incomplete payload")
        except Exception as exc:  # noqa: BLE001
            notes.append(
                f"llm compose fell back to heuristic baseline: {type(exc).__name__}"
            )
            mode = "heuristic_baseline"
        else:
            notes.append("llm compose completed")
    if mode == "heuristic_baseline":
        (
            title,
            summary,
            sections,
            section_bodies,
            claims,
            citation_spans,
        ) = _heuristic_compose(
            request,
            retrieved_assets,
        )
        body = "\n\n".join(
            section for section in section_bodies if section.strip()
        ).strip()

    composition = KnowledgeCompositionDetails(
        target=request.target,
        template_id=request.template_id,
        subject_object_ref=request.subject_object_ref,
        mode=mode,
        provider=(request.provider if mode == "llm" else None),
        model=(
            request.model or default_model_for_provider(request.provider)
            if mode == "llm"
            else None
        ),
        prompt=request.prompt,
        required_sections=required_sections,
        sections=sections,
        claims=claims,
        citation_spans=citation_spans,
        metric_bindings=_best_metrics(retrieved_assets[:4]),
    )
    artifact = KnowledgeAsset(
        asset_id=next_asset_id(store, prefix="ART"),
        kind=request.target,
        title=title or f"{request.target.title()} for {request.subject_object_ref}",
        body=body,
        summary=summary or _truncate_summary(body, 160),
        tags=[request.target, request.template_id or "knowledge-compose"],
        provenance=KnowledgeProvenance(
            source="knowledge.compose_artifact",
            source_id=request.subject_object_ref,
            captured_at=iso_from_ms(resolved_now_ms),
            shelf_life_ms=30 * 86_400_000,
            authority=1.0,
            metadata={"mode": mode},
        ),
        linked_object_refs=[request.subject_object_ref, *request.scope_refs],
        derived_from=[asset.asset_id for asset in retrieved_assets[:8]],
        metrics={
            binding.metric_key: binding.expected_value
            for binding in composition.metric_bindings
        },
        metadata={
            "captured_at_ms": resolved_now_ms,
            "composed_at_ms": resolved_now_ms,
        },
        composition=composition,
    )
    validation = validate_composed_asset(
        artifact,
        assets={**store.assets, artifact.asset_id: artifact},
        now_ms=resolved_now_ms,
        tolerance=0.01,
    )
    artifact.composition.validation = validation
    register_asset(
        store,
        artifact,
        clock_ms=clock_ms,
        now_ms=resolved_now_ms,
        source_kind="knowledge.composed",
    )
    for cited_asset in retrieved_assets[:8]:
        link_asset(
            store,
            from_asset_id=artifact.asset_id,
            kind="cites",
            to_ref=cited_asset.asset_id,
        )
    link_asset(
        store,
        from_asset_id=artifact.asset_id,
        kind="applies_to",
        to_ref=request.subject_object_ref,
    )
    return KnowledgeComposeResult(
        artifact=artifact,
        retrieved_assets=retrieved_assets,
        validation=validation,
        notes=notes,
        usage=usage,
    )


def run_compaction(
    store: KnowledgeStoreSnapshot,
    *,
    clock_ms: int = 0,
    now_ms: int | None = None,
) -> list[dict[str, str]]:
    resolved_now_ms = resolve_knowledge_now_ms(
        store,
        clock_ms=clock_ms,
        now_ms=now_ms,
    )
    _remember_reference_now_ms(
        store,
        clock_ms=clock_ms,
        now_ms=resolved_now_ms,
    )
    assets, edges, changes = apply_compaction(
        store.assets,
        store.edges,
        now_ms=resolved_now_ms,
    )
    store.assets = assets
    store.edges = edges
    return changes


__all__ = [
    "KnowledgeAsset",
    "KnowledgeClaim",
    "KnowledgeComposeRequest",
    "KnowledgeComposeResult",
    "KnowledgeCompositionDetails",
    "KnowledgeCompositionValidation",
    "KnowledgeEdge",
    "KnowledgeMetricBinding",
    "KnowledgeProvenance",
    "KnowledgeRetrieveHit",
    "KnowledgeRetrieveRequest",
    "KnowledgeStoreSnapshot",
    "asset_status_at",
    "compose_artifact",
    "empty_store",
    "expire",
    "iso_from_ms",
    "link_asset",
    "latest_composed_asset_payload",
    "normalize_asset_id",
    "parse_iso_to_ms",
    "register_asset",
    "retrieve",
    "resolve_knowledge_now_ms",
    "run_compaction",
    "store_from_payload",
    "supersede",
    "utc_now_ms",
    "validate_composed_asset",
]
