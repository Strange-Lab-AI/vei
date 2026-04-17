from __future__ import annotations

from typing import Dict, List

from .models import KnowledgeAsset, KnowledgeAssetStatus, KnowledgeEdge


def classify_freshness(
    asset: KnowledgeAsset,
    *,
    now_ms: int,
) -> KnowledgeAssetStatus:
    if asset.status in {"superseded", "expired"}:
        return asset.status
    captured_at_ms = int(asset.metadata.get("captured_at_ms", 0) or 0)
    shelf_life_ms = asset.provenance.shelf_life_ms
    if shelf_life_ms is None or captured_at_ms <= 0:
        return "active"
    age_ms = max(0, int(now_ms) - captured_at_ms)
    if age_ms >= shelf_life_ms:
        return "expired"
    if age_ms >= int(shelf_life_ms * 0.75):
        return "stale"
    return "active"


def apply_compaction(
    assets: Dict[str, KnowledgeAsset],
    edges: List[KnowledgeEdge],
    *,
    now_ms: int,
) -> tuple[Dict[str, KnowledgeAsset], List[KnowledgeEdge], List[dict[str, str]]]:
    updated_assets = {
        asset_id: asset.model_copy(deep=True) for asset_id, asset in assets.items()
    }
    updated_edges = [edge.model_copy(deep=True) for edge in edges]
    changes: List[dict[str, str]] = []

    by_subject_kind: dict[tuple[str, str], list[KnowledgeAsset]] = {}
    for asset in updated_assets.values():
        next_status = classify_freshness(asset, now_ms=now_ms)
        if next_status != asset.status:
            asset.status = next_status
            changes.append({"kind": "freshness", "asset_id": asset.asset_id})
        if not asset.linked_object_refs:
            continue
        subject = asset.linked_object_refs[0]
        by_subject_kind.setdefault((asset.kind, subject), []).append(asset)

    for grouped_assets in by_subject_kind.values():
        active_assets = [
            asset
            for asset in grouped_assets
            if asset.status not in {"expired", "superseded"}
        ]
        if len(active_assets) < 2:
            continue
        ordered = sorted(
            active_assets,
            key=lambda asset: (
                int(asset.metadata.get("captured_at_ms", 0) or 0),
                asset.asset_id,
            ),
            reverse=True,
        )
        latest = ordered[0]
        for older in ordered[1:]:
            if older.status == "superseded":
                continue
            older.status = "superseded"
            if older.asset_id not in latest.supersedes:
                latest.supersedes.append(older.asset_id)
            updated_edges.append(
                KnowledgeEdge(
                    edge_id=f"knowledge-edge-{len(updated_edges) + 1:04d}",
                    kind="supersedes",
                    from_asset_id=latest.asset_id,
                    to_ref=older.asset_id,
                )
            )
            changes.append({"kind": "superseded", "asset_id": older.asset_id})

    dedup_seen: set[tuple[str, str, str]] = set()
    dedup_edges: List[KnowledgeEdge] = []
    for asset in updated_assets.values():
        if asset.status == "expired":
            continue
        key = (
            asset.kind,
            asset.provenance.source_id,
            asset.summary.strip().lower()[:160],
        )
        if key in dedup_seen:
            dedup_edges.append(
                KnowledgeEdge(
                    edge_id=f"knowledge-edge-{len(updated_edges) + len(dedup_edges) + 1:04d}",
                    kind="derived_from",
                    from_asset_id=asset.asset_id,
                    to_ref=asset.provenance.source_id or asset.asset_id,
                    metadata={"compacted": "dedup"},
                )
            )
            changes.append({"kind": "dedup", "asset_id": asset.asset_id})
            continue
        dedup_seen.add(key)
    updated_edges.extend(dedup_edges)
    return updated_assets, updated_edges, changes
