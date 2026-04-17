from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import ValidationError

from vei.knowledge.api import (
    KnowledgeAsset,
    KnowledgeComposeRequest,
    KnowledgeProvenance,
    KnowledgeRetrieveRequest,
    KnowledgeStoreSnapshot,
    compose_artifact,
    empty_store,
    link_asset,
    next_asset_id,
    register_asset,
    resolve_knowledge_now_ms,
    retrieve,
    run_compaction,
    store_from_payload,
    supersede,
)
from vei.project_settings import default_model_for_provider
from vei.world.api import Scenario

from .errors import MCPError
from .tool_providers import PrefixToolProvider
from .tool_registry import ToolSpec


def _seeded_store(scenario: Optional[Scenario]) -> KnowledgeStoreSnapshot:
    if scenario is None or not getattr(scenario, "knowledge_graph", None):
        return empty_store()
    return store_from_payload(scenario.knowledge_graph)


def _invalid_args(exc: Exception) -> MCPError:
    return MCPError("invalid_args", str(exc))


class KnowledgeSim:
    def __init__(self, router: Any, scenario: Optional[Scenario] = None):
        self.router = router
        self.bus = router.bus
        self.store = _seeded_store(scenario)

    def _now_ms(self) -> int:
        return resolve_knowledge_now_ms(self.store, clock_ms=int(self.bus.clock_ms))

    def list_assets(
        self,
        *,
        query: str = "",
        scope_refs: Optional[List[str]] = None,
        kinds: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        limit: int = 12,
    ) -> List[Dict[str, Any]]:
        try:
            request = KnowledgeRetrieveRequest(
                query=query,
                scope_refs=list(scope_refs or []),
                kinds=list(kinds or []),  # type: ignore[arg-type]
                tags=list(tags or []),
                limit=limit,
                now_ms=self._now_ms(),
            )
        except ValidationError as exc:
            raise _invalid_args(exc) from exc
        hits = retrieve(
            self.store,
            request,
        )
        return [
            {
                **hit.asset.model_dump(mode="json"),
                "_score": hit.score,
                "_reasons": hit.reasons,
            }
            for hit in hits
        ]

    def get_asset(self, asset_id: str) -> Dict[str, Any]:
        asset = self.store.assets.get(asset_id)
        if asset is None:
            raise MCPError(
                "knowledge.asset_not_found", f"Unknown knowledge asset: {asset_id}"
            )
        return asset.model_dump(mode="json")

    def ingest_asset(
        self,
        *,
        kind: str,
        title: str,
        body: str,
        source: str,
        source_id: str = "",
        import_id: str = "",
        captured_at: str = "",
        shelf_life_ms: Optional[int] = None,
        authority: float = 1.0,
        summary: str = "",
        tags: Optional[List[str]] = None,
        linked_object_refs: Optional[List[str]] = None,
        derived_from: Optional[List[str]] = None,
        supersedes_asset_id: str = "",
        metrics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        try:
            asset = KnowledgeAsset(
                asset_id=next_asset_id(self.store),
                kind=kind,  # type: ignore[arg-type]
                title=title.strip() or "Knowledge Asset",
                body=body,
                summary=summary.strip() or body[:180],
                tags=[str(item) for item in (tags or []) if str(item).strip()],
                provenance=KnowledgeProvenance(
                    source=source,
                    source_id=source_id,
                    import_id=import_id,
                    captured_at=captured_at,
                    shelf_life_ms=shelf_life_ms,
                    authority=float(authority),
                ),
                linked_object_refs=[
                    str(item)
                    for item in (linked_object_refs or [])
                    if str(item).strip()
                ],
                derived_from=[
                    str(item) for item in (derived_from or []) if str(item).strip()
                ],
                supersedes=[supersedes_asset_id] if supersedes_asset_id else [],
                metrics={
                    str(key): value
                    for key, value in dict(metrics or {}).items()
                    if value is not None
                },
            )
        except ValidationError as exc:
            raise _invalid_args(exc) from exc
        registered = register_asset(
            self.store,
            asset,
            clock_ms=int(self.bus.clock_ms),
            now_ms=self._now_ms(),
        )
        if supersedes_asset_id:
            try:
                link_asset(
                    self.store,
                    from_asset_id=registered.asset_id,
                    kind="supersedes",
                    to_ref=supersedes_asset_id,
                )
            except ValidationError as exc:
                raise _invalid_args(exc) from exc
        for ref in registered.linked_object_refs:
            try:
                link_asset(
                    self.store,
                    from_asset_id=registered.asset_id,
                    kind="applies_to",
                    to_ref=ref,
                )
            except ValidationError as exc:
                raise _invalid_args(exc) from exc
        return registered.model_dump(mode="json")

    def link_asset(
        self,
        *,
        from_asset_id: str,
        kind: str,
        to_ref: str,
    ) -> Dict[str, Any]:
        if from_asset_id not in self.store.assets:
            raise MCPError(
                "knowledge.asset_not_found",
                f"Unknown knowledge asset: {from_asset_id}",
            )
        try:
            edge = link_asset(
                self.store,
                from_asset_id=from_asset_id,
                kind=kind,
                to_ref=to_ref,
            )
        except ValidationError as exc:
            raise _invalid_args(exc) from exc
        return edge.model_dump(mode="json")

    def supersede_asset(
        self, *, asset_id: str, replacement_asset_id: str
    ) -> Dict[str, Any]:
        if asset_id not in self.store.assets:
            raise MCPError(
                "knowledge.asset_not_found", f"Unknown knowledge asset: {asset_id}"
            )
        if replacement_asset_id not in self.store.assets:
            raise MCPError(
                "knowledge.asset_not_found",
                f"Unknown knowledge asset: {replacement_asset_id}",
            )
        asset = supersede(
            self.store,
            asset_id=asset_id,
            replacement_asset_id=replacement_asset_id,
            clock_ms=int(self.bus.clock_ms),
            now_ms=self._now_ms(),
        )
        return asset.model_dump(mode="json")

    def expire_asset(self, *, asset_id: str) -> Dict[str, Any]:
        asset = self.store.assets.get(asset_id)
        if asset is None:
            raise MCPError(
                "knowledge.asset_not_found", f"Unknown knowledge asset: {asset_id}"
            )
        asset.status = "expired"
        register_asset(
            self.store,
            asset,
            clock_ms=int(self.bus.clock_ms),
            now_ms=self._now_ms(),
            source_kind="knowledge.expired",
        )
        return asset.model_dump(mode="json")

    def compose_artifact(
        self,
        *,
        target: str,
        template_id: str,
        subject_object_ref: str,
        scope_refs: Optional[List[str]] = None,
        kinds: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        seed_outline: Optional[List[str]] = None,
        prompt: str = "",
        mode: str = "heuristic_baseline",
        provider: str = "openai",
        model: str = "",
        limit: int = 8,
    ) -> Dict[str, Any]:
        try:
            request = KnowledgeComposeRequest(
                target=target,  # type: ignore[arg-type]
                template_id=template_id,
                subject_object_ref=subject_object_ref,
                scope_refs=list(scope_refs or []),
                kinds=list(kinds or []),  # type: ignore[arg-type]
                tags=list(tags or []),
                seed_outline=list(seed_outline or []),
                prompt=prompt,
                mode=mode,  # type: ignore[arg-type]
                provider=provider,
                model=model or default_model_for_provider(provider),
                limit=limit,
            )
        except ValidationError as exc:
            raise _invalid_args(exc) from exc
        result = compose_artifact(
            self.store,
            request,
            clock_ms=int(self.bus.clock_ms),
            now_ms=self._now_ms(),
        )
        return result.model_dump(mode="json")

    def run_compaction(self) -> Dict[str, Any]:
        changes = run_compaction(
            self.store,
            clock_ms=int(self.bus.clock_ms),
            now_ms=self._now_ms(),
        )
        return {"ok": True, "changes": changes}

    def export_state(self) -> Dict[str, Any]:
        return self.store.model_dump(mode="json")

    def import_state(self, state: Dict[str, Any]) -> None:
        self.store = store_from_payload(state)

    def summary(self) -> str:
        asset_count = len(self.store.assets)
        active_count = sum(
            1 for asset in self.store.assets.values() if asset.status == "active"
        )
        return f"Knowledge has {asset_count} assets and {active_count} active sources."

    def action_menu(self) -> List[Dict[str, Any]]:
        return [
            {
                "tool": "knowledge.list_assets",
                "label": "Review knowledge",
                "args": {},
            },
            {
                "tool": "knowledge.compose_artifact",
                "label": "Draft proposal",
                "args": {
                    "target": "proposal",
                    "template_id": "proposal_v1",
                    "subject_object_ref": "deal:NORTHSTAR-DEAL-7",
                    "mode": "heuristic_baseline",
                },
            },
        ]


class KnowledgeToolProvider(PrefixToolProvider):
    def __init__(self, sim: KnowledgeSim):
        super().__init__("knowledge", prefixes=("knowledge.",))
        self.sim = sim
        self._specs = [
            ToolSpec(
                name="knowledge.list_assets",
                description="List and rank knowledge assets by subject, freshness, and query fit.",
                permissions=("knowledge:read",),
                default_latency_ms=120,
                latency_jitter_ms=25,
            ),
            ToolSpec(
                name="knowledge.get_asset",
                description="Read one knowledge asset by id.",
                permissions=("knowledge:read",),
                default_latency_ms=100,
                latency_jitter_ms=20,
            ),
            ToolSpec(
                name="knowledge.ingest_asset",
                description="Ingest a typed knowledge asset into the deterministic store.",
                permissions=("knowledge:write",),
                side_effects=("knowledge_mutation",),
                default_latency_ms=160,
                latency_jitter_ms=30,
            ),
            ToolSpec(
                name="knowledge.link_asset",
                description="Create a typed relation between a knowledge asset and another asset or subject.",
                permissions=("knowledge:write",),
                side_effects=("knowledge_mutation",),
                default_latency_ms=130,
                latency_jitter_ms=25,
            ),
            ToolSpec(
                name="knowledge.supersede_asset",
                description="Mark an older knowledge asset as superseded by a newer one.",
                permissions=("knowledge:write",),
                side_effects=("knowledge_mutation",),
                default_latency_ms=130,
                latency_jitter_ms=25,
            ),
            ToolSpec(
                name="knowledge.expire_asset",
                description="Expire a knowledge asset explicitly.",
                permissions=("knowledge:write",),
                side_effects=("knowledge_mutation",),
                default_latency_ms=120,
                latency_jitter_ms=20,
            ),
            ToolSpec(
                name="knowledge.compose_artifact",
                description="Compose a grounded business artifact from ranked knowledge assets.",
                permissions=("knowledge:write",),
                side_effects=("knowledge_mutation",),
                default_latency_ms=220,
                latency_jitter_ms=40,
            ),
            ToolSpec(
                name="knowledge.run_compaction",
                description="Apply shelf-life, supersession, and dedup compaction policies.",
                permissions=("knowledge:write",),
                side_effects=("knowledge_mutation",),
                default_latency_ms=160,
                latency_jitter_ms=30,
            ),
        ]

    def specs(self) -> List[ToolSpec]:
        return list(self._specs)

    def call(self, tool: str, args: Dict[str, Any]) -> Any:
        if tool == "knowledge.list_assets":
            return self.sim.list_assets(
                query=str(args.get("query", "")),
                scope_refs=args.get("scope_refs"),
                kinds=args.get("kinds"),
                tags=args.get("tags"),
                limit=int(args.get("limit", 12) or 12),
            )
        if tool == "knowledge.get_asset":
            return self.sim.get_asset(str(args.get("asset_id", "")))
        if tool == "knowledge.ingest_asset":
            return self.sim.ingest_asset(
                kind=str(args.get("kind", "note")),
                title=str(args.get("title", "")),
                body=str(args.get("body", "")),
                source=str(args.get("source", "knowledge")),
                source_id=str(args.get("source_id", "")),
                import_id=str(args.get("import_id", "")),
                captured_at=str(args.get("captured_at", "")),
                shelf_life_ms=(
                    int(args["shelf_life_ms"])
                    if args.get("shelf_life_ms") is not None
                    else None
                ),
                authority=float(args.get("authority", 1.0) or 1.0),
                summary=str(args.get("summary", "")),
                tags=args.get("tags"),
                linked_object_refs=args.get("linked_object_refs"),
                derived_from=args.get("derived_from"),
                supersedes_asset_id=str(args.get("supersedes_asset_id", "")),
                metrics=args.get("metrics"),
            )
        if tool == "knowledge.link_asset":
            return self.sim.link_asset(
                from_asset_id=str(args.get("from_asset_id", "")),
                kind=str(args.get("kind", "derived_from")),
                to_ref=str(args.get("to_ref", "")),
            )
        if tool == "knowledge.supersede_asset":
            return self.sim.supersede_asset(
                asset_id=str(args.get("asset_id", "")),
                replacement_asset_id=str(args.get("replacement_asset_id", "")),
            )
        if tool == "knowledge.expire_asset":
            return self.sim.expire_asset(asset_id=str(args.get("asset_id", "")))
        if tool == "knowledge.compose_artifact":
            return self.sim.compose_artifact(
                target=str(args.get("target", "proposal")),
                template_id=str(args.get("template_id", "")),
                subject_object_ref=str(args.get("subject_object_ref", "")),
                scope_refs=args.get("scope_refs"),
                kinds=args.get("kinds"),
                tags=args.get("tags"),
                seed_outline=args.get("seed_outline"),
                prompt=str(args.get("prompt", "")),
                mode=str(args.get("mode", "heuristic_baseline")),
                provider=str(args.get("provider", "openai")),
                model=str(args.get("model", "")),
                limit=int(args.get("limit", 8) or 8),
            )
        if tool == "knowledge.run_compaction":
            return self.sim.run_compaction()
        raise MCPError("unknown_tool", f"Unknown knowledge tool: {tool}")
