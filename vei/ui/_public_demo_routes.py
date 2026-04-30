from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Sequence

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from vei.whatif.api import (
    NewsStatePointCandidateInput,
    WhatIfBusinessOutcomeHeads,
    WhatIfEvent,
    WhatIfFutureStateHeads,
    WhatIfWorld,
    build_news_state_point,
    load_world,
    run_news_state_point_counterfactual,
)

from ._public_demo_models import (
    PublicDemoCandidateInput,
    PublicDemoChatRequest,
    PublicDemoChatResponse,
    PublicDemoEvidenceEvent,
    PublicDemoScoredCandidate,
    PublicDemoScoreRequest,
    PublicDemoScoreResponse,
    PublicDemoSourceSummary,
    PublicDemoStatusResponse,
    PublicDemoTimelinePoint,
)

PUBLIC_DEMO_MANIFEST_FILE = "public_demo_manifest.json"
PUBLIC_DEMO_DEFAULT_SOURCE_ID = "news_americanstories_public_world"
PUBLIC_DEMO_DEFAULT_TOPIC = "all_public_record"
PUBLIC_DEMO_DEFAULT_AS_OF = "1837-09-06"
PUBLIC_DEMO_JEPA_CHECKPOINT_ENV = "VEI_PUBLIC_DEMO_JEPA_CHECKPOINT"
PUBLIC_DEMO_JEPA_ARTIFACTS_ROOT_ENV = "VEI_PUBLIC_DEMO_ARTIFACTS_ROOT"
PUBLIC_DEMO_DEFAULT_JEPA_CHECKPOINT = (
    "_vei_out/world_model_multitenant_jepa/"
    "enron_dispatch_powr_news_fuller_cap512_h12_20260427/"
    "model_runs/jepa_latent/model.pt"
)
PUBLIC_DEMO_DEFAULT_JEPA_ARTIFACTS_ROOT = "_vei_out/public_history_live_jepa"
PUBLIC_DEMO_CAVEAT = (
    "This is evidence-grounded decision support from public records, not causal "
    "proof. The current news demo uses generic business/future-state heads, so "
    "rankings are exploratory."
)

_REPO_ROOT = Path(__file__).resolve().parents[2]

_DEFAULT_ACTIONS = [
    PublicDemoCandidateInput(
        label="Publish a cross-topic public bulletin",
        action=(
            "Publish a public bulletin that maps the visible market, labor, "
            "public-order, petition, Texas, Seminole, and international-credit "
            "signals without claiming certainty about the future."
        ),
    ),
    PublicDemoCandidateInput(
        label="Open a congressional evidence memo",
        action=(
            "Prepare a public evidence memo for congressional and state actors that "
            "separates what is known by date across Treasury policy, banks, "
            "petitions, Texas, Seminole costs, labor, and relief."
        ),
    ),
    PublicDemoCandidateInput(
        label="Start a relief and employment watch",
        action=(
            "Open a dated relief and employment watch that tracks wages, workshops, "
            "food prices, poor relief, public order, bank credit, and local-budget "
            "stress before recommending intervention."
        ),
    ),
    PublicDemoCandidateInput(
        label="Hold for cross-source verification",
        action=(
            "Do not publish a recommendation yet; hold for cross-source verification "
            "and release only a short uncertainty note that identifies missing reports "
            "and later confirmation needs."
        ),
    ),
]


@dataclass(frozen=True)
class _ActionSignalGroup:
    key: str
    title: str
    keywords: tuple[str, ...]
    labels: tuple[str, ...]
    candidate_type: str
    label_template: str
    action_template: str


_ACTION_SIGNAL_LABELS: tuple[tuple[str, str], ...] = (
    ("bank", "bank credit"),
    ("credit", "credit stress"),
    ("treasury", "Treasury deposits"),
    ("specie", "specie payments"),
    ("currency", "currency pressure"),
    ("congress", "Congress"),
    ("senate", "Senate"),
    ("president", "presidential policy"),
    ("petition", "petitions"),
    ("slavery", "slavery petitions"),
    ("abolition", "abolition petitions"),
    ("labor", "labor reports"),
    ("employment", "employment reports"),
    ("wages", "wage reports"),
    ("relief", "relief requests"),
    ("poor", "poor relief"),
    ("prices", "price reports"),
    ("riot", "public-order reports"),
    ("texas", "Texas"),
    ("mexico", "Mexico"),
    ("canada", "Canada"),
    ("seminole", "Seminole war costs"),
    ("british", "British trade"),
    ("cotton", "cotton trade"),
    ("trade", "trade reports"),
    ("election", "election signals"),
    ("fire", "fire reports"),
    ("flood", "flood reports"),
    ("disease", "disease reports"),
    ("court", "court reports"),
    ("trial", "trial reports"),
    ("railroad", "railroad reports"),
    ("canal", "canal reports"),
    ("crop", "crop reports"),
)

_ACTION_SIGNAL_GROUPS: tuple["_ActionSignalGroup", ...] = (
    # Keep finance first only inside its domain; group ranking below prevents it
    # from crowding out politics, rights, labor, and foreign affairs.
    _ActionSignalGroup(
        key="finance",
        title="bank credit, Treasury deposits, and currency",
        keywords=(
            "bank",
            "banking",
            "credit",
            "treasury",
            "specie",
            "currency",
            "deposit",
        ),
        labels=(
            "bank credit",
            "Treasury deposits",
            "specie payments",
            "currency pressure",
        ),
        candidate_type="customer_status_note",
        label_template="Publish a finance bulletin on {terms}",
        action_template=(
            "Publish a dated public finance bulletin on {terms}, citing only reports "
            "visible by {date} and separating bank credit, Treasury, specie, and "
            "currency uncertainty."
        ),
    ),
    _ActionSignalGroup(
        key="governance",
        title="Congress, the presidency, and public policy",
        keywords=(
            "congress",
            "senate",
            "president",
            "presidential",
            "election",
            "policy",
        ),
        labels=("Congress", "Senate", "presidential policy", "election signals"),
        candidate_type="decision_log_evidence",
        label_template="Prepare a governance memo on {terms}",
        action_template=(
            "Prepare a dated governance memo on {terms} for public officials, "
            "separating newspaper claims from congressional and executive signals "
            "visible by {date}."
        ),
    ),
    _ActionSignalGroup(
        key="rights",
        title="slavery petitions and rights politics",
        keywords=(
            "petition",
            "petitions",
            "slavery",
            "abolition",
            "arkansas",
            "admission",
        ),
        labels=(
            "petitions",
            "slavery petitions",
            "abolition petitions",
            "Arkansas admission",
        ),
        candidate_type="expert_review_gate",
        label_template="Open a petition-rights review on {terms}",
        action_template=(
            "Open a public petition-rights review on {terms}, tracking which claims "
            "were visible by {date} before recommending any official posture."
        ),
    ),
    _ActionSignalGroup(
        key="foreign",
        title="Texas, Mexico, Canada, and foreign affairs",
        keywords=("texas", "mexico", "canada", "seminole", "british", "foreign", "war"),
        labels=("Texas", "Mexico", "Canada", "Seminole war costs", "British trade"),
        candidate_type="cross_function_war_room",
        label_template="Prepare a foreign-risk brief on {terms}",
        action_template=(
            "Prepare a foreign-risk brief on {terms}, comparing visible reports by "
            "{date} and flagging where public confidence or military-cost signals "
            "remain unresolved."
        ),
    ),
    _ActionSignalGroup(
        key="labor",
        title="labor, prices, relief, and employment",
        keywords=("labor", "employment", "wages", "relief", "poor", "prices", "work"),
        labels=(
            "labor reports",
            "employment reports",
            "wage reports",
            "relief requests",
            "price reports",
        ),
        candidate_type="narrow_pilot",
        label_template="Open a relief-and-prices watch on {terms}",
        action_template=(
            "Open a dated relief-and-prices watch on {terms}, updating the visible "
            "record after {date} only when new public reports arrive."
        ),
    ),
    _ActionSignalGroup(
        key="public_order",
        title="public order and local civic stress",
        keywords=(
            "riot",
            "crowd",
            "public order",
            "meeting",
            "local",
            "civic",
            "school",
            "church",
        ),
        labels=("public-order reports", "local civic reports"),
        candidate_type="narrow_pilot",
        label_template="Open a local public-order watch on {terms}",
        action_template=(
            "Open a local public-order watch on {terms}, distinguishing civic notices "
            "from risk reports visible by {date}."
        ),
    ),
    _ActionSignalGroup(
        key="public_resilience",
        title="health, courts, transport, and crop disruption",
        keywords=(
            "fire",
            "flood",
            "disease",
            "health",
            "court",
            "trial",
            "railroad",
            "canal",
            "crop",
            "weather",
        ),
        labels=(
            "fire reports",
            "flood reports",
            "disease reports",
            "court reports",
            "railroad reports",
            "crop reports",
        ),
        candidate_type="expert_review_gate",
        label_template="Open a public-resilience review on {terms}",
        action_template=(
            "Open a public-resilience review on {terms}, tying health, court, "
            "transport, and crop signals to dated reports visible by {date} before "
            "publishing any recommendation."
        ),
    ),
)


def register_public_demo_routes(app: FastAPI, root: Path) -> None:
    @app.get("/api/workspace/public-demo")
    def api_workspace_public_demo(
        as_of: str | None = None,
        topic: str | None = None,
    ) -> JSONResponse:
        manifest = _load_manifest(root)
        source_path = _source_path(root, manifest)
        if source_path is None:
            payload = PublicDemoStatusResponse(
                available=False,
                unavailable_reason="public demo source is not configured",
                caveat=PUBLIC_DEMO_CAVEAT,
            )
            return JSONResponse(payload.model_dump(mode="json"))
        try:
            world = _load_public_world(source_path)
            state_point = _build_state_point(
                world,
                topic=topic or str(manifest["default_topic"]),
                as_of=as_of or str(manifest["default_as_of"]),
            )
        except Exception as exc:  # noqa: BLE001
            payload = PublicDemoStatusResponse(
                available=False,
                unavailable_reason=str(exc),
                caveat=PUBLIC_DEMO_CAVEAT,
            )
            return JSONResponse(payload.model_dump(mode="json"))

        checkpoint_path = _jepa_checkpoint_path(root, manifest)
        payload = PublicDemoStatusResponse(
            available=True,
            source=_source_summary(
                manifest=manifest,
                world=world,
                source_path=source_path,
            ),
            topic=state_point.topic,
            as_of=state_point.as_of,
            historical_cutoff=_historical_cutoff(state_point.as_of),
            state_summary=state_point.state_summary,
            timeline_points=_timeline_points(
                world,
                selected_as_of=state_point.as_of,
            ),
            evidence_events=_evidence_payloads(state_point.evidence_events),
            suggested_candidate_actions=_suggested_actions(state_point),
            scoring_available=checkpoint_path is not None,
            scoring_source="live_jepa",
            scoring_checkpoint_path=str(checkpoint_path or ""),
            scoring_unavailable_reason=(
                ""
                if checkpoint_path is not None
                else _jepa_unavailable_reason(manifest)
            ),
            caveat=PUBLIC_DEMO_CAVEAT,
        )
        return JSONResponse(payload.model_dump(mode="json"))

    @app.post("/api/workspace/public-demo/chat")
    def api_workspace_public_demo_chat(request: PublicDemoChatRequest) -> JSONResponse:
        manifest = _load_manifest(root)
        _require_source_id(request.source_id, manifest)
        world = _load_public_world_or_400(root, manifest)
        state_point = _build_state_point_or_400(
            world,
            topic=request.topic,
            as_of=request.as_of,
        )
        cited = _selected_or_relevant_events(
            state_point.history_events,
            selected_event_ids=request.selected_event_ids,
            message=request.message,
        )
        response = PublicDemoChatResponse(
            source_id=str(manifest["source_id"]),
            topic=state_point.topic,
            as_of=state_point.as_of,
            historical_cutoff=_historical_cutoff(state_point.as_of),
            assistant_text=_chat_answer(
                state_summary=state_point.state_summary,
                cited_events=cited,
            ),
            cited_event_ids=[event.event_id for event in cited],
            cited_events=_evidence_payloads(cited),
            suggested_candidate_actions=_suggested_actions(state_point),
            caveat=PUBLIC_DEMO_CAVEAT,
        )
        return JSONResponse(response.model_dump(mode="json"))

    @app.post("/api/workspace/public-demo/score")
    def api_workspace_public_demo_score(
        request: PublicDemoScoreRequest,
    ) -> JSONResponse:
        manifest = _load_manifest(root)
        _require_source_id(request.source_id, manifest)
        world = _load_public_world_or_400(root, manifest)
        state_point = _build_state_point_or_400(
            world,
            topic=request.topic,
            as_of=request.as_of,
        )
        candidates = request.candidates or _suggested_actions(state_point)
        try:
            candidate_inputs = [
                NewsStatePointCandidateInput(
                    label=candidate.label,
                    action=candidate.action,
                    candidate_type=candidate.candidate_type,
                )
                for candidate in candidates
            ]
        except ValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not candidate_inputs:
            raise HTTPException(
                status_code=400,
                detail=(
                    "at least one candidate action is required; no evidence-grounded "
                    "suggestions were available for this cutoff"
                ),
            )

        checkpoint_path = _jepa_checkpoint_path(root, manifest)
        if checkpoint_path is None:
            raise HTTPException(
                status_code=503,
                detail=_jepa_unavailable_reason(manifest),
            )
        try:
            scored, artifact_path = _score_candidates_with_live_jepa(
                root=root,
                manifest=manifest,
                world=world,
                topic=state_point.topic,
                as_of=state_point.as_of,
                checkpoint_path=checkpoint_path,
                candidates=candidate_inputs,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=500,
                detail=f"live JEPA scoring failed: {exc}",
            ) from exc
        response = PublicDemoScoreResponse(
            source_id=str(manifest["source_id"]),
            topic=state_point.topic,
            as_of=state_point.as_of,
            decision_title=(
                request.decision_title
                or f"{state_point.topic.replace('_', ' ').title()} public response"
            ),
            historical_cutoff=_historical_cutoff(state_point.as_of),
            scoring_source="live_jepa",
            scoring_artifact_path=str(artifact_path),
            scoring_checkpoint_path=str(checkpoint_path),
            candidates=scored,
            evidence_events=_evidence_payloads(state_point.evidence_events),
            caveat=PUBLIC_DEMO_CAVEAT,
        )
        return JSONResponse(response.model_dump(mode="json"))


def _load_manifest(root: Path) -> dict[str, Any]:
    manifest: dict[str, Any] = {
        "source_id": PUBLIC_DEMO_DEFAULT_SOURCE_ID,
        "title": "Public History: AmericanStories News World",
        "summary": (
            "Choose a point from an expanded public-news record, inspect "
            "what was visible by then, and test a scenario from that state."
        ),
        "source_path": "context_snapshot.json",
        "default_topic": PUBLIC_DEMO_DEFAULT_TOPIC,
        "default_as_of": PUBLIC_DEMO_DEFAULT_AS_OF,
        "jepa_checkpoint_path": "",
        "jepa_artifacts_root": PUBLIC_DEMO_DEFAULT_JEPA_ARTIFACTS_ROOT,
    }
    path = root / PUBLIC_DEMO_MANIFEST_FILE
    if path.exists():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            manifest.update(loaded)
    return manifest


def _source_path(root: Path, manifest: dict[str, Any]) -> Path | None:
    env_path = os.environ.get("VEI_PUBLIC_DEMO_SOURCE_DIR", "").strip()
    raw_path = env_path or str(manifest.get("source_path") or "").strip()
    if not raw_path:
        raw_path = "context_snapshot.json"
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        if env_path and candidate.exists():
            candidate = candidate.resolve()
        else:
            candidate = root / candidate
    if candidate.is_dir():
        candidate = candidate / "context_snapshot.json"
    return candidate.resolve() if candidate.exists() else None


def _jepa_checkpoint_path(root: Path, manifest: dict[str, Any]) -> Path | None:
    env_path = os.environ.get(PUBLIC_DEMO_JEPA_CHECKPOINT_ENV, "").strip()
    manifest_path = str(manifest.get("jepa_checkpoint_path") or "").strip()
    if env_path:
        return _resolve_existing_public_demo_path(root, env_path, prefer_runtime=True)
    if manifest_path:
        return _resolve_existing_public_demo_path(root, manifest_path)
    return _resolve_existing_public_demo_path(
        root,
        PUBLIC_DEMO_DEFAULT_JEPA_CHECKPOINT,
        prefer_runtime=True,
    )


def _jepa_unavailable_reason(manifest: dict[str, Any]) -> str:
    configured = (
        os.environ.get(PUBLIC_DEMO_JEPA_CHECKPOINT_ENV, "").strip()
        or str(manifest.get("jepa_checkpoint_path") or "").strip()
        or PUBLIC_DEMO_DEFAULT_JEPA_CHECKPOINT
    )
    return (
        "Live JEPA scoring is unavailable because no checkpoint exists at "
        f"{configured}. Set {PUBLIC_DEMO_JEPA_CHECKPOINT_ENV} to a trained "
        "news-state-point JEPA checkpoint; the public demo will not fabricate "
        "rankings."
    )


def _resolve_existing_public_demo_path(
    root: Path,
    raw_path: str,
    *,
    prefer_runtime: bool = False,
) -> Path | None:
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path.resolve() if path.exists() else None
    bases = [root, _REPO_ROOT, Path.cwd()]
    if prefer_runtime:
        bases = [_REPO_ROOT, Path.cwd(), root]
    for base in bases:
        candidate = (base / path).resolve()
        if candidate.exists():
            return candidate
    return None


def _jepa_artifacts_root(root: Path, manifest: dict[str, Any]) -> Path:
    env_path = os.environ.get(PUBLIC_DEMO_JEPA_ARTIFACTS_ROOT_ENV, "").strip()
    raw_path = (
        env_path
        or str(manifest.get("jepa_artifacts_root") or "").strip()
        or PUBLIC_DEMO_DEFAULT_JEPA_ARTIFACTS_ROOT
    )
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path.resolve()
    base = (
        _REPO_ROOT
        if env_path or raw_path == PUBLIC_DEMO_DEFAULT_JEPA_ARTIFACTS_ROOT
        else root
    )
    return (base / path).resolve()


def _load_public_world(source_path: Path) -> WhatIfWorld:
    return load_world(
        source="company_history",
        source_dir=source_path,
        include_situation_graph=False,
    )


def _load_public_world_or_400(root: Path, manifest: dict[str, Any]) -> WhatIfWorld:
    source_path = _source_path(root, manifest)
    if source_path is None:
        raise HTTPException(
            status_code=400,
            detail="public demo source is not configured for this workspace",
        )
    try:
        return _load_public_world(source_path)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _build_state_point(world: WhatIfWorld, *, topic: str, as_of: str):
    return build_news_state_point(
        world,
        topic=topic,
        as_of=as_of,
        future_horizon_days=90,
        max_history_events=240,
        max_evidence_events=12,
        allow_empty_history=True,
    )


def _build_state_point_or_400(world: WhatIfWorld, *, topic: str, as_of: str):
    try:
        return _build_state_point(world, topic=topic, as_of=as_of)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _source_summary(
    *,
    manifest: dict[str, Any],
    world: WhatIfWorld,
    source_path: Path,
) -> PublicDemoSourceSummary:
    return PublicDemoSourceSummary(
        source_id=str(manifest["source_id"]),
        title=str(manifest.get("title") or world.summary.organization_name),
        summary=str(manifest.get("summary") or ""),
        source_dir=str(source_path),
        default_topic=str(manifest["default_topic"]),
        default_as_of=str(manifest["default_as_of"]),
        first_timestamp=world.summary.first_timestamp,
        last_timestamp=world.summary.last_timestamp,
        event_count=world.summary.event_count,
    )


def _require_source_id(source_id: str, manifest: dict[str, Any]) -> None:
    requested = (source_id or "").strip() or str(manifest["source_id"])
    if requested != str(manifest["source_id"]):
        raise HTTPException(
            status_code=400,
            detail=f"unknown public demo source_id: {requested}",
        )


def _evidence_payloads(events: Sequence[WhatIfEvent]) -> list[PublicDemoEvidenceEvent]:
    return [
        PublicDemoEvidenceEvent(
            event_id=event.event_id,
            timestamp=event.timestamp,
            subject=event.subject,
            snippet=event.snippet,
            actor_id=event.actor_id,
            surface=event.surface,
        )
        for event in events
    ]


def _timeline_points(
    world: WhatIfWorld,
    *,
    selected_as_of: str,
    max_points: int = 12,
) -> list[PublicDemoTimelinePoint]:
    dated_events = sorted(
        world.events, key=lambda event: (event.timestamp_ms, event.event_id)
    )
    if not dated_events:
        return []

    day_rows: list[tuple[str, WhatIfEvent, int]] = []
    day_to_index: dict[str, int] = {}
    current_day = ""
    current_event: WhatIfEvent | None = None
    visible_count = 0
    for event in dated_events:
        visible_count += 1
        day = event.timestamp[:10]
        if day != current_day:
            if current_day and current_event is not None:
                day_rows.append((current_day, current_event, visible_count - 1))
                day_to_index[current_day] = len(day_rows) - 1
            current_day = day
            current_event = event
        elif _timeline_event_score(event) > _timeline_event_score(current_event):
            current_event = event
    if current_day and current_event is not None:
        day_rows.append((current_day, current_event, visible_count))
        day_to_index[current_day] = len(day_rows) - 1

    selected_indexes = _sample_timeline_indexes(
        row_count=len(day_rows),
        max_points=max_points,
    )
    selected_day = selected_as_of[:10]
    exact_index = day_to_index.get(selected_day)
    if exact_index is not None:
        selected_indexes.add(exact_index)
    else:
        prior_indexes = [
            index
            for index, (day, _event, _count) in enumerate(day_rows)
            if day <= selected_day
        ]
        if prior_indexes:
            selected_indexes.add(prior_indexes[-1])

    points = []
    for index in sorted(selected_indexes):
        day, event, count = day_rows[index]
        visible_events = dated_events[:count]
        points.append(
            PublicDemoTimelinePoint(
                event_id=event.event_id,
                timestamp=event.timestamp,
                label=_timeline_signal_label(visible_events),
                summary=_timeline_signal_summary(visible_events),
                visible_event_count=count,
                is_default=day == selected_day,
            )
        )
    return sorted(points, key=lambda item: (item.timestamp, item.event_id))


def _sample_timeline_indexes(*, row_count: int, max_points: int) -> set[int]:
    if row_count <= max_points:
        return set(range(row_count))
    if max_points <= 1:
        return {0}
    return {
        round(index * (row_count - 1) / (max_points - 1)) for index in range(max_points)
    }


def _timeline_event_score(event: WhatIfEvent | None) -> int:
    if event is None:
        return -1
    text = _event_text(event)
    return sum(1 for keyword in _keywords_for_timeline() if keyword in text)


def _keywords_for_timeline() -> tuple[str, ...]:
    return (
        "bank",
        "credit",
        "treasury",
        "congress",
        "president",
        "slavery",
        "abolition",
        "foreign",
        "war",
        "policy",
        "market",
        "trade",
    )


def _timeline_signal_label(events: Sequence[WhatIfEvent]) -> str:
    terms = _timeline_signal_terms(events[-120:])
    if not terms:
        return "Sparse public record"
    return ", ".join(terms[:3])


def _timeline_signal_summary(events: Sequence[WhatIfEvent]) -> str:
    terms = _timeline_signal_terms(events[-120:])
    if not terms:
        return f"{len(events)} public records visible by this date."
    return f"{len(events)} public records visible; recurring signals: {', '.join(terms[:5])}."


def _timeline_signal_terms(events: Sequence[WhatIfEvent]) -> list[str]:
    labels = {
        "bank": "banking",
        "credit": "credit",
        "treasury": "Treasury",
        "congress": "Congress",
        "president": "presidency",
        "slavery": "slavery",
        "abolition": "abolition",
        "foreign": "foreign affairs",
        "war": "war",
        "market": "markets",
        "trade": "trade",
    }
    counts: dict[str, int] = {}
    for event in events:
        text = _event_text(event)
        for keyword, label in labels.items():
            if keyword in text:
                counts[label] = counts.get(label, 0) + 1
    return [
        label
        for label, _count in sorted(
            counts.items(),
            key=lambda item: (-item[1], item[0].lower()),
        )
    ]


def _selected_or_relevant_events(
    history_events: Sequence[WhatIfEvent],
    *,
    selected_event_ids: Sequence[str],
    message: str,
) -> list[WhatIfEvent]:
    by_id = {event.event_id: event for event in history_events}
    if selected_event_ids:
        missing = [event_id for event_id in selected_event_ids if event_id not in by_id]
        if missing:
            raise HTTPException(
                status_code=400,
                detail=(
                    "selected events are not visible as of this historical cutoff: "
                    + ", ".join(missing)
                ),
            )
        return [by_id[event_id] for event_id in selected_event_ids][:4]
    terms = _query_terms(message)
    scored_with_counts = [
        (sum(1 for term in terms if term in _event_text(event)), event)
        for event in history_events
    ]
    if terms:
        scored_with_counts = [
            (count, event) for count, event in scored_with_counts if count > 0
        ]
    scored = [
        event
        for _count, event in sorted(
            scored_with_counts,
            key=lambda item: (-item[0], -item[1].timestamp_ms, item[1].event_id),
        )
    ]
    return scored[:4]


def _query_terms(message: str) -> list[str]:
    terms = [
        part for part in re.split(r"[^a-z0-9]+", message.lower()) if len(part) >= 4
    ]
    stop = {"what", "were", "visible", "with", "this", "that", "about", "from"}
    return [term for term in terms if term not in stop][:12]


def _event_text(event: WhatIfEvent) -> str:
    return " ".join(
        [event.subject, event.snippet, event.actor_id, event.surface]
    ).lower()


def _chat_answer(*, state_summary: str, cited_events: Sequence[WhatIfEvent]) -> str:
    cited_text = " ".join(
        f"{event.timestamp[:10]}: {event.subject} ({event.snippet})"
        for event in cited_events[:3]
    )
    if not cited_text:
        cited_text = "No matching pre-cutoff evidence was found in the bounded sample."
    return (
        f"{state_summary} The evidence I can cite before the cutoff is: "
        f"{cited_text} I am not using later outcomes to answer. Use the action "
        "test to score a concrete scenario against the live model at this cutoff."
    )


def _suggested_actions(
    state_point: Any | None = None,
) -> list[PublicDemoCandidateInput]:
    if state_point is None:
        return []
    groups = _candidate_signal_groups(
        list(state_point.evidence_events) or list(state_point.history_events)[-120:],
        state_summary=str(getattr(state_point, "state_summary", "")),
    )
    if not groups:
        return []

    date = str(state_point.as_of)[:10]
    actions: list[PublicDemoCandidateInput] = []
    for group, terms in groups[:4]:
        joined_terms = _join_signal_terms(terms[:3] or [group.title])
        actions.append(
            PublicDemoCandidateInput(
                label=group.label_template.format(terms=joined_terms),
                action=group.action_template.format(terms=joined_terms, date=date),
                candidate_type=group.candidate_type,
            )
        )
    return actions


def _candidate_signal_groups(
    events: Sequence[WhatIfEvent],
    *,
    state_summary: str = "",
) -> list[tuple[_ActionSignalGroup, list[str]]]:
    counts: dict[str, int] = {group.key: 0 for group in _ACTION_SIGNAL_GROUPS}
    first_seen: dict[str, int] = {}
    labels_by_group: dict[str, dict[str, int]] = {
        group.key: {} for group in _ACTION_SIGNAL_GROUPS
    }
    texts = [_event_text(event) for event in events]
    if state_summary:
        texts.append(state_summary.lower())
    for text_index, text in enumerate(texts):
        for group in _ACTION_SIGNAL_GROUPS:
            if any(keyword in text for keyword in group.keywords):
                counts[group.key] += 1
                first_seen.setdefault(group.key, text_index)
                label_counts = labels_by_group[group.key]
                for label in group.labels:
                    if any(part in text for part in _label_match_parts(label)):
                        label_counts[label] = label_counts.get(label, 0) + 1
    groups = [
        (
            group,
            [
                label
                for label, _count in sorted(
                    labels_by_group[group.key].items(),
                    key=lambda item: (-item[1], item[0].lower()),
                )
            ],
        )
        for group in _ACTION_SIGNAL_GROUPS
        if counts[group.key] > 0
    ]
    return sorted(
        groups,
        key=lambda item: (
            -min(counts[item[0].key], 4),
            first_seen.get(item[0].key, 10_000),
            item[0].key,
        ),
    )


def _label_match_parts(label: str) -> tuple[str, ...]:
    if label == "Arkansas admission":
        return ("arkansas", "admission")
    return tuple(part for part in re.split(r"[^a-z0-9]+", label.lower()) if part)


def _join_signal_terms(terms: Sequence[str]) -> str:
    cleaned = [term for term in terms if term]
    if not cleaned:
        return "the visible public record"
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, and {cleaned[-1]}"


def _score_candidates_with_live_jepa(
    *,
    root: Path,
    manifest: dict[str, Any],
    world: WhatIfWorld,
    topic: str,
    as_of: str,
    checkpoint_path: Path,
    candidates: Sequence[NewsStatePointCandidateInput],
) -> tuple[list[PublicDemoScoredCandidate], Path]:
    result = run_news_state_point_counterfactual(
        world,
        checkpoint_path=checkpoint_path,
        artifacts_root=_jepa_artifacts_root(root, manifest),
        label=_jepa_run_label(
            source_id=str(manifest["source_id"]),
            topic=topic,
            as_of=as_of,
            candidates=candidates,
        ),
        topic=topic,
        as_of=as_of,
        candidates=candidates,
    )
    payload = json.loads(result.artifacts.result_json_path.read_text(encoding="utf-8"))
    rows = payload.get("candidates")
    if not isinstance(rows, list):
        raise ValueError("JEPA result did not contain a candidates list")
    scored = [
        _scored_candidate_from_jepa_row(row, fallback_index=index)
        for index, row in enumerate(rows, start=1)
        if isinstance(row, dict)
    ]
    if not scored:
        raise ValueError("JEPA result contained no scored candidates")
    scored.sort(key=lambda item: (item.rank, -item.score, item.label.lower()))
    for rank, item in enumerate(scored, start=1):
        item.rank = rank
    return scored, result.artifacts.result_json_path


def _jepa_run_label(
    *,
    source_id: str,
    topic: str,
    as_of: str,
    candidates: Sequence[NewsStatePointCandidateInput],
) -> str:
    candidate_text = "\n".join(
        f"{candidate.label}\t{candidate.action}" for candidate in candidates
    )
    digest = sha256(candidate_text.encode("utf-8")).hexdigest()[:10]
    return f"public_demo_{source_id}_{topic}_{as_of[:10]}_{digest}"


def _scored_candidate_from_jepa_row(
    row: dict[str, Any], *, fallback_index: int
) -> PublicDemoScoredCandidate:
    business = WhatIfBusinessOutcomeHeads.model_validate(
        row.get("business_heads") or {}
    )
    future = WhatIfFutureStateHeads.model_validate(row.get("future_state_heads") or {})
    score = float(
        row.get("strategic_usefulness_score") or row.get("balanced_ceo_score") or 0.0
    )
    return PublicDemoScoredCandidate(
        candidate_id=str(row.get("candidate_id") or f"candidate_{fallback_index}"),
        rank=int(row.get("strategic_rank") or fallback_index),
        label=str(row.get("label") or f"Candidate {fallback_index}"),
        action=str(row.get("action") or ""),
        score=score,
        predicted_business_heads=business,
        predicted_future_heads=future,
        reason="",
        source="live_jepa",
    )


def _historical_cutoff(as_of: str) -> str:
    return f"Only evidence dated on or before {as_of[:10]} is visible."


__all__ = ["register_public_demo_routes"]
