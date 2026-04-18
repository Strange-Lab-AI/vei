"""Canonical training-data feed.

Reads CanonicalEvent streams from one or many tenants and emits training
samples of the form:
    (graph_slice, recent_events, candidate_action, next_events,
     state_delta, business_heads)

Text bodies are NOT the primitive — only structured fields plus optional
text_handle with tenant opt-in.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from vei.events.api import CanonicalEvent


class TrainingSample(BaseModel):
    """One training sample for the dynamics model."""

    sample_id: str = ""
    tenant_id: str = ""
    graph_slice: Dict[str, Any] = Field(default_factory=dict)
    recent_events: List[CanonicalEvent] = Field(default_factory=list)
    candidate_action: Dict[str, Any] = Field(default_factory=dict)
    next_events: List[CanonicalEvent] = Field(default_factory=list)
    state_delta: Dict[str, Any] = Field(default_factory=dict)
    business_heads: Dict[str, float] = Field(default_factory=dict)
    provenance: Dict[str, Any] = Field(default_factory=dict)


def build_samples_from_events(
    events: List[CanonicalEvent],
    *,
    window_size: int = 10,
    horizon: int = 5,
    tenant_id: str = "",
) -> List[TrainingSample]:
    """Build training samples from a sequence of canonical events.

    Slides a window of ``window_size`` events, uses the next ``horizon``
    events as the target.
    """
    samples: List[TrainingSample] = []
    for i in range(len(events) - window_size - horizon + 1):
        recent = events[i : i + window_size]
        future = events[i + window_size : i + window_size + horizon]
        sample = TrainingSample(
            sample_id=f"{tenant_id}_{i:06d}",
            tenant_id=tenant_id,
            recent_events=recent,
            next_events=future,
            provenance={"tenant_id": tenant_id, "window_start": i},
        )
        samples.append(sample)
    return samples


def emit_feed(
    events_by_tenant: Dict[str, List[CanonicalEvent]],
    *,
    output_path: Path,
    split: str = "train",
    window_size: int = 10,
    horizon: int = 5,
) -> int:
    """Write a schema-versioned training feed to disk as JSONL."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with output_path.open("w", encoding="utf-8") as fh:
        for tenant_id, events in events_by_tenant.items():
            samples = build_samples_from_events(
                events,
                window_size=window_size,
                horizon=horizon,
                tenant_id=tenant_id,
            )
            for sample in samples:
                fh.write(sample.model_dump_json() + "\n")
                total += 1
    return total
