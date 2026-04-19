from __future__ import annotations

import argparse
import json
from pathlib import Path

from vei.context.api import (
    build_canonical_history_readiness,
    canonical_history_paths,
    load_canonical_history_bundle,
)
from vei.dynamics.feed.canonical_feed import build_samples_from_events

_GAP_THRESHOLD_MS = 7 * 24 * 60 * 60 * 1000


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check whether a file-backed company-history bundle is ready for world-model work."
    )
    parser.add_argument(
        "--root",
        type=Path,
        required=True,
        help="Workspace root or context snapshot path with canonical history sidecars.",
    )
    parser.add_argument(
        "--output",
        default="-",
        help="Output path or stdout when '-' is used.",
    )
    args = parser.parse_args()

    payload = build_report(args.root)
    text = json.dumps(payload, indent=2)
    if args.output == "-":
        print(text)
        return 0

    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text + "\n", encoding="utf-8")
    print(output_path)
    return 0


def build_report(root: Path) -> dict[str, object]:
    resolved_root = root.expanduser().resolve()
    readiness = build_canonical_history_readiness(resolved_root)
    bundle = load_canonical_history_bundle(resolved_root)
    paths = canonical_history_paths(resolved_root)

    if bundle is None:
        return {
            "root": str(resolved_root),
            "available": False,
            "snapshot_path": str(paths.snapshot_path),
            "events_path": str(paths.events_path),
            "index_path": str(paths.index_path),
            "notes": ["canonical history sidecars are missing"],
        }

    events = list(bundle.events)
    holdout = _holdout_next_kind_metrics(events)
    coverage_gaps = _coverage_gaps(events)
    ready_now = bool(
        readiness.ready_for_world_modeling
        and holdout["enough_samples"]
        and float(holdout["top1_next_kind_accuracy"]) >= 0.2
    )

    return {
        "root": str(resolved_root),
        "available": True,
        "snapshot_path": str(paths.snapshot_path),
        "events_path": str(paths.events_path),
        "index_path": str(paths.index_path),
        "readiness": readiness.model_dump(mode="json"),
        "coverage_gaps": coverage_gaps,
        "holdout_next_event": holdout,
        "ready_for_learned_world_model": ready_now,
        "notes": _report_notes(readiness.notes, holdout, coverage_gaps, ready_now),
    }


def _holdout_next_kind_metrics(events) -> dict[str, object]:
    if len(events) < 25:
        return {
            "enough_samples": False,
            "window_size": 0,
            "train_sample_count": 0,
            "holdout_sample_count": 0,
            "top1_next_kind_accuracy": 0.0,
            "global_next_kind_accuracy": 0.0,
            "note": "need at least 25 dated events for a simple holdout check",
        }

    window_size = min(12, max(4, len(events) // 20))
    samples = build_samples_from_events(
        events,
        window_size=window_size,
        horizon=1,
        tenant_id=events[0].tenant_id or "tenant",
    )
    if len(samples) < 10:
        return {
            "enough_samples": False,
            "window_size": window_size,
            "train_sample_count": 0,
            "holdout_sample_count": 0,
            "top1_next_kind_accuracy": 0.0,
            "global_next_kind_accuracy": 0.0,
            "note": "need at least 10 training windows for a stable holdout check",
        }

    split_index = max(5, int(len(samples) * 0.8))
    if split_index >= len(samples):
        split_index = len(samples) - 1
    train_samples = samples[:split_index]
    holdout_samples = samples[split_index:]
    if not holdout_samples:
        return {
            "enough_samples": False,
            "window_size": window_size,
            "train_sample_count": len(train_samples),
            "holdout_sample_count": 0,
            "top1_next_kind_accuracy": 0.0,
            "global_next_kind_accuracy": 0.0,
            "note": "holdout split produced no evaluation rows",
        }

    next_kind_counts: dict[str, int] = {}
    transition_counts: dict[str, dict[str, int]] = {}
    for sample in train_samples:
        branch_kind = str(sample.candidate_action.get("kind") or "")
        next_kind = sample.next_events[0].kind
        next_kind_counts[next_kind] = next_kind_counts.get(next_kind, 0) + 1
        if branch_kind not in transition_counts:
            transition_counts[branch_kind] = {}
        transition_counts[branch_kind][next_kind] = (
            transition_counts[branch_kind].get(next_kind, 0) + 1
        )

    fallback_next_kind = max(
        next_kind_counts.items(),
        key=lambda item: (item[1], item[0]),
    )[0]

    branch_correct = 0
    global_correct = 0
    for sample in holdout_samples:
        branch_kind = str(sample.candidate_action.get("kind") or "")
        actual_next_kind = sample.next_events[0].kind
        predicted_next_kind = _predict_next_kind(
            branch_kind=branch_kind,
            transition_counts=transition_counts,
            fallback_next_kind=fallback_next_kind,
        )
        if predicted_next_kind == actual_next_kind:
            branch_correct += 1
        if fallback_next_kind == actual_next_kind:
            global_correct += 1

    holdout_count = len(holdout_samples)
    return {
        "enough_samples": True,
        "window_size": window_size,
        "train_sample_count": len(train_samples),
        "holdout_sample_count": holdout_count,
        "top1_next_kind_accuracy": round(branch_correct / holdout_count, 6),
        "global_next_kind_accuracy": round(global_correct / holdout_count, 6),
        "fallback_next_kind": fallback_next_kind,
    }


def _predict_next_kind(
    *,
    branch_kind: str,
    transition_counts: dict[str, dict[str, int]],
    fallback_next_kind: str,
) -> str:
    branch_counts = transition_counts.get(branch_kind)
    if not branch_counts:
        return fallback_next_kind
    return max(branch_counts.items(), key=lambda item: (item[1], item[0]))[0]


def _coverage_gaps(events) -> list[dict[str, object]]:
    gaps: list[dict[str, object]] = []
    for previous, current in zip(events, events[1:]):
        gap_ms = current.ts_ms - previous.ts_ms
        if gap_ms < _GAP_THRESHOLD_MS:
            continue
        gaps.append(
            {
                "gap_days": round(gap_ms / (24 * 60 * 60 * 1000), 2),
                "from_event_id": previous.event_id,
                "to_event_id": current.event_id,
                "from_ts_ms": previous.ts_ms,
                "to_ts_ms": current.ts_ms,
            }
        )
    gaps.sort(key=lambda item: float(item["gap_days"]), reverse=True)
    return gaps[:5]


def _report_notes(
    readiness_notes: list[str],
    holdout: dict[str, object],
    coverage_gaps: list[dict[str, object]],
    ready_now: bool,
) -> list[str]:
    notes = list(readiness_notes)
    if coverage_gaps:
        notes.append("there are long gaps in the dated activity history")
    if not bool(holdout.get("enough_samples")):
        notes.append(str(holdout.get("note") or "holdout check is incomplete"))
    if ready_now:
        notes.append(
            "tenant history is strong enough for exploratory learned world-model runs"
        )
    else:
        notes.append(
            "tenant history still needs more depth before treating learned results as strong evidence"
        )
    return notes


if __name__ == "__main__":
    raise SystemExit(main())
