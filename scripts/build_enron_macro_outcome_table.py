from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

from vei.context.api import load_enron_public_context, slice_public_context_to_branch
from vei.whatif.api import score_historical_tail
from vei.whatif.cases import assign_case_ids
from vei.whatif.corpus._enron import ENRON_DOMAIN, build_event
from vei.whatif._benchmark_case_packs import (
    BENCHMARK_CASE_PACKS,
    DEFAULT_BENCHMARK_PACK_ID,
)

DEFAULT_OUTPUT_PATH = Path("data/enron/macro_outcome_rows.jsonl")


def _load_rosetta_events(rosetta_dir: Path):
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - guarded by extras
        raise RuntimeError("pyarrow is required for macro outcome table builds") from exc

    metadata_path = rosetta_dir / "enron_rosetta_events_metadata.parquet"
    rows = pq.read_table(
        metadata_path,
        columns=[
            "event_id",
            "timestamp",
            "actor_id",
            "target_id",
            "event_type",
            "thread_task_id",
            "artifacts",
        ],
    ).to_pylist()
    events = [event for event in (build_event(row, "") for row in rows) if event is not None]
    events.sort(key=lambda item: (item.timestamp_ms, item.event_id))
    return assign_case_ids(events)


def _benchmark_seed_event_ids() -> set[str]:
    return {
        seed.event_id
        for seed in BENCHMARK_CASE_PACKS[DEFAULT_BENCHMARK_PACK_ID]
        if seed.event_id
    }


def _group_by_thread(events) -> dict[str, list]:
    grouped: dict[str, list] = defaultdict(list)
    for event in events:
        grouped[event.thread_id].append(event)
    return grouped


def _eligible_sample_events(
    grouped_events: dict[str, list],
    *,
    excluded_event_ids: set[str],
    limit: int = 48,
) -> list[str]:
    candidate_ids: list[str] = []
    thread_ids = sorted(grouped_events)
    if not thread_ids:
        return []
    step = max(1, len(thread_ids) // max(1, limit * 3))
    for thread_id in thread_ids[::step]:
        timeline = grouped_events[thread_id]
        if len(timeline) < 4:
            continue
        branch_index = max(1, min(len(timeline) // 2, len(timeline) - 2))
        branch_event = timeline[branch_index]
        if branch_event.event_id in excluded_event_ids:
            continue
        if branch_event.timestamp[:4] not in {"1998", "1999", "2000", "2001"}:
            continue
        candidate_ids.append(branch_event.event_id)
        if len(candidate_ids) >= limit:
            break
    return candidate_ids


def _macro_row(
    *,
    split: str,
    branch_event,
    future_events,
    public_context,
) -> dict[str, object]:
    historical = score_historical_tail(
        future_events,
        organization_domain=ENRON_DOMAIN,
        branch_timestamp=branch_event.timestamp,
        public_context=public_context,
    )
    return {
        "row_id": f"{split}:{branch_event.thread_id}:{branch_event.event_id}",
        "split": split,
        "case_id": branch_event.case_id,
        "thread_id": branch_event.thread_id,
        "event_id": branch_event.event_id,
        "branch_timestamp": branch_event.timestamp,
        "actor_id": branch_event.actor_id,
        "subject": branch_event.subject,
        "future_event_count": historical.future_event_count,
        "future_external_event_count": historical.future_external_event_count,
        "future_escalation_count": historical.future_escalation_count,
        "proxy_risk_score": historical.risk_score,
        "stock_return_5d": historical.stock_return_5d,
        "credit_action_30d": historical.credit_action_30d,
        "ferc_action_180d": historical.ferc_action_180d,
    }


def build_macro_outcome_rows(
    *,
    rosetta_dir: Path,
) -> list[dict[str, object]]:
    events = _load_rosetta_events(rosetta_dir)
    grouped_events = _group_by_thread(events)
    full_context = load_enron_public_context()
    seed_event_ids = _benchmark_seed_event_ids()
    sample_event_ids = _eligible_sample_events(
        grouped_events,
        excluded_event_ids=seed_event_ids,
    )
    selected_event_ids = [(event_id, "heldout") for event_id in sorted(seed_event_ids)]
    selected_event_ids.extend((event_id, "sample") for event_id in sample_event_ids)

    rows: list[dict[str, object]] = []
    event_by_id = {event.event_id: event for event in events}
    for event_id, split in selected_event_ids:
        branch_event = event_by_id.get(event_id)
        if branch_event is None:
            continue
        timeline = grouped_events.get(branch_event.thread_id, [])
        branch_index = next(
            (index for index, event in enumerate(timeline) if event.event_id == event_id),
            None,
        )
        if branch_index is None:
            continue
        future_events = timeline[branch_index:]
        public_context = slice_public_context_to_branch(
            full_context,
            branch_timestamp=branch_event.timestamp,
        )
        rows.append(
            _macro_row(
                split=split,
                branch_event=branch_event,
                future_events=future_events,
                public_context=public_context,
            )
        )
    rows.sort(key=lambda item: (str(item["branch_timestamp"]), str(item["event_id"])))
    return rows


def write_macro_outcome_rows(
    *,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    rosetta_dir: Path = Path("data/enron/rosetta"),
) -> Path:
    rows = build_macro_outcome_rows(rosetta_dir=rosetta_dir.resolve())
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the tracked Enron macro outcome supervision table."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Path to write the macro outcome rows JSONL file.",
    )
    parser.add_argument(
        "--rosetta-dir",
        type=Path,
        default=Path("data/enron/rosetta"),
        help="Repo-local Rosetta parquet directory.",
    )
    args = parser.parse_args()
    output_path = write_macro_outcome_rows(
        output_path=args.output.resolve(),
        rosetta_dir=args.rosetta_dir.resolve(),
    )
    print(f"wrote: {output_path}")


if __name__ == "__main__":
    main()
