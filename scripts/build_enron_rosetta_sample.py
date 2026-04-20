#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from vei.whatif._benchmark_case_packs import BENCHMARK_CASE_PACKS
from vei.whatif._enron_dataset import (
    repo_enron_sample_rosetta_dir,
    require_full_enron_rosetta_dir,
    resolve_cached_full_enron_rosetta_dir,
)

DEFAULT_CASE_IDS = tuple(
    seed.case_id
    for seed in BENCHMARK_CASE_PACKS["enron_business_outcome_v1"]
    if seed.event_id
)


def _parse_args() -> argparse.Namespace:
    default_input_dir = (
        resolve_cached_full_enron_rosetta_dir() or repo_enron_sample_rosetta_dir()
    )
    parser = argparse.ArgumentParser(
        description="Build the checked-in Enron Rosetta sample from the full archive."
    )
    parser.add_argument(
        "--input-rosetta-dir",
        type=Path,
        default=default_input_dir,
        help="Full Enron Rosetta parquet directory.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=repo_enron_sample_rosetta_dir(),
        help="Sample output directory.",
    )
    parser.add_argument(
        "--case-id",
        action="append",
        default=[],
        help="Optional case id to include. Repeat to override the default case set.",
    )
    return parser.parse_args()


def _selected_event_ids(case_ids: list[str]) -> dict[str, str]:
    seeds = {
        seed.case_id: seed.event_id
        for seed in BENCHMARK_CASE_PACKS["enron_business_outcome_v1"]
        if seed.event_id
    }
    result: dict[str, str] = {}
    for case_id in case_ids:
        event_id = seeds.get(case_id)
        if event_id:
            result[case_id] = event_id
    if result:
        return result
    missing = ", ".join(case_ids)
    raise RuntimeError(f"no Enron benchmark seeds resolved for: {missing}")


def _load_thread_ids(input_rosetta_dir: Path, event_ids: list[str]) -> dict[str, str]:
    metadata_path = input_rosetta_dir / "enron_rosetta_events_metadata.parquet"
    rows = pq.read_table(
        metadata_path,
        columns=["event_id", "thread_task_id"],
        filters=[("event_id", "in", event_ids)],
    ).to_pylist()
    return {
        str(row["event_id"]): str(row["thread_task_id"])
        for row in rows
        if str(row.get("event_id") or "").strip()
        and str(row.get("thread_task_id") or "").strip()
    }


def _string_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _actor_ids(metadata_rows: list[dict[str, object]]) -> set[str]:
    result: set[str] = set()
    for row in metadata_rows:
        actor_id = str(row.get("actor_id") or "").strip()
        if actor_id and not actor_id.startswith("group:"):
            result.add(actor_id)
        target_id = str(row.get("target_id") or "").strip()
        if target_id and not target_id.startswith("group:"):
            result.add(target_id)
        try:
            artifacts = json.loads(str(row.get("artifacts") or "{}"))
        except json.JSONDecodeError:
            artifacts = {}
        for recipient in _string_list(artifacts.get("to_recipients")):
            result.add(recipient)
        for recipient in _string_list(artifacts.get("cc_recipients")):
            result.add(recipient)
    return result


def _write_csv_preview(table: pa.Table, target_path: Path, *, limit: int = 25) -> None:
    rows = table.slice(0, max(0, limit)).to_pylist()
    if not rows:
        target_path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with target_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_rosetta_sample(
    *,
    input_rosetta_dir: Path,
    output_dir: Path,
    case_ids: list[str],
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    selected_event_ids = _selected_event_ids(case_ids)
    event_to_thread = _load_thread_ids(
        input_rosetta_dir,
        list(selected_event_ids.values()),
    )
    thread_ids = sorted(
        {
            event_to_thread[event_id]
            for event_id in selected_event_ids.values()
            if event_id in event_to_thread
        }
    )
    metadata_path = input_rosetta_dir / "enron_rosetta_events_metadata.parquet"
    metadata_table = pq.read_table(
        metadata_path,
        filters=[("thread_task_id", "in", thread_ids)],
    )
    metadata_rows = metadata_table.to_pylist()
    content_event_ids = sorted(
        {
            str(row["event_id"])
            for row in metadata_rows
            if str(row.get("event_id") or "").strip()
        }
    )
    content_table = pq.read_table(
        input_rosetta_dir / "enron_rosetta_events_content.parquet",
        filters=[("event_id", "in", content_event_ids)],
    )
    talk_actor_ids = sorted(_actor_ids(metadata_rows))
    talk_edges_table = pq.read_table(
        input_rosetta_dir / "enron_talk_graph_edges.parquet",
        filters=[
            [
                ("src_actor_id", "in", talk_actor_ids),
            ],
            [
                ("dst_actor_id", "in", talk_actor_ids),
            ],
        ],
    )
    work_transitions_table = pq.read_table(
        input_rosetta_dir / "enron_work_graph_transitions.parquet",
        filters=[("thread_task_id", "in", thread_ids)],
    )
    work_edges_table = pq.read_table(
        input_rosetta_dir / "enron_work_graph_edges.parquet"
    )

    pq.write_table(
        metadata_table,
        output_dir / "enron_rosetta_events_metadata.parquet",
        compression="zstd",
    )
    pq.write_table(
        content_table,
        output_dir / "enron_rosetta_events_content.parquet",
        compression="zstd",
    )
    pq.write_table(
        talk_edges_table,
        output_dir / "enron_talk_graph_edges.parquet",
        compression="zstd",
    )
    pq.write_table(
        work_transitions_table,
        output_dir / "enron_work_graph_transitions.parquet",
        compression="zstd",
    )
    pq.write_table(
        work_edges_table,
        output_dir / "enron_work_graph_edges.parquet",
        compression="zstd",
    )

    schema_path = input_rosetta_dir / "enron_rosetta_schema.json"
    if schema_path.exists():
        (output_dir / schema_path.name).write_text(
            schema_path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )

    _write_csv_preview(
        metadata_table,
        output_dir / "enron_rosetta_events_metadata_sample.csv",
    )
    _write_csv_preview(
        talk_edges_table,
        output_dir / "enron_talk_graph_edges_sample.csv",
    )
    _write_csv_preview(
        work_transitions_table,
        output_dir / "enron_work_graph_transitions_sample.csv",
    )

    marker_payload = {
        "dataset_kind": "sample",
        "version": "v1",
        "selected_case_ids": sorted(selected_event_ids),
        "selected_thread_ids": thread_ids,
        "event_count": metadata_table.num_rows,
        "content_count": content_table.num_rows,
        "talk_edge_count": talk_edges_table.num_rows,
        "work_transition_count": work_transitions_table.num_rows,
        "work_edge_count": work_edges_table.num_rows,
    }
    (output_dir / "enron_rosetta_dataset.json").write_text(
        json.dumps(marker_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    (output_dir / "enron_rosetta_summary.md").write_text(
        "\n".join(
            [
                "# Enron Rosetta Sample",
                "",
                "This checked-in sample carries the benchmark anchor threads that power the saved Enron bundles and the proof-case smoke tests.",
                "",
                f"- Selected case ids: {', '.join(sorted(selected_event_ids))}",
                f"- Selected thread ids: {', '.join(thread_ids)}",
                f"- Event rows: {metadata_table.num_rows}",
                f"- Content rows: {content_table.num_rows}",
                f"- Talk graph edges: {talk_edges_table.num_rows}",
                f"- Work graph transitions: {work_transitions_table.num_rows}",
                f"- Work graph edges: {work_edges_table.num_rows}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return marker_payload


def main() -> None:
    args = _parse_args()
    selected_case_ids = args.case_id or list(DEFAULT_CASE_IDS)
    input_rosetta_dir = require_full_enron_rosetta_dir(
        args.input_rosetta_dir.expanduser().resolve(),
        purpose="sample build",
    )
    payload = build_rosetta_sample(
        input_rosetta_dir=input_rosetta_dir,
        output_dir=args.output_dir.expanduser().resolve(),
        case_ids=selected_case_ids,
    )
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
