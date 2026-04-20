#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path

import pyarrow.parquet as pq

from vei.whatif._enron_dataset import require_full_enron_rosetta_dir
from vei.whatif.api import resolve_whatif_rosetta_dir

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / "_vei_out" / "rosetta_archive_verdict.json"
EXPECTED_METADATA_COLUMNS = {
    "event_id",
    "timestamp",
    "actor_id",
    "target_id",
    "event_type",
    "thread_task_id",
    "artifacts",
}
EXPECTED_CONTENT_COLUMNS = {"event_id", "content"}
EXPECTED_EVENT_IDS = (
    "enron_bcda1b925800af8c",
    "enron_7e7afce27432edae",
    "enron_0a8a8985b6ae0d47",
    "enron_e2e504e2ff9e60de",
    "enron_2407d1c23ac89a9d",
    "enron_405ee04fb4ce3ff4",
    "enron_ab19d817c2d17b52",
    "enron_543752207e4316e1",
    "enron_466a009e2ef0589f",
    "enron_19d89fb317f5a309",
)


def main() -> None:
    rosetta_dir = resolve_rosetta_dir()
    metadata_path = rosetta_dir / "enron_rosetta_events_metadata.parquet"
    content_path = rosetta_dir / "enron_rosetta_events_content.parquet"

    metadata_table = pq.read_table(metadata_path)
    content_table = pq.read_table(content_path)

    metadata_columns = set(metadata_table.column_names)
    content_columns = set(content_table.column_names)
    missing_metadata_columns = sorted(EXPECTED_METADATA_COLUMNS - metadata_columns)
    missing_content_columns = sorted(EXPECTED_CONTENT_COLUMNS - content_columns)
    if missing_metadata_columns:
        raise SystemExit(
            f"metadata parquet is missing columns: {', '.join(missing_metadata_columns)}"
        )
    if missing_content_columns:
        raise SystemExit(
            f"content parquet is missing columns: {', '.join(missing_content_columns)}"
        )

    metadata_rows = metadata_table.num_rows
    content_rows = content_table.num_rows
    if metadata_rows != content_rows:
        raise SystemExit(
            "metadata and content parquet row counts do not match: "
            f"{metadata_rows} vs {content_rows}"
        )

    resolved_events = pq.read_table(
        metadata_path,
        columns=["event_id", "timestamp", "actor_id"],
        filters=[("event_id", "in", list(EXPECTED_EVENT_IDS))],
    ).to_pylist()
    found_event_ids = {str(row["event_id"]) for row in resolved_events}
    missing_event_ids = sorted(set(EXPECTED_EVENT_IDS) - found_event_ids)
    if missing_event_ids:
        raise SystemExit(
            "expected benchmark case event ids are missing: "
            + ", ".join(missing_event_ids)
        )

    verdict = {
        "status": "ok",
        "rosetta_dir": str(rosetta_dir),
        "metadata_path": str(metadata_path),
        "content_path": str(content_path),
        "metadata_rows": metadata_rows,
        "content_rows": content_rows,
        "metadata_size_bytes": metadata_path.stat().st_size,
        "content_size_bytes": content_path.stat().st_size,
        "metadata_columns": sorted(metadata_columns),
        "content_columns": sorted(content_columns),
        "verified_event_ids": [
            {
                "event_id": row["event_id"],
                "timestamp": _json_timestamp(row["timestamp"]),
                "actor_id": row["actor_id"],
            }
            for row in sorted(resolved_events, key=lambda item: str(item["event_id"]))
        ],
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(verdict, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(verdict, indent=2))


def resolve_rosetta_dir() -> Path:
    configured = os.environ.get("VEI_WHATIF_ROSETTA_DIR", "").strip()
    candidate = Path(configured).expanduser() if configured else resolve_whatif_rosetta_dir(ROOT)
    if candidate is None:
        raise SystemExit(
            "could not find an Enron Rosetta dataset. "
            "Run `make fetch-enron-full` or set VEI_WHATIF_ROSETTA_DIR."
        )
    try:
        return require_full_enron_rosetta_dir(candidate, purpose="archive validation")
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc


def _json_timestamp(value: object) -> str:
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return str(value)


if __name__ == "__main__":
    main()
