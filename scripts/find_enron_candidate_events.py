from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search the vendored Enron Rosetta archive for candidate branch events."
    )
    parser.add_argument(
        "--rosetta-dir",
        type=Path,
        default=Path("data/enron/rosetta"),
        help="Rosetta archive directory.",
    )
    parser.add_argument(
        "--actor",
        action="append",
        default=[],
        help="Actor email filter. Repeat for multiple actors.",
    )
    parser.add_argument(
        "--target",
        action="append",
        default=[],
        help="Target email or thread filter. Repeat for multiple targets.",
    )
    parser.add_argument("--from-ts", default="", help="Inclusive ISO start timestamp.")
    parser.add_argument("--to-ts", default="", help="Inclusive ISO end timestamp.")
    parser.add_argument(
        "--subject-contains",
        action="append",
        default=[],
        help="Case-insensitive subject substring. Repeat for multiple required fragments.",
    )
    parser.add_argument(
        "--body-contains",
        action="append",
        default=[],
        help="Case-insensitive body substring. Repeat for multiple required fragments.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum number of matches to print.",
    )
    return parser.parse_args()


def _load_metadata(rosetta_dir: Path) -> list[dict[str, Any]]:
    table = pq.read_table(
        rosetta_dir / "enron_rosetta_events_metadata.parquet",
        columns=[
            "event_id",
            "timestamp",
            "actor_id",
            "target_id",
            "event_type",
            "thread_task_id",
            "artifacts",
        ],
    )
    rows = table.to_pylist()
    for row in rows:
        artifacts = row.pop("artifacts", "{}")
        try:
            row["artifacts"] = json.loads(artifacts or "{}")
        except json.JSONDecodeError:
            row["artifacts"] = {}
    return rows


def _load_content_map(rosetta_dir: Path) -> dict[str, str]:
    table = pq.read_table(
        rosetta_dir / "enron_rosetta_events_content.parquet",
        columns=["event_id", "content"],
    )
    return {
        str(row["event_id"]): str(row.get("content") or "")
        for row in table.to_pylist()
    }


def _matches_text(text: str, required_parts: list[str]) -> bool:
    haystack = text.lower()
    return all(part in haystack for part in required_parts)


def _normalize_text_values(values: list[str]) -> list[str]:
    return [value.strip().lower() for value in values if value.strip()]


def _timestamp_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    return text.replace(" ", "T").replace("+00:00", "Z")


def _matches_range(timestamp_text: str, *, from_ts: str, to_ts: str) -> bool:
    if from_ts and timestamp_text < from_ts:
        return False
    if to_ts and timestamp_text > to_ts:
        return False
    return True


def main() -> int:
    args = _parse_args()
    rosetta_dir = args.rosetta_dir.expanduser().resolve()
    metadata_rows = _load_metadata(rosetta_dir)
    actor_filters = _normalize_text_values(args.actor)
    target_filters = _normalize_text_values(args.target)
    subject_filters = _normalize_text_values(args.subject_contains)
    body_filters = _normalize_text_values(args.body_contains)
    content_map = _load_content_map(rosetta_dir) if body_filters else {}

    matches: list[dict[str, Any]] = []
    for row in metadata_rows:
        actor_id = str(row.get("actor_id") or "").strip().lower()
        target_id = str(row.get("target_id") or "").strip().lower()
        timestamp_text = _timestamp_text(row.get("timestamp"))
        if actor_filters and actor_id not in actor_filters:
            continue
        if target_filters and target_id not in target_filters:
            continue
        if not _matches_range(
            timestamp_text,
            from_ts=str(args.from_ts or "").strip(),
            to_ts=str(args.to_ts or "").strip(),
        ):
            continue

        artifacts = row.get("artifacts") or {}
        subject = str(artifacts.get("subject") or "").strip()
        snippet = content_map.get(str(row["event_id"]), "")
        if subject_filters and not _matches_text(subject, subject_filters):
            continue
        if body_filters and not _matches_text(snippet, body_filters):
            continue

        matches.append(
            {
                "event_id": row["event_id"],
                "timestamp": timestamp_text,
                "actor_id": row["actor_id"],
                "target_id": row["target_id"],
                "event_type": row["event_type"],
                "thread_task_id": row["thread_task_id"],
                "subject": subject,
                "snippet": snippet[:400],
                "to_recipients": list(artifacts.get("to_recipients") or []),
                "cc_recipients": list(artifacts.get("cc_recipients") or []),
                "custodian_id": str(artifacts.get("custodian_id") or ""),
                "folder": str(artifacts.get("folder") or ""),
                "message_id": str(artifacts.get("message_id") or ""),
            }
        )

    matches.sort(
        key=lambda row: (row["timestamp"], row["event_id"]),
    )
    payload = {
        "rosetta_dir": str(rosetta_dir),
        "returned_count": min(len(matches), max(1, args.limit)),
        "total_matches": len(matches),
        "matches": matches[: max(1, args.limit)],
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
