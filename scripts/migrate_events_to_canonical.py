#!/usr/bin/env python3
"""Migrate legacy events.jsonl (StateStore format) to CanonicalEvent v1 JSONL.

Reads each line from a legacy events.jsonl file produced by
``vei.world.state.StateStore``, converts it to a ``CanonicalEvent`` v1
envelope via ``vei.events.legacy``, and writes one JSON object per line to
the output file.

Usage::

    python scripts/migrate_events_to_canonical.py \
        --input  _vei_out/state/main/events.jsonl \
        --output _vei_out/canonical_events.jsonl

    # Optional round-trip validation (reads output back and checks
    # that event_id and ts_ms survive the conversion):
    python scripts/migrate_events_to_canonical.py \
        --input  _vei_out/state/main/events.jsonl \
        --output _vei_out/canonical_events.jsonl \
        --validate
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from vei.events.legacy import iter_legacy_events_jsonl
from vei.events.models import CanonicalEvent


def _convert(input_path: Path, output_path: Path, tenant_id: str) -> tuple[int, int]:
    """Stream-convert *input_path* and write canonical JSONL to *output_path*.

    Returns ``(read_count, written_count)``.
    """
    read_count = 0
    written_count = 0
    with output_path.open("w", encoding="utf-8") as out:
        for event in iter_legacy_events_jsonl(input_path, tenant_id=tenant_id):
            read_count += 1
            line = event.model_dump_json()
            out.write(line + "\n")
            written_count += 1
    return read_count, written_count


def _validate(input_path: Path, output_path: Path, tenant_id: str) -> list[str]:
    """Re-read the output and verify event_id / ts_ms match the source."""
    source_ids: list[tuple[str, int]] = []
    with input_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            raw = json.loads(line)
            source_ids.append((str(raw.get("event_id", "")), int(raw.get("clock_ms", 0))))

    output_ids: list[tuple[str, int]] = []
    with output_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            ce = CanonicalEvent.model_validate_json(line)
            output_ids.append((ce.event_id, ce.ts_ms))

    errors: list[str] = []
    if len(source_ids) != len(output_ids):
        errors.append(
            f"event count mismatch: source={len(source_ids)}, output={len(output_ids)}"
        )
        return errors

    for idx, (src, out) in enumerate(zip(source_ids, output_ids)):
        src_id, src_ts = src
        out_id, out_ts = out
        if src_id != out_id:
            errors.append(f"row {idx}: event_id mismatch: {src_id!r} != {out_id!r}")
        if src_ts != out_ts:
            errors.append(f"row {idx}: ts_ms mismatch: {src_ts} != {out_ts}")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Convert legacy StateStore events.jsonl to CanonicalEvent v1 JSONL.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to the legacy events.jsonl file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Destination path for canonical JSONL output.",
    )
    parser.add_argument(
        "--tenant-id",
        default="",
        help="Optional tenant identifier to stamp onto every CanonicalEvent.",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="After writing, re-read output and verify event_id/ts_ms round-trip.",
    )
    args = parser.parse_args(argv)

    input_path: Path = args.input.expanduser().resolve()
    output_path: Path = args.output.expanduser().resolve()

    if not input_path.exists():
        print(f"error: input file not found: {input_path}", file=sys.stderr)
        return 1

    output_path.parent.mkdir(parents=True, exist_ok=True)

    read_count, written_count = _convert(input_path, output_path, args.tenant_id)

    print(f"read:      {read_count}")
    print(f"converted: {read_count}")
    print(f"written:   {written_count}")

    if args.validate:
        errors = _validate(input_path, output_path, args.tenant_id)
        if errors:
            print(f"\nvalidation FAILED ({len(errors)} issues):", file=sys.stderr)
            for err in errors:
                print(f"  - {err}", file=sys.stderr)
            return 1
        print("\nvalidation: ok (event_id and ts_ms preserved)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
