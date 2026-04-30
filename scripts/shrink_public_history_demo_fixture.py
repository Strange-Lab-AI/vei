#!/usr/bin/env python3
"""Downsample the checked-in public history demo ``context_snapshot.json``."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workspace",
        type=Path,
        default=Path("docs/examples/news-public-history-demo/workspace"),
    )
    parser.add_argument(
        "--max-documents",
        type=int,
        default=280,
        help="Caps documents under sources[0].data.documents.",
    )
    args = parser.parse_args()
    root = args.workspace.expanduser().resolve()
    snapshot = root / "context_snapshot.json"
    data = json.loads(snapshot.read_text(encoding="utf-8"))
    sources = data.get("sources")
    if not isinstance(sources, list) or not sources:
        raise SystemExit("context_snapshot missing sources array")
    first = sources[0]
    src_data = first.get("data") if isinstance(first, dict) else None
    if not isinstance(src_data, dict):
        raise SystemExit("source missing data dict")
    documents = src_data.get("documents")
    if not isinstance(documents, list):
        raise SystemExit("documents must be a list")

    kept = documents[: args.max_documents]
    src_data["documents"] = kept
    rc = first.setdefault("record_counts", {})
    if isinstance(rc, dict):
        dc = rc.setdefault("documents", {})
        if isinstance(dc, dict):
            dc["kept_for_demo"] = len(kept)

    snapshot.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps({"path": str(snapshot), "document_count": len(kept)}, indent=2),
    )


if __name__ == "__main__":
    raise SystemExit(main())
