#!/usr/bin/env python3
"""Refresh factual Enron headline metrics in docs/EVALS.md from metrics_card.json."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--card",
        type=Path,
        default=Path("data/enron/reference_backend/metrics_card.json"),
    )
    parser.add_argument(
        "--doc",
        type=Path,
        default=Path("docs/EVALS.md"),
    )
    args = parser.parse_args()

    card = json.loads(args.card.read_text(encoding="utf-8"))
    auroc = float(card["factual_next_event_auroc"])
    brier = float(card["factual_next_event_brier"])
    ece = float(card["calibration_ece"])

    text = args.doc.read_text(encoding="utf-8")
    pattern = (
        r"(AUROC `)([0-9.]+)(`,\s*Brier `)([0-9.]+)"
        r"(`,\s*and calibration ECE `)([0-9.]+)(`\.)"
    )
    replacement = rf"\g<1>{auroc:g}\g<3>{brier:g}\g<5>{ece:g}\g<7>"
    new_text, n = re.subn(pattern, replacement, text, count=1)
    if n != 1:
        raise SystemExit(
            "Could not find factual metrics sentence to replace in EVALS.md"
        )
    args.doc.write_text(new_text, encoding="utf-8")
    print(json.dumps({"updated": str(args.doc), "from": str(args.card)}, indent=2))


if __name__ == "__main__":
    raise SystemExit(main())
