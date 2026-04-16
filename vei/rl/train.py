from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Sequence

from .policy_frequency import FrequencyPolicy


class BehaviorCloningTrainer:
    def __init__(self, dataset_paths: Sequence[Path]) -> None:
        self.paths = [Path(p) for p in dataset_paths]

    def load(self) -> list[dict]:
        records: list[dict] = []
        for path in self.paths:
            data = json.loads(Path(path).read_text(encoding="utf-8"))
            for event in data.get("events", []):
                if event.get("channel") == "tool" and event.get("type"):
                    records.append(event)
        return records

    def train(self) -> FrequencyPolicy:
        records = self.load()
        counts: Counter[str] = Counter()
        templates: dict[str, dict] = {}
        for record in records:
            tool = str(record.get("type"))
            if not tool:
                continue
            counts[tool] += 1
            payload = record.get("payload", {})
            args = payload.get("args") if isinstance(payload, dict) else None
            if tool not in templates and isinstance(args, dict):
                templates[tool] = dict(args)
        return FrequencyPolicy(tool_counts=dict(counts), arg_templates=templates)
