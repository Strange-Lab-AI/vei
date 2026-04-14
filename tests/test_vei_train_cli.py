from __future__ import annotations

import json
from pathlib import Path

import pytest

from vei.cli.vei_train import bc

pytestmark = pytest.mark.integration


def test_bc_cli_loads_dataset(tmp_path: Path) -> None:
    dataset = {
        "events": [
            {
                "time_ms": 1,
                "actor_id": "agent",
                "channel": "tool",
                "type": "slack.send_message",
                "payload": {"args": {}},
            }
        ]
    }
    path = tmp_path / "dataset.json"
    path.write_text(json.dumps(dataset), encoding="utf-8")
    output = tmp_path / "policy.json"
    bc(dataset=[str(path)], output=output)
    data = json.loads(output.read_text(encoding="utf-8"))
    assert data["tool_counts"]["slack.send_message"] == 1
