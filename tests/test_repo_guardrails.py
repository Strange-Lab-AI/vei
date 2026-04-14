from __future__ import annotations

from pathlib import Path


def test_hotspot_modules_stay_below_foundation_size_budget() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    budgets = {
        "vei/context/normalize.py": 117,
        "vei/whatif/benchmark.py": 2875,
        "vei/whatif/research.py": 2039,
        "vei/cli/vei_whatif.py": 1122,
        "vei/whatif/episode/_snapshot.py": 1234,
        "vei/twin/_runtime.py": 1200,
        "vei/twin/api.py": 1329,
        "vei/router/core.py": 1200,
        "vei/sdk/api.py": 1295,
        "vei/workspace/api.py": 1200,
    }

    over_budget: list[str] = []
    for relative_path, limit in budgets.items():
        path = repo_root / relative_path
        line_count = sum(1 for _ in path.open("r", encoding="utf-8"))
        if line_count > limit:
            over_budget.append(
                f"{relative_path} has {line_count} lines (limit {limit})"
            )

    assert not over_budget, " | ".join(over_budget)
