from __future__ import annotations

import importlib
from pathlib import Path


def test_hotspot_modules_stay_below_foundation_size_budget() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    budgets = {
        "vei/context/normalize.py": 117,
        "vei/context/hydrate.py": 660,
        "vei/whatif/benchmark.py": 2700,
        "vei/whatif/research.py": 1810,
        "vei/cli/vei_whatif.py": 1122,
        "vei/whatif/episode/_snapshot.py": 1234,
        "vei/ui/_workspace_routes.py": 1040,
        "vei/ui/_api_models.py": 400,
        "vei/twin/_runtime.py": 1200,
        "vei/twin/api.py": 1350,
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


def test_public_api_modules_do_not_export_private_symbols() -> None:
    for module_name in ("vei.whatif.api", "vei.whatif.episode"):
        module = importlib.import_module(module_name)
        exported = getattr(module, "__all__", [])
        private = [name for name in exported if str(name).startswith("_")]
        assert not private, f"{module_name} exports private symbols: {private}"
