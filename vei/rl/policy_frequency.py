"""Tool-frequency heuristic policy for floor baselines.

This is NOT a learned dynamics model.  It counts which tools occurred in
demonstration data, stores the first argument template it saw, and the
resulting policy chooses the highest-frequency tool from the current menu.

Formerly named ``policy_bc`` / ``BCPPolicy``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from .arg_helpers import default_args_for


@dataclass
class FrequencyPolicy:
    """Floor-baseline policy: pick the most-frequent tool from demonstrations."""

    tool_counts: Dict[str, int] = field(default_factory=dict)
    arg_templates: Dict[str, Dict[str, object]] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return sum(self.tool_counts.values()) or 1

    def plan(self, menu: List[Dict[str, object]]) -> Optional[Dict[str, object]]:
        if not menu:
            return None
        best_tool: Optional[str] = None
        best_args: Dict[str, object] = {}
        best_prob = -1.0
        for item in menu:
            tool = str(item.get("tool")) if item.get("tool") else None
            if not tool:
                continue
            count = self.tool_counts.get(tool, 0)
            prob = count / self.total
            if prob <= 0 and best_tool is not None:
                continue
            args = default_args_for(item)
            template = self.arg_templates.get(tool)
            if template:
                args.update(template)
            if prob > best_prob or best_tool is None:
                best_prob = prob
                best_tool = tool
                best_args = args
        if best_tool is None:
            first = menu[0]
            tool = str(first.get("tool", "vei.observe"))
            args = default_args_for(first)
            template = self.arg_templates.get(tool)
            if template:
                args.update(template)
            return {"tool": tool, "args": args}
        return {"tool": best_tool, "args": best_args}

    def save(self, path: Path) -> None:
        data = {"tool_counts": self.tool_counts, "arg_templates": self.arg_templates}
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> "FrequencyPolicy":
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            tool_counts={k: int(v) for k, v in data.get("tool_counts", {}).items()},
            arg_templates={
                k: dict(v) for k, v in data.get("arg_templates", {}).items()
            },
        )


def run_policy(
    router, policy: FrequencyPolicy, max_steps: int = 20  # type: ignore[type-arg]
) -> List[Dict[str, object]]:
    transcript: List[Dict[str, object]] = []
    for _ in range(max_steps):
        obs = router.observe()
        obs_dict = obs.model_dump()
        transcript.append({"observation": obs_dict})
        menu = obs_dict.get("action_menu", [])
        action = policy.plan(menu)
        if not action:
            break
        result = router.call_and_step(action["tool"], action.get("args", {}))
        transcript.append({"action": action, "result": result})
        pending = router.pending()
        if pending.get("mail", 0) == 0 and pending.get("slack", 0) == 0:
            break
    return transcript
