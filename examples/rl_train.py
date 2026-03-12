"""
Train a simple random policy using VEIEnv.

Usage:
  python examples/rl_train.py --episodes 5 --max-steps 20 --out-dir _vei_out/rl_run
"""
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
import sys
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from vei.rl.env import VEIEnv
from vei.score_core import compute_score


def random_policy(obs: dict[str, Any]) -> dict[str, Any]:
    menu = obs.get("action_menu") or []
    candidates: list[dict[str, Any]] = []
    for m in menu:
        tool = m.get("tool", "vei.observe")
        if m.get("args"):
            candidates.append({"tool": tool, "args": m.get("args", {})})
        else:
            schema = m.get("args_schema") or {}
            if all(str(v).endswith("?") for v in schema.values()):
                candidates.append({"tool": tool, "args": {}})
    if not candidates:
        return {"tool": "vei.observe", "args": {}}
    return random.choice(candidates)


def run_episode(env: VEIEnv, policy: Callable[[dict[str, Any]], dict[str, Any]], max_steps: int) -> float:
    obs, _ = env.reset()
    total_reward = 0.0
    for _ in range(max_steps):
        action = policy(obs)
        obs, reward, terminated, _, _ = env.step(action)
        total_reward += reward
        if terminated:
            break
    return total_reward


def main() -> None:
    parser = argparse.ArgumentParser(description="Train a simple policy in VEIEnv")
    parser.add_argument("--episodes", type=int, default=5, help="number of training episodes")
    parser.add_argument("--max-steps", type=int, default=20, help="max steps per episode")
    parser.add_argument("--out-dir", default="_vei_out/rl_run", help="directory for evaluation trace")
    args = parser.parse_args()

    env = VEIEnv()
    for ep in range(args.episodes):
        ret = run_episode(env, random_policy, args.max_steps)
        print(f"episode {ep}: reward={ret}")

    # Evaluate policy once and write trace for compute_score
    eval_env = VEIEnv()
    obs, _ = eval_env.reset()
    for _ in range(args.max_steps):
        action = random_policy(obs)
        obs, _r, done, _, _ = eval_env.step(action)
        if done:
            break

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    trace_path = out_dir / "trace.jsonl"
    with trace_path.open("w", encoding="utf-8") as f:
        for entry in eval_env.router.trace.entries:
            f.write(json.dumps(entry) + "\n")

    score = compute_score(out_dir)
    print(f"score: {score}")


if __name__ == "__main__":
    main()
