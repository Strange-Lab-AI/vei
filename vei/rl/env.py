from __future__ import annotations

import re
from typing import Any, Dict, Tuple

try:
    import gymnasium as gym
    from gymnasium import spaces
except Exception as _e:  # pragma: no cover - optional extra
    gym = None  # type: ignore[assignment]
    spaces = None  # type: ignore[assignment]

from vei.contract.api import ContractSpec
from vei.world.api import create_world_session


class VEIEnv:  # Gymnasium-compatible but avoids hard dependency at import time
    """Gymnasium-style wrapper around the VEI Router.

    Supports two reward modes:

    1. **Contract-driven** (default when a ``contract`` is provided):
       Rewards are derived from ``ContractSpec.reward_terms``, terminal
       conditions from ``success_predicates`` and ``forbidden_predicates``.
       Works for any scenario that ships a contract.

    2. **Legacy** (backward-compatible procurement demo):
       Hardcoded subgoals (browser_read, email_sent, approval, email_parsed).
       Activated when no contract is provided.
    """

    metadata = {"render_modes": []}

    def __init__(
        self,
        seed: int = 42042,
        reward_mode: str = "sparse",
        contract: ContractSpec | None = None,
    ) -> None:
        self.seed_value = int(seed)
        self.reward_mode = reward_mode
        self.contract = contract
        self.session = create_world_session(seed=self.seed_value, artifacts_dir=None)
        self.router = self.session.router

        if spaces is not None:
            self.observation_space = spaces.Dict({})
            self.action_space = spaces.Dict(
                {
                    "tool": spaces.Text(min_length=1, max_length=128),
                    "args": spaces.Dict({}),
                }
            )
        else:  # pragma: no cover
            self.observation_space = None  # type: ignore[assignment]
            self.action_space = None  # type: ignore[assignment]

        # Legacy subgoal flags (used when no contract)
        self._saw_browser_read = False
        self._sent_email = False
        self._saw_approval = False
        self._email_parsed = False

        # Contract-driven tracking
        self._term_flags: Dict[str, bool] = {}
        if self.contract:
            for term in self.contract.reward_terms:
                self._term_flags[term.name] = False

        self.steps = 0
        self.elapsed_ms = 0

    def reset(
        self, *, seed: int | None = None, options: Dict[str, Any] | None = None
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        if seed is not None:
            self.seed_value = int(seed)
        self.session = create_world_session(seed=self.seed_value, artifacts_dir=None)
        self.router = self.session.router
        self._saw_browser_read = False
        self._sent_email = False
        self._saw_approval = False
        self._email_parsed = False
        self._term_flags = {k: False for k in self._term_flags}
        self.steps = 0
        self.elapsed_ms = 0
        obs = self.session.observe()
        return obs, {}

    def step(
        self, action: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], float, bool, bool, Dict[str, Any]]:
        tool = action.get("tool", "vei.observe")
        args = action.get("args", {})
        data = self.session.act_and_observe(tool, args)
        obs = data["observation"]

        self.steps += 1
        self.elapsed_ms = self.router.bus.clock_ms

        if self.contract:
            return self._step_contract(obs, tool)
        return self._step_legacy(obs, tool)

    # ------------------------------------------------------------------
    # Contract-driven rewards
    # ------------------------------------------------------------------

    def _step_contract(
        self, obs: Dict[str, Any], tool: str
    ) -> Tuple[Dict[str, Any], float, bool, bool, Dict[str, Any]]:
        assert self.contract is not None

        oracle_state = self.session.observe(focus_hint="summary")
        pending = self.router.pending()

        from vei.contract.api import evaluate_contract

        evaluation = evaluate_contract(
            self.contract,
            oracle_state=oracle_state,
            visible_observation=obs,
            pending=pending,
            time_ms=self.elapsed_ms,
        )

        # Compute reward from reward_terms
        reward = 0.0
        for term in self.contract.reward_terms:
            if term.term_type == "success":
                passed = evaluation.success_predicates_passed
                total = evaluation.success_predicate_count
                if total > 0:
                    frac = passed / total
                    reward += term.weight * frac
            elif term.term_type == "penalty":
                failed = evaluation.forbidden_predicates_failed
                if failed > 0:
                    reward -= term.weight * failed

        # Step cost penalty
        reward -= 0.01 * self.steps + 1e-5 * self.elapsed_ms

        # Terminal conditions
        all_success = (
            evaluation.success_predicates_passed == evaluation.success_predicate_count
            and evaluation.success_predicate_count > 0
        )
        any_forbidden = evaluation.forbidden_predicates_failed > 0
        terminated = all_success or any_forbidden

        info: Dict[str, Any] = {
            "contract": {
                "ok": evaluation.ok,
                "success_passed": evaluation.success_predicates_passed,
                "success_total": evaluation.success_predicate_count,
                "forbidden_failed": evaluation.forbidden_predicates_failed,
                "invariants_failed": evaluation.policy_invariants_failed,
            },
            "costs": {"actions": self.steps, "time_ms": self.elapsed_ms},
        }
        return obs, float(reward), bool(terminated), False, info

    # ------------------------------------------------------------------
    # Legacy hardcoded rewards (procurement demo, backward compat)
    # ------------------------------------------------------------------

    def _step_legacy(
        self, obs: Dict[str, Any], tool: str
    ) -> Tuple[Dict[str, Any], float, bool, bool, Dict[str, Any]]:
        self._update_subgoals_from_last(tool)
        self._update_subgoals_from_trace()

        reward = self._compute_legacy_reward()
        terminated = self._email_parsed
        pend = self.router.pending()
        no_pending = (pend.get("mail", 0) == 0) and (pend.get("slack", 0) == 0)
        if no_pending and self._sent_email:
            terminated = True

        info = {
            "subgoals": {
                "citations": int(self._saw_browser_read),
                "approval": int(self._saw_approval),
                "email_sent": int(self._sent_email),
                "email_parsed": int(self._email_parsed),
            },
            "costs": {"actions": self.steps, "time_ms": self.elapsed_ms},
        }
        return obs, float(reward), bool(terminated), False, info

    def _compute_legacy_reward(self) -> float:
        if self.reward_mode == "dense":
            base = 0.25 * (
                int(self._saw_browser_read)
                + int(self._saw_approval)
                + int(self._sent_email)
                + int(self._email_parsed)
            )
        else:
            base = 1.0 if self._email_parsed else 0.0
        penalty = 0.01 * self.steps + 1e-5 * self.elapsed_ms
        return base - penalty

    def _update_subgoals_from_last(self, tool: str) -> None:
        if tool == "browser.read":
            self._saw_browser_read = True
        if tool == "mail.compose":
            self._sent_email = True

    def _update_subgoals_from_trace(self) -> None:
        price_ok = False
        eta_ok = False
        for rec in self.router.trace.entries[-50:]:
            if rec.get("type") == "event":
                tgt = rec.get("target")
                payload = rec.get("payload", {})
                text = str(payload.get("text", ""))
                if tgt == "slack":
                    if (":white_check_mark:" in text) or ("approved" in text.lower()):
                        self._saw_approval = True
                if tgt == "mail":
                    body = str(payload.get("body_text", ""))
                    if not body:
                        continue
                    if _has_price(body):
                        price_ok = True
                    if _has_eta(body):
                        eta_ok = True
        self._email_parsed = self._email_parsed or (price_ok and eta_ok)


_PRICE_RE = re.compile(
    r"\b(?:price|total)\s*(?::|-)\s*(?:USD|US\$|\$)?\s*([0-9][0-9,]*(?:\.[0-9]{2})?)",
    re.I,
)
_ETA_RE = re.compile(r"\beta\s*(?::|-)\s*([^\n]+)", re.I)


def _has_price(text: str) -> bool:
    return bool(_PRICE_RE.search(text))


def _has_eta(text: str) -> bool:
    return bool(_ETA_RE.search(text))
