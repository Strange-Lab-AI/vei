"""Actor registry and orchestration for hybrid simulation.

The ActorRegistry manages a set of actors (personas + backends) and
provides a unified interface for the WorldSession event loop to route
messages through actors.
"""

from __future__ import annotations

import logging
from typing import Any

from vei.actors.backends import (
    ActorBackend,
    DeterministicActorBackend,
    LLMActorBackend,
)
from vei.actors.persona import ActorPersona

logger = logging.getLogger(__name__)


class ActorRegistry:
    """Registry of simulated actors in the enterprise world.

    Maps actor identifiers (email, name) to personas and backends.
    Routes incoming messages to the appropriate actor and returns
    their response.
    """

    def __init__(self) -> None:
        self._actors: dict[str, ActorPersona] = {}
        self._backends: dict[str, ActorBackend] = {}
        self._deterministic = DeterministicActorBackend()
        self._llm_backend: LLMActorBackend | None = None
        self._event_log: list[dict[str, Any]] = []

    def register(self, persona: ActorPersona) -> None:
        key = self._normalize_key(persona.email or persona.name)
        self._actors[key] = persona
        if persona.backend == "llm":
            if self._llm_backend is None:
                self._llm_backend = LLMActorBackend()
            self._backends[key] = self._llm_backend
        else:
            self._backends[key] = self._deterministic

    def configure_llm(
        self,
        provider: str = "openai",
        model: str = "gpt-4o-mini",
        max_tokens: int = 50000,
        max_cost_usd: float = 1.0,
    ) -> None:
        self._llm_backend = LLMActorBackend(
            provider=provider,
            model=model,
            max_tokens_per_run=max_tokens,
            max_cost_usd=max_cost_usd,
        )
        for key, persona in self._actors.items():
            if persona.backend == "llm":
                self._backends[key] = self._llm_backend

    def route_message(
        self,
        target: str,
        message: str,
        *,
        channel: str = "",
        world_context: str = "",
    ) -> str | None:
        """Route a message to an actor and get their response.

        Returns None if no actor is registered for the target.
        """
        key = self._normalize_key(target)
        persona = self._actors.get(key)
        if persona is None:
            return None

        backend = self._backends.get(key, self._deterministic)
        response = backend.respond(
            persona,
            message,
            world_context=world_context,
            channel=channel,
        )

        self._event_log.append(
            {
                "actor": persona.name,
                "target": target,
                "channel": channel,
                "message_preview": message[:100],
                "response_preview": response[:100],
                "backend": persona.backend,
            }
        )

        logger.info(
            "actor_responded",
            extra={
                "actor": persona.name,
                "channel": channel,
                "backend": persona.backend,
                "response_length": len(response),
            },
        )
        return response

    def list_actors(self) -> list[dict[str, Any]]:
        return [
            {
                "name": p.name,
                "email": p.email,
                "role": p.role,
                "department": p.department,
                "backend": p.backend,
                "response_bias": p.response_bias,
            }
            for p in self._actors.values()
        ]

    def event_log(self) -> list[dict[str, Any]]:
        return list(self._event_log)

    def usage_summary(self) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "total_actors": len(self._actors),
            "llm_actors": sum(1 for p in self._actors.values() if p.backend == "llm"),
            "deterministic_actors": sum(
                1 for p in self._actors.values() if p.backend == "deterministic"
            ),
            "total_interactions": len(self._event_log),
        }
        if self._llm_backend:
            summary["llm_usage"] = self._llm_backend.usage_summary()
        return summary

    def _normalize_key(self, value: str) -> str:
        return value.strip().lower()
