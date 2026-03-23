from __future__ import annotations

from typing import Dict

from .base import ContextProvider


def get_provider(name: str) -> ContextProvider:
    registry = _build_registry()
    if name not in registry:
        raise KeyError(f"unknown context provider: {name}")
    return registry[name]


def list_providers() -> list[str]:
    return sorted(_build_registry().keys())


def _build_registry() -> Dict[str, ContextProvider]:
    from .google import GoogleContextProvider
    from .jira import JiraContextProvider
    from .okta import OktaContextProvider
    from .slack import SlackContextProvider

    return {
        "slack": SlackContextProvider(),
        "jira": JiraContextProvider(),
        "google": GoogleContextProvider(),
        "okta": OktaContextProvider(),
    }
