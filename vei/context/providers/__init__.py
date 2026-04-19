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
    from .clickup import ClickUpContextProvider
    from .gmail import GmailContextProvider
    from .github import GitHubContextProvider
    from .granola import GranolaContextProvider
    from .gitlab import GitLabContextProvider
    from .google import GoogleContextProvider
    from .jira import JiraContextProvider
    from .linear import LinearContextProvider
    from .notion import NotionContextProvider
    from .okta import OktaContextProvider
    from .slack import SlackContextProvider
    from .teams import TeamsContextProvider

    return {
        "slack": SlackContextProvider(),
        "jira": JiraContextProvider(),
        "google": GoogleContextProvider(),
        "okta": OktaContextProvider(),
        "gmail": GmailContextProvider(),
        "teams": TeamsContextProvider(),
        "notion": NotionContextProvider(),
        "linear": LinearContextProvider(),
        "granola": GranolaContextProvider(),
        "github": GitHubContextProvider(),
        "gitlab": GitLabContextProvider(),
        "clickup": ClickUpContextProvider(),
    }
