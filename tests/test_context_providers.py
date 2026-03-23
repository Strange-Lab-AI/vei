from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from vei.context.models import ContextProviderConfig
from vei.context.providers import get_provider, list_providers
from vei.context.providers.google import GoogleContextProvider
from vei.context.providers.jira import JiraContextProvider
from vei.context.providers.okta import OktaContextProvider
from vei.context.providers.slack import SlackContextProvider


def _mock_urlopen(payload: Any):
    """Return a context-manager mock whose read() yields JSON bytes."""
    body = json.dumps(payload).encode()
    resp = MagicMock()
    resp.read.return_value = body
    resp.headers = {}
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def test_list_providers_returns_all_four() -> None:
    names = list_providers()
    assert "slack" in names
    assert "jira" in names
    assert "google" in names
    assert "okta" in names


def test_get_provider_unknown_raises() -> None:
    with pytest.raises(KeyError, match="unknown"):
        get_provider("nonexistent")


def test_slack_provider_captures_channels(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VEI_SLACK_TOKEN", "xoxb-test-token")

    channels_resp = {
        "ok": True,
        "channels": [
            {"id": "C01", "name": "general", "unread_count": 2},
            {"id": "C02", "name": "random", "unread_count": 0},
        ],
    }
    history_resp = {
        "ok": True,
        "messages": [
            {"ts": "1710000000.000100", "user": "U01", "text": "hello"},
        ],
    }
    users_resp = {
        "ok": True,
        "members": [
            {
                "id": "U01",
                "name": "alice",
                "real_name": "Alice",
                "is_bot": False,
                "deleted": False,
            },
        ],
    }

    call_count = {"n": 0}

    def side_effect(*_args, **_kwargs):
        call_count["n"] += 1
        n = call_count["n"]
        if n <= 1:
            return _mock_urlopen(channels_resp)
        if n <= 3:
            return _mock_urlopen(history_resp)
        return _mock_urlopen(users_resp)

    with patch("vei.context.providers.base.urlopen", side_effect=side_effect):
        provider = SlackContextProvider()
        result = provider.capture(
            ContextProviderConfig(provider="slack", token_env="VEI_SLACK_TOKEN")
        )

    assert result.status == "ok"
    assert result.provider == "slack"
    assert result.record_counts["channels"] == 2
    assert result.record_counts["users"] == 1
    assert len(result.data["channels"]) == 2


def test_jira_provider_captures_issues(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VEI_JIRA_TOKEN", "test-jira-token")

    search_resp = {
        "issues": [
            {
                "key": "PROJ-1",
                "fields": {
                    "summary": "Fix login bug",
                    "status": {"name": "Open"},
                    "assignee": {"displayName": "Bob"},
                    "description": {
                        "type": "doc",
                        "content": [
                            {
                                "type": "paragraph",
                                "content": [{"type": "text", "text": "Login fails"}],
                            }
                        ],
                    },
                    "issuetype": {"name": "Bug"},
                    "priority": {"name": "High"},
                    "updated": "2024-03-10T00:00:00Z",
                },
            }
        ]
    }
    projects_resp = [
        {"key": "PROJ", "name": "My Project", "style": "classic"},
    ]

    call_count = {"n": 0}

    def side_effect(*_args, **_kwargs):
        call_count["n"] += 1
        if call_count["n"] <= 1:
            return _mock_urlopen(search_resp)
        return _mock_urlopen(projects_resp)

    with patch("vei.context.providers.base.urlopen", side_effect=side_effect):
        provider = JiraContextProvider()
        result = provider.capture(
            ContextProviderConfig(
                provider="jira",
                token_env="VEI_JIRA_TOKEN",
                base_url="https://test.atlassian.net",
            )
        )

    assert result.status == "ok"
    assert result.record_counts["issues"] == 1
    assert result.data["issues"][0]["ticket_id"] == "PROJ-1"
    assert result.data["issues"][0]["description"] == "Login fails"


def test_google_provider_captures_users_and_docs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VEI_GOOGLE_TOKEN", "ya29.test-token")

    users_resp = {
        "users": [
            {
                "id": "G01",
                "primaryEmail": "alice@example.com",
                "name": {"givenName": "Alice", "familyName": "Smith"},
                "orgUnitPath": "/Engineering",
                "suspended": False,
                "isAdmin": True,
            }
        ]
    }
    files_resp = {
        "files": [
            {
                "id": "DOC1",
                "name": "Design Doc",
                "mimeType": "application/vnd.google-apps.document",
                "modifiedTime": "2024-03-10T00:00:00Z",
                "shared": True,
            }
        ]
    }

    call_count = {"n": 0}

    def side_effect(*_args, **_kwargs):
        call_count["n"] += 1
        if call_count["n"] <= 1:
            return _mock_urlopen(users_resp)
        return _mock_urlopen(files_resp)

    with patch("vei.context.providers.base.urlopen", side_effect=side_effect):
        provider = GoogleContextProvider()
        result = provider.capture(
            ContextProviderConfig(
                provider="google",
                token_env="VEI_GOOGLE_TOKEN",
            )
        )

    assert result.status == "ok"
    assert result.record_counts["users"] == 1
    assert result.record_counts["documents"] == 1
    assert result.data["users"][0]["email"] == "alice@example.com"


def test_okta_provider_reads_payload_before_tempdir_is_removed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("VEI_OKTA_TOKEN", "okta-test-token")

    def fake_sync_okta_import_package(destination_root: Path, _config: Any) -> Any:
        raw_root = Path(destination_root) / "raw"
        raw_root.mkdir(parents=True, exist_ok=True)
        (raw_root / "okta_users.json").write_text(
            json.dumps({"users": [{"id": "U1"}]}),
            encoding="utf-8",
        )
        (raw_root / "okta_groups.json").write_text(
            json.dumps({"groups": [{"id": "G1"}]}),
            encoding="utf-8",
        )
        (raw_root / "okta_apps.json").write_text(
            json.dumps({"applications": [{"id": "A1"}]}),
            encoding="utf-8",
        )
        return MagicMock(
            package_root=Path(destination_root),
            record_counts={"users": 1, "groups": 1, "applications": 1},
        )

    with patch(
        "vei.context.providers.okta.sync_okta_import_package",
        side_effect=fake_sync_okta_import_package,
    ):
        provider = OktaContextProvider()
        result = provider.capture(
            ContextProviderConfig(
                provider="okta",
                token_env="VEI_OKTA_TOKEN",
                base_url="https://example.okta.com",
            )
        )

    assert result.status == "ok"
    assert result.record_counts == {"users": 1, "groups": 1, "applications": 1}
    assert result.data["users"] == [{"id": "U1"}]
    assert result.data["groups"] == [{"id": "G1"}]
    assert result.data["applications"] == [{"id": "A1"}]
