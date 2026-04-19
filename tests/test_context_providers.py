from __future__ import annotations

import json
import logging
import zipfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from vei.context.api import capture_context
from vei.context.models import ContextProviderConfig
from vei.context.providers import get_provider, list_providers
from vei.context.providers.clickup import ClickUpContextProvider
from vei.context.providers.crm import capture_from_export as capture_crm_export
from vei.context.providers.gmail import GmailContextProvider
from vei.context.providers.github import GitHubContextProvider
from vei.context.providers.gitlab import GitLabContextProvider
from vei.context.providers.google import GoogleContextProvider
from vei.context.providers.google import capture_from_export as capture_google_export
from vei.context.providers.jira import JiraContextProvider
from vei.context.providers.okta import OktaContextProvider
from vei.context.providers.slack import SlackContextProvider
from vei.context.providers.teams import TeamsContextProvider


def _mock_urlopen(payload: Any):
    """Return a context-manager mock whose read() yields JSON bytes."""
    body = json.dumps(payload).encode()
    resp = MagicMock()
    resp.read.return_value = body
    resp.headers = {}
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def test_list_providers_returns_supported_context_sources() -> None:
    names = list_providers()
    for expected in (
        "slack",
        "jira",
        "google",
        "okta",
        "gmail",
        "teams",
        "github",
        "gitlab",
        "clickup",
        "notion",
    ):
        assert expected in names


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


def test_github_provider_captures_issues_and_pull_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VEI_GITHUB_TOKEN", "ghp-test-token")

    repository_resp = {
        "id": 1,
        "full_name": "acme/platform",
        "html_url": "https://github.com/acme/platform",
    }
    issues_resp = [
        {
            "id": 10,
            "number": 7,
            "title": "Fix LEGAL-7 rollover",
            "body": "Review blocker",
            "state": "open",
            "user": {"login": "maya"},
            "created_at": "2026-03-01T10:00:00Z",
            "updated_at": "2026-03-01T11:00:00Z",
            "comments": 1,
            "comments_url": "https://api.github.com/repos/acme/platform/issues/7/comments",
        },
        {
            "id": 11,
            "number": 8,
            "title": "Merge renewal controls",
            "body": "Implements contract hold",
            "state": "closed",
            "user": {"login": "riley"},
            "created_at": "2026-03-01T12:00:00Z",
            "updated_at": "2026-03-01T13:00:00Z",
            "comments": 0,
            "comments_url": "https://api.github.com/repos/acme/platform/issues/8/comments",
            "pull_request": {
                "url": "https://api.github.com/repos/acme/platform/pulls/8"
            },
        },
    ]
    comments_resp = [
        {
            "id": 99,
            "body": "Needs finance signoff.",
            "created_at": "2026-03-01T11:30:00Z",
            "user": {"login": "legal"},
        }
    ]

    call_count = {"n": 0}

    def side_effect(*_args, **_kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return _mock_urlopen(repository_resp)
        if call_count["n"] == 2:
            return _mock_urlopen(issues_resp)
        return _mock_urlopen(comments_resp)

    with patch("vei.context.providers.base.urlopen", side_effect=side_effect):
        provider = GitHubContextProvider()
        result = provider.capture(
            ContextProviderConfig(
                provider="github",
                token_env="VEI_GITHUB_TOKEN",
                filters={"repo": "acme/platform"},
            )
        )

    assert result.status == "ok"
    assert result.record_counts["repositories"] == 1
    assert result.record_counts["issues"] == 1
    assert result.record_counts["pull_requests"] == 1
    assert result.data["issues"][0]["comments"][0]["author"] == "legal"


def test_gitlab_provider_captures_issues_and_merge_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VEI_GITLAB_TOKEN", "glpat-test-token")

    project_resp = {
        "id": 22,
        "path_with_namespace": "acme/platform",
    }
    issues_resp = [
        {
            "id": 31,
            "iid": 5,
            "title": "Fix support incident",
            "description": "INC-9 follow-up",
            "state": "opened",
            "author": {"username": "maya"},
            "created_at": "2026-03-01T10:00:00Z",
            "updated_at": "2026-03-01T11:00:00Z",
            "_links": {
                "notes": "https://gitlab.example/api/v4/projects/22/issues/5/notes"
            },
        }
    ]
    merge_requests_resp = [
        {
            "id": 32,
            "iid": 6,
            "title": "Merge support fix",
            "description": "Implements INC-9 fix",
            "state": "merged",
            "author": {"username": "riley"},
            "created_at": "2026-03-01T12:00:00Z",
            "updated_at": "2026-03-01T13:00:00Z",
            "web_url": "https://gitlab.example/acme/platform/-/merge_requests/6",
            "_links": {
                "notes": "https://gitlab.example/api/v4/projects/22/merge_requests/6/notes"
            },
        }
    ]
    notes_resp = [
        {
            "id": 44,
            "body": "Loop finance into review.",
            "created_at": "2026-03-01T13:10:00Z",
            "author": {"username": "legal"},
        }
    ]

    call_count = {"n": 0}

    def side_effect(*_args, **_kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return _mock_urlopen(project_resp)
        if call_count["n"] == 2:
            return _mock_urlopen(issues_resp)
        if call_count["n"] == 3:
            return _mock_urlopen(merge_requests_resp)
        return _mock_urlopen(notes_resp)

    with patch("vei.context.providers.base.urlopen", side_effect=side_effect):
        provider = GitLabContextProvider()
        result = provider.capture(
            ContextProviderConfig(
                provider="gitlab",
                token_env="VEI_GITLAB_TOKEN",
                filters={"project": "acme/platform"},
            )
        )

    assert result.status == "ok"
    assert result.record_counts["projects"] == 1
    assert result.record_counts["issues"] == 1
    assert result.record_counts["merge_requests"] == 1
    assert result.data["merge_requests"][0]["comments"][0]["author"] == "legal"


def test_clickup_provider_captures_lists_and_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VEI_CLICKUP_TOKEN", "pk_test_clickup")

    list_resp = {
        "id": "list-1",
        "name": "Escalations",
    }
    tasks_resp = {
        "tasks": [
            {
                "id": "task-1",
                "name": "Review enterprise renewal",
                "description": "Need pricing signoff",
                "status": {"status": "in review"},
                "creator": {"email": "maya@acme.example.com"},
                "assignees": [{"email": "legal@acme.example.com"}],
                "date_created": "1772329200000",
                "date_updated": "1772329560000",
            }
        ]
    }

    call_count = {"n": 0}

    def side_effect(*_args, **_kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return _mock_urlopen(list_resp)
        return _mock_urlopen(tasks_resp)

    with patch("vei.context.providers.base.urlopen", side_effect=side_effect):
        provider = ClickUpContextProvider()
        result = provider.capture(
            ContextProviderConfig(
                provider="clickup",
                token_env="VEI_CLICKUP_TOKEN",
                filters={"list_id": "list-1"},
            )
        )

    assert result.status == "ok"
    assert result.record_counts["lists"] == 1
    assert result.record_counts["tasks"] == 1
    assert result.data["tasks"][0]["assignee"] == "legal@acme.example.com"


def test_google_export_directory_merges_docs_and_share_files(tmp_path: Path) -> None:
    (tmp_path / "google_docs.csv").write_text(
        "\n".join(
            [
                "doc_id,title,body,modified_time",
                "DOC-1,Renewal Plan,Need legal approval,2026-03-01T10:00:00Z",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "google_drive_shares.csv").write_text(
        "\n".join(
            [
                "doc_id,title,owner,visibility,classification,shared_with",
                "DOC-1,Renewal Plan,maya@acme.example.com,external,confidential,client@buyer.example.com;legal@acme.example.com",
            ]
        ),
        encoding="utf-8",
    )

    result = capture_google_export(tmp_path)

    assert result.status == "ok"
    assert result.record_counts["documents"] == 1
    assert result.record_counts["drive_shares"] == 1
    assert result.data["documents"][0]["doc_id"] == "DOC-1"
    assert result.data["drive_shares"][0]["owner"] == "maya@acme.example.com"
    assert result.data["drive_shares"][0]["shared_with"] == [
        "client@buyer.example.com",
        "legal@acme.example.com",
    ]


def test_capture_context_logs_warning_on_provider_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class ExplodingProvider:
        def capture(self, _config: ContextProviderConfig) -> Any:
            raise RuntimeError("boom")

    monkeypatch.setattr(
        "vei.context.api.get_provider", lambda _name: ExplodingProvider()
    )

    with caplog.at_level(logging.WARNING):
        snapshot = capture_context(
            [ContextProviderConfig(provider="slack", token_env="VEI_SLACK_TOKEN")],
            organization_name="Acme Cloud",
            organization_domain="acme.example.com",
        )

    assert snapshot.sources[0].status == "error"
    record = next(
        record
        for record in caplog.records
        if "context capture failed for slack" in record.getMessage()
    )
    assert getattr(record, "source") == "context_capture"
    assert getattr(record, "provider") == "slack"
    assert getattr(record, "exception_type") == "RuntimeError"


def test_crm_export_parse_failure_logs_warning(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    export_path = tmp_path / "crm.json"
    export_path.write_text("{bad json", encoding="utf-8")

    with caplog.at_level(logging.WARNING):
        result = capture_crm_export(export_path)

    assert result.status == "error"
    record = next(
        record
        for record in caplog.records
        if "context crm export parse failed" in record.getMessage()
    )
    assert getattr(record, "source") == "context_export"
    assert getattr(record, "provider") == "crm"
    assert getattr(record, "exception_type") == "JSONDecodeError"


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


# ---------------------------------------------------------------------------
# Slack export ingestion
# ---------------------------------------------------------------------------


def test_slack_export_captures_channels_and_users(tmp_path: Path) -> None:
    """Parsing a minimal Slack export directory produces correct counts."""
    from vei.context.providers.slack import capture_from_export

    users_json = [
        {
            "id": "U001",
            "name": "alice",
            "real_name": "Alice Smith",
            "profile": {"email": "alice@example.com"},
            "is_bot": False,
            "deleted": False,
        },
        {
            "id": "U002",
            "name": "bob",
            "real_name": "Bob Jones",
            "profile": {},
            "is_bot": False,
            "deleted": False,
        },
    ]
    channels_json = [
        {"id": "C001", "name": "general"},
        {"id": "C002", "name": "random"},
    ]
    general_day = [
        {"ts": "1700000001.000", "user": "U001", "text": "Hello world"},
        {"ts": "1700000002.000", "user": "U002", "text": "Hi Alice"},
        {
            "ts": "1700000003.000",
            "user": "U001",
            "text": "Let's ship it",
            "subtype": "channel_join",
        },
    ]
    random_day = [
        {"ts": "1700000010.000", "user": "U002", "text": "Random thought"},
    ]

    (tmp_path / "users.json").write_text(json.dumps(users_json))
    (tmp_path / "channels.json").write_text(json.dumps(channels_json))
    (tmp_path / "general").mkdir()
    (tmp_path / "general" / "2023-11-15.json").write_text(json.dumps(general_day))
    (tmp_path / "random").mkdir()
    (tmp_path / "random" / "2023-11-15.json").write_text(json.dumps(random_day))

    result = capture_from_export(tmp_path)

    assert result.status == "ok"
    assert result.record_counts["channels"] == 2
    assert result.record_counts["users"] == 2
    assert result.record_counts["messages"] == 3
    ch_general = next(c for c in result.data["channels"] if c["channel"] == "#general")
    assert len(ch_general["messages"]) == 2
    assert ch_general["messages"][0]["user"] == "alice"


def test_slack_export_channel_filter(tmp_path: Path) -> None:
    """Channel filter restricts which channels are ingested."""
    from vei.context.providers.slack import capture_from_export

    (tmp_path / "users.json").write_text("[]")
    (tmp_path / "channels.json").write_text(
        json.dumps(
            [
                {"id": "C1", "name": "general"},
                {"id": "C2", "name": "secret"},
            ]
        )
    )
    (tmp_path / "general").mkdir()
    (tmp_path / "general" / "2024-01-01.json").write_text(
        json.dumps(
            [
                {"ts": "1", "user": "U1", "text": "public"},
            ]
        )
    )
    (tmp_path / "secret").mkdir()
    (tmp_path / "secret" / "2024-01-01.json").write_text(
        json.dumps(
            [
                {"ts": "2", "user": "U1", "text": "private"},
            ]
        )
    )

    result = capture_from_export(tmp_path, channel_filter=["general"])

    assert result.record_counts["channels"] == 1
    names = [c["channel"] for c in result.data["channels"]]
    assert names == ["#general"]


def test_slack_export_api_integration(tmp_path: Path) -> None:
    """The top-level ingest_slack_export API produces a valid ContextSnapshot."""
    from vei.context.api import ingest_slack_export

    (tmp_path / "users.json").write_text(json.dumps([{"id": "U1", "name": "dev"}]))
    (tmp_path / "channels.json").write_text(json.dumps([{"id": "C1", "name": "eng"}]))
    (tmp_path / "eng").mkdir()
    (tmp_path / "eng" / "2024-06-01.json").write_text(
        json.dumps(
            [
                {"ts": "100", "user": "U1", "text": "shipped"},
            ]
        )
    )

    snap = ingest_slack_export(tmp_path, organization_name="TestCorp")

    assert snap.organization_name == "TestCorp"
    source = snap.source_for("slack")
    assert source is not None
    assert source.status == "ok"
    assert source.record_counts["messages"] == 1


# ---------------------------------------------------------------------------
# Gmail provider (API-based)
# ---------------------------------------------------------------------------


def test_gmail_provider_captures_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VEI_GMAIL_TOKEN", "ya29.gmail-test")

    threads_list = {
        "threads": [{"id": "t1"}, {"id": "t2"}],
    }
    thread_detail = {
        "id": "t1",
        "messages": [
            {
                "id": "m1",
                "labelIds": ["INBOX", "UNREAD"],
                "snippet": "Hi there",
                "payload": {
                    "headers": [
                        {"name": "From", "value": "alice@co.com"},
                        {"name": "To", "value": "bob@co.com"},
                        {"name": "Subject", "value": "Q4 Review"},
                        {"name": "Date", "value": "Mon, 10 Mar 2025 10:00:00 +0000"},
                    ],
                },
            },
        ],
    }
    profile_resp = {
        "emailAddress": "me@co.com",
        "threadsTotal": 42,
        "messagesTotal": 100,
    }

    call_count = {"n": 0}

    def side_effect(*_a, **_kw):
        call_count["n"] += 1
        n = call_count["n"]
        if n == 1:
            return _mock_urlopen(threads_list)
        if n <= 3:
            return _mock_urlopen(thread_detail)
        return _mock_urlopen(profile_resp)

    with patch("vei.context.providers.base.urlopen", side_effect=side_effect):
        provider = GmailContextProvider()
        result = provider.capture(
            ContextProviderConfig(provider="gmail", token_env="VEI_GMAIL_TOKEN")
        )

    assert result.status == "ok"
    assert result.provider == "gmail"
    assert result.record_counts["threads"] >= 1
    assert result.data["threads"][0]["subject"] == "Q4 Review"
    assert result.data["threads"][0]["messages"][0]["from"] == "alice@co.com"
    assert result.data["threads"][0]["messages"][0]["unread"] is True


# ---------------------------------------------------------------------------
# Teams provider (API-based)
# ---------------------------------------------------------------------------


def test_teams_provider_captures_channels(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VEI_TEAMS_TOKEN", "eyJ-teams-test")

    teams_resp = {
        "value": [
            {"id": "team-1", "displayName": "Engineering"},
        ],
    }
    channels_resp = {
        "value": [
            {"id": "ch-1", "displayName": "General"},
        ],
    }
    messages_resp = {
        "value": [
            {
                "id": "msg-1",
                "messageType": "message",
                "createdDateTime": "2025-03-10T10:00:00Z",
                "from": {"user": {"displayName": "Alice"}},
                "body": {"contentType": "text", "content": "Hello team!"},
            },
            {
                "id": "msg-2",
                "messageType": "systemEventMessage",
                "createdDateTime": "2025-03-10T09:00:00Z",
                "from": {},
                "body": {"contentType": "html", "content": "<p>System event</p>"},
            },
        ],
    }
    me_resp = {"mail": "me@company.com", "displayName": "Me"}

    call_count = {"n": 0}

    def side_effect(*_a, **_kw):
        call_count["n"] += 1
        n = call_count["n"]
        if n == 1:
            return _mock_urlopen(teams_resp)
        if n == 2:
            return _mock_urlopen(channels_resp)
        if n == 3:
            return _mock_urlopen(messages_resp)
        return _mock_urlopen(me_resp)

    with patch("vei.context.providers.base.urlopen", side_effect=side_effect):
        provider = TeamsContextProvider()
        result = provider.capture(
            ContextProviderConfig(provider="teams", token_env="VEI_TEAMS_TOKEN")
        )

    assert result.status == "ok"
    assert result.provider == "teams"
    assert result.record_counts["teams"] == 1
    assert result.record_counts["channels"] == 1
    assert result.record_counts["messages"] == 1
    ch = result.data["channels"][0]
    assert ch["channel"] == "#Engineering/General"
    assert ch["messages"][0]["user"] == "Alice"
    assert ch["messages"][0]["text"] == "Hello team!"


# ---------------------------------------------------------------------------
# Gmail MBOX offline ingestion
# ---------------------------------------------------------------------------


def _build_mbox_content(messages: list[dict[str, str]]) -> str:
    """Build a minimal MBOX string from a list of message dicts."""
    parts = []
    for msg in messages:
        from_line = (
            f"From {msg.get('from', 'sender@example.com')} Mon Mar 10 10:00:00 2025"
        )
        headers = (
            f"From: {msg.get('from', 'sender@example.com')}\n"
            f"To: {msg.get('to', 'receiver@example.com')}\n"
            f"Subject: {msg.get('subject', 'No subject')}\n"
            f"Date: Mon, 10 Mar 2025 10:00:00 +0000\n"
            f"Message-ID: {msg.get('message_id', '<test@example.com>')}\n"
        )
        if msg.get("in_reply_to"):
            headers += f"In-Reply-To: {msg['in_reply_to']}\n"
        if msg.get("references"):
            headers += f"References: {msg['references']}\n"
        body = msg.get("body", "Test body")
        parts.append(f"{from_line}\n{headers}\n{body}\n")
    return "\n".join(parts)


def test_gmail_mbox_captures_threads(tmp_path: Path) -> None:
    from vei.context.providers.gmail import capture_from_mbox

    mbox_data = _build_mbox_content(
        [
            {
                "from": "alice@co.com",
                "to": "bob@co.com",
                "subject": "Project Update",
                "message_id": "<msg1@co.com>",
                "body": "Here's the update.",
            },
            {
                "from": "bob@co.com",
                "to": "alice@co.com",
                "subject": "Re: Project Update",
                "message_id": "<msg2@co.com>",
                "in_reply_to": "<msg1@co.com>",
                "references": "<msg1@co.com>",
                "body": "Thanks for the update!",
            },
            {
                "from": "carol@co.com",
                "to": "team@co.com",
                "subject": "Separate topic",
                "message_id": "<msg3@co.com>",
                "body": "Unrelated message.",
            },
        ]
    )
    mbox_path = tmp_path / "All mail Including Spam and Trash.mbox"
    mbox_path.write_text(mbox_data, encoding="utf-8")

    result = capture_from_mbox(mbox_path)

    assert result.status == "ok"
    assert result.provider == "gmail"
    assert result.record_counts["messages"] == 3
    assert result.record_counts["threads"] == 2

    thread_subjects = {t["subject"] for t in result.data["threads"]}
    assert "Project Update" in thread_subjects
    assert "Separate topic" in thread_subjects


def test_gmail_mbox_missing_file(tmp_path: Path) -> None:
    from vei.context.providers.gmail import capture_from_mbox

    result = capture_from_mbox(tmp_path / "nonexistent.mbox")
    assert result.status == "error"


def test_gmail_mbox_api_integration(tmp_path: Path) -> None:
    from vei.context.api import ingest_gmail_export

    mbox_data = _build_mbox_content(
        [
            {
                "from": "a@co.com",
                "to": "b@co.com",
                "subject": "Hello",
                "message_id": "<x@co.com>",
                "body": "Content here",
            },
        ]
    )
    mbox_path = tmp_path / "mail.mbox"
    mbox_path.write_text(mbox_data, encoding="utf-8")

    snap = ingest_gmail_export(mbox_path, organization_name="TestCo")

    assert snap.organization_name == "TestCo"
    source = snap.source_for("gmail")
    assert source is not None
    assert source.status == "ok"
    assert source.record_counts["messages"] == 1


def test_gmail_provider_reads_takeout_zip_via_base_url(tmp_path: Path) -> None:
    mbox_data = _build_mbox_content(
        [
            {
                "from": "founder@dispatch.ai",
                "to": "team@dispatch.ai",
                "subject": "Weekly sync",
                "message_id": "<dispatch-1@dispatch.ai>",
                "body": "Notes from the week.",
            },
            {
                "from": "team@dispatch.ai",
                "to": "founder@dispatch.ai",
                "subject": "Re: Weekly sync",
                "message_id": "<dispatch-2@dispatch.ai>",
                "in_reply_to": "<dispatch-1@dispatch.ai>",
                "references": "<dispatch-1@dispatch.ai>",
                "body": "Follow-up actions.",
            },
        ]
    )
    export_root = tmp_path / "Takeout" / "Mail"
    export_root.mkdir(parents=True)
    (export_root / "All mail Including Spam and Trash.mbox").write_text(
        mbox_data,
        encoding="utf-8",
    )
    archive_path = tmp_path / "dispatch-gmail.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(
            export_root / "All mail Including Spam and Trash.mbox",
            arcname="Takeout/Mail/All mail Including Spam and Trash.mbox",
        )

    provider = GmailContextProvider()
    result = provider.capture(
        ContextProviderConfig(provider="gmail", base_url=str(archive_path), limit=10)
    )

    assert result.status == "ok"
    assert result.record_counts["messages"] == 2
    assert result.record_counts["threads"] == 1
    assert result.data["threads"][0]["subject"] == "Weekly sync"


def test_notion_provider_reads_nested_export_zip(tmp_path: Path) -> None:
    from vei.context.providers.notion import capture_from_export

    inner_root = tmp_path / "inner"
    notes_root = inner_root / "Private & Shared" / "Central Dispatch"
    notes_root.mkdir(parents=True)
    (notes_root / "Weekly priorities 74fdd6b1c536473aa670c3373f5e7f89.md").write_text(
        "\n".join(
            [
                "# Weekly priorities - 2024-05-06T16:30:00Z",
                "",
                "Owner: Zapier",
                "Last edited time: May 6, 2024 9:57 AM",
                "Created time: May 6, 2024 9:57 AM",
                "",
                "Transcript starts here.",
            ]
        ),
        encoding="utf-8",
    )
    (notes_root / "Internal Ops Tasks 3d140d68e2a842b582878efb0c8be893.csv").write_text(
        "\n".join(
            [
                "Name,Assign,Status",
                "Weekly cron for LLM reports based on GH,Robb Chen-Ware,Done",
                "New Demo Video,,Not started",
            ]
        ),
        encoding="utf-8",
    )

    part_zip = tmp_path / "ExportBlock-Part-1.zip"
    with zipfile.ZipFile(part_zip, "w") as archive:
        for file_path in sorted(inner_root.rglob("*")):
            if not file_path.is_file():
                continue
            archive.write(file_path, arcname=str(file_path.relative_to(inner_root)))

    middle_zip = tmp_path / "Dispatch-Export.zip"
    with zipfile.ZipFile(middle_zip, "w") as archive:
        archive.write(part_zip, arcname="ExportBlock-Part-1.zip")

    outer_zip = tmp_path / "dispatch-notion.zip"
    with zipfile.ZipFile(outer_zip, "w") as archive:
        archive.write(middle_zip, arcname="Dispatch-Export.zip")

    result = capture_from_export(outer_zip)

    assert result.status == "ok"
    assert result.record_counts["pages"] == 2
    page_titles = {page["title"] for page in result.data["pages"]}
    assert "Weekly priorities - 2024-05-06T16:30:00Z" in page_titles
    assert "Internal Ops Tasks" in page_titles

    meeting_page = next(
        page
        for page in result.data["pages"]
        if page["title"] == "Weekly priorities - 2024-05-06T16:30:00Z"
    )
    assert meeting_page["owner"] == "Zapier"
    assert meeting_page["updated"] == "May 6, 2024 9:57 AM"


# ---------------------------------------------------------------------------
# Hydration: Gmail -> mail threads, Teams -> comm graph
# ---------------------------------------------------------------------------


def test_hydration_maps_gmail_to_mail_threads() -> None:
    from vei.context.hydrate import hydrate_snapshot_to_blueprint
    from vei.context.models import ContextSnapshot, ContextSourceResult

    gmail_source = ContextSourceResult(
        provider="gmail",
        captured_at="2025-03-10T00:00:00Z",
        status="ok",
        record_counts={"threads": 1, "messages": 2},
        data={
            "threads": [
                {
                    "thread_id": "t1",
                    "subject": "Budget Review",
                    "messages": [
                        {
                            "from": "cfo@co.com",
                            "to": "team@co.com",
                            "subject": "Budget Review",
                            "snippet": "Please review the attached budget.",
                            "unread": True,
                            "labels": ["IMPORTANT"],
                        },
                        {
                            "from": "pm@co.com",
                            "to": "cfo@co.com",
                            "subject": "Re: Budget Review",
                            "snippet": "Looks good.",
                            "unread": False,
                            "labels": [],
                        },
                    ],
                },
            ],
            "profile": {},
        },
    )
    snapshot = ContextSnapshot(
        organization_name="TestCorp",
        captured_at="2025-03-10T00:00:00Z",
        sources=[gmail_source],
    )

    blueprint = hydrate_snapshot_to_blueprint(snapshot)

    assert blueprint.capability_graphs is not None
    comm = blueprint.capability_graphs.comm_graph
    assert comm is not None
    assert len(comm.mail_threads) == 1
    thread = comm.mail_threads[0]
    assert thread.thread_id == "t1"
    assert thread.title == "Budget Review"
    assert thread.category == "important"
    assert len(thread.messages) == 2
    assert thread.messages[0].from_address == "cfo@co.com"
    assert thread.messages[0].unread is True
    assert "mail" in blueprint.requested_facades


def test_hydration_maps_teams_to_comm_graph() -> None:
    from vei.context.hydrate import hydrate_snapshot_to_blueprint
    from vei.context.models import ContextSnapshot, ContextSourceResult

    teams_source = ContextSourceResult(
        provider="teams",
        captured_at="2025-03-10T00:00:00Z",
        status="ok",
        record_counts={"teams": 1, "channels": 1, "messages": 1},
        data={
            "channels": [
                {
                    "channel": "#Engineering/General",
                    "channel_id": "ch-1",
                    "team_id": "t-1",
                    "team_name": "Engineering",
                    "unread": 0,
                    "messages": [
                        {
                            "ts": "2025-03-10T10:00:00Z",
                            "user": "Alice",
                            "text": "Sprint started",
                        },
                    ],
                },
            ],
            "profile": {"email": "me@co.com"},
        },
    )
    snapshot = ContextSnapshot(
        organization_name="TeamsCorp",
        captured_at="2025-03-10T00:00:00Z",
        sources=[teams_source],
    )

    blueprint = hydrate_snapshot_to_blueprint(snapshot)

    assert blueprint.capability_graphs is not None
    comm = blueprint.capability_graphs.comm_graph
    assert comm is not None
    assert len(comm.slack_channels) == 1
    ch = comm.slack_channels[0]
    assert ch.channel == "#Engineering/General"
    assert len(ch.messages) == 1
    assert ch.messages[0].user == "Alice"
    assert "slack" in blueprint.requested_facades


def test_hydration_combines_slack_gmail_teams() -> None:
    from vei.context.hydrate import hydrate_snapshot_to_blueprint
    from vei.context.models import ContextSnapshot, ContextSourceResult

    slack_source = ContextSourceResult(
        provider="slack",
        captured_at="2025-03-10T00:00:00Z",
        status="ok",
        data={
            "channels": [
                {
                    "channel": "#general",
                    "messages": [
                        {"ts": "1", "user": "alice", "text": "hello"},
                    ],
                    "unread": 0,
                },
            ],
        },
    )
    gmail_source = ContextSourceResult(
        provider="gmail",
        captured_at="2025-03-10T00:00:00Z",
        status="ok",
        data={
            "threads": [
                {
                    "thread_id": "g1",
                    "subject": "Invoice",
                    "messages": [
                        {
                            "from": "vendor@ext.com",
                            "to": "ap@co.com",
                            "subject": "Invoice",
                            "snippet": "Attached.",
                            "unread": False,
                            "labels": ["CATEGORY_PROMOTIONS"],
                        },
                    ],
                },
            ],
        },
    )
    teams_source = ContextSourceResult(
        provider="teams",
        captured_at="2025-03-10T00:00:00Z",
        status="ok",
        data={
            "channels": [
                {
                    "channel": "#Sales/Pipeline",
                    "channel_id": "ch-s1",
                    "unread": 0,
                    "messages": [
                        {"ts": "2", "user": "bob", "text": "deal closed"},
                    ],
                },
            ],
        },
    )

    snapshot = ContextSnapshot(
        organization_name="MultiCorp",
        captured_at="2025-03-10T00:00:00Z",
        sources=[slack_source, gmail_source, teams_source],
    )

    bp = hydrate_snapshot_to_blueprint(snapshot)
    comm = bp.capability_graphs.comm_graph
    assert comm is not None
    assert len(comm.slack_channels) == 2
    assert len(comm.mail_threads) == 1
    assert comm.mail_threads[0].category == "external"
    assert "slack" in bp.requested_facades
    assert "mail" in bp.requested_facades


# ---------------------------------------------------------------------------
# Context status API endpoint
# ---------------------------------------------------------------------------


def test_context_status_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient

    from vei.ui.api import create_ui_app

    monkeypatch.setenv("VEI_SLACK_TOKEN", "xoxb-test")
    monkeypatch.delenv("VEI_GMAIL_TOKEN", raising=False)

    app = create_ui_app("/tmp/nonexistent_workspace_for_test")
    client = TestClient(app)

    resp = client.get("/api/context/status")
    assert resp.status_code == 200
    data = resp.json()
    providers = {p["provider"]: p for p in data["providers"]}
    assert providers["slack"]["configured"] is True
    assert providers["gmail"]["configured"] is False
    assert providers["teams"]["configured"] is False
    assert len(data["providers"]) == 6


def test_context_status_requires_base_url_for_jira_and_okta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient

    from vei.ui.api import create_ui_app

    monkeypatch.setenv("VEI_JIRA_TOKEN", "jira-token")
    monkeypatch.setenv("VEI_OKTA_TOKEN", "okta-token")
    monkeypatch.delenv("VEI_JIRA_URL", raising=False)
    monkeypatch.delenv("VEI_OKTA_ORG_URL", raising=False)

    app = create_ui_app("/tmp/nonexistent_workspace_for_test")
    client = TestClient(app)

    resp = client.get("/api/context/status")
    assert resp.status_code == 200
    providers = {p["provider"]: p for p in resp.json()["providers"]}
    assert providers["jira"]["configured"] is False
    assert providers["jira"]["env_var"] == "VEI_JIRA_URL"
    assert providers["okta"]["configured"] is False
    assert providers["okta"]["env_var"] == "VEI_OKTA_ORG_URL"


def test_context_capture_endpoint_accepts_json_body_and_writes_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient

    from vei.context.models import ContextSnapshot, ContextSourceResult
    from vei.ui.api import create_ui_app
    from vei.workspace.api import create_workspace_from_template

    root = tmp_path / "workspace"
    manifest = create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    monkeypatch.setenv("VEI_SLACK_TOKEN", "xoxb-test")

    captured: dict[str, Any] = {}

    def fake_capture_context(
        configs: list[ContextProviderConfig],
        *,
        organization_name: str,
        organization_domain: str,
    ) -> ContextSnapshot:
        captured["configs"] = configs
        captured["organization_name"] = organization_name
        captured["organization_domain"] = organization_domain
        return ContextSnapshot(
            organization_name=organization_name,
            organization_domain=organization_domain,
            captured_at="2025-03-10T00:00:00Z",
            sources=[
                ContextSourceResult(
                    provider="slack",
                    captured_at="2025-03-10T00:00:00Z",
                    status="ok",
                    record_counts={"channels": 1, "messages": 2},
                    data={"channels": []},
                )
            ],
        )

    with patch("vei.context.api.capture_context", side_effect=fake_capture_context):
        client = TestClient(create_ui_app(root))
        openapi_resp = client.get("/openapi.json")
        capture_resp = client.post(
            "/api/context/capture", json={"providers": ["slack"]}
        )

    assert openapi_resp.status_code == 200
    assert capture_resp.status_code == 200
    assert captured["organization_name"] == manifest.title
    assert captured["organization_domain"] == ""
    assert [config.provider for config in captured["configs"]] == ["slack"]

    payload = capture_resp.json()
    assert payload["captured"] == 1
    assert payload["errors"] == 0
    assert payload["sources"][0]["provider"] == "slack"

    snapshot_path = root / "context_snapshot.json"
    assert snapshot_path.exists()
    snapshot = ContextSnapshot.model_validate_json(
        snapshot_path.read_text(encoding="utf-8")
    )
    assert snapshot.organization_name == manifest.title
    assert snapshot.source_for("slack") is not None
