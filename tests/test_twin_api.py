from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from vei.context.models import ContextSnapshot, ContextSourceResult
from vei.twin import build_customer_twin, create_twin_gateway_app, load_customer_twin
from vei.workspace.api import load_workspace, load_workspace_blueprint_asset


def test_build_customer_twin_creates_workspace_and_preserves_external_context(
    tmp_path: Path,
) -> None:
    root = tmp_path / "customer_twin"

    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="acme.ai",
    )

    assert (root / "twin_manifest.json").exists()
    assert (root / "context_snapshot.json").exists()
    assert bundle.organization_name == "Acme Cloud"
    assert bundle.organization_domain == "acme.ai"

    loaded_bundle = load_customer_twin(root)
    manifest = load_workspace(root)
    asset = load_workspace_blueprint_asset(root)

    assert loaded_bundle.organization_name == "Acme Cloud"
    assert manifest.title == "Acme Cloud"
    assert asset.title == "Acme Cloud"
    assert asset.capability_graphs is not None
    assert asset.capability_graphs.organization_domain == "acme.ai"
    assert asset.capability_graphs.comm_graph is not None
    assert asset.capability_graphs.doc_graph is not None
    assert any(
        channel.channel == "#revops-war-room"
        for channel in asset.capability_graphs.comm_graph.slack_channels
    )
    assert any(
        document.title == "Renewal Recovery Plan"
        for document in asset.capability_graphs.doc_graph.documents
    )

    addresses = {
        message.from_address
        for thread in asset.capability_graphs.comm_graph.mail_threads
        for message in thread.messages
    } | {
        message.to_address
        for thread in asset.capability_graphs.comm_graph.mail_threads
        for message in thread.messages
    }
    assert "support@acme.ai" in addresses
    assert "jordan.blake@apexfinancial.example.com" in addresses


def test_twin_gateway_routes_expose_company_state_and_record_external_actions(
    tmp_path: Path,
) -> None:
    root = tmp_path / "customer_twin"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="acme.ai",
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        status_response = client.get("/api/twin")
        assert status_response.status_code == 200
        status_payload = status_response.json()
        assert status_payload["runtime"]["status"] == "running"
        assert status_payload["manifest"]["runner"] == "external"

        slack_response = client.get(
            "/slack/api/conversations.list",
            headers=auth_headers,
        )
        assert slack_response.status_code == 200
        slack_payload = slack_response.json()
        assert slack_payload["ok"] is True
        channel_id = slack_payload["channels"][0]["id"]

        post_response = client.post(
            "/slack/api/chat.postMessage",
            headers=auth_headers,
            json={
                "channel": channel_id,
                "text": "Engineering hotfix is approved. Send the customer note now.",
            },
        )
        assert post_response.status_code == 200
        assert post_response.json()["ok"] is True

        jira_response = client.get(
            "/jira/rest/api/3/search",
            headers=auth_headers,
            params={"jql": "status = open", "maxResults": 2},
        )
        assert jira_response.status_code == 200
        assert jira_response.json()["issues"]

        messages_response = client.get(
            "/graph/v1.0/me/messages",
            headers=auth_headers,
        )
        assert messages_response.status_code == 200
        messages_payload = messages_response.json()
        assert messages_payload["value"]

        before_count = client.get("/api/twin").json()["runtime"]["request_count"]
        message_id = messages_payload["value"][0]["id"]
        message_response = client.get(
            f"/graph/v1.0/me/messages/{message_id}",
            headers=auth_headers,
        )
        assert message_response.status_code == 200
        after_count = client.get("/api/twin").json()["runtime"]["request_count"]
        assert after_count == before_count + 1

        crm_response = client.get(
            "/salesforce/services/data/v60.0/query",
            headers=auth_headers,
            params={"q": "SELECT Id, Name FROM Opportunity LIMIT 2"},
        )
        assert crm_response.status_code == 200
        assert crm_response.json()["records"]

        history_response = client.get("/api/twin/history")
        assert history_response.status_code == 200
        history_payload = history_response.json()
        assert any(
            item["label"] == "slack.chat.postMessage" for item in history_payload
        )

        surfaces_response = client.get("/api/twin/surfaces")
        assert surfaces_response.status_code == 200
        panel_map = {
            panel["surface"]: panel for panel in surfaces_response.json()["panels"]
        }
        assert panel_map["slack"]["items"]
        assert panel_map["mail"]["items"]

        finalize_response = client.post("/api/twin/finalize")
        assert finalize_response.status_code == 200
        finalize_payload = finalize_response.json()
        assert finalize_payload["runtime"]["status"] == "completed"
        assert finalize_payload["manifest"]["status"] == "ok"


def _sample_snapshot() -> ContextSnapshot:
    return ContextSnapshot(
        organization_name="Acme Cloud",
        organization_domain="acme.ai",
        captured_at="2026-03-24T16:00:00+00:00",
        sources=[
            ContextSourceResult(
                provider="slack",
                captured_at="2026-03-24T16:00:00+00:00",
                status="ok",
                record_counts={"channels": 1, "messages": 3},
                data={
                    "channels": [
                        {
                            "channel": "#revops-war-room",
                            "unread": 2,
                            "messages": [
                                {
                                    "ts": "1710300000.000100",
                                    "user": "maya.ops",
                                    "text": "Renewal is exposed unless we land the onboarding fix today.",
                                },
                                {
                                    "ts": "1710300060.000200",
                                    "user": "evan.sales",
                                    "text": "Jordan wants one accountable owner and a customer-safe timeline.",
                                },
                                {
                                    "ts": "1710300120.000300",
                                    "user": "maya.ops",
                                    "text": "Drafting the recovery note now.",
                                    "thread_ts": "1710300060.000200",
                                },
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="jira",
                captured_at="2026-03-24T16:00:00+00:00",
                status="ok",
                record_counts={"issues": 2},
                data={
                    "issues": [
                        {
                            "ticket_id": "ACME-101",
                            "title": "Renewal blocker: onboarding API still timing out",
                            "status": "open",
                            "assignee": "maya.ops",
                            "description": "Customer onboarding export is timing out on larger tenants.",
                        },
                        {
                            "ticket_id": "ACME-102",
                            "title": "Prepare customer-safe release note",
                            "status": "in_progress",
                            "assignee": "evan.sales",
                            "description": "Release note needs an ETA and rollback summary.",
                        },
                    ]
                },
            ),
            ContextSourceResult(
                provider="google",
                captured_at="2026-03-24T16:00:00+00:00",
                status="ok",
                record_counts={"documents": 1, "users": 1},
                data={
                    "documents": [
                        {
                            "doc_id": "DOC-ACME-001",
                            "title": "Renewal Recovery Plan",
                            "body": (
                                "Goal: stabilize the renewal by restoring onboarding reliability, "
                                "sending a customer-safe update, and scheduling an executive follow-up."
                            ),
                            "mime_type": "application/vnd.google-apps.document",
                        }
                    ],
                    "users": [
                        {
                            "id": "g-001",
                            "email": "maya@acme.ai",
                            "name": "Maya Ops",
                            "org_unit": "RevOps",
                            "suspended": False,
                        }
                    ],
                },
            ),
            ContextSourceResult(
                provider="gmail",
                captured_at="2026-03-24T16:00:00+00:00",
                status="ok",
                record_counts={"threads": 1},
                data={
                    "threads": [
                        {
                            "thread_id": "thr-001",
                            "subject": "Renewal risk review",
                            "messages": [
                                {
                                    "from": "jordan.blake@apexfinancial.example.com",
                                    "to": "support@acme.ai",
                                    "subject": "Renewal risk review",
                                    "snippet": "Need a clear owner and a confirmed recovery timeline today.",
                                    "labels": ["IMPORTANT"],
                                    "unread": True,
                                }
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="okta",
                captured_at="2026-03-24T16:00:00+00:00",
                status="ok",
                record_counts={"users": 1, "groups": 1, "applications": 1},
                data={
                    "users": [
                        {
                            "id": "okta-u-001",
                            "status": "active",
                            "profile": {
                                "login": "maya@acme.ai",
                                "email": "maya@acme.ai",
                                "firstName": "Maya",
                                "lastName": "Ops",
                                "displayName": "Maya Ops",
                                "department": "RevOps",
                                "title": "Revenue Operations Lead",
                            },
                            "group_ids": ["grp-ops"],
                        }
                    ],
                    "groups": [
                        {
                            "id": "grp-ops",
                            "profile": {"name": "Revenue Operations"},
                            "members": ["okta-u-001"],
                        }
                    ],
                    "applications": [
                        {
                            "id": "app-001",
                            "label": "Jira Cloud",
                            "status": "active",
                            "assignments": ["okta-u-001"],
                        }
                    ],
                },
            ),
        ],
    )
