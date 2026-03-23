from __future__ import annotations

from vei.context.api import hydrate_blueprint
from vei.context.models import ContextSnapshot, ContextSourceResult


def _sample_snapshot() -> ContextSnapshot:
    return ContextSnapshot(
        organization_name="Acme Corp",
        organization_domain="acme.example.com",
        captured_at="2024-03-10T00:00:00+00:00",
        sources=[
            ContextSourceResult(
                provider="slack",
                captured_at="2024-03-10T00:00:00+00:00",
                status="ok",
                record_counts={"channels": 1, "messages": 2, "users": 1},
                data={
                    "channels": [
                        {
                            "channel": "#general",
                            "channel_id": "C01",
                            "unread": 1,
                            "messages": [
                                {"ts": "100.001", "user": "alice", "text": "Hello"},
                                {"ts": "100.002", "user": "bob", "text": "Hi"},
                            ],
                        }
                    ],
                    "users": [
                        {
                            "id": "U01",
                            "name": "alice",
                            "real_name": "Alice",
                            "email": "alice@acme.example.com",
                        },
                    ],
                },
            ),
            ContextSourceResult(
                provider="jira",
                captured_at="2024-03-10T00:00:00+00:00",
                status="ok",
                record_counts={"issues": 1, "projects": 1},
                data={
                    "issues": [
                        {
                            "ticket_id": "ACME-1",
                            "title": "Fix onboarding flow",
                            "status": "open",
                            "assignee": "alice",
                            "description": "Onboarding breaks for new users.",
                        }
                    ],
                    "projects": [{"key": "ACME", "name": "Acme Project"}],
                },
            ),
            ContextSourceResult(
                provider="google",
                captured_at="2024-03-10T00:00:00+00:00",
                status="ok",
                record_counts={"users": 1, "documents": 1},
                data={
                    "users": [
                        {
                            "id": "G01",
                            "email": "alice@acme.example.com",
                            "name": "Alice Smith",
                            "org_unit": "/Eng",
                        },
                    ],
                    "documents": [
                        {
                            "doc_id": "DOC1",
                            "title": "Design Doc",
                            "body": "Architecture overview.",
                            "mime_type": "document",
                        },
                    ],
                },
            ),
        ],
    )


def test_hydrate_produces_valid_blueprint() -> None:
    snapshot = _sample_snapshot()
    asset = hydrate_blueprint(snapshot)

    assert asset.title == "Acme Corp"
    assert asset.capability_graphs is not None
    graphs = asset.capability_graphs

    assert graphs.organization_name == "Acme Corp"
    assert graphs.organization_domain == "acme.example.com"

    assert graphs.comm_graph is not None
    assert len(graphs.comm_graph.slack_channels) == 1
    assert len(graphs.comm_graph.slack_channels[0].messages) == 2

    assert graphs.work_graph is not None
    assert len(graphs.work_graph.tickets) == 1
    assert graphs.work_graph.tickets[0].ticket_id == "ACME-1"

    assert graphs.doc_graph is not None
    assert len(graphs.doc_graph.documents) == 1
    assert graphs.doc_graph.documents[0].title == "Design Doc"

    assert "slack" in asset.requested_facades
    assert "jira" in asset.requested_facades
    assert "docs" in asset.requested_facades


def test_hydrate_handles_empty_snapshot() -> None:
    snapshot = ContextSnapshot(
        organization_name="Empty Corp",
        captured_at="2024-01-01T00:00:00+00:00",
    )
    asset = hydrate_blueprint(snapshot)
    assert asset.title == "Empty Corp"
    assert asset.capability_graphs is not None
    assert asset.capability_graphs.comm_graph is None
    assert asset.capability_graphs.work_graph is None


def test_hydrate_skips_errored_sources() -> None:
    snapshot = ContextSnapshot(
        organization_name="Partial Corp",
        captured_at="2024-01-01T00:00:00+00:00",
        sources=[
            ContextSourceResult(
                provider="slack",
                captured_at="2024-01-01T00:00:00+00:00",
                status="error",
                error="connection refused",
            ),
        ],
    )
    asset = hydrate_blueprint(snapshot)
    assert asset.capability_graphs.comm_graph is None
    assert "slack" not in asset.requested_facades
