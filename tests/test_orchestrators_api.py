from __future__ import annotations

from vei.orchestrators.api import OrchestratorConfig, PaperclipOrchestratorClient


def test_paperclip_client_normalizes_snapshot(monkeypatch) -> None:
    client = PaperclipOrchestratorClient(
        OrchestratorConfig(
            provider="paperclip",
            base_url="http://paperclip.local",
            company_id="company-1",
            api_key_env="PAPERCLIP_TEST_KEY",
        )
    )
    responses = {
        "/api/companies/company-1": {
            "id": "company-1",
            "name": "Acme AI",
            "status": "active",
            "budgetMonthlyCents": 500000,
        },
        "/api/companies/company-1/dashboard": {
            "agentCounts": {"running": 1, "paused": 1},
            "taskCounts": {"todo": 1, "in_progress": 1},
            "staleTaskCount": 1,
        },
        "/api/companies/company-1/agents": [
            {
                "id": "eng-1",
                "name": "Backend Engineer",
                "role": "engineer",
                "title": "Senior Backend Engineer",
                "status": "running",
                "adapterConfig": {
                    "vei": {
                        "integrationMode": "proxy",
                        "allowedSurfaces": ["slack", "jira"],
                    }
                },
            },
            {
                "id": "ops-1",
                "provider": "internal",
                "name": "Operations Analyst",
                "role": "analyst",
                "title": "Operations Analyst",
                "status": "paused",
                "metadata": {"vei": {"integrationMode": "ingest"}},
            },
        ],
        "/api/companies/company-1/issues": [
            {
                "id": "issue-1",
                "identifier": "ACME-1",
                "title": "Ship orchestrator bridge",
                "status": "in_progress",
                "assigneeAgentId": "eng-1",
                "priority": "high",
                "project": {"name": "Bridge"},
                "goal": {"name": "Launch"},
            },
            {
                "id": "issue-2",
                "title": "Summarize support inbox",
                "status": "todo",
                "assigneeAgentId": "ops-1",
                "priority": "medium",
            },
        ],
        "/api/companies/company-1/activity": [
            {
                "actor": {"id": "eng-1", "name": "Backend Engineer"},
                "action": "issue.comment_added",
                "entityType": "issue",
                "entityId": "issue-1",
                "details": {
                    "runId": "run-7",
                    "identifier": "ACME-1",
                    "bodySnippet": "Use VEI as the control plane.",
                    "approvalId": "approval-1",
                },
                "createdAt": "2026-04-02T02:00:00+00:00",
            },
            {
                "actor": {
                    "id": "ops-1",
                    "provider": "internal",
                    "name": "Operations Analyst",
                },
                "action": "queued",
                "entityType": "issue",
                "entityId": "issue-2",
                "details": {"runId": "run-8"},
                "createdAt": "2026-04-02T02:01:00+00:00",
            },
        ],
        "/api/companies/company-1/approvals": [
            {
                "id": "approval-1",
                "type": "hire_agent",
                "status": "pending",
                "requestedByAgentId": "eng-1",
                "payload": {
                    "title": "Founding Engineer",
                    "role": "engineer",
                },
                "createdAt": "2026-04-02T01:59:00+00:00",
            }
        ],
        "/api/companies/company-1/costs/summary": {
            "spentMonthlyCents": 1200,
            "budgetMonthlyCents": 5000,
        },
        "/api/companies/company-1/costs/by-agent": [
            {"agentId": "eng-1", "spentMonthlyCents": 700},
            {"agentId": "ops-1", "spentMonthlyCents": 500},
        ],
        "/api/issues/issue-1/comments": [
            {
                "id": "comment-1",
                "authorAgentId": "eng-1",
                "body": "Use VEI as the control plane.",
                "createdAt": "2026-04-02T02:00:00+00:00",
            }
        ],
        "/api/issues/issue-2/comments": [],
        "/api/approvals/approval-1/comments": [
            {
                "id": "approval-comment-1",
                "authorAgentId": "eng-1",
                "body": "Need board approval before hiring.",
                "createdAt": "2026-04-02T01:59:30+00:00",
            }
        ],
    }

    monkeypatch.setattr(
        client,
        "_request_json",
        lambda path, **_kwargs: responses[path],
    )

    snapshot = client.fetch_snapshot()

    assert snapshot.summary.company_name == "Acme AI"
    assert snapshot.summary.agent_counts["running"] == 1
    assert snapshot.summary.stale_task_count == 1
    assert snapshot.budget.monthly_spend_cents == 1200
    assert snapshot.agents[0].agent_id == "paperclip:eng-1"
    assert snapshot.agents[0].integration_mode == "proxy"
    assert snapshot.agents[0].allowed_surfaces == ["slack", "jira"]
    assert snapshot.agents[0].policy_profile_id == "operator"
    assert snapshot.agents[0].task_ids == ["paperclip:issue-1"]
    assert snapshot.agents[1].agent_id == "internal:ops-1"
    assert snapshot.agents[1].integration_mode == "ingest"
    assert snapshot.agents[1].task_ids == ["paperclip:issue-2"]
    assert snapshot.tasks[0].identifier == "ACME-1"
    assert snapshot.tasks[0].linked_approval_ids == ["paperclip:approval-1"]
    assert snapshot.tasks[0].latest_comment_preview == "Use VEI as the control plane."
    assert snapshot.tasks[0].assignee_agent_id == "paperclip:eng-1"
    assert snapshot.tasks[1].assignee_agent_id == "internal:ops-1"
    assert snapshot.approvals[0].approval_id == "paperclip:approval-1"
    assert snapshot.approvals[0].requested_by_name == "Backend Engineer"
    assert snapshot.approvals[0].task_ids == ["paperclip:issue-1"]
    assert (
        snapshot.approvals[0].comments[0].body == "Need board approval before hiring."
    )
    assert snapshot.recent_activity[0].object_refs == [
        "paperclip:issue-1",
        "paperclip:approval-1",
        "run:run-7",
    ]
    assert snapshot.recent_activity[0].detail == "Use VEI as the control plane."
    assert snapshot.recent_activity[1].agent_id == "internal:ops-1"
