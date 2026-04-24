from __future__ import annotations

from json import JSONDecodeError
from pathlib import Path
from time import monotonic, sleep
from threading import Event, Thread

from fastapi.testclient import TestClient
import pytest

from vei.context.models import ContextSnapshot, ContextSourceResult
from vei.governor import GovernorAgentSpec, default_governor_workspace_config
from vei.orchestrators.api import (
    OrchestratorAgent,
    OrchestratorApproval,
    OrchestratorSnapshot,
    OrchestratorSummary,
    OrchestratorSyncCapabilities,
    OrchestratorSyncHealth,
    OrchestratorTask,
)
from vei.run.api import build_run_timeline
from vei.twin import (
    TWIN_MANIFEST_FILE,
    build_customer_twin,
    create_twin_gateway_app,
    load_customer_twin,
)
from vei.twin.models import ContextMoldConfig
from vei.ui.api import create_ui_app
from vei.workforce.api import WorkforceCommandRecord, build_workforce_state
from vei.workspace.api import load_workspace, load_workspace_blueprint_asset


def _register_proxy_agent(
    client: TestClient,
    auth_headers: dict[str, str],
    *,
    agent_id: str = "proxy-agent",
    allowed_surfaces: list[str] | None = None,
    policy_profile_id: str | None = None,
) -> dict[str, str]:
    response = client.post(
        "/api/governor/agents",
        headers=auth_headers,
        json={
            "agent_id": agent_id,
            "name": agent_id.replace("-", " ").title(),
            "mode": "proxy",
            "allowed_surfaces": allowed_surfaces or [],
            "policy_profile_id": policy_profile_id,
        },
    )
    assert response.status_code == 201
    return {
        **auth_headers,
        "x-vei-agent-id": agent_id,
        "x-vei-agent-name": response.json()["name"],
    }


def _wait_for_workspace_governor_status(
    client: TestClient,
    *,
    attempts: int = 20,
    delay_s: float = 0.2,
) -> dict[str, object]:
    last_payload: dict[str, object] | None = None
    last_error: JSONDecodeError | None = None

    for _ in range(attempts):
        response = client.get("/api/workspace/governor")
        try:
            payload = response.json()
        except JSONDecodeError as exc:
            last_error = exc
            sleep(delay_s)
            continue

        if isinstance(payload, dict):
            last_payload = payload
            if (
                payload.get("pending_demo_steps") == 0
                and payload.get("autoplay_running") is False
            ):
                return payload

        sleep(delay_s)

    if last_payload is not None:
        return last_payload
    if last_error is not None:
        raise last_error
    raise AssertionError("workspace governor status never returned a JSON object")


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


def test_gateway_runtime_uses_bundle_experiment_seed(tmp_path: Path) -> None:
    root = tmp_path / "seeded_customer_twin"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
    )
    bundle.metadata["experiment_seed"] = 8675309
    (root / TWIN_MANIFEST_FILE).write_text(
        bundle.model_dump_json(indent=2),
        encoding="utf-8",
    )

    app = create_twin_gateway_app(root)

    assert app.state.runtime.seed == 8675309


def test_build_customer_twin_normalizes_snapshot_identity_to_bundle_identity(
    tmp_path: Path,
) -> None:
    root = tmp_path / "customer_twin_identity"

    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_name="Renamed Service Ops",
        organization_domain="renamed.example.com",
    )

    saved_snapshot = ContextSnapshot.model_validate_json(
        (root / "context_snapshot.json").read_text(encoding="utf-8")
    )

    assert bundle.organization_name == "Renamed Service Ops"
    assert bundle.organization_domain == "renamed.example.com"
    assert saved_snapshot.organization_name == bundle.organization_name
    assert saved_snapshot.organization_domain == bundle.organization_domain
    assert load_workspace(root).title == bundle.organization_name
    assert load_workspace_blueprint_asset(root).title == bundle.organization_name


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
        proxy_headers = _register_proxy_agent(client, auth_headers)
        status_response = client.get("/api/twin")
        assert status_response.status_code == 200
        status_payload = status_response.json()
        assert status_payload["runtime"]["status"] == "running"
        assert status_payload["manifest"]["runner"] == "external"

        slack_response = client.get(
            "/slack/api/conversations.list",
            headers=proxy_headers,
        )
        assert slack_response.status_code == 200
        slack_payload = slack_response.json()
        assert slack_payload["ok"] is True
        channel_id = slack_payload["channels"][0]["id"]

        post_response = client.post(
            "/slack/api/chat.postMessage",
            headers=proxy_headers,
            json={
                "channel": channel_id,
                "text": "Engineering hotfix is approved. Send the customer note now.",
            },
        )
        assert post_response.status_code == 200
        assert post_response.json()["ok"] is True

        jira_response = client.get(
            "/jira/rest/api/3/search",
            headers=proxy_headers,
            params={"jql": "status = open", "maxResults": 2},
        )
        assert jira_response.status_code == 200
        assert jira_response.json()["issues"]

        messages_response = client.get(
            "/graph/v1.0/me/messages",
            headers=proxy_headers,
        )
        assert messages_response.status_code == 200
        messages_payload = messages_response.json()
        assert messages_payload["value"]

        before_count = client.get("/api/twin").json()["runtime"]["request_count"]
        message_id = messages_payload["value"][0]["id"]
        message_response = client.get(
            f"/graph/v1.0/me/messages/{message_id}",
            headers=proxy_headers,
        )
        assert message_response.status_code == 200
        after_count = client.get("/api/twin").json()["runtime"]["request_count"]
        assert after_count == before_count + 1

        crm_response = client.get(
            "/salesforce/services/data/v60.0/query",
            headers=proxy_headers,
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


def test_twin_gateway_workforce_sync_updates_control_room_world_state(
    tmp_path: Path,
) -> None:
    root = tmp_path / "workforce_twin"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="acme.ai",
        mold=ContextMoldConfig(archetype="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}
    workforce_state = build_workforce_state(
        snapshot=OrchestratorSnapshot(
            provider="paperclip",
            company_id="company-1",
            fetched_at="2026-04-03T08:00:00+00:00",
            summary=OrchestratorSummary(
                provider="paperclip",
                company_id="company-1",
                company_name="VEI Service Ops Lab",
            ),
            capabilities=OrchestratorSyncCapabilities(
                can_pause_agents=True,
                can_resume_agents=True,
                can_comment_on_tasks=True,
                can_manage_approvals=True,
                routeable_surfaces=["slack", "service_ops"],
            ),
            agents=[
                OrchestratorAgent(
                    provider="paperclip",
                    agent_id="paperclip:ceo",
                    external_agent_id="ceo",
                    name="CEO",
                    role="chief executive officer",
                    status="running",
                    integration_mode="proxy",
                    allowed_surfaces=["slack", "service_ops"],
                    policy_profile_id="approver",
                    task_ids=["paperclip:issue-1"],
                ),
                OrchestratorAgent(
                    provider="vendorx",
                    agent_id="vendorx:watcher",
                    external_agent_id="watcher",
                    name="Watcher",
                    role="observer",
                    status="paused",
                    integration_mode="observe",
                ),
            ],
            tasks=[
                OrchestratorTask(
                    provider="paperclip",
                    task_id="paperclip:issue-1",
                    external_task_id="issue-1",
                    title="Protect the customer",
                    identifier="LAB-1",
                    status="in_progress",
                    latest_comment_preview="Need a customer-safe plan before moving.",
                    linked_approval_ids=["paperclip:approval-1"],
                )
            ],
            approvals=[
                OrchestratorApproval(
                    provider="paperclip",
                    approval_id="paperclip:approval-1",
                    external_approval_id="approval-1",
                    approval_type="hire_agent",
                    status="pending",
                    requested_by_name="CEO",
                    summary="Hire a recovery engineer",
                    task_ids=["paperclip:issue-1"],
                )
            ],
        ),
        sync=OrchestratorSyncHealth(
            provider="paperclip",
            status="healthy",
            last_success_at="2026-04-03T08:00:00+00:00",
            synced_agent_count=1,
        ),
    )
    command = WorkforceCommandRecord(
        provider="paperclip",
        action="request_revision",
        created_at="2026-04-03T08:05:00+00:00",
        approval_id="paperclip:approval-1",
        task_id="paperclip:issue-1",
        decision_note="Need a safer staffing plan before approving this.",
    )

    with TestClient(create_twin_gateway_app(root)) as client:
        first_sync = client.post(
            "/api/workforce/sync",
            headers=auth_headers,
            json=workforce_state.model_dump(mode="json"),
        )
        assert first_sync.status_code == 200
        second_sync = client.post(
            "/api/workforce/sync",
            headers=auth_headers,
            json=workforce_state.model_dump(mode="json"),
        )
        assert second_sync.status_code == 200

        workforce_response = client.get("/api/workforce", headers=auth_headers)
        assert workforce_response.status_code == 200
        assert workforce_response.json()["summary"] == {
            "provider": "paperclip",
            "company_name": "VEI Service Ops Lab",
            "sync_status": "healthy",
            "observed_agent_count": 2,
            "governable_agent_count": 1,
            "steerable_agent_count": 2,
            "active_agent_count": 1,
            "task_count": 1,
            "pending_approval_count": 1,
            "routeable_surface_count": 2,
            "latest_activity_at": None,
            "vei_action_count": 0,
            "downstream_response_count": 0,
            "completed_task_count": 0,
            "approved_count": 0,
        }

        surfaces_response = client.get("/api/twin/surfaces")
        assert surfaces_response.status_code == 200
        panel_map = {
            panel["surface"]: panel for panel in surfaces_response.json()["panels"]
        }
        assert panel_map["workforce"]["title"] == "Control Room"
        assert (
            panel_map["workforce"]["headline"]
            == "2 agents · 1 tasks · 1 waiting decisions"
        )

        history_after_sync = client.get("/api/twin/history").json()
        assert (
            sum(1 for item in history_after_sync if item["kind"] == "workforce_sync")
            == 1
        )

        command_response = client.post(
            "/api/workforce/commands",
            headers=auth_headers,
            json=command.model_dump(mode="json"),
        )
        assert command_response.status_code == 200
        assert command_response.json()["commands"][0]["decision_note"].startswith(
            "Need a safer staffing plan"
        )

        history_after_command = client.get("/api/twin/history").json()
        assert any(
            item["kind"] == "workforce_control"
            and item["payload"]["action"] == "request_revision"
            for item in history_after_command
        )

        twin_payload = client.get("/api/twin").json()
        assert (
            twin_payload["manifest"]["metadata"]["workforce"]["commands"][0]["action"]
            == "request_revision"
        )


def test_service_ops_twin_governor_demo_exposes_agents_and_generates_activity(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_twin"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(
            demo_mode=True,
            autoplay=False,
            hero_world="service_ops",
        ),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        mirror_response = client.get("/api/governor", headers=auth_headers)
        assert mirror_response.status_code == 200
        mirror_payload = mirror_response.json()
        assert mirror_payload["config"]["demo_mode"] is True
        assert mirror_payload["pending_demo_steps"] >= 8
        assert mirror_payload["connector_status"]
        assert mirror_payload["policy_profiles"]

        agents_response = client.get("/api/governor/agents", headers=auth_headers)
        assert agents_response.status_code == 200
        agent_ids = {item["agent_id"] for item in agents_response.json()["agents"]}
        assert {"dispatch-bot", "billing-bot", "control-lead"} <= agent_ids

        tick_response = client.post("/api/governor/demo/tick", headers=auth_headers)
        assert tick_response.status_code == 200
        tick_payload = tick_response.json()
        assert tick_payload["handled_by"] == "dispatch"

        history_payload = client.get("/api/twin/history").json()
        assert any(
            item["label"] == "slack.chat.postMessage" for item in history_payload
        )

        runtime_payload = client.get("/api/twin").json()
        assert runtime_payload["runtime"]["request_count"] >= 1


def test_service_ops_twin_governor_demo_runs_salesforce_step_without_error(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_demo_salesforce"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(
            demo_mode=True,
            autoplay=False,
            hero_world="service_ops",
        ),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        pending_steps = client.get("/api/governor", headers=auth_headers).json()[
            "pending_demo_steps"
        ]
        for _ in range(pending_steps + 2):
            response = client.post("/api/governor/demo/tick", headers=auth_headers)
            assert response.status_code == 200
            if response.json().get("remaining_demo_steps") == 0:
                break

        mirror_payload = client.get("/api/governor", headers=auth_headers).json()
        history_payload = client.get("/api/twin/history").json()

        assert mirror_payload["pending_demo_steps"] == 0
        assert any(
            item["tool"] == "salesforce.query.opportunity" for item in history_payload
        )
        assert not any(
            item["status"] == "error"
            and str(item.get("tool", "")).startswith("salesforce.")
            for item in history_payload
        )


def test_service_ops_twin_governor_demo_skips_stale_approval_step_after_manual_approval(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_demo_manual_approval"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(
            demo_mode=True,
            autoplay=False,
            hero_world="service_ops",
        ),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        for _ in range(6):
            response = client.post("/api/governor/demo/tick", headers=auth_headers)
            assert response.status_code == 200

        approvals_payload = client.get(
            "/api/governor/approvals",
            headers=auth_headers,
        ).json()
        approval_id = approvals_payload["approvals"][-1]["approval_id"]

        approval_response = client.post(
            f"/api/governor/approvals/{approval_id}/approve",
            headers=auth_headers,
            json={"resolver_agent_id": "control-lead"},
        )
        assert approval_response.status_code == 200
        assert approval_response.json()["status"] == "executed"

        while True:
            mirror_payload = client.get("/api/governor", headers=auth_headers).json()
            if mirror_payload["pending_demo_steps"] == 0:
                break
            response = client.post("/api/governor/demo/tick", headers=auth_headers)
            assert response.status_code == 200

        history_payload = client.get("/api/twin/history").json()
        assert any(
            item["tool"] == "salesforce.query.opportunity" for item in history_payload
        )
        assert not any(item["status"] == "error" for item in history_payload)


def test_governor_policy_profiles_hold_or_deny_as_expected(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_policy_profiles"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        observer = client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "observer-bot",
                "name": "Observer Bot",
                "mode": "ingest",
                "allowed_surfaces": ["slack"],
                "policy_profile_id": "observer",
            },
        )
        assert observer.status_code == 201

        denied = client.post(
            "/api/governor/events",
            headers=auth_headers,
            json={
                "agent_id": "observer-bot",
                "external_tool": "slack.chat.postMessage",
                "resolved_tool": "slack.send_message",
                "focus_hint": "slack",
                "args": {
                    "channel": "#clearwater-dispatch",
                    "text": "Observer should not be able to write.",
                },
            },
        )
        assert denied.status_code == 202
        assert denied.json()["handled_by"] == "denied"
        assert denied.json()["result"]["code"] == "mirror.profile_denied"

        operator = client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "operator-bot",
                "name": "Operator Bot",
                "mode": "ingest",
                "allowed_surfaces": ["service_ops"],
                "policy_profile_id": "operator",
            },
        )
        assert operator.status_code == 201

        held = client.post(
            "/api/governor/events",
            headers=auth_headers,
            json={
                "agent_id": "operator-bot",
                "external_tool": "service_ops.update_policy",
                "resolved_tool": "service_ops.update_policy",
                "focus_hint": "service_ops",
                "args": {
                    "billing_hold_on_dispute": False,
                    "approval_threshold_usd": 2500,
                    "reason": "Need review before changing the policy.",
                },
            },
        )
        assert held.status_code == 202
        assert held.json()["handled_by"] == "pending_approval"
        assert held.json()["result"]["approval_required"] is True

        mirror = client.get("/api/governor", headers=auth_headers).json()
        assert len(mirror["pending_approvals"]) == 1
        operator_snapshot = next(
            item for item in mirror["agents"] if item["agent_id"] == "operator-bot"
        )
        assert operator_snapshot["resolved_policy_profile"]["profile_id"] == "operator"


def test_mirror_approval_resolution_executes_held_action(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_policy_approval"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "operator-bot",
                "name": "Operator Bot",
                "mode": "ingest",
                "allowed_surfaces": ["service_ops"],
                "policy_profile_id": "operator",
            },
        )
        client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "control-lead",
                "name": "Control Lead",
                "mode": "ingest",
                "allowed_surfaces": ["service_ops"],
                "policy_profile_id": "approver",
            },
        )

        held = client.post(
            "/api/governor/events",
            headers=auth_headers,
            json={
                "agent_id": "operator-bot",
                "external_tool": "service_ops.update_policy",
                "resolved_tool": "service_ops.update_policy",
                "focus_hint": "service_ops",
                "args": {
                    "billing_hold_on_dispute": False,
                    "approval_threshold_usd": 2500,
                    "reason": "Approval flow test.",
                },
            },
        ).json()
        approval_id = held["result"]["approval_id"]

        approval = client.post(
            f"/api/governor/approvals/{approval_id}/approve",
            headers=auth_headers,
            json={"resolver_agent_id": "control-lead"},
        )
        assert approval.status_code == 200
        assert approval.json()["status"] == "executed"

        surfaces = client.get("/api/twin/surfaces").json()
        vertical = next(
            panel
            for panel in surfaces["panels"]
            if panel["surface"] == "vertical_heartbeat"
        )
        assert vertical["policy"]["billing_hold_on_dispute"] is False
        assert vertical["policy"]["approval_threshold_usd"] == 2500.0


def test_proxy_risky_action_returns_approval_required(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_proxy_approval"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        proxy_headers = _register_proxy_agent(
            client,
            auth_headers,
            agent_id="jira-operator",
            allowed_surfaces=["jira"],
            policy_profile_id="operator",
        )

        response = client.post(
            "/jira/rest/api/3/issue/ACME-101/transitions",
            headers=proxy_headers,
            json={"transition": {"id": "closed"}},
        )
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "mirror.approval_required"


def test_workspace_approval_rule_can_hold_safe_write_for_approval(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_workspace_rule_approval"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(
            hero_world="service_ops",
            approval_rules=[
                {
                    "surface": "mail",
                    "resolved_tools": ["mail.compose"],
                    "operation_classes": ["write_safe"],
                    "reason": (
                        "Customer-facing incident mail requires approval before it is sent."
                    ),
                }
            ],
        ),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        operator = client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "ops-bot",
                "name": "Ops Bot",
                "mode": "ingest",
                "allowed_surfaces": ["mail"],
                "policy_profile_id": "operator",
            },
        )
        assert operator.status_code == 201

        approver = client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "legal-bot",
                "name": "Legal Bot",
                "mode": "ingest",
                "allowed_surfaces": ["mail"],
                "policy_profile_id": "approver",
            },
        )
        assert approver.status_code == 201

        held = client.post(
            "/api/governor/events",
            headers=auth_headers,
            json={
                "agent_id": "ops-bot",
                "external_tool": "mail.compose",
                "resolved_tool": "mail.compose",
                "focus_hint": "mail",
                "args": {
                    "to": "facilities@clearwatermedical.example.com",
                    "subj": "Delay explanation",
                    "body_text": "Cause line: technician no-show created the delay.",
                },
            },
        )
        assert held.status_code == 202
        assert held.json()["handled_by"] == "pending_approval"
        assert held.json()["result"]["reason"] == (
            "Customer-facing incident mail requires approval before it is sent."
        )
        approval_id = held.json()["result"]["approval_id"]

        approval = client.post(
            f"/api/governor/approvals/{approval_id}/approve",
            headers=auth_headers,
            json={"resolver_agent_id": "legal-bot"},
        )
        assert approval.status_code == 200
        assert approval.json()["status"] == "executed"


def test_mirror_rate_limit_denial_tracks_throttled_count(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_rate_limit"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "rate-bot",
                "name": "Rate Bot",
                "mode": "ingest",
                "allowed_surfaces": ["slack"],
                "policy_profile_id": "admin",
            },
        )
        mirror_runtime = client.app.state.runtime.mirror
        assert mirror_runtime is not None
        mirror_runtime._total_action_windows["rate-bot"] = [monotonic()] * 60

        response = client.post(
            "/api/governor/events",
            headers=auth_headers,
            json={
                "agent_id": "rate-bot",
                "external_tool": "slack.chat.postMessage",
                "resolved_tool": "slack.send_message",
                "focus_hint": "slack",
                "args": {
                    "channel": "#clearwater-dispatch",
                    "text": "This one should hit the rate limiter.",
                },
            },
        )
        assert response.status_code == 202
        assert response.json()["result"]["code"] == "mirror.rate_limited"

        mirror = client.get("/api/governor", headers=auth_headers).json()
        agent = next(
            item for item in mirror["agents"] if item["agent_id"] == "rate-bot"
        )
        assert mirror["throttled_event_count"] == 1
        assert agent["throttled_count"] == 1


def test_proxy_requests_require_registered_agent_id(
    tmp_path: Path,
) -> None:
    root = tmp_path / "customer_twin_require_id"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="acme.ai",
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        response = client.post(
            "/slack/api/chat.postMessage",
            headers=auth_headers,
            json={"channel": "#revops-war-room", "text": "No agent id should fail."},
        )

    assert response.status_code == 200
    assert response.json() == {"ok": False, "error": "mirror.agent_id_required"}


@pytest.mark.slow
def test_workspace_mirror_marks_autoplay_stopped_after_demo_finishes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_autoplay_status"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(
            demo_mode=True,
            autoplay=True,
            demo_interval_ms=250,
            hero_world="service_ops",
        ),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with (
        TestClient(create_twin_gateway_app(root)) as gateway_client,
        TestClient(create_ui_app(root)) as ui_client,
    ):
        for _ in range(20):
            mirror = gateway_client.get("/api/governor", headers=auth_headers).json()
            if (
                mirror["pending_demo_steps"] == 0
                and mirror["autoplay_running"] is False
            ):
                break
            sleep(0.2)

        gateway_mirror = gateway_client.get(
            "/api/governor", headers=auth_headers
        ).json()
        workspace_mirror = _wait_for_workspace_governor_status(ui_client)

        assert gateway_mirror["pending_demo_steps"] == 0
        assert gateway_mirror["autoplay_running"] is False
        assert workspace_mirror["pending_demo_steps"] == 0
        assert workspace_mirror["autoplay_running"] is False


def test_mirror_ingest_event_updates_history_and_slack_surface(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_ingest"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        register_response = client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "vendor-bot",
                "name": "Vendor Bot",
                "mode": "ingest",
                "role": "vendor_dispatch_partner",
                "team": "partners",
                "allowed_surfaces": ["slack"],
            },
        )
        assert register_response.status_code == 201

        ingest_response = client.post(
            "/api/governor/events",
            headers=auth_headers,
            json={
                "agent_id": "vendor-bot",
                "external_tool": "vendor.slack.message",
                "focus_hint": "slack",
                "target": "slack",
                "payload": {
                    "channel": "#clearwater-dispatch",
                    "text": "Vendor Bot: backup crew is en route to Clearwater Medical.",
                    "user": "vendor.bot",
                },
            },
        )
        assert ingest_response.status_code == 202
        assert ingest_response.json()["handled_by"] == "inject"

        history_payload = client.get("/api/twin/history").json()
        assert any(item["label"] == "vendor.slack.message" for item in history_payload)

        surfaces_payload = client.get("/api/twin/surfaces").json()
        slack_panel = next(
            panel for panel in surfaces_payload["panels"] if panel["surface"] == "slack"
        )
        assert any(
            "backup crew is en route" in str(item.get("body", ""))
            for item in slack_panel["items"]
        )


def test_mirror_ingest_requires_registered_agent(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_ingest_unknown_agent"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        ingest_response = client.post(
            "/api/governor/events",
            headers=auth_headers,
            json={
                "agent_id": "unknown-bot",
                "external_tool": "vendor.slack.message",
                "focus_hint": "slack",
                "target": "slack",
                "payload": {
                    "channel": "#clearwater-dispatch",
                    "text": "This should be rejected.",
                },
            },
        )
        assert ingest_response.status_code == 400
        assert ingest_response.json()["detail"]["code"] == "mirror.agent_not_registered"


def test_workspace_mirror_state_reflects_live_activity(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_ingest_ui"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with (
        TestClient(create_twin_gateway_app(root)) as gateway_client,
        TestClient(create_ui_app(root)) as ui_client,
    ):
        register_response = gateway_client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "vendor-bot",
                "name": "Vendor Bot",
                "mode": "ingest",
                "allowed_surfaces": ["slack"],
            },
        )
        assert register_response.status_code == 201

        workspace_mirror = ui_client.get("/api/workspace/governor").json()
        agent_ids = {item["agent_id"] for item in workspace_mirror["agents"]}
        assert "vendor-bot" in agent_ids

        event_response = gateway_client.post(
            "/api/governor/events",
            headers=auth_headers,
            json={
                "agent_id": "vendor-bot",
                "external_tool": "slack.chat.postMessage",
                "resolved_tool": "slack.send_message",
                "focus_hint": "slack",
                "args": {
                    "channel": "#clearwater-dispatch",
                    "text": "Vendor Bot: live mirror check-in.",
                },
            },
        )
        assert event_response.status_code == 202

        denied_response = gateway_client.post(
            "/api/governor/events",
            headers=auth_headers,
            json={
                "agent_id": "vendor-bot",
                "external_tool": "service_ops.assign_dispatch",
                "resolved_tool": "service_ops.assign_dispatch",
                "focus_hint": "service_ops",
                "args": {
                    "work_order_id": "WO-CFS-100",
                    "technician_id": "TECH-CFS-02",
                },
            },
        )
        assert denied_response.status_code == 202
        assert denied_response.json()["handled_by"] == "denied"

        workspace_mirror = ui_client.get("/api/workspace/governor").json()
        vendor_bot = next(
            agent
            for agent in workspace_mirror["agents"]
            if agent["agent_id"] == "vendor-bot"
        )
        assert workspace_mirror["event_count"] == 2
        assert vendor_bot["denied_count"] == 1
        assert vendor_bot["last_action"] == "service_ops.assign_dispatch"
        assert workspace_mirror["recent_events"][-1]["handled_by"] == "denied"


def test_workspace_mirror_uses_current_external_run_only(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_current_run"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as first_gateway:
        register_response = first_gateway.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "old-bot",
                "name": "Old Bot",
                "mode": "ingest",
                "allowed_surfaces": ["slack"],
            },
        )
        assert register_response.status_code == 201

    with (
        TestClient(create_twin_gateway_app(root)),
        TestClient(create_ui_app(root)) as ui_client,
    ):
        mirror = ui_client.get("/api/workspace/governor").json()
        assert mirror["config"]["hero_world"] == "service_ops"
        assert mirror["agents"] == []
        assert mirror["event_count"] == 0
        assert mirror["recent_events"] == []


def test_registered_proxy_agent_updates_mirror_state_from_gateway_route(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_proxy_ui"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with (
        TestClient(create_twin_gateway_app(root)) as gateway_client,
        TestClient(create_ui_app(root)) as ui_client,
    ):
        register_response = gateway_client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "proxy-bot",
                "name": "Proxy Bot",
                "mode": "proxy",
                "allowed_surfaces": ["slack"],
            },
        )
        assert register_response.status_code == 201

        proxy_headers = {
            **auth_headers,
            "x-vei-agent-id": "proxy-bot",
            "x-vei-agent-name": "Proxy Bot",
        }
        response = gateway_client.post(
            "/slack/api/chat.postMessage",
            headers=proxy_headers,
            json={
                "channel": "#clearwater-dispatch",
                "text": "Proxy Bot: dispatch is confirmed.",
            },
        )
        assert response.status_code == 200
        assert response.json()["ok"] is True

        workspace_mirror = ui_client.get("/api/workspace/governor").json()
        proxy_bot = next(
            agent
            for agent in workspace_mirror["agents"]
            if agent["agent_id"] == "proxy-bot"
        )
        assert workspace_mirror["event_count"] == 1
        assert proxy_bot["last_action"] == "slack.chat.postMessage"
        assert proxy_bot["denied_count"] == 0
        assert workspace_mirror["recent_events"][-1]["handled_by"] == "dispatch"


def test_ingest_mode_agent_cannot_use_proxy_routes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_proxy_mode_denied"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        register_response = client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "ingest-only",
                "name": "Ingest Only",
                "mode": "ingest",
                "allowed_surfaces": ["slack"],
            },
        )
        assert register_response.status_code == 201

        proxy_headers = {
            **auth_headers,
            "x-vei-agent-id": "ingest-only",
            "x-vei-agent-name": "Ingest Only",
        }
        response = client.post(
            "/slack/api/chat.postMessage",
            headers=proxy_headers,
            json={
                "channel": "#clearwater-dispatch",
                "text": "This should be denied by mode.",
            },
        )

        assert response.status_code == 200
        assert response.json() == {"ok": False, "error": "mirror.mode_denied"}

        mirror = client.get("/api/governor", headers=auth_headers).json()
        ingest_only = next(
            agent for agent in mirror["agents"] if agent["agent_id"] == "ingest-only"
        )
        assert mirror["event_count"] == 1
        assert ingest_only["denied_count"] == 1
        assert mirror["recent_events"][-1]["handled_by"] == "denied"


def test_unregistered_proxy_agent_is_rejected(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_proxy_unknown_agent"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as gateway_client:
        proxy_headers = {
            **auth_headers,
            "x-vei-agent-id": "unknown-proxy",
            "x-vei-agent-name": "Unknown Proxy",
        }
        response = gateway_client.post(
            "/slack/api/chat.postMessage",
            headers=proxy_headers,
            json={
                "channel": "#clearwater-dispatch",
                "text": "This should not bypass registration.",
            },
        )
        assert response.status_code == 200
        assert response.json() == {
            "ok": False,
            "error": "mirror.agent_not_registered",
        }


def test_mirror_registration_does_not_deadlock_with_dispatch(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "service_ops_deadlock"
    build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    app = create_twin_gateway_app(root)
    runtime = app.state.runtime

    dispatch_entered = Event()
    release_dispatch = Event()
    register_entered = Event()
    errors: list[BaseException] = []
    original_call_tool = runtime.session.call_tool
    original_register = runtime.register_mirror_agent

    def blocking_call_tool(tool: str, args=None):
        if tool == "slack.list_channels":
            dispatch_entered.set()
            if not release_dispatch.wait(timeout=2.0):
                raise AssertionError("dispatch release timed out")
        return original_call_tool(tool, args)

    def observed_register(agent: GovernorAgentSpec) -> None:
        register_entered.set()
        original_register(agent)

    monkeypatch.setattr(runtime.session, "call_tool", blocking_call_tool)
    monkeypatch.setattr(runtime, "register_mirror_agent", observed_register)

    def dispatch_call() -> None:
        try:
            runtime.dispatch(
                external_tool="slack.conversations.list",
                resolved_tool="slack.list_channels",
                args={},
                focus_hint="slack",
            )
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def register_call() -> None:
        try:
            runtime.mirror.register_agent(
                GovernorAgentSpec(agent_id="vendor-bot", name="Vendor Bot")
            )
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    dispatch_thread = Thread(target=dispatch_call, daemon=True)
    register_thread = Thread(target=register_call, daemon=True)

    dispatch_thread.start()
    assert dispatch_entered.wait(timeout=1.0)

    register_thread.start()
    assert register_entered.wait(timeout=1.0)

    release_dispatch.set()

    dispatch_thread.join(timeout=1.0)
    register_thread.join(timeout=1.0)

    assert not dispatch_thread.is_alive()
    assert not register_thread.is_alive()
    assert errors == []
    assert "vendor-bot" in runtime.session.router.actor_states


def test_mirror_agent_removal_deletes_from_registry(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_removal"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "ephemeral-bot",
                "name": "Ephemeral Bot",
                "mode": "ingest",
                "allowed_surfaces": ["slack"],
            },
        )
        listed = client.get("/api/governor/agents", headers=auth_headers).json()
        ids_before = {a["agent_id"] for a in listed["agents"]}
        assert "ephemeral-bot" in ids_before

        delete_resp = client.delete(
            "/api/governor/agents/ephemeral-bot", headers=auth_headers
        )
        assert delete_resp.status_code == 200
        assert delete_resp.json()["agent_id"] == "ephemeral-bot"

        listed_after = client.get("/api/governor/agents", headers=auth_headers).json()
        ids_after = {a["agent_id"] for a in listed_after["agents"]}
        assert "ephemeral-bot" not in ids_after

        not_found = client.delete(
            "/api/governor/agents/ephemeral-bot", headers=auth_headers
        )
        assert not_found.status_code == 404


def test_mirror_surface_access_enforcement_denies_unauthorized_surfaces(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_enforcement"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "restricted-bot",
                "name": "Restricted Bot",
                "mode": "ingest",
                "allowed_surfaces": ["slack"],
            },
        )

        allowed_response = client.post(
            "/api/governor/events",
            headers=auth_headers,
            json={
                "agent_id": "restricted-bot",
                "external_tool": "slack.chat.postMessage",
                "resolved_tool": "slack.send_message",
                "focus_hint": "slack",
                "args": {
                    "channel": "#clearwater-dispatch",
                    "text": "Test allowed message",
                },
            },
        )
        assert allowed_response.status_code == 202
        assert allowed_response.json()["handled_by"] == "dispatch"
        assert allowed_response.json()["ok"] is True

        denied_response = client.post(
            "/api/governor/events",
            headers=auth_headers,
            json={
                "agent_id": "restricted-bot",
                "external_tool": "service_ops.assign_dispatch",
                "resolved_tool": "service_ops.assign_dispatch",
                "focus_hint": "service_ops",
                "args": {
                    "work_order_id": "WO-CFS-100",
                    "technician_id": "TECH-CFS-02",
                },
            },
        )
        assert denied_response.status_code == 202
        assert denied_response.json()["handled_by"] == "denied"
        assert denied_response.json()["ok"] is False

        history = client.get("/api/twin/history").json()
        denial_events = [e for e in history if e["kind"] == "mirror_denied"]
        assert len(denial_events) >= 1
        assert denial_events[0]["payload"]["agent_id"] == "restricted-bot"

        mirror = client.get("/api/governor", headers=auth_headers).json()
        restricted = next(
            a for a in mirror["agents"] if a["agent_id"] == "restricted-bot"
        )
        assert restricted["denied_count"] == 1
        assert restricted["last_action"] is not None

        timeline = build_run_timeline(root, client.app.state.runtime.run_id)
        allowed_event = next(
            event
            for event in timeline
            if event.kind == "workflow_step" and event.label == "slack.chat.postMessage"
        )
        denied_event = next(
            event for event in timeline if event.kind == "mirror_denied"
        )
        assert denied_event.time_ms >= allowed_event.time_ms


def test_registered_proxy_agent_cannot_bypass_surface_restrictions(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_proxy_enforcement"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        register_response = client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "slack-only",
                "name": "Slack Only",
                "mode": "proxy",
                "allowed_surfaces": ["slack"],
            },
        )
        assert register_response.status_code == 201

        proxy_headers = {
            **auth_headers,
            "x-vei-agent-id": "slack-only",
            "x-vei-agent-name": "Slack Only",
        }
        denied_response = client.post(
            "/jira/rest/api/3/issue",
            headers=proxy_headers,
            json={"fields": {"summary": "This should be blocked."}},
        )
        assert denied_response.status_code == 403
        assert denied_response.json()["detail"]["code"] == "mirror.surface_denied"

        mirror = client.get("/api/governor", headers=auth_headers).json()
        proxy_agent = next(
            agent for agent in mirror["agents"] if agent["agent_id"] == "slack-only"
        )
        assert mirror["event_count"] == 1
        assert proxy_agent["denied_count"] == 1
        assert proxy_agent["last_action"] == "jira.issue.create"

        history = client.get("/api/twin/history").json()
        denial_events = [event for event in history if event["kind"] == "mirror_denied"]
        assert denial_events
        assert denial_events[-1]["payload"]["agent_id"] == "slack-only"


def test_proxy_mode_agent_cannot_use_ingest_events(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_ingest_mode_denied"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        register_response = client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "proxy-only",
                "name": "Proxy Only",
                "mode": "proxy",
                "allowed_surfaces": ["slack"],
            },
        )
        assert register_response.status_code == 201

        response = client.post(
            "/api/governor/events",
            headers=auth_headers,
            json={
                "agent_id": "proxy-only",
                "external_tool": "slack.chat.postMessage",
                "resolved_tool": "slack.send_message",
                "focus_hint": "slack",
                "args": {
                    "channel": "#clearwater-dispatch",
                    "text": "This should be denied by mode.",
                },
            },
        )

        assert response.status_code == 202
        assert response.json()["handled_by"] == "denied"
        assert response.json()["ok"] is False
        assert response.json()["result"]["reason"].startswith(
            "agent 'proxy-only' is registered for proxy mode"
        )

        mirror = client.get("/api/governor", headers=auth_headers).json()
        proxy_only = next(
            agent for agent in mirror["agents"] if agent["agent_id"] == "proxy-only"
        )
        assert mirror["event_count"] == 1
        assert proxy_only["denied_count"] == 1
        assert mirror["recent_events"][-1]["handled_by"] == "denied"


def test_mirror_unrestricted_agent_can_dispatch_any_surface(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_unrestricted"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "open-bot",
                "name": "Open Bot",
                "mode": "ingest",
                "allowed_surfaces": [],
            },
        )

        response = client.post(
            "/api/governor/events",
            headers=auth_headers,
            json={
                "agent_id": "open-bot",
                "external_tool": "slack.chat.postMessage",
                "resolved_tool": "slack.send_message",
                "focus_hint": "slack",
                "args": {
                    "channel": "#clearwater-dispatch",
                    "text": "Unrestricted agent message",
                },
            },
        )
        assert response.status_code == 202
        assert response.json()["handled_by"] == "dispatch"
        assert response.json()["ok"] is True

        mirror = client.get("/api/governor", headers=auth_headers).json()
        open_bot = next(a for a in mirror["agents"] if a["agent_id"] == "open-bot")
        assert open_bot["denied_count"] == 0


def test_mirror_inject_denial_for_restricted_surface(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_inject_deny"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_governor_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        client.post(
            "/api/governor/agents",
            headers=auth_headers,
            json={
                "agent_id": "slack-only-bot",
                "name": "Slack Only Bot",
                "mode": "ingest",
                "allowed_surfaces": ["slack"],
            },
        )

        denied_inject = client.post(
            "/api/governor/events",
            headers=auth_headers,
            json={
                "agent_id": "slack-only-bot",
                "external_tool": "vendor.ticket.inject",
                "focus_hint": "slack",
                "target": "tickets",
                "payload": {"text": "should be blocked"},
            },
        )
        assert denied_inject.status_code == 202
        assert denied_inject.json()["handled_by"] == "denied"
        assert denied_inject.json()["ok"] is False

        mirror = client.get("/api/governor", headers=auth_headers).json()
        bot = next(a for a in mirror["agents"] if a["agent_id"] == "slack-only-bot")
        assert bot["denied_count"] == 1


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
