from __future__ import annotations

from pathlib import Path
from threading import Event, Thread

from fastapi.testclient import TestClient

from vei.context.models import ContextSnapshot, ContextSourceResult
from vei.mirror import MirrorAgentSpec, default_mirror_workspace_config
from vei.run.api import build_run_timeline
from vei.twin import build_customer_twin, create_twin_gateway_app, load_customer_twin
from vei.twin.models import ContextMoldConfig
from vei.ui.api import create_ui_app
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


def test_service_ops_twin_mirror_demo_exposes_agents_and_generates_activity(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_twin"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_mirror_workspace_config(
            demo_mode=True,
            autoplay=False,
            hero_world="service_ops",
        ),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        mirror_response = client.get("/api/mirror", headers=auth_headers)
        assert mirror_response.status_code == 200
        mirror_payload = mirror_response.json()
        assert mirror_payload["config"]["demo_mode"] is True
        assert mirror_payload["pending_demo_steps"] >= 4

        agents_response = client.get("/api/mirror/agents", headers=auth_headers)
        assert agents_response.status_code == 200
        agent_ids = {item["agent_id"] for item in agents_response.json()["agents"]}
        assert {"dispatch-bot", "billing-bot"} <= agent_ids

        tick_response = client.post("/api/mirror/demo/tick", headers=auth_headers)
        assert tick_response.status_code == 200
        tick_payload = tick_response.json()
        assert tick_payload["handled_by"] == "dispatch"

        history_payload = client.get("/api/twin/history").json()
        assert any(
            item["label"] == "slack.chat.postMessage" for item in history_payload
        )

        runtime_payload = client.get("/api/twin").json()
        assert runtime_payload["runtime"]["request_count"] >= 1


def test_mirror_ingest_event_updates_history_and_slack_surface(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_ingest"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_mirror_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        register_response = client.post(
            "/api/mirror/agents",
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
            "/api/mirror/events",
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


def test_workspace_mirror_state_reflects_live_activity(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_ingest_ui"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_mirror_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with (
        TestClient(create_twin_gateway_app(root)) as gateway_client,
        TestClient(create_ui_app(root)) as ui_client,
    ):
        register_response = gateway_client.post(
            "/api/mirror/agents",
            headers=auth_headers,
            json={
                "agent_id": "vendor-bot",
                "name": "Vendor Bot",
                "mode": "ingest",
                "allowed_surfaces": ["slack"],
            },
        )
        assert register_response.status_code == 201

        workspace_mirror = ui_client.get("/api/workspace/mirror").json()
        agent_ids = {item["agent_id"] for item in workspace_mirror["agents"]}
        assert "vendor-bot" in agent_ids

        event_response = gateway_client.post(
            "/api/mirror/events",
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
            "/api/mirror/events",
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

        workspace_mirror = ui_client.get("/api/workspace/mirror").json()
        vendor_bot = next(
            agent
            for agent in workspace_mirror["agents"]
            if agent["agent_id"] == "vendor-bot"
        )
        assert workspace_mirror["event_count"] == 2
        assert vendor_bot["denied_count"] == 1
        assert vendor_bot["last_action"] == "service_ops.assign_dispatch"
        assert workspace_mirror["recent_events"][-1]["handled_by"] == "denied"


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
        mirror_config=default_mirror_workspace_config(hero_world="service_ops"),
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

    def observed_register(agent: MirrorAgentSpec) -> None:
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
                MirrorAgentSpec(agent_id="vendor-bot", name="Vendor Bot")
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


def test_mirror_surface_access_enforcement_denies_unauthorized_surfaces(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_enforcement"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_mirror_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        client.post(
            "/api/mirror/agents",
            headers=auth_headers,
            json={
                "agent_id": "restricted-bot",
                "name": "Restricted Bot",
                "mode": "ingest",
                "allowed_surfaces": ["slack"],
            },
        )

        allowed_response = client.post(
            "/api/mirror/events",
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
            "/api/mirror/events",
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

        mirror = client.get("/api/mirror", headers=auth_headers).json()
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


def test_mirror_unrestricted_agent_can_dispatch_any_surface(
    tmp_path: Path,
) -> None:
    root = tmp_path / "service_ops_unrestricted"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="clearwater.example.com",
        mold=ContextMoldConfig(archetype="service_ops"),
        mirror_config=default_mirror_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        client.post(
            "/api/mirror/agents",
            headers=auth_headers,
            json={
                "agent_id": "open-bot",
                "name": "Open Bot",
                "mode": "ingest",
                "allowed_surfaces": [],
            },
        )

        response = client.post(
            "/api/mirror/events",
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

        mirror = client.get("/api/mirror", headers=auth_headers).json()
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
        mirror_config=default_mirror_workspace_config(hero_world="service_ops"),
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    with TestClient(create_twin_gateway_app(root)) as client:
        client.post(
            "/api/mirror/agents",
            headers=auth_headers,
            json={
                "agent_id": "slack-only-bot",
                "name": "Slack Only Bot",
                "mode": "ingest",
                "allowed_surfaces": ["slack"],
            },
        )

        denied_inject = client.post(
            "/api/mirror/events",
            headers=auth_headers,
            json={
                "agent_id": "slack-only-bot",
                "external_tool": "vendor.ticket.inject",
                "focus_hint": "tickets",
                "target": "tickets",
                "payload": {"text": "should be blocked"},
            },
        )
        assert denied_inject.status_code == 202
        assert denied_inject.json()["handled_by"] == "denied"
        assert denied_inject.json()["ok"] is False

        mirror = client.get("/api/mirror", headers=auth_headers).json()
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
