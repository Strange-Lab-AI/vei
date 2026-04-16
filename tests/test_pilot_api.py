from __future__ import annotations

from pathlib import Path

import pytest

from vei.context.models import (
    ContextProviderConfig,
    ContextSnapshot,
    ContextSourceResult,
)
from vei.orchestrators.api import (
    OrchestratorAgent,
    OrchestratorApproval,
    OrchestratorBudgetSummary,
    OrchestratorCommandResult,
    OrchestratorConfig,
    OrchestratorSnapshot,
    OrchestratorSummary,
    OrchestratorSyncCapabilities,
    OrchestratorSyncHealth,
    OrchestratorTask,
)
from vei.pilot import api as pilot_api
from vei.pilot.models import (
    PilotManifest,
    PilotOutcomeSummary,
    PilotRuntime,
    PilotServiceRecord,
    PilotStatus,
)
from vei.twin.models import (
    CompatibilitySurfaceSpec,
    ContextMoldConfig,
    CustomerTwinBundle,
    TwinGatewayConfig,
)
from vei.workspace.api import create_workspace_from_template

GUIDE_FILE = pilot_api.TWIN_LAUNCH_GUIDE_FILE
MANIFEST_FILE = pilot_api.TWIN_LAUNCH_MANIFEST_FILE
ORCHESTRATOR_CACHE_FILE = pilot_api.TWIN_ORCHESTRATOR_CACHE_FILE
ORCHESTRATOR_SYNC_FILE = pilot_api.TWIN_ORCHESTRATOR_SYNC_FILE
RUNTIME_FILE = pilot_api.TWIN_LAUNCH_RUNTIME_FILE


def test_start_pilot_writes_handoff_files_and_status(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "pilot_workspace"
    alive_pids: set[int] = set()

    def fake_spawn(command: list[str], *, log_path: Path) -> int:
        pid = 4100 + len(alive_pids)
        alive_pids.add(pid)
        return pid

    def fake_stop(pid: int) -> None:
        alive_pids.discard(pid)

    def fake_service_alive(service) -> bool:
        return service.pid in alive_pids

    def fake_wait(_: str, *, timeout_s: float = 20.0) -> None:
        return None

    def fake_fetch(url: str):
        if url.endswith("/healthz"):
            return {"ok": True}
        if url.endswith("/api/workspace"):
            return {"manifest": {"name": "pilot_workspace"}}
        if url.endswith("/api/twin"):
            return {
                "runtime": {
                    "run_id": "external_renewal_run",
                    "status": "running",
                    "request_count": 3,
                    "metadata": {
                        "agents": [
                            {
                                "name": "starter-agent",
                                "role": "external-agent",
                                "team": "external",
                                "source": "vei-pilot-client/1.0",
                            }
                        ]
                    },
                },
                "manifest": {
                    "contract": {
                        "ok": False,
                        "issue_count": 2,
                    }
                },
            }
        if url.endswith("/api/twin/history"):
            return [
                {
                    "kind": "workflow_step",
                    "label": "slack.chat.postMessage",
                    "channel": "Communications",
                    "resolved_tool": "slack.send_message",
                    "status": "ok",
                    "object_refs": ["channel:#renewal-watch"],
                    "payload": {
                        "agent": {
                            "name": "starter-agent",
                            "role": "external-agent",
                            "team": "external",
                            "source": "vei-pilot-client/1.0",
                        }
                    },
                }
            ]
        if url.endswith("/api/twin/surfaces"):
            return {
                "current_tension": "Northstar renewal is slipping without a clear owner.",
                "panels": [
                    {"surface": "mail", "title": "Email", "status": "warning"},
                ],
            }
        return None

    monkeypatch.setattr(pilot_api, "_spawn_service", fake_spawn)
    monkeypatch.setattr(pilot_api, "_stop_pid", fake_stop)
    monkeypatch.setattr(pilot_api, "_service_alive", fake_service_alive)
    monkeypatch.setattr(pilot_api, "_wait_for_ready", fake_wait)
    monkeypatch.setattr(pilot_api, "_fetch_json", fake_fetch)

    status = pilot_api.start_pilot(
        root,
        snapshot=_sample_snapshot(),
        gateway_port=3320,
        studio_port=3311,
    )

    assert status.services_ready is True
    assert status.active_run == "external_renewal_run"
    assert status.request_count == 3
    assert status.outcome.issue_count == 2
    assert status.activity[0].tool == "slack.send_message"
    assert status.activity[0].agent_name == "starter-agent"
    assert status.active_agents[0].role == "external-agent"
    assert (root / MANIFEST_FILE).exists()
    assert (root / GUIDE_FILE).exists()
    assert (root / RUNTIME_FILE).exists()

    manifest = pilot_api.load_pilot_manifest(root)
    runtime = pilot_api.load_pilot_runtime(root)
    guide = (root / GUIDE_FILE).read_text(encoding="utf-8")

    assert manifest.organization_name == "Acme Cloud"
    assert manifest.crisis_name
    assert manifest.snippets[0].language == "bash"
    assert manifest.sample_client_path.endswith("examples/governor_client.py")
    assert len(runtime.services) == 2
    assert "python " in guide
    assert "governor_client.py" in guide

    stopped = pilot_api.stop_pilot(root)
    assert stopped.services_ready is False
    assert all(service.state == "stopped" for service in stopped.runtime.services)
    assert alive_pids == set()


def test_ensure_twin_bundle_preserves_provider_capture_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_build_customer_twin(
        root: str | Path,
        *,
        snapshot: ContextSnapshot | None = None,
        provider_configs: list[ContextProviderConfig] | None = None,
        organization_name: str | None = None,
        organization_domain: str = "",
        mold: ContextMoldConfig | None = None,
        mirror_config=None,
        gateway_token: str | None = None,
        overwrite: bool = True,
    ) -> CustomerTwinBundle:
        captured["snapshot"] = snapshot
        captured["provider_configs"] = provider_configs
        captured["organization_name"] = organization_name
        captured["organization_domain"] = organization_domain
        captured["mold"] = mold
        captured["mirror_config"] = mirror_config
        return CustomerTwinBundle(
            workspace_root=Path(root),
            workspace_name="pilot",
            organization_name=organization_name or "Acme Cloud",
            organization_domain=organization_domain,
            mold=mold or ContextMoldConfig(),
            context_snapshot_path="context_snapshot.json",
            blueprint_asset_path="customer.blueprint.json",
            gateway=TwinGatewayConfig(
                auth_token=gateway_token or "pilot-token",
                surfaces=[
                    CompatibilitySurfaceSpec(
                        name="slack",
                        title="Slack",
                        base_path="/slack/api",
                    )
                ],
            ),
            summary="pilot",
        )

    monkeypatch.setattr(pilot_api, "build_customer_twin", fake_build_customer_twin)

    provider_configs = [
        ContextProviderConfig(provider="slack", token_env="VEI_SLACK_TOKEN")
    ]
    bundle = pilot_api._ensure_twin_bundle(
        tmp_path / "provider_pilot",
        snapshot=None,
        provider_configs=provider_configs,
        organization_name="Provider Company",
        organization_domain="provider.example.com",
        archetype="b2b_saas",
        scenario_variant="renewal_save",
        contract_variant="customer_safe_recovery",
        connector_mode="live",
        governor_demo=False,
        governor_demo_interval_ms=2500,
        gateway_token="pilot-token",
        rebuild=True,
    )

    assert bundle.organization_name == "Provider Company"
    assert captured["snapshot"] is None
    assert captured["provider_configs"] == provider_configs
    assert isinstance(captured["mold"], ContextMoldConfig)
    assert captured["mold"].archetype == "b2b_saas"
    assert captured["mirror_config"].connector_mode == "live"
    assert captured["mirror_config"].demo_mode is False


def test_ensure_twin_bundle_rejects_live_demo_combo(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        pilot_api,
        "build_customer_twin",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("build_customer_twin should not run")
        ),
    )

    with pytest.raises(
        ValueError, match="governor demo mode requires connector_mode='sim'"
    ):
        pilot_api._ensure_twin_bundle(
            tmp_path / "provider_pilot",
            snapshot=None,
            provider_configs=[
                ContextProviderConfig(provider="slack", token_env="VEI_SLACK_TOKEN")
            ],
            organization_name="Provider Company",
            organization_domain="provider.example.com",
            archetype="b2b_saas",
            scenario_variant="renewal_save",
            contract_variant="customer_safe_recovery",
            connector_mode="live",
            governor_demo=True,
            governor_demo_interval_ms=2500,
            gateway_token="pilot-token",
            rebuild=True,
        )


def test_ensure_twin_bundle_uses_existing_workspace_identity(
    tmp_path: Path,
) -> None:
    root = tmp_path / "existing_service_ops_workspace"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="service_ops",
    )

    bundle = pilot_api._ensure_twin_bundle(
        root,
        snapshot=None,
        provider_configs=None,
        organization_name=None,
        organization_domain="",
        archetype="service_ops",
        scenario_variant=None,
        contract_variant=None,
        connector_mode="sim",
        governor_demo=False,
        governor_demo_interval_ms=1500,
        gateway_token="pilot-token",
        rebuild=False,
    )

    assert bundle.organization_name == "Clearwater Field Services"
    assert bundle.organization_domain == "clearwater-field-services.example.com"
    assert "Clearwater Field Services" in bundle.summary


def test_start_pilot_rebuild_stops_existing_services_first(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "rebuild_pilot"
    root.mkdir(parents=True, exist_ok=True)
    gateway_port = 49220
    studio_port = 49211
    runtime = PilotRuntime(
        workspace_root=root,
        services=[
            PilotServiceRecord(
                name="gateway",
                host="127.0.0.1",
                port=gateway_port,
                url=f"http://127.0.0.1:{gateway_port}",
                pid=4101,
                state="running",
            ),
            PilotServiceRecord(
                name="studio",
                host="127.0.0.1",
                port=studio_port,
                url=f"http://127.0.0.1:{studio_port}",
                pid=4102,
                state="running",
            ),
        ],
        started_at="2026-03-25T18:00:00+00:00",
        updated_at="2026-03-25T18:00:00+00:00",
    )
    (root / RUNTIME_FILE).write_text(
        runtime.model_dump_json(indent=2),
        encoding="utf-8",
    )

    alive_pids: set[int] = set()
    stopped_roots: list[Path] = []

    def fake_stop_pilot(path: str | Path):
        stopped_roots.append(Path(path))
        stopped_runtime = runtime.model_copy(deep=True)
        for service in stopped_runtime.services:
            service.pid = None
            service.state = "stopped"
        (root / RUNTIME_FILE).write_text(
            stopped_runtime.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return PilotStatus(
            manifest=PilotManifest(
                workspace_root=root,
                workspace_name="pilot",
                organization_name="Pinnacle Analytics",
                organization_domain="pinnacle.example.com",
                archetype="b2b_saas",
                crisis_name="Enterprise Renewal at Risk",
                studio_url=f"http://127.0.0.1:{studio_port}",
                pilot_console_url=f"http://127.0.0.1:{studio_port}/",
                gateway_url=f"http://127.0.0.1:{gateway_port}",
                gateway_status_url=f"http://127.0.0.1:{gateway_port}/api/twin",
                bearer_token="pilot-token",
                recommended_first_exercise="Read Slack first.",
                sample_client_path="/tmp/governor_client.py",
            ),
            runtime=stopped_runtime,
            outcome=PilotOutcomeSummary(
                status="stopped",
                summary="stopped",
            ),
        )

    def fake_ensure(*args, **kwargs):
        return _sample_bundle(root)

    def fake_spawn(command: list[str], *, log_path: Path) -> int:
        pid = 5100 + len(alive_pids)
        alive_pids.add(pid)
        return pid

    def fake_wait(_: str, *, timeout_s: float = 20.0) -> None:
        return None

    def fake_fetch(url: str):
        if url.endswith("/healthz"):
            return {"ok": True}
        if url.endswith("/api/workspace"):
            return {"manifest": {"name": "pilot"}}
        if url.endswith("/api/twin"):
            return {"runtime": {"run_id": "external_run", "status": "running"}}
        if url.endswith("/api/twin/history"):
            return []
        if url.endswith("/api/twin/surfaces"):
            return {"current_tension": "", "panels": []}
        return None

    monkeypatch.setattr(pilot_api, "stop_pilot", fake_stop_pilot)
    monkeypatch.setattr(pilot_api, "_ensure_twin_bundle", fake_ensure)
    monkeypatch.setattr(pilot_api, "_spawn_service", fake_spawn)
    monkeypatch.setattr(pilot_api, "_wait_for_ready", fake_wait)
    monkeypatch.setattr(pilot_api, "_fetch_json", fake_fetch)
    monkeypatch.setattr(
        pilot_api,
        "_service_alive",
        lambda service: service.pid in alive_pids if service.pid is not None else False,
    )

    status = pilot_api.start_pilot(
        root, rebuild=True, gateway_port=gateway_port, studio_port=studio_port
    )

    assert stopped_roots == [root]
    assert status.services_ready is True


def test_build_pilot_status_merges_orchestrator_snapshot_and_syncs_mirror_agents(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "pilot_orchestrator"
    root.mkdir(parents=True, exist_ok=True)
    manifest = PilotManifest(
        workspace_root=root,
        workspace_name="pilot",
        organization_name="Pinnacle Analytics",
        organization_domain="pinnacle.example.com",
        archetype="service_ops",
        crisis_name="VIP outage",
        studio_url="http://127.0.0.1:3011",
        pilot_console_url="http://127.0.0.1:3011/",
        gateway_url="http://127.0.0.1:3020",
        gateway_status_url="http://127.0.0.1:3020/api/twin",
        bearer_token="pilot-token",
        supported_surfaces=[
            CompatibilitySurfaceSpec(
                name="slack", title="Slack", base_path="/slack/api"
            ),
            CompatibilitySurfaceSpec(
                name="jira", title="Jira", base_path="/jira/rest/api/3"
            ),
            CompatibilitySurfaceSpec(
                name="graph", title="Graph", base_path="/graph/v1.0"
            ),
        ],
        recommended_first_exercise="Keep the customer safe.",
        sample_client_path="/tmp/governor_client.py",
        orchestrator=OrchestratorConfig(
            provider="paperclip",
            base_url="http://paperclip.local",
            company_id="company-1",
        ),
    )
    runtime = PilotRuntime(
        workspace_root=root,
        services=[
            PilotServiceRecord(
                name="gateway",
                host="127.0.0.1",
                port=3020,
                url="http://127.0.0.1:3020",
                pid=4101,
                state="running",
            ),
            PilotServiceRecord(
                name="studio",
                host="127.0.0.1",
                port=3011,
                url="http://127.0.0.1:3011",
                pid=4102,
                state="running",
            ),
        ],
        started_at="2026-04-01T10:00:00+00:00",
        updated_at="2026-04-01T10:05:00+00:00",
    )
    (root / MANIFEST_FILE).write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / RUNTIME_FILE).write_text(
        runtime.model_dump_json(indent=2),
        encoding="utf-8",
    )

    snapshot = OrchestratorSnapshot(
        provider="paperclip",
        company_id="company-1",
        fetched_at="2026-04-02T02:10:00+00:00",
        summary=OrchestratorSummary(
            provider="paperclip",
            company_id="company-1",
            company_name="Acme AI",
            agent_counts={"running": 2, "paused": 1},
            task_counts={"in_progress": 2},
            stale_task_count=1,
        ),
        budget=OrchestratorBudgetSummary(
            monthly_budget_cents=5000,
            monthly_spend_cents=2100,
            utilization_ratio=0.42,
        ),
        capabilities=OrchestratorSyncCapabilities(
            can_pause_agents=True,
            can_resume_agents=True,
            routeable_surfaces=["slack", "jira", "graph"],
        ),
        agents=[
            OrchestratorAgent(
                provider="paperclip",
                agent_id="paperclip:eng-1",
                external_agent_id="eng-1",
                name="Backend Engineer",
                role="engineer",
                title="Backend Engineer",
                status="running",
                integration_mode="proxy",
                allowed_surfaces=["slack", "jira"],
                task_ids=["paperclip:issue-1"],
            ),
            OrchestratorAgent(
                provider="homegrown",
                agent_id="homegrown:ana-1",
                external_agent_id="ana-1",
                name="Operations Analyst",
                role="analyst",
                status="running",
                integration_mode="ingest",
                allowed_surfaces=["graph", "unknown"],
                task_ids=["homegrown:task-2"],
            ),
            OrchestratorAgent(
                provider="vendorx",
                agent_id="vendorx:closed-1",
                external_agent_id="closed-1",
                name="Closed Vendor Bot",
                role="vendor",
                status="running",
                integration_mode="observe",
            ),
        ],
        tasks=[
            OrchestratorTask(
                provider="paperclip",
                task_id="paperclip:issue-1",
                external_task_id="issue-1",
                title="Prepare customer-safe update",
                identifier="ACME-1",
                status="in_progress",
                assignee_agent_id="paperclip:eng-1",
                project_name="Bridge",
                goal_name="Launch",
                latest_comment_preview="Need a safe recovery plan before we ship.",
            ),
            OrchestratorTask(
                provider="homegrown",
                task_id="homegrown:task-2",
                external_task_id="task-2",
                title="Collect support context",
                status="in_progress",
                assignee_agent_id="homegrown:ana-1",
            ),
        ],
        approvals=[
            OrchestratorApproval(
                provider="paperclip",
                approval_id="paperclip:approval-1",
                external_approval_id="approval-1",
                approval_type="hire_agent",
                status="pending",
                requested_by_agent_id="paperclip:eng-1",
                requested_by_name="Backend Engineer",
                summary="Hire Founding Engineer · engineer",
                task_ids=["paperclip:issue-1"],
            )
        ],
    )

    class _FakeClient:
        def fetch_snapshot(self) -> OrchestratorSnapshot:
            return snapshot

        def pause_agent(self, _agent_id: str):
            raise AssertionError("pause should not run here")

        def resume_agent(self, _agent_id: str):
            raise AssertionError("resume should not run here")

        def comment_on_task(self, _task_id: str, _body: str):
            raise AssertionError("comment should not run here")

        def approve_approval(
            self, _approval_id: str, *, decision_note: str | None = None
        ):
            raise AssertionError("approve should not run here")

        def reject_approval(
            self, _approval_id: str, *, decision_note: str | None = None
        ):
            raise AssertionError("reject should not run here")

        def request_approval_revision(
            self,
            _approval_id: str,
            *,
            decision_note: str | None = None,
        ):
            raise AssertionError("request revision should not run here")

        def sync_capabilities(self) -> OrchestratorSyncCapabilities:
            return snapshot.capabilities

    captured_sync_payloads: list[dict[str, object]] = []
    captured_workforce_sync_payloads: list[dict[str, object]] = []
    captured_workforce_headers: list[dict[str, str] | None] = []

    def fake_fetch(url: str):
        if url.endswith("/healthz"):
            return {"ok": True}
        if url.endswith("/api/workspace"):
            return {"manifest": {"name": "pilot"}}
        if url.endswith("/api/twin"):
            return {
                "runtime": {
                    "run_id": "external_run",
                    "status": "running",
                    "request_count": 3,
                    "metadata": {
                        "agents": [
                            {
                                "agent_id": "paperclip:eng-1",
                                "name": "Backend Engineer",
                                "role": "engineer",
                                "source": "paperclip",
                            }
                        ]
                    },
                },
                "manifest": {"contract": {"ok": False, "issue_count": 1}},
            }
        if url.endswith("/api/twin/history"):
            return [
                {
                    "kind": "workflow_step",
                    "label": "slack.chat.postMessage",
                    "channel": "Communications",
                    "resolved_tool": "slack.send_message",
                    "status": "ok",
                    "object_refs": ["channel:#dispatch"],
                    "payload": {
                        "agent": {
                            "agent_id": "paperclip:eng-1",
                            "name": "Backend Engineer",
                            "role": "engineer",
                            "source": "paperclip",
                        }
                    },
                }
            ]
        if url.endswith("/api/twin/surfaces"):
            return {"current_tension": "Customer trust is fragile.", "panels": []}
        return None

    def fake_request_json(
        url: str,
        *,
        method: str = "GET",
        payload: dict[str, object] | None = None,
        headers=None,
        timeout_s: float = 4.0,
    ):
        if url.endswith("/api/governor/agents") and method == "GET":
            return {"agents": []}
        if url.endswith("/api/governor/agents"):
            captured_sync_payloads.append(payload or {})
            return {"ok": True}
        if url.endswith("/api/workforce/sync"):
            captured_workforce_sync_payloads.append(payload or {})
            captured_workforce_headers.append(headers)
            return {"ok": True}
        return None

    monkeypatch.setattr(
        pilot_api, "build_orchestrator_client", lambda _config: _FakeClient()
    )
    monkeypatch.setattr(pilot_api, "_fetch_json", fake_fetch)
    monkeypatch.setattr(pilot_api, "_request_json", fake_request_json)
    monkeypatch.setattr(pilot_api, "_service_alive", lambda _service: True)

    status = pilot_api.build_pilot_status(root)

    assert status.orchestrator is not None
    assert status.orchestrator.summary.company_name == "Acme AI"
    assert status.orchestrator_sync is not None
    assert status.orchestrator_sync.status == "healthy"
    assert status.orchestrator_sync.synced_agent_count == 2
    assert [item["agent_id"] for item in captured_sync_payloads] == [
        "paperclip:eng-1",
        "homegrown:ana-1",
    ]
    assert captured_sync_payloads[1]["allowed_surfaces"] == ["graph"]
    assert len(captured_workforce_sync_payloads) == 1
    assert captured_workforce_headers == [{"Authorization": "Bearer pilot-token"}]
    assert captured_workforce_sync_payloads[0]["summary"] == {
        "provider": "paperclip",
        "company_name": "Acme AI",
        "sync_status": "healthy",
        "observed_agent_count": 3,
        "governable_agent_count": 2,
        "steerable_agent_count": 3,
        "active_agent_count": 3,
        "task_count": 2,
        "pending_approval_count": 1,
        "routeable_surface_count": 3,
        "latest_activity_at": None,
        "vei_action_count": 0,
        "downstream_response_count": 0,
        "completed_task_count": 0,
        "approved_count": 0,
    }
    assert "paperclip:issue-1" in status.activity[0].object_refs
    assert status.activity[0].source_label == "VEI"
    assert status.orchestrator.approvals[0].approval_id == "paperclip:approval-1"


def test_build_pilot_status_uses_cached_orchestrator_snapshot_without_refresh(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "pilot_orchestrator_cache"
    root.mkdir(parents=True, exist_ok=True)
    manifest = PilotManifest(
        workspace_root=root,
        workspace_name="pilot",
        organization_name="Pinnacle Analytics",
        organization_domain="pinnacle.example.com",
        archetype="service_ops",
        crisis_name="VIP outage",
        studio_url="http://127.0.0.1:3011",
        pilot_console_url="http://127.0.0.1:3011/",
        gateway_url="http://127.0.0.1:3020",
        gateway_status_url="http://127.0.0.1:3020/api/twin",
        bearer_token="pilot-token",
        recommended_first_exercise="Keep the customer safe.",
        sample_client_path="/tmp/governor_client.py",
        orchestrator=OrchestratorConfig(
            provider="paperclip",
            base_url="http://paperclip.local",
            company_id="company-1",
        ),
    )
    runtime = PilotRuntime(
        workspace_root=root,
        services=[
            PilotServiceRecord(
                name="gateway",
                host="127.0.0.1",
                port=3020,
                url="http://127.0.0.1:3020",
                pid=4101,
                state="running",
            ),
            PilotServiceRecord(
                name="studio",
                host="127.0.0.1",
                port=3011,
                url="http://127.0.0.1:3011",
                pid=4102,
                state="running",
            ),
        ],
    )
    cached_snapshot = OrchestratorSnapshot(
        provider="paperclip",
        company_id="company-1",
        fetched_at="2026-04-02T01:00:00+00:00",
        summary=OrchestratorSummary(
            provider="paperclip",
            company_id="company-1",
            company_name="Cached Company",
        ),
    )
    cached_sync = OrchestratorSyncHealth(
        provider="paperclip",
        status="healthy",
        last_success_at="2026-04-02T01:00:00+00:00",
        message="Cached previously.",
    )
    (root / MANIFEST_FILE).write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / RUNTIME_FILE).write_text(
        runtime.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / ORCHESTRATOR_CACHE_FILE).write_text(
        cached_snapshot.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / ORCHESTRATOR_SYNC_FILE).write_text(
        cached_sync.model_dump_json(indent=2),
        encoding="utf-8",
    )

    class _BrokenClient:
        def fetch_snapshot(self):
            raise AssertionError("fetch_snapshot should not run here")

        def pause_agent(self, _agent_id: str):
            raise AssertionError("pause should not run here")

        def resume_agent(self, _agent_id: str):
            raise AssertionError("resume should not run here")

        def comment_on_task(self, _task_id: str, _body: str):
            raise AssertionError("comment should not run here")

        def approve_approval(
            self, _approval_id: str, *, decision_note: str | None = None
        ):
            raise AssertionError("approve should not run here")

        def reject_approval(
            self, _approval_id: str, *, decision_note: str | None = None
        ):
            raise AssertionError("reject should not run here")

        def request_approval_revision(
            self,
            _approval_id: str,
            *,
            decision_note: str | None = None,
        ):
            raise AssertionError("request revision should not run here")

        def sync_capabilities(self):
            return OrchestratorSyncCapabilities()

    def fake_fetch(url: str):
        if url.endswith("/healthz"):
            return {"ok": True}
        if url.endswith("/api/workspace"):
            return {"manifest": {"name": "pilot"}}
        if url.endswith("/api/twin"):
            return {"runtime": {"run_id": "external_run", "status": "running"}}
        if url.endswith("/api/twin/history"):
            return []
        if url.endswith("/api/twin/surfaces"):
            return {"current_tension": "", "panels": []}
        return None

    monkeypatch.setattr(
        pilot_api, "build_orchestrator_client", lambda _config: _BrokenClient()
    )
    monkeypatch.setattr(pilot_api, "_fetch_json", fake_fetch)
    monkeypatch.setattr(pilot_api, "_service_alive", lambda _service: True)

    status = pilot_api.build_pilot_status(root)

    assert status.orchestrator is not None
    assert status.orchestrator.summary.company_name == "Cached Company"
    assert status.orchestrator_sync is not None
    assert status.orchestrator_sync.status == "healthy"
    assert status.orchestrator_sync.cache_used is False


def test_build_pilot_status_force_sync_uses_cached_orchestrator_snapshot_when_refresh_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "pilot_orchestrator_cache_force_sync"
    root.mkdir(parents=True, exist_ok=True)
    manifest = PilotManifest(
        workspace_root=root,
        workspace_name="pilot",
        organization_name="Pinnacle Analytics",
        organization_domain="pinnacle.example.com",
        archetype="service_ops",
        crisis_name="VIP outage",
        studio_url="http://127.0.0.1:3011",
        pilot_console_url="http://127.0.0.1:3011/",
        gateway_url="http://127.0.0.1:3020",
        gateway_status_url="http://127.0.0.1:3020/api/twin",
        bearer_token="pilot-token",
        recommended_first_exercise="Keep the customer safe.",
        sample_client_path="/tmp/governor_client.py",
        orchestrator=OrchestratorConfig(
            provider="paperclip",
            base_url="http://paperclip.local",
            company_id="company-1",
        ),
    )
    runtime = PilotRuntime(
        workspace_root=root,
        services=[
            PilotServiceRecord(
                name="gateway",
                host="127.0.0.1",
                port=3020,
                url="http://127.0.0.1:3020",
                pid=4101,
                state="running",
            ),
            PilotServiceRecord(
                name="studio",
                host="127.0.0.1",
                port=3011,
                url="http://127.0.0.1:3011",
                pid=4102,
                state="running",
            ),
        ],
    )
    cached_snapshot = OrchestratorSnapshot(
        provider="paperclip",
        company_id="company-1",
        fetched_at="2026-04-02T01:00:00+00:00",
        summary=OrchestratorSummary(
            provider="paperclip",
            company_id="company-1",
            company_name="Cached Company",
        ),
    )
    cached_sync = OrchestratorSyncHealth(
        provider="paperclip",
        status="healthy",
        last_success_at="2026-04-02T01:00:00+00:00",
        message="Cached previously.",
    )
    (root / MANIFEST_FILE).write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / RUNTIME_FILE).write_text(
        runtime.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / ORCHESTRATOR_CACHE_FILE).write_text(
        cached_snapshot.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / ORCHESTRATOR_SYNC_FILE).write_text(
        cached_sync.model_dump_json(indent=2),
        encoding="utf-8",
    )

    class _BrokenClient:
        def fetch_snapshot(self):
            raise RuntimeError("paperclip is down")

        def pause_agent(self, _agent_id: str):
            raise AssertionError("pause should not run here")

        def resume_agent(self, _agent_id: str):
            raise AssertionError("resume should not run here")

        def comment_on_task(self, _task_id: str, _body: str):
            raise AssertionError("comment should not run here")

        def approve_approval(
            self, _approval_id: str, *, decision_note: str | None = None
        ):
            raise AssertionError("approve should not run here")

        def reject_approval(
            self, _approval_id: str, *, decision_note: str | None = None
        ):
            raise AssertionError("reject should not run here")

        def request_approval_revision(
            self,
            _approval_id: str,
            *,
            decision_note: str | None = None,
        ):
            raise AssertionError("request revision should not run here")

        def sync_capabilities(self):
            return OrchestratorSyncCapabilities()

    def fake_fetch(url: str):
        if url.endswith("/healthz"):
            return {"ok": True}
        if url.endswith("/api/workspace"):
            return {"manifest": {"name": "pilot"}}
        if url.endswith("/api/twin"):
            return {"runtime": {"run_id": "external_run", "status": "running"}}
        if url.endswith("/api/twin/history"):
            return []
        if url.endswith("/api/twin/surfaces"):
            return {"current_tension": "", "panels": []}
        return None

    monkeypatch.setattr(
        pilot_api, "build_orchestrator_client", lambda _config: _BrokenClient()
    )
    monkeypatch.setattr(pilot_api, "_fetch_json", fake_fetch)
    monkeypatch.setattr(pilot_api, "_service_alive", lambda _service: True)

    status = pilot_api.build_pilot_status(root, force_orchestrator_sync=True)

    assert status.orchestrator is not None
    assert status.orchestrator.summary.company_name == "Cached Company"
    assert status.orchestrator_sync is not None
    assert status.orchestrator_sync.status == "stale"
    assert status.orchestrator_sync.cache_used is True


def test_build_pilot_status_prunes_stale_orchestrator_agents_from_mirror(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "pilot_orchestrator_prune"
    root.mkdir(parents=True, exist_ok=True)
    manifest = PilotManifest(
        workspace_root=root,
        workspace_name="pilot",
        organization_name="Pinnacle Analytics",
        organization_domain="pinnacle.example.com",
        archetype="service_ops",
        crisis_name="VIP outage",
        studio_url="http://127.0.0.1:3011",
        pilot_console_url="http://127.0.0.1:3011/",
        gateway_url="http://127.0.0.1:3020",
        gateway_status_url="http://127.0.0.1:3020/api/twin",
        bearer_token="pilot-token",
        supported_surfaces=[
            CompatibilitySurfaceSpec(
                name="slack", title="Slack", base_path="/slack/api"
            )
        ],
        recommended_first_exercise="Keep the customer safe.",
        sample_client_path="/tmp/governor_client.py",
        orchestrator=OrchestratorConfig(
            provider="paperclip",
            base_url="http://paperclip.local",
            company_id="company-2",
        ),
    )
    runtime = PilotRuntime(
        workspace_root=root,
        services=[
            PilotServiceRecord(
                name="gateway",
                host="127.0.0.1",
                port=3020,
                url="http://127.0.0.1:3020",
                pid=4101,
                state="running",
            ),
            PilotServiceRecord(
                name="studio",
                host="127.0.0.1",
                port=3011,
                url="http://127.0.0.1:3011",
                pid=4102,
                state="running",
            ),
        ],
    )
    previous_snapshot = OrchestratorSnapshot(
        provider="paperclip",
        company_id="company-2",
        fetched_at="2026-04-02T01:00:00+00:00",
        summary=OrchestratorSummary(
            provider="paperclip",
            company_id="company-2",
            company_name="Acme AI",
        ),
        agents=[
            OrchestratorAgent(
                provider="paperclip",
                agent_id="paperclip:eng-1",
                external_agent_id="eng-1",
                name="Backend Engineer",
                integration_mode="proxy",
            ),
            OrchestratorAgent(
                provider="paperclip",
                agent_id="paperclip:eng-2",
                external_agent_id="eng-2",
                name="Removed Engineer",
                integration_mode="proxy",
            ),
        ],
    )
    (root / MANIFEST_FILE).write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / RUNTIME_FILE).write_text(
        runtime.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / ORCHESTRATOR_CACHE_FILE).write_text(
        previous_snapshot.model_dump_json(indent=2),
        encoding="utf-8",
    )

    current_snapshot = OrchestratorSnapshot(
        provider="paperclip",
        company_id="company-2",
        fetched_at="2026-04-03T01:00:00+00:00",
        summary=OrchestratorSummary(
            provider="paperclip",
            company_id="company-2",
            company_name="Acme AI",
        ),
        agents=[
            OrchestratorAgent(
                provider="paperclip",
                agent_id="paperclip:eng-1",
                external_agent_id="eng-1",
                name="Backend Engineer",
                integration_mode="proxy",
            )
        ],
    )

    class _FakeClient:
        def fetch_snapshot(self) -> OrchestratorSnapshot:
            return current_snapshot

        def pause_agent(self, _agent_id: str):
            raise AssertionError("pause should not run here")

        def resume_agent(self, _agent_id: str):
            raise AssertionError("resume should not run here")

        def comment_on_task(self, _task_id: str, _body: str):
            raise AssertionError("comment should not run here")

        def approve_approval(
            self, _approval_id: str, *, decision_note: str | None = None
        ):
            raise AssertionError("approve should not run here")

        def reject_approval(
            self, _approval_id: str, *, decision_note: str | None = None
        ):
            raise AssertionError("reject should not run here")

        def request_approval_revision(
            self,
            _approval_id: str,
            *,
            decision_note: str | None = None,
        ):
            raise AssertionError("request revision should not run here")

        def sync_capabilities(self) -> OrchestratorSyncCapabilities:
            return current_snapshot.capabilities

    captured_posts: list[dict[str, object]] = []
    captured_deletes: list[str] = []

    def fake_fetch(url: str):
        if url.endswith("/healthz"):
            return {"ok": True}
        if url.endswith("/api/workspace"):
            return {"manifest": {"name": "pilot"}}
        if url.endswith("/api/twin"):
            return {"runtime": {"run_id": "external_run", "status": "running"}}
        if url.endswith("/api/twin/history"):
            return []
        if url.endswith("/api/twin/surfaces"):
            return {"current_tension": "", "panels": []}
        return None

    def fake_request_json(
        url: str,
        *,
        method: str = "GET",
        payload: dict[str, object] | None = None,
        headers=None,
        timeout_s: float = 4.0,
    ):
        del headers, timeout_s
        if url.endswith("/api/governor/agents") and method == "GET":
            return {
                "agents": [
                    {
                        "agent_id": "paperclip:eng-1",
                        "metadata": {"managed_by": "pilot_orchestrator_bridge"},
                    },
                    {
                        "agent_id": "paperclip:eng-2",
                        "metadata": {"managed_by": "pilot_orchestrator_bridge"},
                    },
                    {"agent_id": "starter-agent", "metadata": {}},
                ]
            }
        if url.endswith("/api/governor/agents") and method == "POST":
            captured_posts.append(payload or {})
            return {"ok": True}
        if (
            url.endswith("/api/governor/agents/paperclip%3Aeng-2")
            and method == "DELETE"
        ):
            captured_deletes.append(url)
            return {"ok": True}
        if url.endswith("/api/workforce/sync"):
            return {"ok": True}
        return None

    monkeypatch.setattr(
        pilot_api, "build_orchestrator_client", lambda _config: _FakeClient()
    )
    monkeypatch.setattr(pilot_api, "_fetch_json", fake_fetch)
    monkeypatch.setattr(pilot_api, "_request_json", fake_request_json)
    monkeypatch.setattr(pilot_api, "_service_alive", lambda _service: True)

    status = pilot_api.build_pilot_status(root, force_orchestrator_sync=True)

    assert status.orchestrator is not None
    assert [item["agent_id"] for item in captured_posts] == ["paperclip:eng-1"]
    assert captured_deletes == [
        "http://127.0.0.1:3020/api/governor/agents/paperclip%3Aeng-2"
    ]


def test_start_pilot_updates_live_manifest_with_new_orchestrator_config(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "pilot_live_manifest"
    root.mkdir(parents=True, exist_ok=True)
    manifest = PilotManifest(
        workspace_root=root,
        workspace_name="pilot",
        organization_name="Pinnacle Analytics",
        organization_domain="pinnacle.example.com",
        archetype="b2b_saas",
        crisis_name="VIP outage",
        studio_url="http://127.0.0.1:3011",
        pilot_console_url="http://127.0.0.1:3011/",
        gateway_url="http://127.0.0.1:3020",
        gateway_status_url="http://127.0.0.1:3020/api/twin",
        bearer_token="pilot-token",
        supported_surfaces=[
            CompatibilitySurfaceSpec(
                name="slack", title="Slack", base_path="/slack/api"
            )
        ],
        recommended_first_exercise="Keep the customer safe.",
        sample_client_path="/tmp/governor_client.py",
        orchestrator=OrchestratorConfig(
            provider="paperclip",
            base_url="http://paperclip.old",
            company_id="company-old",
        ),
    )
    runtime = PilotRuntime(
        workspace_root=root,
        services=[
            PilotServiceRecord(
                name="gateway",
                host="127.0.0.1",
                port=3020,
                url="http://127.0.0.1:3020",
                pid=4101,
                state="running",
            ),
            PilotServiceRecord(
                name="studio",
                host="127.0.0.1",
                port=3011,
                url="http://127.0.0.1:3011",
                pid=4102,
                state="running",
            ),
        ],
    )
    (root / MANIFEST_FILE).write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / RUNTIME_FILE).write_text(
        runtime.model_dump_json(indent=2),
        encoding="utf-8",
    )

    expected_status = PilotStatus(
        manifest=manifest,
        runtime=runtime,
        outcome=PilotOutcomeSummary(status="running", summary="ok"),
    )
    captured_force_sync: list[bool] = []

    monkeypatch.setattr(
        pilot_api,
        "_ensure_twin_bundle",
        lambda *args, **kwargs: _sample_bundle(root),
    )
    monkeypatch.setattr(pilot_api, "_service_alive", lambda _service: True)
    monkeypatch.setattr(
        pilot_api,
        "_fetch_json",
        lambda url: (
            {"ok": True}
            if url.endswith("/healthz")
            else (
                {"manifest": {"name": "pilot"}}
                if url.endswith("/api/workspace")
                else None
            )
        ),
    )

    def fake_build_pilot_status(*_args, **kwargs):
        captured_force_sync.append(kwargs.get("force_orchestrator_sync", False))
        return expected_status

    monkeypatch.setattr(
        pilot_api,
        "build_pilot_status",
        fake_build_pilot_status,
    )

    status = pilot_api.start_pilot(
        root,
        orchestrator="paperclip",
        orchestrator_url="http://paperclip.new",
        orchestrator_company_id="company-new",
    )

    updated_manifest = pilot_api.load_pilot_manifest(root)
    guide = (root / GUIDE_FILE).read_text(encoding="utf-8")

    assert status == expected_status
    assert captured_force_sync == [True]
    assert updated_manifest.orchestrator is not None
    assert updated_manifest.orchestrator.base_url == "http://paperclip.new"
    assert updated_manifest.orchestrator.company_id == "company-new"
    assert "http://paperclip.new" in guide
    assert "company-new" in guide


def test_pause_pilot_orchestrator_agent_raises_when_workforce_recording_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "pilot_pause_record_failure"
    root.mkdir(parents=True, exist_ok=True)
    manifest = PilotManifest(
        workspace_root=root,
        workspace_name="pilot",
        organization_name="Pinnacle Analytics",
        organization_domain="pinnacle.example.com",
        archetype="service_ops",
        crisis_name="VIP outage",
        studio_url="http://127.0.0.1:3011",
        pilot_console_url="http://127.0.0.1:3011/",
        gateway_url="http://127.0.0.1:3020",
        gateway_status_url="http://127.0.0.1:3020/api/twin",
        bearer_token="pilot-token",
        recommended_first_exercise="Keep the customer safe.",
        sample_client_path="/tmp/governor_client.py",
        orchestrator=OrchestratorConfig(
            provider="paperclip",
            base_url="http://paperclip.local",
            company_id="company-1",
        ),
    )
    snapshot = OrchestratorSnapshot(
        provider="paperclip",
        company_id="company-1",
        fetched_at="2026-04-02T01:00:00+00:00",
        summary=OrchestratorSummary(
            provider="paperclip",
            company_id="company-1",
            company_name="Acme AI",
        ),
        agents=[
            OrchestratorAgent(
                provider="paperclip",
                agent_id="paperclip:eng-1",
                external_agent_id="eng-1",
                name="Backend Engineer",
            )
        ],
    )
    (root / MANIFEST_FILE).write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / ORCHESTRATOR_CACHE_FILE).write_text(
        snapshot.model_dump_json(indent=2),
        encoding="utf-8",
    )

    class _FakeClient:
        def fetch_snapshot(self):
            raise AssertionError("fetch_snapshot should not run here")

        def pause_agent(self, agent_id: str):
            return OrchestratorCommandResult(
                provider="paperclip",
                action="pause",
                agent_id="paperclip:eng-1",
                external_agent_id=agent_id,
                message="Agent paused.",
            )

        def resume_agent(self, agent_id: str):
            raise AssertionError("resume should not run here")

        def comment_on_task(self, _task_id: str, _body: str):
            raise AssertionError("comment should not run here")

        def approve_approval(
            self, _approval_id: str, *, decision_note: str | None = None
        ):
            raise AssertionError("approve should not run here")

        def reject_approval(
            self, _approval_id: str, *, decision_note: str | None = None
        ):
            raise AssertionError("reject should not run here")

        def request_approval_revision(
            self,
            _approval_id: str,
            *,
            decision_note: str | None = None,
        ):
            raise AssertionError("request revision should not run here")

        def sync_capabilities(self):
            return OrchestratorSyncCapabilities()

    monkeypatch.setattr(
        pilot_api, "build_orchestrator_client", lambda _config: _FakeClient()
    )
    monkeypatch.setattr(pilot_api, "_post_json", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="could not record it"):
        pilot_api.pause_pilot_orchestrator_agent(root, "paperclip:eng-1")


def test_build_pilot_status_merges_vei_workforce_commands_into_activity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "pilot_workforce_activity"
    root.mkdir(parents=True, exist_ok=True)
    manifest = PilotManifest(
        workspace_root=root,
        workspace_name="pilot",
        organization_name="Pinnacle Analytics",
        organization_domain="pinnacle.example.com",
        archetype="service_ops",
        crisis_name="VIP outage",
        studio_url="http://127.0.0.1:3011",
        pilot_console_url="http://127.0.0.1:3011/",
        gateway_url="http://127.0.0.1:3020",
        gateway_status_url="http://127.0.0.1:3020/api/twin",
        bearer_token="pilot-token",
        recommended_first_exercise="Keep the customer safe.",
        sample_client_path="/tmp/governor_client.py",
    )
    runtime = PilotRuntime(
        workspace_root=root,
        services=[
            PilotServiceRecord(
                name="gateway",
                host="127.0.0.1",
                port=3020,
                url="http://127.0.0.1:3020",
                pid=4101,
                state="running",
            ),
            PilotServiceRecord(
                name="studio",
                host="127.0.0.1",
                port=3011,
                url="http://127.0.0.1:3011",
                pid=4102,
                state="running",
            ),
        ],
    )
    (root / MANIFEST_FILE).write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / RUNTIME_FILE).write_text(
        runtime.model_dump_json(indent=2),
        encoding="utf-8",
    )

    captured_request_headers: list[dict[str, str] | None] = []

    def fake_fetch(url: str):
        if url.endswith("/api/twin"):
            return {"runtime": {"run_id": "external_run", "status": "running"}}
        if url.endswith("/api/twin/history"):
            return []
        if url.endswith("/api/twin/surfaces"):
            return {"current_tension": "", "panels": []}
        return None

    def fake_request_json(
        url: str,
        *,
        method: str = "GET",
        payload: dict[str, object] | None = None,
        headers=None,
        timeout_s: float = 4.0,
    ):
        del method, payload, timeout_s
        if url.endswith("/api/workforce"):
            captured_request_headers.append(headers)
            return {
                "commands": [
                    {
                        "provider": "paperclip",
                        "action": "comment_task",
                        "created_at": "2026-04-03T18:20:00+00:00",
                        "message": "Guidance posted.",
                        "task_id": "paperclip:issue-7",
                        "external_task_id": "issue-7",
                        "decision_note": "Ask for a budget check before moving ahead.",
                    }
                ]
            }
        return None

    monkeypatch.setattr(pilot_api, "_fetch_json", fake_fetch)
    monkeypatch.setattr(pilot_api, "_request_json", fake_request_json)
    monkeypatch.setattr(pilot_api, "_service_alive", lambda _service: True)

    status = pilot_api.build_pilot_status(root)

    assert captured_request_headers == [{"Authorization": "Bearer pilot-token"}]
    assert status.activity[0].source_label == "VEI"
    assert status.activity[0].channel == "VEI"
    assert status.activity[0].label == "Guided task"
    assert status.activity[0].agent_name == "VEI operator"
    assert status.activity[0].tool == "paperclip.comment_on_task"
    assert status.activity[0].detail == (
        "Guidance posted. Ask for a budget check before moving ahead."
    )
    assert status.activity[0].object_refs == ["paperclip:issue-7", "issue-7"]


def test_comment_on_pilot_orchestrator_task_posts_guidance_and_refreshes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "pilot_comment_task"
    root.mkdir(parents=True, exist_ok=True)
    manifest = PilotManifest(
        workspace_root=root,
        workspace_name="pilot",
        organization_name="Pinnacle Analytics",
        organization_domain="pinnacle.example.com",
        archetype="service_ops",
        crisis_name="VIP outage",
        studio_url="http://127.0.0.1:3011",
        pilot_console_url="http://127.0.0.1:3011/",
        gateway_url="http://127.0.0.1:3020",
        gateway_status_url="http://127.0.0.1:3020/api/twin",
        bearer_token="pilot-token",
        recommended_first_exercise="Keep the customer safe.",
        sample_client_path="/tmp/governor_client.py",
        orchestrator=OrchestratorConfig(
            provider="paperclip",
            base_url="http://paperclip.local",
            company_id="company-1",
        ),
    )
    snapshot = OrchestratorSnapshot(
        provider="paperclip",
        company_id="company-1",
        fetched_at="2026-04-02T01:00:00+00:00",
        summary=OrchestratorSummary(
            provider="paperclip",
            company_id="company-1",
            company_name="Acme AI",
        ),
        tasks=[
            OrchestratorTask(
                provider="paperclip",
                task_id="paperclip:issue-1",
                external_task_id="issue-1",
                title="Ship orchestrator bridge",
            )
        ],
    )
    (root / MANIFEST_FILE).write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / ORCHESTRATOR_CACHE_FILE).write_text(
        snapshot.model_dump_json(indent=2),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}
    posted_commands: list[dict[str, object]] = []

    class _FakeClient:
        def fetch_snapshot(self):
            raise AssertionError("fetch_snapshot should not run here")

        def pause_agent(self, _agent_id: str):
            raise AssertionError("pause should not run here")

        def resume_agent(self, _agent_id: str):
            raise AssertionError("resume should not run here")

        def comment_on_task(self, task_id: str, body: str):
            captured["task_id"] = task_id
            captured["body"] = body
            return OrchestratorCommandResult(
                provider="paperclip",
                action="comment_task",
                task_id="paperclip:issue-1",
                external_task_id=task_id,
                message="Guidance posted.",
            )

        def approve_approval(
            self, _approval_id: str, *, decision_note: str | None = None
        ):
            raise AssertionError("approve should not run here")

        def reject_approval(
            self, _approval_id: str, *, decision_note: str | None = None
        ):
            raise AssertionError("reject should not run here")

        def request_approval_revision(
            self,
            _approval_id: str,
            *,
            decision_note: str | None = None,
        ):
            raise AssertionError("request revision should not run here")

        def sync_capabilities(self):
            return OrchestratorSyncCapabilities()

    expected_status = PilotStatus(
        manifest=manifest,
        runtime=PilotRuntime(workspace_root=root),
        outcome=PilotOutcomeSummary(status="running", summary="ok"),
    )

    monkeypatch.setattr(
        pilot_api, "build_orchestrator_client", lambda _config: _FakeClient()
    )
    monkeypatch.setattr(
        pilot_api, "build_pilot_status", lambda *_args, **_kwargs: expected_status
    )
    monkeypatch.setattr(
        pilot_api,
        "_post_json",
        lambda url, *, payload, headers=None: posted_commands.append(
            {"url": url, "payload": payload, "headers": headers}
        )
        or {"ok": True},
    )

    status = pilot_api.comment_on_pilot_orchestrator_task(
        root,
        "paperclip:issue-1",
        body="  Ask for a risk review before shipping.  ",
    )

    assert status == expected_status
    assert captured == {
        "task_id": "issue-1",
        "body": "Ask for a risk review before shipping.",
    }
    assert posted_commands == [
        {
            "url": "http://127.0.0.1:3020/api/workforce/commands",
            "headers": {"Authorization": "Bearer pilot-token"},
            "payload": {
                "provider": "paperclip",
                "action": "comment_task",
                "created_at": posted_commands[0]["payload"]["created_at"],
                "message": "Guidance posted.",
                "agent_id": None,
                "external_agent_id": None,
                "task_id": "paperclip:issue-1",
                "external_task_id": "issue-1",
                "approval_id": None,
                "external_approval_id": None,
                "comment_id": None,
                "decision_note": "Ask for a risk review before shipping.",
            },
        }
    ]


@pytest.mark.parametrize(
    ("action_name", "method_name"),
    [
        ("approve", "approve_approval"),
        ("reject", "reject_approval"),
        ("request_revision", "request_approval_revision"),
    ],
)
def test_pilot_orchestrator_approval_actions_refresh_status(
    tmp_path: Path,
    monkeypatch,
    action_name: str,
    method_name: str,
) -> None:
    root = tmp_path / f"pilot_{action_name}_approval"
    root.mkdir(parents=True, exist_ok=True)
    manifest = PilotManifest(
        workspace_root=root,
        workspace_name="pilot",
        organization_name="Pinnacle Analytics",
        organization_domain="pinnacle.example.com",
        archetype="service_ops",
        crisis_name="VIP outage",
        studio_url="http://127.0.0.1:3011",
        pilot_console_url="http://127.0.0.1:3011/",
        gateway_url="http://127.0.0.1:3020",
        gateway_status_url="http://127.0.0.1:3020/api/twin",
        bearer_token="pilot-token",
        recommended_first_exercise="Keep the customer safe.",
        sample_client_path="/tmp/governor_client.py",
        orchestrator=OrchestratorConfig(
            provider="paperclip",
            base_url="http://paperclip.local",
            company_id="company-1",
        ),
    )
    snapshot = OrchestratorSnapshot(
        provider="paperclip",
        company_id="company-1",
        fetched_at="2026-04-02T01:00:00+00:00",
        summary=OrchestratorSummary(
            provider="paperclip",
            company_id="company-1",
            company_name="Acme AI",
        ),
        approvals=[
            OrchestratorApproval(
                provider="paperclip",
                approval_id="paperclip:approval-1",
                external_approval_id="approval-1",
                approval_type="hire_agent",
            )
        ],
    )
    (root / MANIFEST_FILE).write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / ORCHESTRATOR_CACHE_FILE).write_text(
        snapshot.model_dump_json(indent=2),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}
    posted_commands: list[dict[str, object]] = []

    class _FakeClient:
        def fetch_snapshot(self):
            raise AssertionError("fetch_snapshot should not run here")

        def pause_agent(self, _agent_id: str):
            raise AssertionError("pause should not run here")

        def resume_agent(self, _agent_id: str):
            raise AssertionError("resume should not run here")

        def comment_on_task(self, _task_id: str, _body: str):
            raise AssertionError("comment should not run here")

        def approve_approval(
            self, approval_id: str, *, decision_note: str | None = None
        ):
            captured["action"] = "approve"
            captured["approval_id"] = approval_id
            captured["decision_note"] = decision_note
            return OrchestratorCommandResult(
                provider="paperclip",
                action="approve",
                approval_id="paperclip:approval-1",
                external_approval_id=approval_id,
                message="Approval accepted.",
            )

        def reject_approval(
            self, approval_id: str, *, decision_note: str | None = None
        ):
            captured["action"] = "reject"
            captured["approval_id"] = approval_id
            captured["decision_note"] = decision_note
            return OrchestratorCommandResult(
                provider="paperclip",
                action="reject",
                approval_id="paperclip:approval-1",
                external_approval_id=approval_id,
                message="Approval rejected.",
            )

        def request_approval_revision(
            self,
            approval_id: str,
            *,
            decision_note: str | None = None,
        ):
            captured["action"] = "request_revision"
            captured["approval_id"] = approval_id
            captured["decision_note"] = decision_note
            return OrchestratorCommandResult(
                provider="paperclip",
                action="request_revision",
                approval_id="paperclip:approval-1",
                external_approval_id=approval_id,
                message="Revision requested.",
            )

        def sync_capabilities(self):
            return OrchestratorSyncCapabilities()

    expected_status = PilotStatus(
        manifest=manifest,
        runtime=PilotRuntime(workspace_root=root),
        outcome=PilotOutcomeSummary(status="running", summary="ok"),
    )

    monkeypatch.setattr(
        pilot_api, "build_orchestrator_client", lambda _config: _FakeClient()
    )
    monkeypatch.setattr(
        pilot_api, "build_pilot_status", lambda *_args, **_kwargs: expected_status
    )
    monkeypatch.setattr(
        pilot_api,
        "_post_json",
        lambda url, *, payload, headers=None: posted_commands.append(
            {"url": url, "payload": payload, "headers": headers}
        )
        or {"ok": True},
    )

    status = getattr(pilot_api, f"{action_name}_pilot_orchestrator_approval")(
        root,
        "paperclip:approval-1",
        decision_note="Need a clearer staffing plan.",
    )

    assert status == expected_status
    assert captured == {
        "action": method_name.replace("_approval", ""),
        "approval_id": "approval-1",
        "decision_note": "Need a clearer staffing plan.",
    }
    assert posted_commands == [
        {
            "url": "http://127.0.0.1:3020/api/workforce/commands",
            "headers": {"Authorization": "Bearer pilot-token"},
            "payload": {
                "provider": "paperclip",
                "action": action_name,
                "created_at": posted_commands[0]["payload"]["created_at"],
                "message": {
                    "approve": "Approval accepted.",
                    "reject": "Approval rejected.",
                    "request_revision": "Revision requested.",
                }[action_name],
                "agent_id": None,
                "external_agent_id": None,
                "task_id": None,
                "external_task_id": None,
                "approval_id": "paperclip:approval-1",
                "external_approval_id": "approval-1",
                "comment_id": None,
                "decision_note": "Need a clearer staffing plan.",
            },
        }
    ]


@pytest.mark.parametrize("action_name", ["pause", "resume"])
def test_pilot_orchestrator_agent_actions_record_workforce_command(
    tmp_path: Path,
    monkeypatch,
    action_name: str,
) -> None:
    root = tmp_path / f"pilot_{action_name}_agent"
    root.mkdir(parents=True, exist_ok=True)
    manifest = PilotManifest(
        workspace_root=root,
        workspace_name="pilot",
        organization_name="Pinnacle Analytics",
        organization_domain="pinnacle.example.com",
        archetype="service_ops",
        crisis_name="VIP outage",
        studio_url="http://127.0.0.1:3011",
        pilot_console_url="http://127.0.0.1:3011/",
        gateway_url="http://127.0.0.1:3020",
        gateway_status_url="http://127.0.0.1:3020/api/twin",
        bearer_token="pilot-token",
        recommended_first_exercise="Keep the customer safe.",
        sample_client_path="/tmp/governor_client.py",
        orchestrator=OrchestratorConfig(
            provider="paperclip",
            base_url="http://paperclip.local",
            company_id="company-1",
        ),
    )
    snapshot = OrchestratorSnapshot(
        provider="paperclip",
        company_id="company-1",
        fetched_at="2026-04-02T01:00:00+00:00",
        summary=OrchestratorSummary(
            provider="paperclip",
            company_id="company-1",
            company_name="Acme AI",
        ),
        agents=[
            OrchestratorAgent(
                provider="paperclip",
                agent_id="paperclip:eng-1",
                external_agent_id="eng-1",
                name="Backend Engineer",
            )
        ],
    )
    (root / MANIFEST_FILE).write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (root / ORCHESTRATOR_CACHE_FILE).write_text(
        snapshot.model_dump_json(indent=2),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}
    posted_commands: list[dict[str, object]] = []

    class _FakeClient:
        def fetch_snapshot(self):
            raise AssertionError("fetch_snapshot should not run here")

        def pause_agent(self, agent_id: str):
            captured["action"] = "pause"
            captured["agent_id"] = agent_id
            return OrchestratorCommandResult(
                provider="paperclip",
                action="pause",
                agent_id="paperclip:eng-1",
                external_agent_id=agent_id,
                message="Agent paused.",
            )

        def resume_agent(self, agent_id: str):
            captured["action"] = "resume"
            captured["agent_id"] = agent_id
            return OrchestratorCommandResult(
                provider="paperclip",
                action="resume",
                agent_id="paperclip:eng-1",
                external_agent_id=agent_id,
                message="Agent resumed.",
            )

        def comment_on_task(self, _task_id: str, _body: str):
            raise AssertionError("comment should not run here")

        def approve_approval(
            self, _approval_id: str, *, decision_note: str | None = None
        ):
            raise AssertionError("approve should not run here")

        def reject_approval(
            self, _approval_id: str, *, decision_note: str | None = None
        ):
            raise AssertionError("reject should not run here")

        def request_approval_revision(
            self,
            _approval_id: str,
            *,
            decision_note: str | None = None,
        ):
            raise AssertionError("request revision should not run here")

        def sync_capabilities(self):
            return OrchestratorSyncCapabilities()

    expected_status = PilotStatus(
        manifest=manifest,
        runtime=PilotRuntime(workspace_root=root),
        outcome=PilotOutcomeSummary(status="running", summary="ok"),
    )

    monkeypatch.setattr(
        pilot_api, "build_orchestrator_client", lambda _config: _FakeClient()
    )
    monkeypatch.setattr(
        pilot_api, "build_pilot_status", lambda *_args, **_kwargs: expected_status
    )
    monkeypatch.setattr(
        pilot_api,
        "_post_json",
        lambda url, *, payload, headers=None: posted_commands.append(
            {"url": url, "payload": payload, "headers": headers}
        )
        or {"ok": True},
    )

    status = getattr(pilot_api, f"{action_name}_pilot_orchestrator_agent")(
        root,
        "paperclip:eng-1",
    )

    assert status == expected_status
    assert captured == {
        "action": action_name,
        "agent_id": "eng-1",
    }
    assert posted_commands == [
        {
            "url": "http://127.0.0.1:3020/api/workforce/commands",
            "headers": {"Authorization": "Bearer pilot-token"},
            "payload": {
                "provider": "paperclip",
                "action": action_name,
                "created_at": posted_commands[0]["payload"]["created_at"],
                "message": {
                    "pause": "Agent paused.",
                    "resume": "Agent resumed.",
                }[action_name],
                "agent_id": "paperclip:eng-1",
                "external_agent_id": "eng-1",
                "task_id": None,
                "external_task_id": None,
                "approval_id": None,
                "external_approval_id": None,
                "comment_id": None,
                "decision_note": None,
            },
        }
    ]


def _sample_snapshot() -> ContextSnapshot:
    return ContextSnapshot(
        organization_name="Acme Cloud",
        organization_domain="acme.ai",
        captured_at="2026-03-25T16:00:00+00:00",
        sources=[
            ContextSourceResult(
                provider="slack",
                captured_at="2026-03-25T16:00:00+00:00",
                status="ok",
                data={
                    "channels": [
                        {
                            "channel": "#renewal-watch",
                            "unread": 1,
                            "messages": [
                                {
                                    "ts": "1710500100.000100",
                                    "user": "maya.revops",
                                    "text": "The renewal needs one clear owner and a recovery date.",
                                }
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="jira",
                captured_at="2026-03-25T16:00:00+00:00",
                status="ok",
                data={
                    "issues": [
                        {
                            "ticket_id": "ACME-201",
                            "title": "Renewal blocker: export job stalls before customer handoff",
                            "status": "open",
                            "assignee": "maya.revops",
                            "description": "Northstar renewal is blocked until onboarding export is stable.",
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="gmail",
                captured_at="2026-03-25T16:00:00+00:00",
                status="ok",
                data={
                    "threads": [
                        {
                            "thread_id": "thread-001",
                            "subject": "Need a clean recovery plan today",
                            "messages": [
                                {
                                    "from": "olivia@northstarcapital.example.com",
                                    "to": "support@acme.ai",
                                    "subject": "Need a clean recovery plan today",
                                    "snippet": "Still waiting for a timeline and owner.",
                                    "labels": ["IMPORTANT"],
                                    "unread": True,
                                }
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="google",
                captured_at="2026-03-25T16:00:00+00:00",
                status="ok",
                data={
                    "documents": [
                        {
                            "doc_id": "DOC-001",
                            "title": "Renewal recovery plan",
                            "body": "Owner, customer-safe update, fallback path, and follow-up call.",
                            "mime_type": "application/vnd.google-apps.document",
                        }
                    ]
                },
            ),
        ],
    )


def _sample_bundle(root: Path) -> CustomerTwinBundle:
    return CustomerTwinBundle(
        workspace_root=root,
        workspace_name="pilot",
        organization_name="Pinnacle Analytics",
        organization_domain="pinnacle.example.com",
        mold=ContextMoldConfig(archetype="b2b_saas"),
        context_snapshot_path="context_snapshot.json",
        blueprint_asset_path="customer.blueprint.json",
        gateway=TwinGatewayConfig(
            auth_token="pilot-token",
            surfaces=[
                CompatibilitySurfaceSpec(
                    name="slack",
                    title="Slack",
                    base_path="/slack/api",
                )
            ],
        ),
        summary="pilot",
        metadata={
            "preview": {
                "active_scenario_variant": "enterprise_renewal_risk",
                "available_scenario_variants": [
                    {
                        "name": "enterprise_renewal_risk",
                        "title": "Enterprise Renewal at Risk",
                    }
                ],
            }
        },
    )
