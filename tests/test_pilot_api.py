from __future__ import annotations

from pathlib import Path

import pytest

from vei.context.models import (
    ContextProviderConfig,
    ContextSnapshot,
    ContextSourceResult,
)
from vei.pilot import api as pilot_api
from vei.twin.models import (
    CompatibilitySurfaceSpec,
    ContextMoldConfig,
    CustomerTwinBundle,
    TwinGatewayConfig,
)


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
                                "role": "exercise-runner",
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
                            "role": "exercise-runner",
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
    assert status.active_agents[0].role == "exercise-runner"
    assert (root / pilot_api.PILOT_MANIFEST_FILE).exists()
    assert (root / pilot_api.PILOT_GUIDE_FILE).exists()
    assert (root / pilot_api.PILOT_RUNTIME_FILE).exists()

    manifest = pilot_api.load_pilot_manifest(root)
    runtime = pilot_api.load_pilot_runtime(root)
    guide = (root / pilot_api.PILOT_GUIDE_FILE).read_text(encoding="utf-8")

    assert manifest.organization_name == "Acme Cloud"
    assert manifest.crisis_name
    assert manifest.snippets[0].language == "bash"
    assert manifest.sample_client_path.endswith("examples/pilot_client.py")
    assert len(runtime.services) == 2
    assert "python " in guide
    assert "pilot_client.py" in guide

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
        mirror_demo=False,
        mirror_demo_interval_ms=2500,
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
        ValueError, match="mirror demo mode requires connector_mode='sim'"
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
            mirror_demo=True,
            mirror_demo_interval_ms=2500,
            gateway_token="pilot-token",
            rebuild=True,
        )


def test_start_pilot_rebuild_stops_existing_services_first(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "rebuild_pilot"
    root.mkdir(parents=True, exist_ok=True)
    gateway_port = 49220
    studio_port = 49211
    runtime = pilot_api.PilotRuntime(
        workspace_root=root,
        services=[
            pilot_api.PilotServiceRecord(
                name="gateway",
                host="127.0.0.1",
                port=gateway_port,
                url=f"http://127.0.0.1:{gateway_port}",
                pid=4101,
                state="running",
            ),
            pilot_api.PilotServiceRecord(
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
    (root / pilot_api.PILOT_RUNTIME_FILE).write_text(
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
        (root / pilot_api.PILOT_RUNTIME_FILE).write_text(
            stopped_runtime.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return pilot_api.PilotStatus(
            manifest=pilot_api.PilotManifest(
                workspace_root=root,
                workspace_name="pilot",
                organization_name="Pinnacle Analytics",
                organization_domain="pinnacle.example.com",
                archetype="b2b_saas",
                crisis_name="Enterprise Renewal at Risk",
                studio_url=f"http://127.0.0.1:{studio_port}",
                pilot_console_url=f"http://127.0.0.1:{studio_port}/pilot",
                gateway_url=f"http://127.0.0.1:{gateway_port}",
                gateway_status_url=f"http://127.0.0.1:{gateway_port}/api/twin",
                bearer_token="pilot-token",
                recommended_first_exercise="Read Slack first.",
                sample_client_path="/tmp/pilot_client.py",
            ),
            runtime=stopped_runtime,
            outcome=pilot_api.PilotOutcomeSummary(
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
