from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from vei.cli import vei_quickstart
from vei.cli.vei import app
from vei.twin.models import (
    TwinLaunchManifest,
    TwinLaunchRuntime,
    TwinLaunchStatus,
    TwinOutcomeSummary,
    TwinServiceRecord,
)
from vei.twin.models import CompatibilitySurfaceSpec

pytestmark = pytest.mark.integration


def test_quickstart_reports_invalid_live_demo_combo(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runner = CliRunner()
    root = tmp_path / "quickstart_workspace"

    fake_state = SimpleNamespace(
        mission=SimpleNamespace(mission_name="service_day_collision"),
        run_id="human_play_123",
        world_name="Clearwater Field Services",
    )

    monkeypatch.setattr(
        "vei.playable.prepare_playable_workspace",
        lambda *args, **kwargs: fake_state,
    )
    monkeypatch.setattr(
        vei_quickstart,
        "start_twin",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            ValueError("governor demo mode requires connector_mode='sim'")
        ),
    )

    result = runner.invoke(
        app,
        [
            "quickstart",
            "run",
            "--world",
            "service_ops",
            "--root",
            str(root),
            "--governor-demo",
            "--connector-mode",
            "live",
            "--no-baseline",
        ],
    )

    assert result.exit_code == 2
    assert "governor demo mode requires connector_mode='sim'" in result.output
    assert "Traceback" not in result.output


def test_quickstart_uses_shared_twin_launcher(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runner = CliRunner()
    root = tmp_path / "quickstart_workspace"
    fake_state = SimpleNamespace(
        mission=SimpleNamespace(mission_name="service_day_collision"),
        run_id="human_play_123",
        world_name="Clearwater Field Services",
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "vei.playable.prepare_playable_workspace",
        lambda *args, **kwargs: fake_state,
    )

    def fake_start_twin(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return _sample_pilot_status(root)

    monkeypatch.setattr(vei_quickstart, "start_twin", fake_start_twin)
    monkeypatch.setattr(
        vei_quickstart,
        "build_twin_status",
        lambda _root: _sample_pilot_status(root),
    )
    monkeypatch.setattr(
        vei_quickstart.signal,
        "pause",
        lambda: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    stopped: list[Path] = []
    monkeypatch.setattr(vei_quickstart, "stop_twin", lambda path: stopped.append(path))

    result = runner.invoke(
        app,
        [
            "quickstart",
            "run",
            "--world",
            "service_ops",
            "--root",
            str(root),
            "--studio-port",
            "3311",
            "--gateway-port",
            "3312",
            "--governor-demo",
            "--no-baseline",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["args"] == (root,)
    assert captured["kwargs"]["archetype"] == "service_ops"
    assert captured["kwargs"]["organization_name"] == "Clearwater Field Services"
    assert captured["kwargs"]["studio_port"] == 3311
    assert captured["kwargs"]["gateway_port"] == 3312
    assert captured["kwargs"]["governor_demo"] is True
    assert stopped == [root]
    quickstart_info = json.loads((root / ".vei" / "quickstart.json").read_text())
    assert quickstart_info["studio_url"] == "http://127.0.0.1:3011"
    assert quickstart_info["gateway_url"] == "http://127.0.0.1:3020"
    assert quickstart_info["supported_surfaces"][0]["name"] == "slack"
    assert "/slack/api/" in result.output
    assert "/salesforce/services/data/v60.0/" not in result.output


def _sample_pilot_status(root: Path) -> TwinLaunchStatus:
    return TwinLaunchStatus(
        manifest=TwinLaunchManifest(
            workspace_root=root,
            workspace_name="quickstart_workspace",
            organization_name="Acme Cloud",
            organization_domain="acme.ai",
            archetype="service_ops",
            crisis_name="Dispatch overload",
            studio_url="http://127.0.0.1:3011",
            control_room_url="http://127.0.0.1:3011/",
            gateway_url="http://127.0.0.1:3020",
            gateway_status_url="http://127.0.0.1:3020/api/twin",
            bearer_token="token-123",
            supported_surfaces=[
                CompatibilitySurfaceSpec(
                    name="slack",
                    title="Slack",
                    base_path="/slack/api",
                )
            ],
            recommended_first_move="Read the queue before acting.",
            sample_client_path="/tmp/governor_client.py",
        ),
        runtime=TwinLaunchRuntime(
            workspace_root=root,
            services=[
                TwinServiceRecord(
                    name="gateway",
                    host="127.0.0.1",
                    port=3020,
                    url="http://127.0.0.1:3020",
                    state="running",
                ),
                TwinServiceRecord(
                    name="studio",
                    host="127.0.0.1",
                    port=3011,
                    url="http://127.0.0.1:3011",
                    state="running",
                ),
            ],
        ),
        active_run="external-run",
        twin_status="running",
        request_count=3,
        services_ready=True,
        outcome=TwinOutcomeSummary(
            status="running",
            summary="Outside work is active.",
        ),
    )
