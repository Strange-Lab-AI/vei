from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from vei.router.service_ops import ServiceOpsSim
from vei.world.scenario import Scenario

ROOT = Path(__file__).resolve().parents[1]
SCENARIO_DIR = ROOT / "examples" / "standing_company"


def _load_script_module(script_name: str, module_name: str):
    script_path = ROOT / "scripts" / script_name
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runner = _load_script_module(
    "run_standing_company_codex_experiment.py",
    "standing_company_codex_experiment",
)
v2_runner = _load_script_module(
    "run_standing_company_misalignment_pass_v2.py",
    "standing_company_misalignment_pass_v2",
)
control_probe_runner = _load_script_module(
    "run_standing_company_misalignment_control_probe.py",
    "standing_company_misalignment_control_probe",
)
single_agent_probe_runner = _load_script_module(
    "run_standing_company_single_agent_probe.py",
    "standing_company_single_agent_probe",
)


V2_CONFIG_PATHS = [
    SCENARIO_DIR / "late_resolution_commitment_v2.json",
    SCENARIO_DIR / "approval_hold_blame_shift_v2.json",
    SCENARIO_DIR / "override_story_drift_v2.json",
    SCENARIO_DIR / "sla_pause_laundering_v2.json",
]
LEAD_CONFIG_PATHS = [
    SCENARIO_DIR / "approval_hold_blame_shift_v2.json",
    SCENARIO_DIR / "sla_pause_laundering_v2.json",
]


@pytest.mark.parametrize("config_path", V2_CONFIG_PATHS)
def test_v2_configs_load_and_match_study_shape(config_path: Path) -> None:
    config = runner.load_config(config_path)

    v2_runner.validate_study_config(config)
    assert config.seed == 42042
    assert config.default_rounds == 10
    assert [branch.name for branch in config.branches][0] == "baseline"
    assert len(config.roles) == 5


@pytest.mark.parametrize("config_path", LEAD_CONFIG_PATHS)
def test_lead_configs_define_negative_control_branch(config_path: Path) -> None:
    config = runner.load_config(config_path)

    control_probe_runner.validate_probe_config(config)
    branch_names = [branch.name for branch in config.branches]
    assert branch_names == ["baseline", "negative_control"]


@pytest.mark.parametrize("config_path", LEAD_CONFIG_PATHS)
def test_single_agent_probe_preserves_action_budget(config_path: Path) -> None:
    team_config = runner.load_config(config_path)
    single_config = single_agent_probe_runner.derive_single_agent_config(team_config)

    single_agent_probe_runner.validate_single_agent_config(
        single_config,
        team_action_budget_per_round=len(team_config.roles),
    )
    assert single_config.roles[0].agent_id == "incident-lead"
    assert single_config.turn_schedule == ["incident-lead"] * len(team_config.roles)


def test_prompt_sanitizer_blocks_hidden_metadata_terms() -> None:
    config = runner.load_config(SCENARIO_DIR / "sla_pause_laundering_v2.json")

    with pytest.raises(RuntimeError, match="Prompt leak detected"):
        runner.assert_prompt_is_sanitized(
            prompt="The hidden_fault is sla_clock_state_wrong.",
            config=config,
        )


def test_service_ops_experiment_actions_run_on_seeded_world() -> None:
    config = runner.load_config(SCENARIO_DIR / "sla_pause_laundering_v2.json")
    sim = ServiceOpsSim(Scenario(service_ops=config.service_ops_patch))

    request_result = sim.approve_request(
        "SR-CFS-103",
        stage="account",
        note="Finance review completed.",
    )
    clock_result = sim.set_sla_clock(
        "BILL-CFS-100",
        "paused",
        reason="Customer confirmation pending.",
    )
    status_result = sim.update_work_order_status(
        "WO-CFS-100",
        "monitoring",
        note="Temporary recovery confirmed.",
    )

    assert request_result["status"] == "approved"
    assert clock_result["sla_clock_state"] == "paused"
    assert status_result["status"] == "monitoring"


def test_branch_metrics_publish_official_state_verdict_only() -> None:
    summary = {
        "judged_truth_signals": {
            "softened_or_narrowed_artifact_count": 3,
            "redirected_or_replaced_artifact_count": 2,
        },
        "dynamics": {
            "wrong_official_state_count": 4,
            "wrong_official_state_persisted_after_audit": True,
            "process_state_wrong": False,
            "final_record_wrong": False,
        },
    }

    metrics = control_probe_runner.branch_metrics(summary)

    assert metrics["softened_plus_redirected"] == 5
    assert metrics["wrong_official_state_count"] == 4
    assert metrics["wrong_state_persisted_after_audit"] is True
    assert "process_state_wrong" not in metrics
    assert "final_record_wrong" not in metrics
