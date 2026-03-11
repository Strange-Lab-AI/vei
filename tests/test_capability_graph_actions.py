from __future__ import annotations

from vei.blueprint.api import (
    build_blueprint_asset_for_example,
    create_world_session_from_blueprint,
)
from vei.world.api import create_world_session, get_catalog_scenario


def test_identity_builder_graph_plan_and_step_action() -> None:
    asset = build_blueprint_asset_for_example("acquired_user_cutover")
    session = create_world_session_from_blueprint(asset, seed=17)

    plan = session.graph_plan(limit=8)

    assert plan.scenario_name == "acquired_user_cutover"
    assert plan.available_domains == [
        "comm_graph",
        "doc_graph",
        "work_graph",
        "identity_graph",
        "revenue_graph",
    ]
    assert any(step.action == "restrict_drive_share" for step in plan.suggested_steps)
    assert any(step.action == "assign_application" for step in plan.suggested_steps)

    restrict_step = next(
        step for step in plan.suggested_steps if step.action == "restrict_drive_share"
    )
    result = session.graph_action({"step_id": restrict_step.step_id})

    assert result.ok is True
    assert result.tool == "google_admin.restrict_drive_share"
    assert result.domain == "doc_graph"
    assert result.result["visibility"] == "internal"
    shares = result.graph["drive_shares"]
    assert shares[0]["visibility"] == "internal"


def test_revenue_ops_graph_plan_and_explicit_rollout_action() -> None:
    session = create_world_session(
        scenario=get_catalog_scenario("checkout_spike_mitigation"),
        seed=23,
    )

    graphs = session.capability_graphs()
    assert "data_graph" in graphs.available_domains
    assert "obs_graph" in graphs.available_domains
    assert "ops_graph" in graphs.available_domains

    plan = session.graph_plan(limit=10)
    actions = {step.action for step in plan.suggested_steps}
    assert "ack_incident" in actions
    assert "update_rollout" in actions
    assert "upsert_row" in actions

    result = session.graph_action(
        {
            "domain": "ops_graph",
            "action": "update_rollout",
            "args": {
                "flag_key": "checkout_v2",
                "rollout_pct": 0,
                "env": "prod",
                "reason": "Regression test graph action",
            },
        }
    )

    assert result.ok is True
    assert result.tool == "feature_flags.update_rollout"
    flags = {row["flag_key"]: row for row in result.graph["flags"]}
    assert flags["checkout_v2"]["rollout_pct"] == 0
    assert "feature_flags" in result.next_focuses
