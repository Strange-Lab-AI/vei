from __future__ import annotations

from vei.blueprint.api import (
    build_blueprint_asset_for_example,
    build_blueprint_asset_for_family,
    create_world_session_from_blueprint,
)


def test_world_orientation_from_identity_builder_example() -> None:
    asset = build_blueprint_asset_for_example("acquired_user_cutover")
    session = create_world_session_from_blueprint(asset, seed=19)

    orientation = session.orientation()

    assert orientation.scenario_name == "acquired_user_cutover"
    assert orientation.builder_mode == "capability_graphs"
    assert orientation.organization_name == "MacroCompute"
    assert "identity_graph" in orientation.available_domains
    assert "google_admin" in orientation.available_surfaces
    assert orientation.active_policies[0].policy_id == "POL-WAVE2"
    assert any(item.object_id == "EMP-2201" for item in orientation.key_objects)
    assert "identity_graph" in orientation.suggested_focuses
    assert orientation.next_questions


def test_world_orientation_prioritizes_revenue_ops_questions() -> None:
    asset = build_blueprint_asset_for_family(
        "revenue_incident_mitigation",
        variant_name="revenue_ops_flightdeck",
    )
    session = create_world_session_from_blueprint(asset, seed=23)

    orientation = session.orientation()

    assert orientation.scenario_name == "checkout_spike_mitigation"
    assert "rollout control" in orientation.next_questions[0]
    assert "revenue object" in orientation.next_questions[1]
