from __future__ import annotations

from vei.blueprint.api import (
    build_blueprint_asset_for_example,
    create_world_session_from_blueprint,
)


def test_runtime_capability_graphs_from_identity_builder_example() -> None:
    asset = build_blueprint_asset_for_example("acquired_user_cutover")
    session = create_world_session_from_blueprint(asset, seed=11)

    graphs = session.capability_graphs()

    assert graphs.available_domains == [
        "comm_graph",
        "doc_graph",
        "work_graph",
        "identity_graph",
        "revenue_graph",
    ]
    assert graphs.identity_graph is not None
    assert graphs.identity_graph.policies[0].policy_id == "POL-WAVE2"
    assert len(graphs.identity_graph.users) == 2
    assert graphs.doc_graph is not None
    assert len(graphs.doc_graph.drive_shares) == 1
    assert graphs.work_graph is not None
    assert len(graphs.work_graph.tickets) == 1
    assert graphs.revenue_graph is not None
    assert graphs.revenue_graph.deals[0].deal_id == "D-100"
