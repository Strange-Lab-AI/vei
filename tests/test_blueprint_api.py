from __future__ import annotations

from vei.blueprint.api import (
    build_blueprint_asset_for_family,
    compile_blueprint,
    build_blueprint_for_family,
    build_blueprint_for_scenario,
    get_facade_manifest,
    list_blueprint_specs,
    list_facade_manifest,
)


def test_facade_catalog_exposes_existing_router_twins() -> None:
    facades = {item.name: item for item in list_facade_manifest()}

    assert "google_admin" in facades
    assert "siem" in facades
    assert "identity" in facades
    assert facades["google_admin"].domain == "identity_graph"
    assert "mcp" in facades["siem"].surfaces
    assert get_facade_manifest("crm").domain == "revenue_graph"


def test_build_blueprint_for_family_wraps_scenario_facades_and_contract() -> None:
    blueprint = build_blueprint_for_family("security_containment")

    facade_names = {item.name for item in blueprint.facades}
    assert blueprint.name == "security_containment.blueprint"
    assert blueprint.family_name == "security_containment"
    assert blueprint.workflow_name == "security_containment"
    assert blueprint.workflow_variant == "customer_notify"
    assert blueprint.contract is not None
    assert blueprint.contract.name == "security_containment.contract"
    assert blueprint.contract.success_predicate_count == 8
    assert "google_admin" in facade_names
    assert "siem" in facade_names
    assert "identity_graph" in blueprint.capability_domains
    assert "obs_graph" in blueprint.capability_domains
    assert "components.google_admin" in blueprint.state_roots


def test_compile_blueprint_promotes_asset_to_compiled_root() -> None:
    asset = build_blueprint_asset_for_family(
        "revenue_incident_mitigation", variant_name="revenue_ops_flightdeck"
    )
    compiled = compile_blueprint(asset)

    facade_names = {item.name for item in compiled.facades}
    assert compiled.asset.name == "revenue_incident_mitigation.blueprint"
    assert compiled.workflow_variant == "revenue_ops_flightdeck"
    assert compiled.run_defaults.scenario_name == "checkout_spike_mitigation"
    assert "spreadsheet" in facade_names
    assert "crm" in facade_names
    assert "components.spreadsheet" in compiled.state_roots
    assert "spreadsheets" in compiled.scenario_seed_fields
    assert compiled.contract_defaults.success_predicate_count >= 10


def test_compile_blueprint_rejects_unknown_requested_facade() -> None:
    try:
        build_blueprint_for_scenario(
            "checkout_spike_mitigation",
            requested_facades=["not_real"],
        )
    except ValueError as exc:
        assert "unknown requested facade" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected unknown requested facade to raise ValueError")


def test_build_blueprint_for_nonfamily_scenario_still_wraps_facades() -> None:
    blueprint = build_blueprint_for_scenario("multi_channel")

    facade_names = {item.name for item in blueprint.facades}
    assert blueprint.family_name is None
    assert blueprint.contract is None
    assert blueprint.scenario.name == "multi_channel"
    assert "slack" in facade_names
    assert "mail" in facade_names
    assert blueprint.surfaces


def test_list_blueprint_specs_returns_family_blueprints() -> None:
    names = {item.name for item in list_blueprint_specs()}
    assert "security_containment.blueprint" in names
    assert "enterprise_onboarding_migration.blueprint" in names
    assert "revenue_incident_mitigation.blueprint" in names
