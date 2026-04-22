from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from vei.blueprint.api import build_blueprint_asset_for_example, compile_blueprint
from vei.imports.api import get_import_package_example_path, scaffold_mapping_override
from vei.workspace.api import (
    activate_workspace_contract_variant,
    activate_workspace_scenario,
    activate_workspace_scenario_variant,
    bootstrap_workspace_contract,
    create_workspace_scenario,
    create_workspace_from_template,
    generate_workspace_scenarios_from_import,
    import_workspace,
    list_workspace_contract_variants,
    list_workspace_source_syncs,
    list_workspace_sources,
    list_workspace_scenario_variants,
    load_workspace_generated_scenarios,
    load_workspace_import_review,
    load_workspace_provenance,
    load_workspace_contract,
    preview_workspace_scenario,
    show_workspace,
    sync_workspace_source,
    validate_workspace_contract,
)
from vei.imports.api import load_import_package


def test_workspace_template_compile_and_preview(tmp_path: Path) -> None:
    root = tmp_path / "workspace"

    manifest = create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    summary = show_workspace(root)
    preview = preview_workspace_scenario(root)
    validation = validate_workspace_contract(root)

    assert manifest.name == summary.manifest.name
    assert summary.run_count == 0
    assert len(summary.compiled_scenarios) == 1
    assert summary.compiled_scenarios[0].scenario_name == "default"
    assert summary.compiled_scenarios[0].contract_bootstrapped is True
    assert preview["scenario"]["name"] == "default"
    assert (
        preview["compiled_blueprint"]["workflow_name"]
        == "enterprise_onboarding_migration"
    )
    assert preview["compiled_blueprint"]["metadata"]["scenario_materialization"] == (
        "capability_graphs"
    )
    assert preview["contract"]["workflow_name"] == "enterprise_onboarding_migration"
    assert preview["scenario_seed"]["identity_users"]
    assert validation["ok"] is True


def test_workspace_overwrite_replaces_stale_contracts(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    stale_contract = root / "contracts" / "default.contract.json"
    stale_contract.write_text(
        '{"name":"stale.contract","workflow_name":"stale"}', encoding="utf-8"
    )

    create_workspace_from_template(
        root=root,
        source_kind="family",
        source_ref="revenue_incident_mitigation",
        overwrite=True,
    )

    contract = load_workspace_contract(root)
    assert contract.workflow_name == "revenue_incident_mitigation"


def test_import_workspace_preserves_compiled_blueprint(tmp_path: Path) -> None:
    compiled = compile_blueprint(
        build_blueprint_asset_for_example("acquired_user_cutover")
    )
    source_path = tmp_path / "source_compiled.json"
    source_path.write_text(compiled.model_dump_json(indent=2), encoding="utf-8")

    root = tmp_path / "workspace"
    import_workspace(root=root, compiled_blueprint_path=source_path)

    compiled_path = root / "compiled" / "default" / "blueprint.json"
    assert compiled_path.exists()
    assert compiled_path.read_text(encoding="utf-8") == source_path.read_text(
        encoding="utf-8"
    )


def test_import_workspace_from_package_generates_import_artifacts(
    tmp_path: Path,
) -> None:
    root = tmp_path / "workspace"
    package_path = get_import_package_example_path("macrocompute_identity_export")

    manifest = import_workspace(root=root, package_path=package_path)
    summary = show_workspace(root)
    generated = load_workspace_generated_scenarios(root)

    assert manifest.source_kind == "import_package"
    assert summary.imports is not None
    assert summary.imports.package_name == "macrocompute_identity_export"
    assert summary.imports.source_count == 10
    assert len(generated) >= 6
    assert (root / "imports" / "raw_sources" / "raw" / "okta_users.csv").exists()

    scenarios = generate_workspace_scenarios_from_import(root)
    assert any(item.name == "oversharing_remediation" for item in scenarios)

    contract = bootstrap_workspace_contract(
        root, scenario_name="oversharing_remediation", overwrite=True
    )
    preview = preview_workspace_scenario(root, "oversharing_remediation")
    provenance = load_workspace_provenance(root, "document:CUTOVER-EMP-2201")

    assert contract.metadata["import_policy_id"] == "POL-WAVE2"
    assert preview["contract"]["metadata"]["contract_bootstrap"] == "import_policy_acl"
    assert validate_workspace_contract(root, "oversharing_remediation")["ok"] is True
    assert provenance
    assert provenance[0].origin == "derived"


def test_import_workspace_review_includes_copied_overrides(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    source = get_import_package_example_path("macrocompute_identity_export")
    package_path = tmp_path / "macrocompute_identity_export"
    shutil.copytree(source, package_path)
    destination, _ = scaffold_mapping_override(
        package_path,
        source_id="okta_users",
    )
    payload = json.loads(destination.read_text(encoding="utf-8"))
    payload["field_aliases"]["primary_email"] = "email"
    destination.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    import_workspace(root=root, package_path=package_path)
    review = load_workspace_import_review(root)

    assert review is not None
    assert (
        review.suggested_override_paths["okta_users"]
        == "imports/overrides/okta_users.json"
    )
    assert any(item.source_id == "okta_users" for item in review.source_overrides)
    assert review.normalization_report.identity_reconciliation is not None


def test_activate_workspace_scenario_updates_active_scenario_and_contract(
    tmp_path: Path,
) -> None:
    root = tmp_path / "workspace"
    import_workspace(
        root=root,
        package_path=get_import_package_example_path("macrocompute_identity_export"),
    )
    generate_workspace_scenarios_from_import(root)

    scenario = activate_workspace_scenario(
        root,
        "oversharing_remediation",
        bootstrap_contract=True,
    )
    summary = show_workspace(root)
    preview = preview_workspace_scenario(root)

    assert scenario.name == "oversharing_remediation"
    assert summary.manifest.active_scenario == "oversharing_remediation"
    assert preview["scenario"]["name"] == "oversharing_remediation"
    assert preview["contract"]["metadata"]["contract_bootstrap"] == "import_policy_acl"


def test_activate_created_workspace_scenario_bootstraps_contract(
    tmp_path: Path,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="real_estate_management",
    )
    created = create_workspace_scenario(
        root,
        name="inspection_followup",
        title="Inspection Followup",
        description="Custom scenario added after the workspace already exists.",
        hidden_faults={
            "inspection_note_gap": {
                "description": "Inspection note still missing from the handoff packet."
            }
        },
        tags=["regression"],
    )

    activated = activate_workspace_scenario(
        root,
        created.name,
        bootstrap_contract=True,
    )
    summary = show_workspace(root)
    preview = preview_workspace_scenario(root)
    compiled = next(
        item
        for item in summary.compiled_scenarios
        if item.scenario_name == created.name
    )
    contract = load_workspace_contract(root, created.name)

    assert activated.name == created.name
    assert summary.manifest.active_scenario == created.name
    assert compiled.contract_bootstrapped is True
    assert preview["scenario"]["name"] == created.name
    assert contract.workflow_name == preview["compiled_blueprint"]["workflow_name"]


def test_vertical_workspace_variant_activation_updates_preview_and_contract(
    tmp_path: Path,
) -> None:
    root = tmp_path / "vertical-workspace"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="real_estate_management",
    )

    scenario_variants = list_workspace_scenario_variants(root)
    contract_variants = list_workspace_contract_variants(root)
    activate_workspace_scenario_variant(root, "vendor_no_show", bootstrap_contract=True)
    contract = activate_workspace_contract_variant(root, "safety_over_speed")
    preview = preview_workspace_scenario(root)

    assert len(scenario_variants) == 4
    assert len(contract_variants) == 3
    assert preview["active_scenario_variant"] == "vendor_no_show"
    assert preview["active_contract_variant"] == "safety_over_speed"
    assert preview["scenario"]["workflow_variant"] == "vendor_no_show"
    assert (
        preview["compiled_blueprint"]["asset"]["capability_graphs"]["metadata"][
            "active_scenario_variant"
        ]
        == "vendor_no_show"
    )
    assert contract.metadata["vertical_contract_variant"] == "safety_over_speed"


def test_import_workspace_fails_cleanly_for_incomplete_import_package(
    tmp_path: Path,
) -> None:
    source = get_import_package_example_path("macrocompute_identity_export")
    package_path = tmp_path / "broken_import"
    shutil.copytree(source, package_path)
    users_path = package_path / "raw" / "okta_users.csv"
    users_path.write_text(
        users_path.read_text(encoding="utf-8").replace("email", "primary_email", 1),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="review normalization diagnostics and mapping overrides first",
    ):
        import_workspace(root=tmp_path / "workspace", package_path=package_path)


def test_workspace_source_sync_registers_connector_and_refreshes_import_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    package_source = get_import_package_example_path("macrocompute_identity_export")
    config_path = tmp_path / "okta.json"
    config_path.write_text(
        json.dumps({"base_url": "https://macrocompute.okta.com", "token": "test"}),
        encoding="utf-8",
    )

    def fake_sync(sync_root, config, *, source_prefix="okta_live"):
        package_root = Path(sync_root)
        shutil.copytree(package_source, package_root, dirs_exist_ok=True)
        package = load_import_package(package_root)
        for source in package.sources:
            source.source_kind = "connector_snapshot"
            source.connector_id = source_prefix
        (package_root / "package.json").write_text(
            package.model_dump_json(indent=2), encoding="utf-8"
        )
        return SimpleNamespace(
            connector="okta",
            package_root=package_root,
            package=package,
            record_counts={"users": 2, "groups": 2, "applications": 2},
            metadata={"source_prefix": source_prefix},
        )

    monkeypatch.setattr("vei.workspace.api.sync_okta_import_package", fake_sync)

    record = sync_workspace_source(
        root,
        connector="okta",
        config_path=config_path,
        source_id="macro_okta",
    )
    summary = show_workspace(root)
    sources = list_workspace_sources(root)
    syncs = list_workspace_source_syncs(root)

    assert record.status == "ok"
    assert record.source_id == "macro_okta"
    assert summary.imports is not None
    assert summary.imports.connected_source_count == 1
    assert summary.imports.source_sync_count == 1
    assert summary.imports.package_name == "macrocompute_identity_export"
    assert sources[0].source_id == "macro_okta"
    assert syncs[0].record_counts["users"] == 2
    assert load_workspace_import_review(root) is not None
