from __future__ import annotations

from pathlib import Path

from vei.blueprint.api import build_blueprint_asset_for_example, compile_blueprint
from vei.imports.api import get_import_package_example_path
from vei.workspace.api import (
    bootstrap_workspace_contract,
    create_workspace_from_template,
    generate_workspace_scenarios_from_import,
    import_workspace,
    load_workspace_generated_scenarios,
    load_workspace_provenance,
    load_workspace_contract,
    preview_workspace_scenario,
    show_workspace,
    validate_workspace_contract,
)


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
