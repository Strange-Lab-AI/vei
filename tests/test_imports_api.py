from __future__ import annotations

from pathlib import Path

from vei.imports.api import (
    bootstrap_contract_from_import_bundle,
    get_import_package_example_path,
    list_import_package_examples,
    normalize_identity_import_package,
    validate_import_package,
)


def test_import_package_fixture_normalizes_into_identity_bundle() -> None:
    examples = list_import_package_examples()
    assert "macrocompute_identity_export" in examples

    package_path = get_import_package_example_path("macrocompute_identity_export")
    report = validate_import_package(package_path)
    artifacts = normalize_identity_import_package(package_path)

    assert report.ok is True
    assert artifacts.normalization_report.ok is True
    assert artifacts.normalized_bundle.name == "macrocompute_identity_export"
    assert len(artifacts.normalized_bundle.capability_graphs.identity_graph.users) == 2
    assert (
        len(artifacts.normalized_bundle.capability_graphs.identity_graph.policies) == 1
    )
    assert len(artifacts.generated_scenarios) >= 6
    assert any(
        item.object_ref == "identity_user:USR-ACQ-1" for item in artifacts.provenance
    )
    assert any(
        item.object_ref == "document:CUTOVER-EMP-2201" for item in artifacts.provenance
    )


def test_bootstrap_contract_from_import_bundle_adds_policy_constraints() -> None:
    package_path = get_import_package_example_path("macrocompute_identity_export")
    bundle = normalize_identity_import_package(package_path).normalized_bundle

    payload = bootstrap_contract_from_import_bundle(
        bundle=bundle,
        contract_payload={
            "name": "test.contract",
            "workflow_name": "identity_access_governance",
        },
        scenario_name="stale_entitlement_cleanup",
        workflow_parameters={
            "doc_id": "GDRIVE-2201",
            "user_id": "USR-ACQ-1",
            "stale_app_id": "APP-analytics",
        },
    )

    assert payload["metadata"]["import_policy_id"] == "POL-WAVE2"
    assert any(
        item["name"] == "import_policy:manager" for item in payload["policy_invariants"]
    )
    assert any(
        item["name"] == "forbidden_share_domain:example.net"
        for item in payload["forbidden_predicates"]
    )
    assert any(
        item["name"] == "stale_app_removed:APP-analytics"
        for item in payload["forbidden_predicates"]
    )


def test_import_package_validation_flags_missing_required_fields(
    tmp_path: Path,
) -> None:
    source = get_import_package_example_path("macrocompute_identity_export")
    broken = tmp_path / "broken_import"
    broken.mkdir()
    for item in source.rglob("*"):
        target = broken / item.relative_to(source)
        if item.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(item.read_bytes())

    users_path = broken / "raw" / "okta_users.csv"
    payload = users_path.read_text(encoding="utf-8")
    users_path.write_text(payload.replace("USR-ACQ-1", "", 1), encoding="utf-8")

    report = normalize_identity_import_package(broken).normalization_report

    assert report.ok is False
    assert report.error_count >= 1
    assert any(
        item.code == "field.required" and item.field == "user_id"
        for item in report.issues
    )
