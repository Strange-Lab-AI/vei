from __future__ import annotations

import json
import shutil
from pathlib import Path

from vei.imports.api import (
    bootstrap_contract_from_import_bundle,
    get_import_package_example_path,
    list_import_package_examples,
    normalize_identity_import_package,
    review_import_package,
    scaffold_mapping_override,
    validate_import_package,
)


def _copy_fixture_package(tmp_path: Path) -> Path:
    source = get_import_package_example_path("macrocompute_identity_export")
    target = tmp_path / "macrocompute_identity_export"
    shutil.copytree(source, target)
    return target


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
    assert payload["metadata"]["contract_bootstrap_summary"]["applied_rule_count"] >= 6
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
    assert any(
        item["name"] == "primary_app_present:APP-crm"
        for item in payload["success_predicates"]
    )
    assert any(
        item["name"] == "tracker_followthrough:JRA-204"
        for item in payload["success_predicates"]
    )
    assert any(
        item["name"] == "stakeholder_summary_sent:#identity-cutover"
        for item in payload["success_predicates"]
    )
    assert any(
        item["name"] == "stale_app_removed:APP-analytics"
        and item["origin"] == "imported"
        for item in payload["metadata"]["rule_provenance"]
    )


def test_generated_identity_candidates_include_origin_labels_and_refs() -> None:
    package_path = get_import_package_example_path("macrocompute_identity_export")
    artifacts = normalize_identity_import_package(package_path)

    oversharing = next(
        item
        for item in artifacts.generated_scenarios
        if item.name == "oversharing_remediation"
    )

    assert oversharing.metadata["priority"] == "high"
    assert "drive_share:GDRIVE-2201" in oversharing.metadata["supporting_refs"]
    assert oversharing.metadata["state_labels"]["drive_share:GDRIVE-2201"] == "imported"
    assert oversharing.metadata["origin_counts"]["imported"] >= 1
    assert oversharing.metadata["generation_reasons"]


def test_import_package_validation_flags_missing_required_fields(
    tmp_path: Path,
) -> None:
    broken = _copy_fixture_package(tmp_path)

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


def test_import_review_surfaces_override_paths_and_generated_scenarios() -> None:
    package_path = get_import_package_example_path("macrocompute_identity_export")

    review = review_import_package(package_path)

    assert review.package.name == "macrocompute_identity_export"
    assert "okta_users" in review.suggested_override_paths
    assert review.suggested_override_paths["okta_users"] == "overrides/okta_users.json"
    assert len(review.generated_scenarios) >= 6
    assert review.source_overrides == []


def test_mapping_override_can_recover_renamed_required_field(tmp_path: Path) -> None:
    package_path = _copy_fixture_package(tmp_path)
    users_path = package_path / "raw" / "okta_users.csv"
    payload = users_path.read_text(encoding="utf-8")
    users_path.write_text(
        payload.replace("email", "primary_email", 1),
        encoding="utf-8",
    )

    broken_review = review_import_package(package_path)
    assert broken_review.normalization_report.ok is False
    assert any(
        item.code == "bundle.incomplete"
        for item in broken_review.normalization_report.issues
    )
    assert broken_review.generated_scenarios == []

    destination, override = scaffold_mapping_override(
        package_path,
        source_id="okta_users",
    )
    payload = json.loads(destination.read_text(encoding="utf-8"))
    payload["field_aliases"]["primary_email"] = "email"
    destination.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    after = normalize_identity_import_package(package_path)

    assert override.source_id == "okta_users"
    assert destination.name == "okta_users.json"
    assert after.normalization_report.ok is True
    okta_summary = next(
        item
        for item in after.normalization_report.source_summaries
        if item.source_id == "okta_users"
    )
    assert okta_summary.override_applied is True
    assert okta_summary.override_path == "overrides/okta_users.json"
    assert any(
        item.code == "field.alias_applied" and item.field == "email"
        for item in after.normalization_report.issues
    )
