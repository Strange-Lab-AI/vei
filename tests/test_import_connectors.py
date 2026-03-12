from __future__ import annotations

from pathlib import Path

from vei.imports.api import review_import_package
from vei.imports.connectors import OktaConnectorConfig, sync_okta_import_package


def test_sync_okta_import_package_writes_connector_snapshot_and_normalizes(
    tmp_path: Path, monkeypatch
) -> None:
    config = OktaConnectorConfig(
        base_url="https://macrocompute.okta.com",
        token="test-token",
        organization_name="MacroCompute",
        organization_domain="macrocompute.example",
    )

    def fake_okta_get_json(url: str, *, token: str, timeout_s: int):
        assert token == "test-token"
        assert timeout_s == 30
        if url.endswith("/api/v1/users?limit=200"):
            return (
                [
                    {
                        "id": "00u-acq-1",
                        "status": "ACTIVE",
                        "lastLogin": "2026-03-12T08:00:00Z",
                        "profile": {
                            "email": "ava@macrocompute.example",
                            "login": "ava@macrocompute.example",
                            "firstName": "Ava",
                            "lastName": "Shah",
                            "department": "Sales",
                            "title": "AE",
                            "manager": "mgr-sales",
                            "orgUnit": "Revenue",
                        },
                    },
                    {
                        "id": "00u-acq-2",
                        "status": "SUSPENDED",
                        "lastLogin": None,
                        "profile": {
                            "email": "leo@macrocompute.example",
                            "login": "leo@macrocompute.example",
                            "firstName": "Leo",
                            "lastName": "Ng",
                            "department": "Sales",
                            "title": "Sales Ops",
                            "manager": "mgr-sales",
                            "orgUnit": "Revenue",
                        },
                    },
                ],
                None,
            )
        if url.endswith("/api/v1/groups?limit=200"):
            return (
                [
                    {
                        "id": "00g-sales",
                        "profile": {
                            "name": "Sales Team",
                            "description": "Acquired sales wave",
                        },
                    }
                ],
                None,
            )
        if url.endswith("/api/v1/apps?limit=200"):
            return (
                [
                    {
                        "id": "0oa-crm",
                        "label": "Salesforce",
                        "status": "ACTIVE",
                        "name": "salesforce",
                        "signOnMode": "SAML_2_0",
                    }
                ],
                None,
            )
        if url.endswith("/api/v1/groups/00g-sales/users?limit=200"):
            return ([{"id": "00u-acq-1"}, {"id": "00u-acq-2"}], None)
        if url.endswith("/api/v1/apps/0oa-crm/users?limit=200"):
            return ([{"id": "00u-acq-1"}], None)
        raise AssertionError(f"unexpected Okta URL: {url}")

    monkeypatch.setattr("vei.imports.connectors._okta_get_json", fake_okta_get_json)

    result = sync_okta_import_package(
        tmp_path / "okta_sync", config, source_prefix="okta_demo"
    )
    review = review_import_package(result.package_root)

    assert result.connector == "okta"
    assert result.record_counts == {"users": 2, "groups": 1, "applications": 1}
    assert (result.package_root / "package.json").exists()
    assert review.package.metadata["source_connector"] == "okta"
    assert review.package.sources[0].source_kind == "connector_snapshot"
    assert review.package.sources[0].connector_id == "okta_demo"
    assert review.normalization_report.ok is True
    assert review.normalization_report.normalized_counts["identity_users"] == 2
    assert any(
        item.source_id == "okta_demo_users"
        and item.mapping_profile == "okta_users_live_v1"
        for item in review.normalization_report.source_summaries
    )
    assert len(review.generated_scenarios) >= 3
