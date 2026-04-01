from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI

from vei.context.models import ContextSnapshot
from vei.dataset import api as dataset_api
from vei.dataset.models import DatasetBuildSpec, DatasetBundle
from vei.twin.models import ContextMoldConfig, TwinVariantSpec
from vei.workspace.api import create_workspace_from_template


def test_build_dataset_bundle_from_workspace_root_writes_exports(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    output_root = tmp_path / "dataset"
    create_workspace_from_template(
        root=workspace_root,
        source_kind="vertical",
        source_ref="real_estate_management",
    )

    bundle = dataset_api.build_dataset_bundle(
        DatasetBuildSpec(
            output_root=output_root,
            workspace_roots=[workspace_root],
            include_external_sample=False,
            formats=["conversations", "demonstrations"],
        )
    )

    assert bundle.environment_count == 1
    assert bundle.run_count == 2
    assert bundle.exports
    assert (output_root / dataset_api.DATASET_BUNDLE_FILE).exists()
    assert (workspace_root / dataset_api.WORKSPACE_DATASET_FILE).exists()
    loaded = dataset_api.load_workspace_dataset_bundle(workspace_root)
    assert loaded is not None
    assert loaded.run_count == 2


def test_build_dataset_bundle_can_generate_matrix_from_snapshot(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "matrix_dataset"
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_text(
        _sample_snapshot().model_dump_json(indent=2), encoding="utf-8"
    )

    bundle = dataset_api.build_dataset_bundle(
        DatasetBuildSpec(
            output_root=output_root,
            snapshot_path=str(snapshot_path),
            organization_name="Acme Cloud",
            organization_domain="acme.ai",
            archetypes=["b2b_saas"],
            density_levels=["small"],
            crisis_levels=["calm"],
            seeds=[42042],
            include_external_sample=False,
            formats=["conversations"],
        )
    )

    assert bundle.environment_count == 1
    assert bundle.run_count == 2
    assert bundle.matrix_path == "matrix/twin_matrix.json"
    assert (output_root / "matrix" / "twin_matrix.json").exists()


def test_dataset_loader_helpers_cover_direct_workspace_and_missing_paths(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    output_root = tmp_path / "dataset"
    create_workspace_from_template(
        root=workspace_root,
        source_kind="vertical",
        source_ref="b2b_saas",
    )

    bundle = dataset_api.build_dataset_bundle(
        DatasetBuildSpec(
            output_root=output_root,
            workspace_roots=[workspace_root],
            include_external_sample=False,
            formats=["conversations"],
        )
    )

    assert dataset_api.load_dataset_bundle(output_root).run_count == bundle.run_count
    assert dataset_api.load_dataset_bundle(workspace_root).run_count == bundle.run_count
    assert dataset_api.load_workspace_dataset_bundle(tmp_path / "missing") is None
    with pytest.raises(FileNotFoundError, match="dataset bundle not found"):
        dataset_api.load_dataset_bundle(tmp_path / "missing")


def test_dataset_helpers_cover_external_samples_and_run_path_expansion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    variant = TwinVariantSpec(
        variant_id="variant-1",
        workspace_root=workspace_root,
        organization_name="Acme",
        organization_domain="acme.ai",
        archetype="b2b_saas",
        mold=ContextMoldConfig(
            archetype="b2b_saas",
            density_level="medium",
            crisis_family="escalated",
        ),
        scenario_variant="default",
        contract_variant="standard",
    )

    monkeypatch.setattr(
        dataset_api,
        "launch_workspace_run",
        lambda root, runner: SimpleNamespace(run_id=f"{runner}-run"),
    )
    original_run_external_sample = dataset_api._run_external_sample
    monkeypatch.setattr(
        dataset_api, "_run_external_sample", lambda root: "external-run"
    )
    assert dataset_api._run_variant_paths(variant, include_external=False) == [
        "workflow-run",
        "scripted-run",
    ]
    assert dataset_api._run_variant_paths(variant, include_external=True) == [
        "workflow-run",
        "scripted-run",
        "external-run",
    ]
    monkeypatch.setattr(
        dataset_api,
        "_run_external_sample",
        original_run_external_sample,
    )

    monkeypatch.setattr(
        dataset_api,
        "load_customer_twin",
        lambda root: (_ for _ in ()).throw(FileNotFoundError()),
    )
    assert dataset_api._run_external_sample(workspace_root) is None

    app = FastAPI()

    @app.get("/slack/api/conversations.list")
    def slack_channels() -> dict[str, object]:
        return {"channels": [{"id": "C123"}]}

    @app.post("/slack/api/chat.postMessage")
    def post_message() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/jira/rest/api/3/search")
    def jira_search() -> dict[str, list[object]]:
        return {"issues": []}

    @app.get("/graph/v1.0/me/messages")
    def graph_messages() -> dict[str, list[object]]:
        return {"value": []}

    @app.get("/salesforce/services/data/v60.0/query")
    def salesforce_query() -> dict[str, list[object]]:
        return {"records": []}

    @app.get("/api/twin")
    def twin_status() -> dict[str, dict[str, str]]:
        return {"runtime": {"run_id": "external-run"}}

    monkeypatch.setattr(
        dataset_api,
        "load_customer_twin",
        lambda root: SimpleNamespace(
            gateway=SimpleNamespace(auth_token="token"),
        ),
    )
    monkeypatch.setattr(dataset_api, "create_twin_gateway_app", lambda root: app)

    assert dataset_api._run_external_sample(workspace_root) == "external-run"


def test_dataset_small_helpers_cover_split_reward_and_workspace_views(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    bundle = DatasetBundle(
        spec=DatasetBuildSpec(output_root=tmp_path),
        generated_at="2026-03-21T00:00:00+00:00",
    )
    variant = TwinVariantSpec(
        variant_id="variant-1",
        workspace_root=workspace_root,
        organization_name="Acme",
        organization_domain="acme.ai",
        archetype="b2b_saas",
        mold=ContextMoldConfig(
            archetype="b2b_saas",
            density_level="medium",
            crisis_family="escalated",
        ),
        scenario_variant="default",
        contract_variant="standard",
    )

    monkeypatch.setattr(
        dataset_api,
        "sha1",
        lambda payload, usedforsecurity=False: SimpleNamespace(hexdigest=lambda: "00"),
    )
    assert dataset_api._assign_split("variant-1") == "train"
    monkeypatch.setattr(
        dataset_api,
        "sha1",
        lambda payload, usedforsecurity=False: SimpleNamespace(hexdigest=lambda: "b4"),
    )
    assert dataset_api._assign_split("variant-1") == "validation"
    monkeypatch.setattr(
        dataset_api,
        "sha1",
        lambda payload, usedforsecurity=False: SimpleNamespace(hexdigest=lambda: "ff"),
    )
    assert dataset_api._assign_split("variant-1") == "test"

    assert dataset_api._reward_summary([]) == {
        "success_rate": 0.0,
        "contract_ok_rate": 0.0,
        "avg_action_count": 0.0,
    }
    assert dataset_api._matrix_path(tmp_path, None) is None
    dataset_api._write_workspace_dataset_views(
        bundle, [variant, variant.model_copy(deep=True)]
    )
    assert (workspace_root / dataset_api.WORKSPACE_DATASET_FILE).exists()

    assert dataset_api._workspace_archetype("b2b_saas") == "b2b_saas"
    assert (
        dataset_api._workspace_archetype("digital_marketing_agency")
        == "digital_marketing_agency"
    )
    assert (
        dataset_api._workspace_archetype("real_estate_management")
        == "real_estate_management"
    )
    assert dataset_api._workspace_archetype("storage_solutions") == "storage_solutions"
    assert dataset_api._workspace_archetype("unknown") == "b2b_saas"


def _sample_snapshot() -> ContextSnapshot:
    return ContextSnapshot.model_validate_json("""
        {
          "version": "1",
          "organization_name": "Acme Cloud",
          "organization_domain": "acme.ai",
          "captured_at": "2026-03-24T16:00:00+00:00",
          "sources": [
            {
              "provider": "slack",
              "captured_at": "2026-03-24T16:00:00+00:00",
              "status": "ok",
              "record_counts": {"channels": 1, "messages": 2},
              "data": {
                "channels": [
                  {
                    "channel": "#revops-war-room",
                    "unread": 1,
                    "messages": [
                      {
                        "ts": "1710300000.000100",
                        "user": "maya.ops",
                        "text": "Renewal is exposed unless we land the onboarding fix today."
                      },
                      {
                        "ts": "1710300060.000200",
                        "user": "evan.sales",
                        "text": "Jordan wants one accountable owner and a customer-safe timeline."
                      }
                    ]
                  }
                ]
              }
            },
            {
              "provider": "jira",
              "captured_at": "2026-03-24T16:00:00+00:00",
              "status": "ok",
              "record_counts": {"issues": 1},
              "data": {
                "issues": [
                  {
                    "ticket_id": "ACME-101",
                    "title": "Renewal blocker: onboarding API still timing out",
                    "status": "open",
                    "assignee": "maya.ops",
                    "description": "Customer onboarding export is timing out on larger tenants."
                  }
                ]
              }
            },
            {
              "provider": "google",
              "captured_at": "2026-03-24T16:00:00+00:00",
              "status": "ok",
              "record_counts": {"documents": 1, "users": 1},
              "data": {
                "documents": [
                  {
                    "doc_id": "DOC-ACME-001",
                    "title": "Renewal Recovery Plan",
                    "body": "Stabilize the renewal and send a customer-safe update.",
                    "mime_type": "application/vnd.google-apps.document"
                  }
                ],
                "users": [
                  {
                    "id": "g-001",
                    "email": "maya@acme.ai",
                    "name": "Maya Ops",
                    "org_unit": "RevOps",
                    "suspended": false
                  }
                ]
              }
            }
          ]
        }
        """)
