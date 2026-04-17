from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from vei.cli.vei_knowledge import app
from vei.knowledge.api import empty_store
from vei.knowledge.models import KnowledgeAsset, KnowledgeProvenance
from vei.workspace.api import create_workspace_from_template

runner = CliRunner()


def test_knowledge_compose_reads_workspace_snapshot(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    create_workspace_from_template(
        root=workspace_root,
        source_kind="vertical",
        source_ref="digital_marketing_agency",
        overwrite=True,
    )

    result = runner.invoke(
        app,
        [
            "compose",
            "--workspace",
            str(workspace_root),
            "--subject",
            "crm_deal:CRM-NSG-D1",
            "--template",
            "proposal_v1",
            "--output",
            "-",
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["artifact"]["kind"] == "proposal"
    assert payload["artifact"]["composition"]["validation"]["citations_present"] is True


def test_knowledge_compose_uses_wall_clock_for_standalone_snapshots(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "knowledge_snapshot.json"
    store = empty_store()
    store.assets["KA-OLD"] = KnowledgeAsset(
        asset_id="KA-OLD",
        kind="note",
        title="Old note",
        body="Remember this",
        summary="Remember this",
        provenance=KnowledgeProvenance(
            source="test",
            source_id="KA-OLD",
            captured_at="2020-01-01T00:00:00+00:00",
        ),
        metadata={"captured_at_ms": 1_577_836_800_000},
    )
    snapshot_path.write_text(store.model_dump_json(indent=2), encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "compose",
            "--snapshot",
            str(snapshot_path),
            "--subject",
            "crm_deal:CRM-1",
            "--output",
            "-",
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["artifact"]["metadata"]["captured_at_ms"] > 1_577_836_800_000
