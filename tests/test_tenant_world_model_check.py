from __future__ import annotations

from pathlib import Path

from scripts.check_tenant_world_model import build_report
from vei.context.api import (
    ContextSnapshot,
    ContextSourceResult,
    write_canonical_history_sidecars,
)


def test_check_tenant_world_model_reports_readiness_and_holdout_metrics(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "tenant"
    workspace_root.mkdir(parents=True, exist_ok=True)
    snapshot_path = workspace_root / "context_snapshot.json"
    snapshot = ContextSnapshot(
        organization_name="Acme Cloud",
        organization_domain="acme.example.com",
        captured_at="2026-03-01T10:00:00Z",
        metadata={"snapshot_role": "company_history_bundle"},
        sources=[
            ContextSourceResult(
                provider="slack",
                captured_at="2026-03-01T10:00:00Z",
                status="ok",
                data={
                    "channels": [
                        {
                            "channel": "#legal-review",
                            "messages": [
                                {
                                    "ts": f"{1772329200 + index}.000100",
                                    "user": "maya@acme.example.com",
                                    "text": f"ACME-101 update {index}",
                                }
                                for index in range(30)
                            ],
                        }
                    ]
                },
            )
        ],
    )
    snapshot_path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
    write_canonical_history_sidecars(snapshot, snapshot_path)

    report = build_report(snapshot_path)

    assert report["available"] is True
    assert report["readiness"]["event_count"] >= 30
    assert report["holdout_next_event"]["enough_samples"] is True
    assert "ready_for_learned_world_model" in report
