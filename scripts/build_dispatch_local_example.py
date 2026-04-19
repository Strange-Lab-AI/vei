from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from vei.context.api import (
    ContextProviderConfig,
    build_canonical_history_readiness,
    query_canonical_history,
)
from vei.twin.api import ContextMoldConfig, build_customer_twin

DEFAULT_GMAIL_EXPORT_CANDIDATES = (
    Path("~/Downloads/dispatch-gmail.zip"),
    Path("~/Downloads/dispatch-gmail.mbox"),
    Path("~/Downloads/dispatch-gmail"),
)

DEFAULT_NOTION_EXPORT_CANDIDATES = (
    Path("~/Downloads/dispatch-notion.zip"),
    Path("~/Downloads/dispatch-notion"),
)


def build_dispatch_local_example(
    *,
    root: Path,
    gmail_export: Path,
    notion_export: Path,
    organization_name: str,
    organization_domain: str,
    archetype: str,
    gmail_limit: int,
    overwrite: bool,
) -> dict[str, object]:
    provider_configs = [
        ContextProviderConfig(
            provider="gmail",
            base_url=str(gmail_export),
            limit=gmail_limit,
        ),
        ContextProviderConfig(
            provider="notion",
            base_url=str(notion_export),
        ),
    ]
    bundle = build_customer_twin(
        root,
        provider_configs=provider_configs,
        organization_name=organization_name,
        organization_domain=organization_domain,
        mold=ContextMoldConfig(archetype=archetype),  # type: ignore[arg-type]
        overwrite=overwrite,
    )
    snapshot_path = root / bundle.context_snapshot_path
    readiness = build_canonical_history_readiness(snapshot_path)
    timeline = query_canonical_history(snapshot_path, limit=25)
    payload = {
        "workspace_root": str(root),
        "snapshot_path": str(snapshot_path),
        "timeline_files": {
            "events": str(snapshot_path.parent / "canonical_events.jsonl"),
            "index": str(snapshot_path.parent / "canonical_event_index.json"),
        },
        "capture": {
            "gmail_export": str(gmail_export),
            "notion_export": str(notion_export),
            "gmail_limit": gmail_limit,
        },
        "bundle": {
            "workspace_name": bundle.workspace_name,
            "organization_name": bundle.organization_name,
            "organization_domain": bundle.organization_domain,
            "context_snapshot_path": bundle.context_snapshot_path,
            "blueprint_asset_path": bundle.blueprint_asset_path,
            "summary": bundle.summary,
            "source_providers": bundle.metadata.get("source_providers", []),
            "gateway_surfaces": [
                surface.model_dump(mode="json") for surface in bundle.gateway.surfaces
            ],
        },
        "readiness": readiness.model_dump(mode="json"),
        "timeline_preview": timeline.model_dump(mode="json"),
    }
    summary_path = root / "dispatch_example_summary.json"
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def _resolve_export_path(
    explicit_path: str | None,
    *,
    candidates: Iterable[Path],
    label: str,
) -> Path:
    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        if path.exists():
            return path
        raise FileNotFoundError(f"{label} export not found: {path}")
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if resolved.exists():
            return resolved
    raise FileNotFoundError(
        f"{label} export not found. Pass --{label}-export or place it in ~/Downloads."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build a local Dispatch customer-twin workspace from Gmail Takeout and "
            "Notion export archives."
        )
    )
    parser.add_argument(
        "--root",
        default="_vei_out/dispatch-real-example",
        help="Workspace root for the generated Dispatch example.",
    )
    parser.add_argument(
        "--gmail-export",
        default="",
        help="Path to the Dispatch Gmail export zip, directory, or MBOX file.",
    )
    parser.add_argument(
        "--notion-export",
        default="",
        help="Path to the Dispatch Notion export zip or directory.",
    )
    parser.add_argument(
        "--org",
        default="Dispatch",
        help="Organization name for the generated workspace.",
    )
    parser.add_argument(
        "--domain",
        default="thedispatch.ai",
        help="Organization domain for the generated workspace.",
    )
    parser.add_argument(
        "--archetype",
        default="b2b_saas",
        help="Base world archetype to mold around the imported history.",
    )
    parser.add_argument(
        "--gmail-limit",
        type=int,
        default=5000,
        help="Maximum number of Gmail messages to import.",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep any existing workspace contents instead of overwriting them.",
    )
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    gmail_export = _resolve_export_path(
        args.gmail_export or None,
        candidates=DEFAULT_GMAIL_EXPORT_CANDIDATES,
        label="gmail",
    )
    notion_export = _resolve_export_path(
        args.notion_export or None,
        candidates=DEFAULT_NOTION_EXPORT_CANDIDATES,
        label="notion",
    )
    payload = build_dispatch_local_example(
        root=root,
        gmail_export=gmail_export,
        notion_export=notion_export,
        organization_name=args.org,
        organization_domain=args.domain,
        archetype=args.archetype,
        gmail_limit=max(1, args.gmail_limit),
        overwrite=not args.keep,
    )
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
