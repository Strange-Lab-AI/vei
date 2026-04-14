from __future__ import annotations

from pathlib import Path

from vei.context._normalize_cleanup import cleanup_normalized_snapshot
from vei.context._normalize_extract import (
    cleanup_temp_dir,
    detect_export_sources,
    extract_archive_if_needed,
    load_existing_snapshot,
    looks_like_import_package,
    merge_source_results,
    snapshot_from_import_package,
)
from vei.context import _normalize_verify as _normalize_verify
from vei.context.models import ContextSnapshot
from vei.context.public_context import (
    build_public_context as _build_public_context,
    empty_public_context as _empty_public_context,
)
from vei.context.providers.base import iso_now

summarize_context_snapshot = _normalize_verify.summarize_context_snapshot
verify_context_snapshot = _normalize_verify.verify_context_snapshot


def normalize_raw_exports(
    source_dir: str | Path,
    *,
    organization_name: str,
    organization_domain: str = "",
) -> ContextSnapshot:
    raw = Path(source_dir).expanduser().resolve()
    extracted_root = extract_archive_if_needed(raw)
    try:
        sources = []
        existing_snapshot = load_existing_snapshot(extracted_root)
        if existing_snapshot is not None:
            sources.extend(existing_snapshot.sources)

        if looks_like_import_package(extracted_root):
            import_snapshot = snapshot_from_import_package(extracted_root)
            sources = merge_source_results(sources + import_snapshot.sources)
            organization_name = organization_name or import_snapshot.organization_name
            organization_domain = (
                organization_domain or import_snapshot.organization_domain
            )
        else:
            sources = merge_source_results(
                sources + detect_export_sources(extracted_root)
            )

        if not sources:
            raise ValueError(f"no supported exports detected under: {extracted_root}")

        resolved_org_name = (
            organization_name
            or (existing_snapshot.organization_name if existing_snapshot else "")
            or "Imported Context"
        )
        resolved_org_domain = organization_domain or (
            existing_snapshot.organization_domain if existing_snapshot else ""
        )
        snapshot = ContextSnapshot(
            organization_name=resolved_org_name,
            organization_domain=resolved_org_domain,
            captured_at=iso_now(),
            sources=sources,
            metadata={"normalized_from": str(raw)},
        )
        cleaned_snapshot = cleanup_normalized_snapshot(snapshot)
        status_summary = summarize_context_snapshot(cleaned_snapshot)
        metadata = dict(cleaned_snapshot.metadata)
        metadata["timestamp_quality"] = [
            item.model_dump(mode="json") for item in status_summary.timestamp_quality
        ]
        return cleaned_snapshot.model_copy(update={"metadata": metadata})
    finally:
        cleanup_temp_dir(extracted_root, original=raw)


def build_public_context_template(
    *,
    organization_name: str,
    organization_domain: str,
):
    return build_public_context_sidecar(
        organization_name=organization_name,
        organization_domain=organization_domain,
        live=False,
    )


def build_public_context_sidecar(
    *,
    organization_name: str,
    organization_domain: str,
    live: bool = True,
):
    if live:
        return _build_public_context(
            organization_name=organization_name,
            organization_domain=organization_domain,
            live=True,
        )
    template = _empty_public_context(
        organization_name=organization_name,
        organization_domain=organization_domain,
    )
    return template.model_copy(
        update={
            "prepared_at": iso_now(),
            "integration_hint": (
                "Fill in financial snapshots and public news events for this company."
            ),
        }
    )
