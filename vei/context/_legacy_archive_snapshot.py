from __future__ import annotations

from typing import Any

from .models import ContextSnapshot, ContextSourceResult


def legacy_threads_payload_to_snapshot(
    payload: dict[str, Any],
    *,
    default_captured_at: str = "",
    fallback_organization_name: str = "",
    include_payload_metadata: bool = False,
    mark_empty_when_no_threads: bool = False,
) -> ContextSnapshot:
    organization_name = str(payload.get("organization_name", "") or "").strip()
    organization_domain = str(payload.get("organization_domain", "") or "").strip()
    threads = payload.get("threads", [])
    actors = payload.get("actors", [])
    captured_at = str(payload.get("captured_at") or default_captured_at).strip()

    thread_list = threads if isinstance(threads, list) else []
    actor_list = actors if isinstance(actors, list) else []
    status = "ok"
    if mark_empty_when_no_threads and not thread_list:
        status = "empty"

    metadata: dict[str, Any] = {}
    if include_payload_metadata:
        metadata = dict(payload.get("metadata", {}) or {})

    return ContextSnapshot(
        organization_name=organization_name or fallback_organization_name,
        organization_domain=organization_domain,
        captured_at=captured_at,
        sources=[
            ContextSourceResult(
                provider="mail_archive",
                captured_at=captured_at,
                status=status,
                record_counts={
                    "threads": len(thread_list),
                    "actors": len(actor_list),
                },
                data={
                    "threads": thread_list,
                    "actors": actor_list,
                },
            )
        ],
        metadata=metadata,
    )


__all__ = ["legacy_threads_payload_to_snapshot"]
