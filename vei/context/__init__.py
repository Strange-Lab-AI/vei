from importlib import import_module
from typing import Any

from vei.context.api import (
    capture_context,
    diff_snapshots,
    hydrate_blueprint,
    ingest_mail_archive_threads,
)
from vei.context.models import (
    BundleVerificationCheck,
    BundleVerificationResult,
    ContextDiff,
    ContextDiffEntry,
    ContextProviderConfig,
    ContextSnapshot,
    ContextSourceResult,
)

__all__ = [
    "BundleVerificationCheck",
    "BundleVerificationResult",
    "ContextDiff",
    "ContextDiffEntry",
    "ContextProviderConfig",
    "ContextSnapshot",
    "ContextSourceResult",
    "capture_context",
    "diff_snapshots",
    "build_public_context_sidecar",
    "hydrate_blueprint",
    "ingest_mail_archive_threads",
    "build_public_context_template",
    "normalize_raw_exports",
    "verify_context_snapshot",
]

_NORMALIZE_EXPORTS = {
    "build_public_context_sidecar",
    "build_public_context_template",
    "normalize_raw_exports",
    "verify_context_snapshot",
}


def __getattr__(name: str) -> Any:
    if name in _NORMALIZE_EXPORTS:
        module = import_module("vei.context.normalize")
        return getattr(module, name)
    raise AttributeError(f"module 'vei.context' has no attribute {name!r}")
