from __future__ import annotations

from pathlib import Path

from vei.context.models import ContextSourceResult

from .crm import capture_from_export as _capture_crm_export


def capture_from_export(export_path: str | Path) -> ContextSourceResult:
    return _capture_crm_export(export_path, provider="salesforce")
