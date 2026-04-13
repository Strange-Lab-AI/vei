from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any

from vei.context.models import ContextSourceResult

from .base import iso_now

logger = logging.getLogger(__name__)


def capture_from_export(
    export_path: str | Path,
    *,
    provider: str = "crm",
) -> ContextSourceResult:
    path = _resolve_export_path(Path(export_path), provider=provider)
    if path is None or not path.exists():
        return ContextSourceResult(
            provider=provider,
            captured_at=iso_now(),
            status="error",
            error=f"{provider} export not found: {export_path}",
        )

    try:
        data = _load_export_payload(path)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "context %s export parse failed for %s (%s)",
            provider,
            path,
            type(exc).__name__,
            extra={
                "source": "context_export",
                "provider": provider,
                "file_path": str(path),
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return ContextSourceResult(
            provider=provider,
            captured_at=iso_now(),
            status="error",
            error=f"failed to parse {provider} export: {exc}",
        )

    companies = _companies(data)
    contacts = _contacts(data)
    deals = _deals(data)
    warnings = list(data.get("parse_warnings", [])) if isinstance(data, dict) else []
    status = "ok" if companies or contacts or deals else "empty"
    return ContextSourceResult(
        provider=provider,
        captured_at=iso_now(),
        status=status,
        record_counts={
            "companies": len(companies),
            "contacts": len(contacts),
            "deals": len(deals),
        },
        data={
            "companies": companies,
            "contacts": contacts,
            "deals": deals,
            "parse_warnings": warnings,
        },
    )


def _resolve_export_path(root: Path, *, provider: str) -> Path | None:
    if root.is_file():
        return root
    names = (
        f"{provider}.json",
        f"{provider}.csv",
        f"{provider}_deals.json",
        f"{provider}_deals.csv",
        "crm_deals.json",
        "crm_deals.csv",
        "salesforce_deals.json",
        "salesforce_deals.csv",
    )
    for name in names:
        candidate = root / name
        if candidate.exists():
            return candidate
    raw_dir = root / "raw"
    if raw_dir.is_dir():
        return _resolve_export_path(raw_dir, provider=provider)
    return None


def _load_export_payload(path: Path) -> dict[str, Any]:
    if path.suffix.lower() == ".csv":
        rows = list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
        return {"deals": rows}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        return {"deals": payload}
    raise ValueError(f"unsupported CRM payload: {path}")


def _companies(data: dict[str, Any]) -> list[dict[str, Any]]:
    companies = data.get("companies", [])
    if not isinstance(companies, list):
        return []
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(companies):
        if not isinstance(item, dict):
            continue
        company_id = str(
            item.get("id") or item.get("company_id") or f"company-{index + 1}"
        ).strip()
        if not company_id:
            continue
        normalized.append(
            {
                "id": company_id,
                "name": str(item.get("name") or company_id).strip(),
                "domain": str(item.get("domain") or "").strip(),
                "created_ms": _time_ms(item.get("created_ms") or item.get("created")),
            }
        )
    return normalized


def _contacts(data: dict[str, Any]) -> list[dict[str, Any]]:
    contacts = data.get("contacts", [])
    if not isinstance(contacts, list):
        return []
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(contacts):
        if not isinstance(item, dict):
            continue
        contact_id = str(
            item.get("id") or item.get("contact_id") or f"contact-{index + 1}"
        ).strip()
        if not contact_id:
            continue
        normalized.append(
            {
                "id": contact_id,
                "email": str(item.get("email") or "").strip(),
                "first_name": str(item.get("first_name") or "").strip(),
                "last_name": str(item.get("last_name") or "").strip(),
                "do_not_contact": bool(item.get("do_not_contact")),
                "company_id": str(item.get("company_id") or "").strip() or None,
                "created_ms": _time_ms(item.get("created_ms") or item.get("created")),
            }
        )
    return normalized


def _deals(data: dict[str, Any]) -> list[dict[str, Any]]:
    deals = data.get("deals", [])
    if not isinstance(deals, list):
        return []
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(deals):
        if not isinstance(item, dict):
            continue
        deal_id = str(
            item.get("id") or item.get("deal_id") or f"deal-{index + 1}"
        ).strip()
        if not deal_id:
            continue
        normalized.append(
            {
                "id": deal_id,
                "name": str(item.get("name") or deal_id).strip(),
                "amount": _amount(item.get("amount") or item.get("amount_usd")),
                "stage": str(item.get("stage") or "open").strip(),
                "owner": str(item.get("owner") or "").strip(),
                "contact_id": str(item.get("contact_id") or "").strip() or None,
                "company_id": str(item.get("company_id") or "").strip() or None,
                "created_ms": _time_ms(item.get("created_ms") or item.get("created")),
                "updated_ms": _time_ms(item.get("updated_ms") or item.get("updated")),
                "closed_ms": _time_ms(item.get("closed_ms") or item.get("closed_at")),
                "close_date": str(item.get("close_date") or "").strip(),
                "history": (
                    item.get("history", [])
                    if isinstance(item.get("history"), list)
                    else []
                ),
            }
        )
    return normalized


def _amount(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    cleaned = text.replace("$", "").replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _time_ms(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        number = int(value)
        return number if number > 1_000_000_000_000 else number * 1000
    text = str(value).strip()
    if not text:
        return 0
    try:
        number = int(float(text))
    except ValueError:
        return 0
    return number if number > 1_000_000_000_000 else number * 1000
