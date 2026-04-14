from __future__ import annotations

import csv
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List

from vei.context.models import ContextProviderConfig, ContextSourceResult

from .base import api_get_json, iso_now, resolve_token

logger = logging.getLogger(__name__)


class GoogleContextProvider:
    name = "google"

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult:
        token = _resolve_google_token(config)
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        timeout = config.timeout_s
        limit = min(config.limit, 200)
        domain = config.filters.get("domain", "")

        users = _fetch_directory_users(headers, timeout, domain=domain, limit=limit)
        docs = _fetch_drive_files(headers, timeout, limit=limit)

        return ContextSourceResult(
            provider="google",
            captured_at=iso_now(),
            status="ok",
            record_counts={
                "users": len(users),
                "documents": len(docs),
            },
            data={
                "users": users,
                "documents": docs,
            },
        )


def _resolve_google_token(config: ContextProviderConfig) -> str:
    """Resolve a Google access token.

    Supports two modes:
    - token_env points to an env var containing an OAuth2 access token directly
    - filters.credentials_path points to a service account JSON (for future use)
    """
    return resolve_token(config)


def _fetch_directory_users(
    headers: Dict[str, str],
    timeout: int,
    *,
    domain: str,
    limit: int,
) -> List[Dict[str, Any]]:
    url = (
        "https://admin.googleapis.com/admin/directory/v1/users"
        f"?maxResults={limit}&orderBy=email"
    )
    if domain:
        url += f"&domain={domain}"
    result = api_get_json(url, headers=headers, timeout_s=timeout)
    raw_users = result.get("users", []) if isinstance(result, dict) else []
    return [
        {
            "id": str(u.get("id", "")),
            "email": str((u.get("primaryEmail", ""))),
            "name": _user_full_name(u.get("name")),
            "org_unit": str(u.get("orgUnitPath", "")),
            "suspended": bool(u.get("suspended")),
            "is_admin": bool(u.get("isAdmin")),
        }
        for u in raw_users
        if isinstance(u, dict)
    ]


def _fetch_drive_files(
    headers: Dict[str, str],
    timeout: int,
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    url = (
        "https://www.googleapis.com/drive/v3/files"
        f"?pageSize={limit}"
        "&fields=files(id,name,mimeType,modifiedTime,owners,shared)"
        "&orderBy=modifiedTime desc"
        "&q=mimeType%3D'application/vnd.google-apps.document'"
    )
    result = api_get_json(url, headers=headers, timeout_s=timeout)
    raw_files = result.get("files", []) if isinstance(result, dict) else []
    return [
        {
            "doc_id": str(f.get("id", "")),
            "title": str(f.get("name", "")),
            "mime_type": str(f.get("mimeType", "")),
            "modified_time": str(f.get("modifiedTime", "")),
            "shared": bool(f.get("shared")),
        }
        for f in raw_files
        if isinstance(f, dict)
    ]


def _user_full_name(name_obj: Any) -> str:
    if isinstance(name_obj, dict):
        given = str(name_obj.get("givenName", ""))
        family = str(name_obj.get("familyName", ""))
        return f"{given} {family}".strip()
    return ""


def capture_from_export(export_path: str | Path) -> ContextSourceResult:
    paths = _resolve_google_export_paths(Path(export_path))
    if not paths:
        return ContextSourceResult(
            provider="google",
            captured_at=iso_now(),
            status="error",
            error=f"google export not found: {export_path}",
        )

    payloads: list[dict[str, Any]] = []
    parse_warnings: list[str] = []
    for path in paths:
        try:
            payloads.append(_load_google_export_payload(path))
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "context google export parse failed for %s (%s)",
                path,
                type(exc).__name__,
                extra={
                    "source": "context_export",
                    "provider": "google",
                    "file_path": str(path),
                    "exception_type": type(exc).__name__,
                },
                exc_info=True,
            )
            parse_warnings.append(f"{path.name}: {type(exc).__name__}")

    if not payloads:
        return ContextSourceResult(
            provider="google",
            captured_at=iso_now(),
            status="error",
            error=f"failed to parse google export: {', '.join(parse_warnings)}",
        )

    payload = _merge_google_export_payloads(payloads)
    documents = _google_documents(payload)
    users = _google_users(payload)
    drive_shares = _google_drive_shares(payload)
    warnings = list(payload.get("parse_warnings", []))
    warnings.extend(parse_warnings)

    status = "ok"
    if warnings:
        status = "partial"
    if not documents and not users and not drive_shares:
        status = "empty" if not warnings else "partial"
    return ContextSourceResult(
        provider="google",
        captured_at=iso_now(),
        status=status,
        record_counts={
            "users": len(users),
            "documents": len(documents),
            "drive_shares": len(drive_shares),
        },
        data={
            "users": users,
            "documents": documents,
            "drive_shares": drive_shares,
            "parse_warnings": warnings,
        },
    )


def _resolve_google_export_paths(root: Path) -> list[Path]:
    if root.is_file():
        return [root]
    paths: list[Path] = []
    for name in (
        "google.json",
        "google_docs.json",
        "google_docs.csv",
        "drive_export.json",
        "drive_export.csv",
        "google_drive_shares.csv",
    ):
        candidate = root / name
        if candidate.exists():
            paths.append(candidate)
    if paths:
        return paths
    raw_dir = root / "raw"
    if raw_dir.is_dir():
        return _resolve_google_export_paths(raw_dir)
    return []


def _merge_google_export_payloads(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    users_by_key: dict[str, dict[str, Any]] = {}
    documents_by_key: dict[str, dict[str, Any]] = {}
    drive_shares: list[dict[str, Any]] = []
    parse_warnings: list[str] = []
    saw_explicit_drive_shares = False

    for payload in payloads:
        for index, item in enumerate(payload.get("users", [])):
            if not isinstance(item, dict):
                continue
            key = _google_user_key(item, index=index)
            existing = users_by_key.get(key)
            users_by_key[key] = _merge_google_record(existing, item)

        for index, item in enumerate(payload.get("documents", [])):
            if not isinstance(item, dict):
                continue
            key = _google_document_key(item, index=index)
            existing = documents_by_key.get(key)
            documents_by_key[key] = _merge_google_record(existing, item)

        raw_drive_shares = payload.get("drive_shares", [])
        if isinstance(raw_drive_shares, list):
            saw_explicit_drive_shares = True
            for item in raw_drive_shares:
                if isinstance(item, dict):
                    drive_shares.append(item)

        raw_warnings = payload.get("parse_warnings", [])
        if isinstance(raw_warnings, list):
            parse_warnings.extend(
                str(item) for item in raw_warnings if str(item).strip()
            )

    merged: dict[str, Any] = {
        "users": list(users_by_key.values()),
        "documents": list(documents_by_key.values()),
        "parse_warnings": parse_warnings,
    }
    if saw_explicit_drive_shares:
        merged["drive_shares"] = _dedupe_drive_shares(drive_shares)
    return merged


def _google_user_key(item: dict[str, Any], *, index: int) -> str:
    user_id = str(item.get("id") or "").strip()
    if user_id:
        return user_id
    email = str(item.get("email") or item.get("primaryEmail") or "").strip().lower()
    if email:
        return email
    return f"user-{index + 1}"


def _google_document_key(item: dict[str, Any], *, index: int) -> str:
    doc_id = str(item.get("doc_id") or item.get("id") or "").strip()
    if doc_id:
        return doc_id
    title = str(item.get("title") or item.get("name") or "").strip().lower()
    if title:
        return title
    return f"doc-{index + 1}"


def _merge_google_record(
    existing: dict[str, Any] | None,
    incoming: dict[str, Any],
) -> dict[str, Any]:
    if existing is None:
        return dict(incoming)

    merged = dict(existing)
    for key, value in incoming.items():
        if isinstance(value, list):
            if not value:
                continue
            current = merged.get(key, [])
            if not isinstance(current, list):
                merged[key] = list(value)
                continue
            merged[key] = _merge_record_lists(current, value)
            continue
        if isinstance(value, bool):
            merged[key] = bool(merged.get(key)) or value
            continue
        if str(value or "").strip():
            merged[key] = value
    return merged


def _merge_record_lists(existing: list[Any], incoming: list[Any]) -> list[Any]:
    merged = list(existing)
    seen = {_list_identity(item) for item in merged}
    for item in incoming:
        identity = _list_identity(item)
        if identity in seen:
            continue
        seen.add(identity)
        merged.append(item)
    return merged


def _dedupe_drive_shares(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    for item in items:
        key = json.dumps(
            {
                "doc_id": str(item.get("doc_id") or "").strip(),
                "title": str(item.get("title") or "").strip(),
                "owner": str(item.get("owner") or "").strip(),
                "visibility": str(item.get("visibility") or "").strip(),
                "classification": str(item.get("classification") or "").strip(),
                "shared_with": (
                    list(item.get("shared_with", []))
                    if isinstance(item.get("shared_with"), list)
                    else []
                ),
            },
            sort_keys=True,
        )
        unique[key] = item
    return list(unique.values())


def _list_identity(value: Any) -> str:
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    return str(value)


def _load_google_export_payload(path: Path) -> dict[str, Any]:
    fallback_modified_time = _fallback_export_modified_time(path)
    if path.suffix.lower() == ".csv":
        rows = list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
        lower_name = path.name.lower()
        if lower_name == "google_drive_shares.csv":
            return {"drive_shares": _csv_google_drive_shares(rows)}
        documents = _csv_google_documents(
            rows,
            fallback_modified_time=fallback_modified_time,
        )
        if lower_name == "google_docs.csv":
            return {"documents": documents}
        return {
            "documents": documents,
            "drive_shares": _csv_google_drive_shares(rows),
        }

    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return _backfill_google_payload_timestamps(
            payload,
            fallback_modified_time=fallback_modified_time,
        )
    if isinstance(payload, list):
        return {
            "documents": _normalize_payload_documents(
                payload,
                fallback_modified_time=fallback_modified_time,
            )
        }
    raise ValueError(f"unsupported google export payload: {path}")


def _google_users(payload: dict[str, Any]) -> list[dict[str, Any]]:
    users = payload.get("users", [])
    if not isinstance(users, list):
        return []
    return [item for item in users if isinstance(item, dict)]


def _csv_google_documents(
    rows: list[dict[str, Any]],
    *,
    fallback_modified_time: str,
) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    seen_doc_ids: set[str] = set()
    for row in rows:
        doc_id = str(row.get("doc_id") or row.get("id") or "").strip()
        title = str(row.get("title") or row.get("name") or doc_id).strip()
        if not doc_id or doc_id in seen_doc_ids:
            continue
        seen_doc_ids.add(doc_id)
        modified_time = str(
            row.get("modified_time")
            or row.get("updated")
            or row.get("exported_at")
            or ""
        ).strip()
        timestamp_quality = "provided" if modified_time else ""
        if not modified_time and fallback_modified_time:
            modified_time = fallback_modified_time
            timestamp_quality = "backfilled_from_export_file"
        documents.append(
            {
                "doc_id": doc_id,
                "title": title,
                "body": str(row.get("body") or "").strip(),
                "tags": _split_tokens(row.get("tags")),
                "modified_time": modified_time,
                "timestamp_quality": timestamp_quality or "missing_state_only",
            }
        )
    return documents


def _csv_google_drive_shares(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    drive_shares: list[dict[str, Any]] = []
    for row in rows:
        doc_id = str(row.get("doc_id") or row.get("id") or "").strip()
        title = str(row.get("title") or row.get("name") or doc_id).strip()
        if not doc_id:
            continue
        drive_shares.append(
            {
                "doc_id": doc_id,
                "title": title,
                "owner": str(row.get("owner") or "").strip(),
                "visibility": str(row.get("visibility") or "internal").strip(),
                "classification": str(row.get("classification") or "internal").strip(),
                "shared_with": _split_tokens(row.get("shared_with")),
            }
        )
    return drive_shares


def _google_documents(payload: dict[str, Any]) -> list[dict[str, Any]]:
    documents = payload.get("documents", [])
    if not isinstance(documents, list):
        return []
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(documents):
        if not isinstance(item, dict):
            continue
        doc_id = str(item.get("doc_id") or item.get("id") or f"doc-{index + 1}").strip()
        if not doc_id:
            continue
        normalized.append(
            {
                "doc_id": doc_id,
                "title": str(item.get("title") or item.get("name") or doc_id).strip(),
                "body": str(item.get("body") or "").strip(),
                "tags": (
                    list(item.get("tags", []))
                    if isinstance(item.get("tags"), list)
                    else _split_tokens(item.get("tags"))
                ),
                "modified_time": str(
                    item.get("modified_time") or item.get("updated") or ""
                ).strip(),
                "timestamp_quality": str(item.get("timestamp_quality") or "").strip(),
                "owner": str(item.get("owner") or "").strip(),
                "shared": bool(item.get("shared")),
                "comments": (
                    item.get("comments", [])
                    if isinstance(item.get("comments"), list)
                    else []
                ),
                "versions": (
                    item.get("versions", [])
                    if isinstance(item.get("versions"), list)
                    else []
                ),
                "permissions": (
                    item.get("permissions", [])
                    if isinstance(item.get("permissions"), list)
                    else []
                ),
            }
        )
    return normalized


def _backfill_google_payload_timestamps(
    payload: dict[str, Any],
    *,
    fallback_modified_time: str,
) -> dict[str, Any]:
    merged_payload = dict(payload)
    merged_payload["documents"] = _normalize_payload_documents(
        payload.get("documents", []),
        fallback_modified_time=fallback_modified_time,
    )
    return merged_payload


def _normalize_payload_documents(
    documents: Any,
    *,
    fallback_modified_time: str,
) -> list[dict[str, Any]]:
    normalized_documents: list[dict[str, Any]] = []
    if not isinstance(documents, list):
        return normalized_documents
    for index, item in enumerate(documents):
        if not isinstance(item, dict):
            continue
        document = dict(item)
        document.setdefault("doc_id", document.get("id") or f"doc-{index + 1}")
        modified_time = str(
            document.get("modified_time")
            or document.get("updated")
            or document.get("exported_at")
            or ""
        ).strip()
        timestamp_quality = str(document.get("timestamp_quality") or "").strip()
        if modified_time and not timestamp_quality:
            timestamp_quality = "provided"
        if not modified_time and fallback_modified_time:
            modified_time = fallback_modified_time
            timestamp_quality = "backfilled_from_export_file"
        if not modified_time and not timestamp_quality:
            timestamp_quality = "missing_state_only"
        document["modified_time"] = modified_time
        document["timestamp_quality"] = timestamp_quality
        normalized_documents.append(document)
    return normalized_documents


def _fallback_export_modified_time(path: Path) -> str:
    try:
        return (
            datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
            .isoformat()
            .replace(
                "+00:00",
                "Z",
            )
        )
    except OSError:
        return ""


def _google_drive_shares(payload: dict[str, Any]) -> list[dict[str, Any]]:
    drive_shares = payload.get("drive_shares")
    if isinstance(drive_shares, list):
        return [item for item in drive_shares if isinstance(item, dict)]
    documents = _google_documents(payload)
    shares: list[dict[str, Any]] = []
    for item in documents:
        shared_with = item.get("shared_with")
        if not isinstance(shared_with, list):
            shared_with = []
        shares.append(
            {
                "doc_id": item["doc_id"],
                "title": item["title"],
                "owner": str(item.get("owner") or "").strip(),
                "visibility": str(item.get("visibility") or "internal").strip(),
                "classification": str(item.get("classification") or "internal").strip(),
                "shared_with": shared_with,
            }
        )
    return shares


def _split_tokens(value: object) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    delimiter = ";" if ";" in text else ","
    return [item.strip() for item in text.split(delimiter) if item.strip()]
