from __future__ import annotations

from typing import Any, Dict, List

from vei.context.models import ContextProviderConfig, ContextSourceResult

from .base import api_get_json, iso_now, resolve_token


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
