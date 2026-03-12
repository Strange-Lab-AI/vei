from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen

from pydantic import BaseModel, Field

from .models import ImportPackage, ImportSourceManifest


class ImportConnector(Protocol):
    def sync(self, destination_root: str | Path) -> ImportPackage: ...


class OktaConnectorConfig(BaseModel):
    base_url: str
    token: str | None = None
    token_env: str | None = None
    timeout_s: int = 30
    limit: int = 200
    organization_name: str | None = None
    organization_domain: str | None = None
    timezone: str = "UTC"
    package_name: str = "okta_live_identity_export"
    package_title: str = "Okta Live Identity Export"
    package_description: str = (
        "Read-only identity export package synchronized from a live Okta tenant."
    )
    redaction_status: str = "source_live"
    metadata: dict[str, Any] = Field(default_factory=dict)


class SourceSyncResult(BaseModel):
    connector: str
    package_root: Path
    package: ImportPackage
    record_counts: dict[str, int] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class OktaImportConnector:
    def __init__(self, config: OktaConnectorConfig) -> None:
        self.config = config

    def sync(self, destination_root: str | Path) -> ImportPackage:
        return sync_okta_import_package(destination_root, self.config).package


def load_okta_connector_config(path: str | Path) -> OktaConnectorConfig:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return OktaConnectorConfig.model_validate(payload)


def sync_okta_import_package(
    destination_root: str | Path,
    config: OktaConnectorConfig,
    *,
    source_prefix: str = "okta_live",
) -> SourceSyncResult:
    root = Path(destination_root).expanduser().resolve()
    raw_root = root / "raw"
    raw_root.mkdir(parents=True, exist_ok=True)

    users = _fetch_okta_collection(config, "/api/v1/users")
    groups = _fetch_okta_collection(config, "/api/v1/groups")
    apps = _fetch_okta_collection(config, "/api/v1/apps")

    group_memberships = {
        str(group.get("id")): [
            str(item.get("id"))
            for item in _fetch_okta_collection(
                config, f"/api/v1/groups/{group.get('id')}/users"
            )
            if item.get("id")
        ]
        for group in groups
        if group.get("id")
    }
    app_assignments = {
        str(app.get("id")): [
            str(item.get("id"))
            for item in _fetch_okta_collection(
                config, f"/api/v1/apps/{app.get('id')}/users"
            )
            if item.get("id")
        ]
        for app in apps
        if app.get("id")
    }

    user_group_ids: dict[str, list[str]] = {}
    for group_id, member_ids in group_memberships.items():
        for user_id in member_ids:
            user_group_ids.setdefault(user_id, []).append(group_id)

    user_app_ids: dict[str, list[str]] = {}
    for app_id, member_ids in app_assignments.items():
        for user_id in member_ids:
            user_app_ids.setdefault(user_id, []).append(app_id)

    user_rows = [
        {
            "id": user.get("id"),
            "profile": {
                **dict(user.get("profile") or {}),
                "login": (user.get("profile") or {}).get("login")
                or (user.get("profile") or {}).get("email"),
            },
            "status": user.get("status"),
            "last_login_ms": _to_epoch_ms(user.get("lastLogin")),
            "group_ids": sorted(user_group_ids.get(str(user.get("id")), [])),
            "application_ids": sorted(user_app_ids.get(str(user.get("id")), [])),
        }
        for user in users
    ]
    group_rows = [
        {
            **dict(group),
            "members": sorted(group_memberships.get(str(group.get("id")), [])),
        }
        for group in groups
    ]
    app_rows = [
        {
            **dict(app),
            "assignments": sorted(app_assignments.get(str(app.get("id")), [])),
        }
        for app in apps
    ]

    users_path = raw_root / "okta_users.json"
    groups_path = raw_root / "okta_groups.json"
    apps_path = raw_root / "okta_apps.json"
    users_path.write_text(json.dumps({"users": user_rows}, indent=2), encoding="utf-8")
    groups_path.write_text(
        json.dumps({"groups": group_rows}, indent=2), encoding="utf-8"
    )
    apps_path.write_text(
        json.dumps({"applications": app_rows}, indent=2), encoding="utf-8"
    )

    collected_at = _iso_now()
    organization_name = config.organization_name or _default_org_name(config.base_url)
    organization_domain = config.organization_domain or _default_org_domain(
        config.base_url
    )
    package = ImportPackage(
        name=config.package_name,
        title=config.package_title,
        description=config.package_description,
        organization_name=organization_name,
        organization_domain=organization_domain,
        timezone=config.timezone,
        sources=[
            ImportSourceManifest(
                source_id=f"{source_prefix}_users",
                source_system="okta",
                source_kind="connector_snapshot",
                connector_id=source_prefix,
                file_type="json",
                relative_path="raw/okta_users.json",
                collected_at=collected_at,
                mapping_profile="okta_users_live_v1",
                redaction_status=config.redaction_status,
                description="Users snapshot synchronized from live Okta.",
                connector_metadata={"endpoint": "/api/v1/users"},
            ),
            ImportSourceManifest(
                source_id=f"{source_prefix}_groups",
                source_system="okta",
                source_kind="connector_snapshot",
                connector_id=source_prefix,
                file_type="json",
                relative_path="raw/okta_groups.json",
                collected_at=collected_at,
                mapping_profile="okta_groups_live_v1",
                redaction_status=config.redaction_status,
                description="Groups snapshot synchronized from live Okta.",
                connector_metadata={"endpoint": "/api/v1/groups"},
            ),
            ImportSourceManifest(
                source_id=f"{source_prefix}_apps",
                source_system="okta",
                source_kind="connector_snapshot",
                connector_id=source_prefix,
                file_type="json",
                relative_path="raw/okta_apps.json",
                collected_at=collected_at,
                mapping_profile="okta_apps_live_v1",
                redaction_status=config.redaction_status,
                description="Applications snapshot synchronized from live Okta.",
                connector_metadata={"endpoint": "/api/v1/apps"},
            ),
        ],
        metadata={
            **dict(config.metadata),
            "source_connector": "okta",
            "source_mode": "live",
            "synced_at": collected_at,
        },
    )
    (root / "package.json").write_text(
        package.model_dump_json(indent=2), encoding="utf-8"
    )
    return SourceSyncResult(
        connector="okta",
        package_root=root,
        package=package,
        record_counts={
            "users": len(user_rows),
            "groups": len(group_rows),
            "applications": len(app_rows),
        },
        metadata={"source_prefix": source_prefix},
    )


def _fetch_okta_collection(
    config: OktaConnectorConfig, endpoint: str, *, params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    token = _resolve_token(config)
    url = _with_query(
        urljoin(config.base_url.rstrip("/") + "/", endpoint.lstrip("/")),
        {"limit": str(config.limit), **dict(params or {})},
    )
    items: list[dict[str, Any]] = []
    while url:
        payload, next_url = _okta_get_json(url, token=token, timeout_s=config.timeout_s)
        if isinstance(payload, list):
            items.extend(dict(item) for item in payload)
        else:
            raise ValueError(f"Expected list payload from Okta endpoint: {endpoint}")
        url = next_url
    return items


def _okta_get_json(url: str, *, token: str, timeout_s: int) -> tuple[Any, str | None]:
    _validate_okta_url(url)
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"SSWS {token}",
        },
        method="GET",
    )
    with urlopen(request, timeout=timeout_s) as response:  # nosec B310
        payload = json.loads(response.read().decode("utf-8"))
        link_header = response.headers.get("Link")
    return payload, _parse_next_link(link_header)


def _resolve_token(config: OktaConnectorConfig) -> str:
    if config.token:
        return config.token
    if config.token_env:
        import os

        value = os.environ.get(config.token_env)
        if value:
            return value
        raise ValueError(f"missing Okta token in env var: {config.token_env}")
    raise ValueError("Okta connector config requires token or token_env")


def _parse_next_link(header: str | None) -> str | None:
    if not header:
        return None
    for chunk in header.split(","):
        match = re.match(r'\s*<([^>]+)>;\s*rel="([^"]+)"', chunk.strip())
        if match and match.group(2) == "next":
            return match.group(1)
    return None


def _with_query(url: str, params: dict[str, str]) -> str:
    parts = urlparse(url)
    query = parse_qs(parts.query)
    for key, value in params.items():
        query[key] = [value]
    return urlunparse(parts._replace(query=urlencode(query, doseq=True)))


def _validate_okta_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError("Okta connector requires https URLs with an explicit host")


def _to_epoch_ms(value: Any) -> int | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(
                datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000
            )
        except ValueError:
            return None
    return None


def _default_org_name(base_url: str) -> str:
    host = urlparse(base_url).netloc or base_url
    return host.split(".")[0].replace("-", " ").title() or "Imported Organization"


def _default_org_domain(base_url: str) -> str:
    host = urlparse(base_url).netloc or base_url
    return host or "example.com"


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


__all__ = [
    "ImportConnector",
    "OktaConnectorConfig",
    "OktaImportConnector",
    "SourceSyncResult",
    "load_okta_connector_config",
    "sync_okta_import_package",
]
