from __future__ import annotations

import json
import tempfile
from pathlib import Path

from vei.context.models import ContextProviderConfig, ContextSourceResult
from vei.imports import (
    OktaConnectorConfig,
    sync_okta_import_package,
)

from .base import iso_now, resolve_token


class OktaContextProvider:
    name = "okta"

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult:
        token = resolve_token(config)
        base_url = config.base_url
        if not base_url:
            raise ValueError("okta provider requires base_url")

        okta_config = OktaConnectorConfig(
            base_url=base_url,
            token=token,
            timeout_s=config.timeout_s,
            limit=config.limit,
        )
        with tempfile.TemporaryDirectory(prefix="vei_okta_") as tmp:
            result = sync_okta_import_package(Path(tmp), okta_config)
            users, groups, apps = _load_okta_raw_payloads(result.package_root)

        return ContextSourceResult(
            provider="okta",
            captured_at=iso_now(),
            status="ok",
            record_counts=dict(result.record_counts),
            data={
                "users": users,
                "groups": groups,
                "applications": apps,
            },
        )


def _load_okta_raw_payloads(
    package_root: Path,
) -> tuple[list[dict], list[dict], list[dict]]:
    raw_dir = package_root / "raw"
    return (
        _load_json_key(raw_dir / "okta_users.json", "users"),
        _load_json_key(raw_dir / "okta_groups.json", "groups"),
        _load_json_key(raw_dir / "okta_apps.json", "applications"),
    )


def _load_json_key(path: Path, key: str) -> list[dict]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    value = payload.get(key, [])
    return value if isinstance(value, list) else []
