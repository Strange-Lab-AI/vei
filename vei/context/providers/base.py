from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen

from vei.context.models import ContextProviderConfig, ContextSourceResult


class ContextProvider(Protocol):
    name: str

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult: ...


def resolve_token(config: ContextProviderConfig) -> str:
    if config.token_env:
        value = os.environ.get(config.token_env)
        if value:
            return value
        raise ValueError(
            f"context provider {config.provider}: "
            f"missing token in env var {config.token_env}"
        )
    raise ValueError(f"context provider {config.provider}: token_env is required")


def iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def api_get_json(
    url: str,
    *,
    headers: dict[str, str],
    timeout_s: int = 30,
) -> Any:
    request = Request(url, headers=headers, method="GET")
    with urlopen(request, timeout=timeout_s) as response:  # nosec B310
        return json.loads(response.read().decode("utf-8"))


def api_get_json_with_headers(
    url: str,
    *,
    headers: dict[str, str],
    timeout_s: int = 30,
) -> tuple[Any, dict[str, str]]:
    request = Request(url, headers=headers, method="GET")
    with urlopen(request, timeout=timeout_s) as response:  # nosec B310
        payload = json.loads(response.read().decode("utf-8"))
        resp_headers = {k.lower(): v for k, v in response.headers.items()}
    return payload, resp_headers


def with_query(url: str, params: dict[str, str]) -> str:
    parts = urlparse(url)
    query = parse_qs(parts.query)
    for key, value in params.items():
        query[key] = [value]
    return urlunparse(parts._replace(query=urlencode(query, doseq=True)))


def join_url(base: str, path: str) -> str:
    return urljoin(base.rstrip("/") + "/", path.lstrip("/"))
