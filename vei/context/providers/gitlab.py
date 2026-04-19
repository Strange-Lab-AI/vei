from __future__ import annotations

from typing import Any
from urllib.parse import quote_plus

from vei.context.models import ContextProviderConfig, ContextSourceResult

from .base import api_get_json, iso_now, join_url, resolve_token


class GitLabContextProvider:
    name = "gitlab"

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult:
        token = resolve_token(config)
        project = str(config.filters.get("project") or "").strip()
        if not project:
            raise ValueError("gitlab provider requires filters.project")
        base_url = str(config.base_url or "https://gitlab.com/api/v4").strip()
        headers = {
            "PRIVATE-TOKEN": token,
            "Accept": "application/json",
        }
        timeout = config.timeout_s
        limit = min(config.limit, 100)
        project_key = quote_plus(project)

        project_payload = api_get_json(
            join_url(base_url, f"/projects/{project_key}"),
            headers=headers,
            timeout_s=timeout,
        )
        raw_issues = api_get_json(
            join_url(
                base_url,
                f"/projects/{project_key}/issues?state=all&per_page={limit}",
            ),
            headers=headers,
            timeout_s=timeout,
        )
        raw_merge_requests = api_get_json(
            join_url(
                base_url,
                f"/projects/{project_key}/merge_requests?state=all&per_page={limit}",
            ),
            headers=headers,
            timeout_s=timeout,
        )
        issues = [
            _issue_like_item("issues", item, headers=headers, timeout=timeout)
            for item in raw_issues
            if isinstance(item, dict)
        ]
        merge_requests = [
            _issue_like_item("merge_requests", item, headers=headers, timeout=timeout)
            for item in raw_merge_requests
            if isinstance(item, dict)
        ]

        return ContextSourceResult(
            provider="gitlab",
            captured_at=iso_now(),
            status="ok" if issues or merge_requests else "empty",
            record_counts={
                "projects": 1 if isinstance(project_payload, dict) else 0,
                "issues": len(issues),
                "merge_requests": len(merge_requests),
            },
            data={
                "projects": (
                    [project_payload] if isinstance(project_payload, dict) else []
                ),
                "issues": issues,
                "merge_requests": merge_requests,
            },
        )


def _issue_like_item(
    resource_name: str,
    item: dict[str, Any],
    *,
    headers: dict[str, str],
    timeout: int,
) -> dict[str, Any]:
    web_url = str(item.get("web_url", "") or "")
    notes_url = str(item.get("_links", {}).get("notes", "") or "").strip()
    comments: list[dict[str, Any]] = []
    if notes_url:
        raw_comments = api_get_json(notes_url, headers=headers, timeout_s=timeout)
        comments = [
            {
                "id": str(comment.get("id", "")),
                "author": str((comment.get("author") or {}).get("username", "")),
                "body": str(comment.get("body", "") or ""),
                "created_at": str(comment.get("created_at", "") or ""),
            }
            for comment in raw_comments
            if isinstance(comment, dict)
        ]
    return {
        "id": str(item.get("id", "")),
        "iid": int(item.get("iid", 0) or 0),
        "title": str(item.get("title", "") or ""),
        "body": str(item.get("description", "") or ""),
        "state": str(item.get("state", "") or ""),
        "author": str((item.get("author") or {}).get("username", "") or ""),
        "updated_at": str(item.get("updated_at", "") or ""),
        "created_at": str(item.get("created_at", "") or ""),
        "resource_name": resource_name,
        "web_url": web_url,
        "comments": comments,
    }
