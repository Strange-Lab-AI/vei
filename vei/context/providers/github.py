from __future__ import annotations

from typing import Any

from vei.context.models import ContextProviderConfig, ContextSourceResult

from .base import api_get_json, iso_now, join_url, resolve_token


class GitHubContextProvider:
    name = "github"

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult:
        token = resolve_token(config)
        repo = str(
            config.filters.get("repo") or config.filters.get("repository") or ""
        ).strip()
        if not repo:
            raise ValueError("github provider requires filters.repo")
        base_url = str(config.base_url or "https://api.github.com").strip()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        timeout = config.timeout_s
        limit = min(config.limit, 100)

        repository = api_get_json(
            join_url(base_url, f"/repos/{repo}"),
            headers=headers,
            timeout_s=timeout,
        )
        raw_items = api_get_json(
            join_url(base_url, f"/repos/{repo}/issues?state=all&per_page={limit}"),
            headers=headers,
            timeout_s=timeout,
        )
        issues: list[dict[str, Any]] = []
        pull_requests: list[dict[str, Any]] = []
        for item in raw_items if isinstance(raw_items, list) else []:
            if not isinstance(item, dict):
                continue
            parsed = _issue_like_item(
                item,
                headers=headers,
                timeout=timeout,
            )
            if item.get("pull_request"):
                pull_requests.append(parsed)
                continue
            issues.append(parsed)

        return ContextSourceResult(
            provider="github",
            captured_at=iso_now(),
            status="ok" if issues or pull_requests else "empty",
            record_counts={
                "repositories": 1 if isinstance(repository, dict) else 0,
                "issues": len(issues),
                "pull_requests": len(pull_requests),
            },
            data={
                "repositories": [repository] if isinstance(repository, dict) else [],
                "issues": issues,
                "pull_requests": pull_requests,
            },
        )


def _issue_like_item(
    item: dict[str, Any],
    *,
    headers: dict[str, str],
    timeout: int,
) -> dict[str, Any]:
    comments_url = str(item.get("comments_url") or "").strip()
    comments: list[dict[str, Any]] = []
    if comments_url and int(item.get("comments", 0) or 0) > 0:
        raw_comments = api_get_json(comments_url, headers=headers, timeout_s=timeout)
        comments = [
            {
                "id": str(comment.get("id", "")),
                "author": str((comment.get("user") or {}).get("login", "")),
                "body": str(comment.get("body", "") or ""),
                "created_at": str(comment.get("created_at", "") or ""),
            }
            for comment in raw_comments
            if isinstance(comment, dict)
        ]
    return {
        "id": str(item.get("id", "")),
        "number": int(item.get("number", 0) or 0),
        "title": str(item.get("title", "") or ""),
        "body": str(item.get("body", "") or ""),
        "state": str(item.get("state", "") or ""),
        "author": str((item.get("user") or {}).get("login", "") or ""),
        "updated_at": str(item.get("updated_at", "") or ""),
        "created_at": str(item.get("created_at", "") or ""),
        "comments": comments,
    }
