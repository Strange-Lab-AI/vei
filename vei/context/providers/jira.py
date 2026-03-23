from __future__ import annotations

from typing import Any, Dict, List

from vei.context.models import ContextProviderConfig, ContextSourceResult

from .base import api_get_json, iso_now, join_url, resolve_token


class JiraContextProvider:
    name = "jira"

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult:
        token = resolve_token(config)
        base_url = config.base_url
        if not base_url:
            raise ValueError("jira provider requires base_url")

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        timeout = config.timeout_s
        limit = min(config.limit, 100)

        project_key = config.filters.get("project")
        jql = config.filters.get("jql", "")
        if not jql:
            jql = (
                f"project = {project_key} ORDER BY updated DESC"
                if project_key
                else "ORDER BY updated DESC"
            )

        issues = _fetch_issues(base_url, headers, timeout, jql=jql, limit=limit)
        projects = _fetch_projects(base_url, headers, timeout)

        return ContextSourceResult(
            provider="jira",
            captured_at=iso_now(),
            status="ok",
            record_counts={
                "issues": len(issues),
                "projects": len(projects),
            },
            data={
                "issues": issues,
                "projects": projects,
            },
        )


def _fetch_issues(
    base_url: str,
    headers: Dict[str, str],
    timeout: int,
    *,
    jql: str,
    limit: int,
) -> List[Dict[str, Any]]:
    url = join_url(
        base_url,
        f"/rest/api/3/search?jql={jql}&maxResults={limit}"
        "&fields=summary,status,assignee,description,issuetype,priority,updated",
    )
    result = api_get_json(url, headers=headers, timeout_s=timeout)
    raw_issues = result.get("issues", []) if isinstance(result, dict) else []
    issues: List[Dict[str, Any]] = []
    for issue in raw_issues:
        if not isinstance(issue, dict):
            continue
        fields = issue.get("fields") or {}
        issues.append(
            {
                "ticket_id": str(issue.get("key", "")),
                "title": str(fields.get("summary", "")),
                "status": _nested_name(fields.get("status")),
                "assignee": _nested_name(fields.get("assignee"), key="displayName"),
                "description": _adf_to_text(fields.get("description")),
                "issue_type": _nested_name(fields.get("issuetype")),
                "priority": _nested_name(fields.get("priority")),
                "updated": str(fields.get("updated", "")),
            }
        )
    return issues


def _fetch_projects(
    base_url: str,
    headers: Dict[str, str],
    timeout: int,
) -> List[Dict[str, Any]]:
    url = join_url(base_url, "/rest/api/3/project")
    result = api_get_json(url, headers=headers, timeout_s=timeout)
    if not isinstance(result, list):
        return []
    return [
        {
            "key": str(p.get("key", "")),
            "name": str(p.get("name", "")),
            "style": str(p.get("style", "")),
        }
        for p in result
        if isinstance(p, dict)
    ]


def _nested_name(value: Any, *, key: str = "name") -> str:
    if isinstance(value, dict):
        return str(value.get(key, ""))
    return ""


def _adf_to_text(value: Any) -> str:
    """Extract plain text from Atlassian Document Format (ADF) or return as-is."""
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        return ""
    parts: list[str] = []
    _walk_adf(value, parts)
    return " ".join(parts).strip()


def _walk_adf(node: Any, parts: list[str]) -> None:
    if isinstance(node, dict):
        if node.get("type") == "text":
            text = node.get("text")
            if isinstance(text, str):
                parts.append(text)
        for child in node.get("content", []):
            _walk_adf(child, parts)
    elif isinstance(node, list):
        for child in node:
            _walk_adf(child, parts)
