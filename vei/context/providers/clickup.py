from __future__ import annotations

from typing import Any

from vei.context.models import ContextProviderConfig, ContextSourceResult

from .base import api_get_json, iso_now, join_url, resolve_token


class ClickUpContextProvider:
    name = "clickup"

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult:
        token = resolve_token(config)
        list_id = str(config.filters.get("list_id") or "").strip()
        team_id = str(config.filters.get("team_id") or "").strip()
        if not list_id and not team_id:
            raise ValueError(
                "clickup provider requires filters.list_id or filters.team_id"
            )
        base_url = str(config.base_url or "https://api.clickup.com/api/v2").strip()
        headers = {
            "Authorization": token,
            "Accept": "application/json",
        }
        timeout = config.timeout_s
        limit = min(config.limit, 100)

        lists: list[dict[str, Any]] = []
        tasks: list[dict[str, Any]] = []
        if list_id:
            list_payload = api_get_json(
                join_url(base_url, f"/list/{list_id}"),
                headers=headers,
                timeout_s=timeout,
            )
            if isinstance(list_payload, dict):
                lists.append(list_payload)
            task_payload = api_get_json(
                join_url(
                    base_url,
                    f"/list/{list_id}/task?include_closed=true&subtasks=true&page=0&order_by=updated",
                ),
                headers=headers,
                timeout_s=timeout,
            )
            raw_tasks = (
                task_payload.get("tasks", []) if isinstance(task_payload, dict) else []
            )
            tasks.extend(
                _task_item(item) for item in raw_tasks[:limit] if isinstance(item, dict)
            )
        else:
            task_payload = api_get_json(
                join_url(
                    base_url,
                    f"/team/{team_id}/task?include_closed=true&subtasks=true&page=0",
                ),
                headers=headers,
                timeout_s=timeout,
            )
            raw_tasks = (
                task_payload.get("tasks", []) if isinstance(task_payload, dict) else []
            )
            tasks.extend(
                _task_item(item) for item in raw_tasks[:limit] if isinstance(item, dict)
            )

        return ContextSourceResult(
            provider="clickup",
            captured_at=iso_now(),
            status="ok" if tasks else "empty",
            record_counts={
                "lists": len(lists),
                "tasks": len(tasks),
            },
            data={
                "lists": lists,
                "tasks": tasks,
            },
        )


def _task_item(item: dict[str, Any]) -> dict[str, Any]:
    assignees = item.get("assignees") or []
    first_assignee = assignees[0] if isinstance(assignees, list) and assignees else {}
    return {
        "id": str(item.get("id", "")),
        "name": str(item.get("name", "") or ""),
        "description": str(item.get("description", "") or ""),
        "status": str((item.get("status") or {}).get("status", "") or ""),
        "creator": str((item.get("creator") or {}).get("email", "") or ""),
        "assignee": str((first_assignee or {}).get("email", "") or ""),
        "date_created": str(item.get("date_created", "") or ""),
        "date_updated": str(item.get("date_updated", "") or ""),
        "comments": [],
    }
