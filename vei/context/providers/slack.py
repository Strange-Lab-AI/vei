from __future__ import annotations

from typing import Any, Dict, List

from vei.context.models import ContextProviderConfig, ContextSourceResult

from .base import api_get_json, iso_now, resolve_token


class SlackContextProvider:
    name = "slack"

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult:
        token = resolve_token(config)
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        timeout = config.timeout_s
        limit = min(config.limit, 200)

        channels = _fetch_channels(headers, timeout, limit, config.filters)
        channel_filter = config.filters.get("channels")

        captured_channels: List[Dict[str, Any]] = []
        total_messages = 0
        for channel in channels:
            name = channel.get("name", "")
            if channel_filter and name not in channel_filter:
                continue
            cid = channel.get("id", "")
            messages = _fetch_channel_history(headers, timeout, cid, limit=limit)
            total_messages += len(messages)
            captured_channels.append(
                {
                    "channel": f"#{name}",
                    "channel_id": cid,
                    "unread": channel.get("unread_count", 0) or 0,
                    "messages": messages,
                }
            )

        users = _fetch_users(headers, timeout, limit)

        return ContextSourceResult(
            provider="slack",
            captured_at=iso_now(),
            status="ok",
            record_counts={
                "channels": len(captured_channels),
                "messages": total_messages,
                "users": len(users),
            },
            data={
                "channels": captured_channels,
                "users": users,
            },
        )


def _fetch_channels(
    headers: Dict[str, str],
    timeout: int,
    limit: int,
    filters: Dict[str, Any],
) -> List[Dict[str, Any]]:
    url = "https://slack.com/api/conversations.list"
    params = f"?types=public_channel&limit={limit}&exclude_archived=true"
    result = api_get_json(url + params, headers=headers, timeout_s=timeout)
    channels = result.get("channels", []) if result.get("ok") else []
    return [c for c in channels if isinstance(c, dict)]


def _fetch_channel_history(
    headers: Dict[str, str],
    timeout: int,
    channel_id: str,
    *,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    url = f"https://slack.com/api/conversations.history?channel={channel_id}&limit={limit}"
    result = api_get_json(url, headers=headers, timeout_s=timeout)
    if not result.get("ok"):
        return []
    messages = result.get("messages", [])
    return [
        {
            "ts": str(m.get("ts", "")),
            "user": str(m.get("user", m.get("username", "unknown"))),
            "text": str(m.get("text", "")),
            "thread_ts": m.get("thread_ts"),
        }
        for m in messages
        if isinstance(m, dict)
    ]


def _fetch_users(
    headers: Dict[str, str],
    timeout: int,
    limit: int,
) -> List[Dict[str, Any]]:
    url = f"https://slack.com/api/users.list?limit={limit}"
    result = api_get_json(url, headers=headers, timeout_s=timeout)
    if not result.get("ok"):
        return []
    members = result.get("members", [])
    return [
        {
            "id": str(m.get("id", "")),
            "name": str(m.get("name", "")),
            "real_name": str(m.get("real_name", m.get("name", ""))),
            "email": (m.get("profile") or {}).get("email", ""),
            "is_bot": bool(m.get("is_bot")),
            "deleted": bool(m.get("deleted")),
        }
        for m in members
        if isinstance(m, dict)
    ]
