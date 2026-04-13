from __future__ import annotations

import logging
from typing import Any, Dict, List

from vei.context.models import ContextProviderConfig, ContextSourceResult

from .base import api_get_json, iso_now, resolve_token

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
logger = logging.getLogger(__name__)


class TeamsContextProvider:
    name = "teams"

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult:
        token = resolve_token(config)
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        timeout = config.timeout_s
        limit = min(config.limit, 50)

        teams = _fetch_joined_teams(headers, timeout)
        team_filter = config.filters.get("teams")

        captured_channels: List[Dict[str, Any]] = []
        total_messages = 0

        for team in teams:
            team_name = team.get("displayName", "")
            if team_filter and team_name not in team_filter:
                continue
            team_id = team.get("id", "")
            channels = _fetch_channels(headers, timeout, team_id)
            for channel in channels:
                channel_id = channel.get("id", "")
                channel_name = channel.get("displayName", "")
                messages = _fetch_channel_messages(
                    headers, timeout, team_id, channel_id, limit=limit
                )
                total_messages += len(messages)
                captured_channels.append(
                    {
                        "channel": f"#{team_name}/{channel_name}",
                        "channel_id": channel_id,
                        "team_id": team_id,
                        "team_name": team_name,
                        "unread": 0,
                        "messages": messages,
                    }
                )

        profile = _fetch_me(headers, timeout)

        return ContextSourceResult(
            provider="teams",
            captured_at=iso_now(),
            status="ok",
            record_counts={
                "teams": len(teams),
                "channels": len(captured_channels),
                "messages": total_messages,
            },
            data={
                "channels": captured_channels,
                "profile": profile,
            },
        )


def _fetch_joined_teams(
    headers: Dict[str, str],
    timeout: int,
) -> List[Dict[str, Any]]:
    url = f"{GRAPH_BASE}/me/joinedTeams"
    result = api_get_json(url, headers=headers, timeout_s=timeout)
    return result.get("value", []) if isinstance(result, dict) else []


def _fetch_channels(
    headers: Dict[str, str],
    timeout: int,
    team_id: str,
) -> List[Dict[str, Any]]:
    url = f"{GRAPH_BASE}/teams/{team_id}/channels"
    result = api_get_json(url, headers=headers, timeout_s=timeout)
    return result.get("value", []) if isinstance(result, dict) else []


def _fetch_channel_messages(
    headers: Dict[str, str],
    timeout: int,
    team_id: str,
    channel_id: str,
    *,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    url = (
        f"{GRAPH_BASE}/teams/{team_id}/channels/{channel_id}/messages" f"?$top={limit}"
    )
    try:
        result = api_get_json(url, headers=headers, timeout_s=timeout)
    except Exception as exc:
        logger.warning(
            "context teams channel message fetch failed for %s/%s (%s)",
            team_id,
            channel_id,
            type(exc).__name__,
            extra={
                "source": "context_capture",
                "provider": "teams",
                "file_path": url,
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return []
    raw_messages = result.get("value", []) if isinstance(result, dict) else []
    return [
        {
            "ts": str(m.get("createdDateTime", "")),
            "user": _extract_sender(m),
            "text": _extract_body(m),
            "thread_ts": m.get("replyToId"),
        }
        for m in raw_messages
        if isinstance(m, dict) and m.get("messageType") == "message"
    ]


def _fetch_me(
    headers: Dict[str, str],
    timeout: int,
) -> Dict[str, Any]:
    url = f"{GRAPH_BASE}/me"
    try:
        result = api_get_json(url, headers=headers, timeout_s=timeout)
        return {
            "email": str(result.get("mail", result.get("userPrincipalName", ""))),
            "name": str(result.get("displayName", "")),
        }
    except Exception as exc:
        logger.warning(
            "context teams profile fetch failed (%s)",
            type(exc).__name__,
            extra={
                "source": "context_capture",
                "provider": "teams",
                "file_path": url,
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return {}


def _extract_sender(msg: Dict[str, Any]) -> str:
    from_obj = msg.get("from") or {}
    user = from_obj.get("user") or {}
    return str(user.get("displayName", user.get("id", "unknown")))


def _extract_body(msg: Dict[str, Any]) -> str:
    body = msg.get("body") or {}
    content = str(body.get("content", ""))
    if body.get("contentType") == "html":
        import re

        content = re.sub(r"<[^>]+>", "", content)
    return content[:500]
