from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

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


# ---------------------------------------------------------------------------
# Offline export ingestion (Slack workspace export zip/directory)
# ---------------------------------------------------------------------------


def capture_from_export(
    export_path: Union[str, Path],
    *,
    channel_filter: Optional[List[str]] = None,
    message_limit: int = 200,
) -> ContextSourceResult:
    """Parse a standard Slack workspace export directory into a ContextSourceResult.

    A Slack export directory typically contains:
      - users.json             (list of user objects)
      - channels.json          (list of channel metadata objects)
      - <channel_name>/        (directory per channel)
        - YYYY-MM-DD.json      (messages for that day)
    """
    root = Path(export_path)
    if not root.is_dir():
        return ContextSourceResult(
            provider="slack",
            captured_at=iso_now(),
            status="error",
            error=f"export path is not a directory: {root}",
        )

    users = _load_export_json(root / "users.json", default=[])
    channel_meta = _load_export_json(root / "channels.json", default=[])

    user_map: Dict[str, str] = {}
    parsed_users: List[Dict[str, Any]] = []
    for u in users:
        if not isinstance(u, dict):
            continue
        uid = str(u.get("id", ""))
        name = str(u.get("name", ""))
        user_map[uid] = name
        parsed_users.append(
            {
                "id": uid,
                "name": name,
                "real_name": str(
                    u.get("real_name", u.get("profile", {}).get("real_name", name))
                ),
                "email": (u.get("profile") or {}).get("email", ""),
                "is_bot": bool(u.get("is_bot")),
                "deleted": bool(u.get("deleted")),
            }
        )

    captured_channels: List[Dict[str, Any]] = []
    total_messages = 0

    channel_dirs = _discover_channel_dirs(root, channel_meta)
    for channel_name, channel_dir in sorted(channel_dirs.items()):
        if channel_filter and channel_name not in channel_filter:
            continue
        messages = _load_channel_messages(channel_dir, user_map, limit=message_limit)
        total_messages += len(messages)
        meta = next(
            (
                c
                for c in channel_meta
                if isinstance(c, dict) and c.get("name") == channel_name
            ),
            {},
        )
        captured_channels.append(
            {
                "channel": f"#{channel_name}",
                "channel_id": str(meta.get("id", channel_name)),
                "unread": 0,
                "messages": messages,
            }
        )

    return ContextSourceResult(
        provider="slack",
        captured_at=iso_now(),
        status="ok",
        record_counts={
            "channels": len(captured_channels),
            "messages": total_messages,
            "users": len(parsed_users),
        },
        data={
            "channels": captured_channels,
            "users": parsed_users,
        },
    )


def _load_export_json(path: Path, *, default: Any = None) -> Any:
    if not path.exists():
        return default
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _discover_channel_dirs(
    root: Path,
    channel_meta: List[Dict[str, Any]],
) -> Dict[str, Path]:
    dirs: Dict[str, Path] = {}
    known_names = {
        str(c.get("name", ""))
        for c in channel_meta
        if isinstance(c, dict) and c.get("name")
    }

    for entry in root.iterdir():
        if not entry.is_dir():
            continue
        name = entry.name
        if name.startswith(".") or name == "__MACOSX":
            continue
        if known_names and name not in known_names:
            continue
        dirs[name] = entry

    if not dirs and known_names:
        for name in known_names:
            candidate = root / name
            if candidate.is_dir():
                dirs[name] = candidate

    return dirs


def _load_channel_messages(
    channel_dir: Path,
    user_map: Dict[str, str],
    *,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    all_messages: List[Dict[str, Any]] = []
    day_files = sorted(channel_dir.glob("*.json"), reverse=True)

    for day_file in day_files:
        try:
            with open(day_file, encoding="utf-8") as fh:
                day_data = json.load(fh)
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(day_data, list):
            continue
        for msg in day_data:
            if not isinstance(msg, dict):
                continue
            if msg.get("subtype") in ("channel_join", "channel_leave", "channel_topic"):
                continue
            uid = str(msg.get("user", ""))
            all_messages.append(
                {
                    "ts": str(msg.get("ts", "")),
                    "user": user_map.get(uid, uid),
                    "text": str(msg.get("text", "")),
                    "thread_ts": msg.get("thread_ts"),
                }
            )
            if len(all_messages) >= limit:
                return all_messages

    return all_messages
