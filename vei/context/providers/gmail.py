from __future__ import annotations

import logging
import mailbox
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from vei.context.models import ContextProviderConfig, ContextSourceResult

from .base import api_get_json, iso_now, resolve_token

logger = logging.getLogger(__name__)


class GmailContextProvider:
    name = "gmail"

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult:
        token = resolve_token(config)
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        timeout = config.timeout_s
        limit = min(config.limit, 100)

        label_filter = config.filters.get("labels")
        query = config.filters.get("query", "")

        raw_threads = _fetch_threads(headers, timeout, limit=limit, query=query)
        threads: List[Dict[str, Any]] = []
        total_messages = 0

        for thread_meta in raw_threads:
            tid = thread_meta.get("id", "")
            thread_detail = _fetch_thread(headers, timeout, tid)
            if not thread_detail:
                continue
            messages = _extract_messages(thread_detail, label_filter=label_filter)
            if not messages:
                continue
            total_messages += len(messages)
            threads.append(
                {
                    "thread_id": tid,
                    "subject": messages[0].get("subject", ""),
                    "messages": messages,
                }
            )

        profile = _fetch_profile(headers, timeout)

        return ContextSourceResult(
            provider="gmail",
            captured_at=iso_now(),
            status="ok",
            record_counts={
                "threads": len(threads),
                "messages": total_messages,
            },
            data={
                "threads": threads,
                "profile": profile,
            },
        )


def _fetch_threads(
    headers: Dict[str, str],
    timeout: int,
    *,
    limit: int = 50,
    query: str = "",
) -> List[Dict[str, Any]]:
    url = f"https://gmail.googleapis.com/gmail/v1/users/me/threads?maxResults={limit}"
    if query:
        from urllib.parse import quote

        url += f"&q={quote(query)}"
    result = api_get_json(url, headers=headers, timeout_s=timeout)
    return result.get("threads", []) if isinstance(result, dict) else []


def _fetch_thread(
    headers: Dict[str, str],
    timeout: int,
    thread_id: str,
) -> Optional[Dict[str, Any]]:
    url = (
        f"https://gmail.googleapis.com/gmail/v1/users/me/threads/{thread_id}"
        "?format=metadata"
        "&metadataHeaders=From&metadataHeaders=To"
        "&metadataHeaders=Subject&metadataHeaders=Date"
    )
    try:
        return api_get_json(url, headers=headers, timeout_s=timeout)
    except Exception as exc:
        logger.warning(
            "context gmail thread fetch failed for %s (%s)",
            thread_id,
            type(exc).__name__,
            extra={
                "source": "context_capture",
                "provider": "gmail",
                "file_path": url,
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return None


def _fetch_profile(
    headers: Dict[str, str],
    timeout: int,
) -> Dict[str, Any]:
    url = "https://gmail.googleapis.com/gmail/v1/users/me/profile"
    try:
        result = api_get_json(url, headers=headers, timeout_s=timeout)
        return {
            "email": str(result.get("emailAddress", "")),
            "threads_total": result.get("threadsTotal", 0),
            "messages_total": result.get("messagesTotal", 0),
        }
    except Exception as exc:
        logger.warning(
            "context gmail profile fetch failed (%s)",
            type(exc).__name__,
            extra={
                "source": "context_capture",
                "provider": "gmail",
                "file_path": url,
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return {}


def _extract_messages(
    thread: Dict[str, Any],
    *,
    label_filter: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    raw_messages = thread.get("messages", [])
    result: List[Dict[str, Any]] = []
    for msg in raw_messages:
        if not isinstance(msg, dict):
            continue
        labels = msg.get("labelIds", [])
        if label_filter and not any(lb in labels for lb in label_filter):
            continue
        headers_list = (msg.get("payload") or {}).get("headers", [])
        header_map = {
            h["name"].lower(): h["value"]
            for h in headers_list
            if isinstance(h, dict) and "name" in h and "value" in h
        }
        result.append(
            {
                "message_id": str(msg.get("id", "")),
                "from": header_map.get("from", ""),
                "to": header_map.get("to", ""),
                "subject": header_map.get("subject", ""),
                "date": header_map.get("date", ""),
                "snippet": str(msg.get("snippet", "")),
                "labels": labels,
                "unread": "UNREAD" in labels,
            }
        )
    return result


# ---------------------------------------------------------------------------
# Offline MBOX ingestion (Gmail Takeout)
# ---------------------------------------------------------------------------


def capture_from_mbox(
    mbox_path: Union[str, Path],
    *,
    message_limit: int = 200,
) -> ContextSourceResult:
    """Parse a Gmail Takeout MBOX file into a ContextSourceResult."""
    path = Path(mbox_path)
    if not path.exists():
        return ContextSourceResult(
            provider="gmail",
            captured_at=iso_now(),
            status="error",
            error=f"mbox file not found: {path}",
        )

    try:
        mbox = mailbox.mbox(str(path))
    except Exception as exc:
        logger.warning(
            "context gmail mbox open failed for %s (%s)",
            path,
            type(exc).__name__,
            extra={
                "source": "context_export",
                "provider": "gmail",
                "file_path": str(path),
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return ContextSourceResult(
            provider="gmail",
            captured_at=iso_now(),
            status="error",
            error=f"failed to open mbox: {exc}",
        )

    thread_map: Dict[str, List[Dict[str, Any]]] = {}
    count = 0

    for msg in mbox:
        if count >= message_limit:
            break
        try:
            parsed = _parse_mbox_message(msg)
        except Exception as exc:
            logger.warning(
                "context gmail mbox message parse failed for %s (%s)",
                path,
                type(exc).__name__,
                extra={
                    "source": "context_export",
                    "provider": "gmail",
                    "file_path": str(path),
                    "exception_type": type(exc).__name__,
                },
                exc_info=True,
            )
            continue
        if not parsed:
            continue
        tid = parsed.get("thread_id", parsed.get("message_id", f"t-{count}"))
        thread_map.setdefault(tid, []).append(parsed)
        count += 1

    threads: List[Dict[str, Any]] = []
    for tid, messages in thread_map.items():
        subject = messages[0].get("subject", "") if messages else ""
        threads.append(
            {
                "thread_id": tid,
                "subject": subject,
                "messages": messages,
            }
        )

    return ContextSourceResult(
        provider="gmail",
        captured_at=iso_now(),
        status="ok",
        record_counts={
            "threads": len(threads),
            "messages": count,
        },
        data={
            "threads": threads,
            "profile": {},
        },
    )


def _parse_mbox_message(msg: mailbox.mboxMessage) -> Optional[Dict[str, Any]]:
    subject = str(msg.get("Subject", ""))
    from_addr = str(msg.get("From", ""))
    to_addr = str(msg.get("To", ""))
    date = str(msg.get("Date", ""))
    message_id = str(msg.get("Message-ID", ""))
    references = str(msg.get("References", ""))
    in_reply_to = str(msg.get("In-Reply-To", ""))

    thread_id = ""
    if references:
        thread_id = references.strip().split()[0]
    elif in_reply_to:
        thread_id = in_reply_to.strip()
    else:
        thread_id = message_id

    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain":
                raw = part.get_payload(decode=True)
                if isinstance(raw, bytes):
                    body = raw.decode("utf-8", errors="replace")[:500]
                break
    else:
        raw = msg.get_payload(decode=True)
        if isinstance(raw, bytes):
            body = raw.decode("utf-8", errors="replace")[:500]

    if not from_addr and not subject:
        return None

    return {
        "message_id": message_id,
        "from": from_addr,
        "to": to_addr,
        "subject": subject,
        "date": date,
        "snippet": body[:200],
        "labels": [],
        "unread": False,
        "thread_id": thread_id,
    }
