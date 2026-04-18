from __future__ import annotations

from typing import List

from vei.blueprint.api import (
    BlueprintMailMessageAsset,
    BlueprintMailThreadAsset,
    BlueprintSlackChannelAsset,
    BlueprintSlackMessageAsset,
)


def _slack_message(
    ts: str,
    user: str,
    text: str,
    *,
    thread_ts: str | None = None,
) -> BlueprintSlackMessageAsset:
    return BlueprintSlackMessageAsset(
        ts=ts,
        user=user,
        text=text,
        thread_ts=thread_ts,
    )


def _channel(
    channel: str,
    *,
    unread: int = 0,
    messages: List[BlueprintSlackMessageAsset],
) -> BlueprintSlackChannelAsset:
    return BlueprintSlackChannelAsset(
        channel=channel,
        unread=unread,
        messages=messages,
    )


def _mail_message(
    from_address: str,
    to_address: str,
    subject: str,
    body_text: str,
    *,
    unread: bool = True,
    time_ms: int | None = None,
) -> BlueprintMailMessageAsset:
    return BlueprintMailMessageAsset(
        from_address=from_address,
        to_address=to_address,
        subject=subject,
        body_text=body_text,
        unread=unread,
        time_ms=time_ms,
    )


def _mail_thread(
    thread_id: str,
    *,
    title: str,
    category: str,
    messages: List[BlueprintMailMessageAsset],
) -> BlueprintMailThreadAsset:
    return BlueprintMailThreadAsset(
        thread_id=thread_id,
        title=title,
        category=category,
        messages=messages,
    )
