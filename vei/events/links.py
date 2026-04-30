"""Typed provenance links stored inside ``StateDelta.data``."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class EventLink(BaseModel):
    kind: str
    event_id: str


def normalize_links(
    *,
    links: list[EventLink | dict[str, Any]] | None = None,
    link_refs: list[str] | None = None,
) -> tuple[list[dict[str, str]], list[str]]:
    typed: list[dict[str, str]] = []
    legacy: list[str] = []
    for item in links or []:
        link = item if isinstance(item, EventLink) else EventLink.model_validate(item)
        if not link.event_id:
            continue
        typed.append(link.model_dump(mode="json"))
        legacy.append(link.event_id)
    for event_id in link_refs or []:
        if not event_id:
            continue
        legacy.append(str(event_id))
    deduped_legacy = list(dict.fromkeys(legacy))
    return typed, deduped_legacy


def merge_event_links(
    data: dict[str, Any],
    *,
    links: list[EventLink | dict[str, Any]] | None = None,
    link_refs: list[str] | None = None,
) -> dict[str, Any]:
    typed, legacy = normalize_links(links=links, link_refs=link_refs)
    merged = dict(data)
    existing_typed = merged.get("links")
    if isinstance(existing_typed, list):
        for item in existing_typed:
            if isinstance(item, dict) and item.get("event_id"):
                typed.append(
                    {
                        "kind": str(item.get("kind", "")),
                        "event_id": str(item.get("event_id", "")),
                    }
                )
    existing_refs = merged.get("link_refs")
    if isinstance(existing_refs, list):
        legacy.extend(str(ref) for ref in existing_refs if ref)
    if typed:
        seen: set[tuple[str, str]] = set()
        deduped: list[dict[str, str]] = []
        for link in typed:
            key = (link.get("kind", ""), link.get("event_id", ""))
            if key in seen or not key[1]:
                continue
            seen.add(key)
            deduped.append(link)
        merged["links"] = deduped
    merged["link_refs"] = list(dict.fromkeys(legacy))
    return merged


def link_event_ids(data: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    for link in typed_event_links(data):
        ids.append(link.event_id)
    refs = data.get("link_refs")
    if isinstance(refs, list):
        ids.extend(str(ref) for ref in refs if ref)
    return list(dict.fromkeys(ids))


def typed_event_links(data: dict[str, Any]) -> list[EventLink]:
    links = data.get("links")
    if not isinstance(links, list):
        return []
    parsed: list[EventLink] = []
    for item in links:
        if not isinstance(item, dict) or not item.get("event_id"):
            continue
        try:
            parsed.append(EventLink.model_validate(item))
        except ValueError:
            continue
    return parsed
