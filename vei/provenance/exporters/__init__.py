"""Exporter registry for provenance reports."""

from __future__ import annotations

from typing import Callable

from vei.events.api import CanonicalEvent

Exporter = Callable[[list[CanonicalEvent]], dict]
_EXPORTERS: dict[str, Exporter] = {}


def register_exporter(name: str, fn: Exporter) -> None:
    _EXPORTERS[name] = fn


def get_exporter(name: str) -> Exporter:
    if name not in _EXPORTERS:
        raise KeyError(f"unknown provenance exporter: {name}")
    return _EXPORTERS[name]


def export_events(name: str, events: list[CanonicalEvent]) -> dict:
    return get_exporter(name)(events)
