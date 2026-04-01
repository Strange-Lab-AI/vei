from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal

from pydantic import BaseModel, Field

FaultOperation = Literal[
    "set",
    "append",
    "remove",
    "increment",
    "shift_deadline_ms",
]
FaultVisibility = Literal["hidden", "visible"]


class FaultOverlaySpec(BaseModel):
    name: str
    path: str
    operation: FaultOperation
    value: Any | None = None
    label: str
    origin: str = "simulated"
    rationale: str | None = None
    visibility: FaultVisibility = "hidden"
    metadata: dict[str, Any] = Field(default_factory=dict)


def apply_fault_overlays(
    payload: dict[str, Any], overlays: list[FaultOverlaySpec]
) -> dict[str, Any]:
    updated = deepcopy(payload)
    for overlay in overlays:
        _apply_fault_overlay(updated, overlay)
    return updated


def overlay_summaries(overlays: list[FaultOverlaySpec]) -> list[str]:
    lines: list[str] = []
    for overlay in overlays:
        if overlay.rationale:
            lines.append(f"{overlay.label}: {overlay.rationale}")
        else:
            lines.append(overlay.label)
    return lines


def _apply_fault_overlay(payload: dict[str, Any], overlay: FaultOverlaySpec) -> None:
    parent, key = _resolve_parent(payload, overlay.path)
    if overlay.operation == "set":
        if isinstance(parent, list):
            parent[int(key)] = deepcopy(overlay.value)
        else:
            parent[key] = deepcopy(overlay.value)
        return
    if overlay.operation == "append":
        target = parent[int(key)] if isinstance(parent, list) else parent[key]
        if not isinstance(target, list):
            raise ValueError(f"append target is not a list: {overlay.path}")
        target.append(deepcopy(overlay.value))
        return
    if overlay.operation == "remove":
        if isinstance(parent, list):
            del parent[int(key)]
            return
        target = parent[key]
        if isinstance(target, list):
            if overlay.value is None:
                raise ValueError(f"remove from list requires value: {overlay.path}")
            parent[key] = [item for item in target if item != overlay.value]
            return
        parent.pop(key, None)
        return
    if overlay.operation in {"increment", "shift_deadline_ms"}:
        current = parent[int(key)] if isinstance(parent, list) else parent[key]
        amount = int(overlay.value or 0)
        updated = int(current) + amount
        if isinstance(parent, list):
            parent[int(key)] = updated
        else:
            parent[key] = updated
        return
    raise ValueError(f"unsupported fault operation: {overlay.operation}")


def _resolve_parent(payload: dict[str, Any], path: str) -> tuple[Any, str]:
    tokens = [token for token in path.split(".") if token]
    if not tokens:
        raise ValueError("fault overlay path must not be empty")
    node: Any = payload
    for token in tokens[:-1]:
        node = _resolve_token(node, token)
    return node, _final_key(tokens[-1], node)


def _resolve_token(node: Any, token: str) -> Any:
    if "[" not in token:
        if isinstance(node, list):
            return node[int(token)]
        return node[token]
    name, selector = token.split("[", 1)
    selector = selector.rstrip("]")
    selected = node[name] if name else node
    if not isinstance(selected, list):
        raise ValueError(f"selector target is not a list: {token}")
    field, expected = selector.split("=", 1)
    for item in selected:
        if isinstance(item, dict) and str(item.get(field)) == expected:
            return item
    raise ValueError(f"selector did not match any item: {token}")


def _final_key(token: str, node: Any) -> str:
    if "[" not in token:
        return token
    name, selector = token.split("[", 1)
    selector = selector.rstrip("]")
    selected = node[name] if name else node
    if not isinstance(selected, list):
        raise ValueError(f"selector target is not a list: {token}")
    field, expected = selector.split("=", 1)
    for index, item in enumerate(selected):
        if isinstance(item, dict) and str(item.get(field)) == expected:
            return str(index)
    raise ValueError(f"selector did not match any item: {token}")


__all__ = [
    "FaultOverlaySpec",
    "apply_fault_overlays",
    "overlay_summaries",
]
