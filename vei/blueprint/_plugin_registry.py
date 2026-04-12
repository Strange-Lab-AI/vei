from __future__ import annotations

from dataclasses import replace
from typing import Dict, Iterable, List, TYPE_CHECKING

from ._plugin_types import FacadePlugin, GatewaySurfaceBinding

if TYPE_CHECKING:
    from vei.world.api import Scenario


_PLUGINS: Dict[str, FacadePlugin] = {}


def register_facade_plugin(plugin: FacadePlugin) -> FacadePlugin:
    key = plugin.manifest.name.strip().lower()
    _PLUGINS[key] = replace(plugin)
    return _PLUGINS[key]


def get_facade_plugin(name: str) -> FacadePlugin:
    key = name.strip().lower()
    if key not in _PLUGINS:
        raise KeyError(f"unknown facade plugin: {name}")
    return _PLUGINS[key]


def list_facade_plugins() -> List[FacadePlugin]:
    return sorted(_PLUGINS.values(), key=lambda item: item.manifest.name)


def resolve_facade_plugins_for_tool_families(
    tool_families: Iterable[str],
) -> List[FacadePlugin]:
    resolved: List[FacadePlugin] = []
    seen: set[str] = set()
    for tool_family in tool_families:
        key = tool_family.strip().lower()
        for plugin in _PLUGINS.values():
            if plugin.matches_tool_family(key) and plugin.manifest.name not in seen:
                resolved.append(plugin)
                seen.add(plugin.manifest.name)
    resolved.sort(key=lambda item: item.manifest.name)
    return resolved


def infer_tool_families_for_scenario(scenario: "Scenario") -> List[str]:
    families: set[str] = set()
    for plugin in _PLUGINS.values():
        if plugin.supports_scenario(scenario):
            families.update(plugin.tool_families)
    return sorted(families)


def list_runtime_facade_plugins() -> List[FacadePlugin]:
    return [plugin for plugin in list_facade_plugins() if plugin.component_attr]


def resolve_facade_plugins_for_included_surfaces(
    included_surfaces: Iterable[str],
    *,
    facade_names: Iterable[str] | None = None,
    default_only: bool = False,
) -> List[FacadePlugin]:
    requested = {item.strip().lower() for item in included_surfaces if item}
    plugins = (
        [get_facade_plugin(name) for name in facade_names]
        if facade_names is not None
        else list_facade_plugins()
    )
    resolved: List[FacadePlugin] = []
    for plugin in plugins:
        if default_only and not plugin.default_gateway_surface:
            continue
        if not requested or any(
            plugin.matches_included_surface(item) for item in requested
        ):
            resolved.append(plugin)
    resolved.sort(key=lambda item: item.manifest.name)
    return resolved


def resolve_gateway_surface_bindings(
    *,
    included_surfaces: Iterable[str] = (),
    facade_names: Iterable[str] | None = None,
    default_only: bool = False,
) -> List[GatewaySurfaceBinding]:
    preferred_order = ["slack", "jira", "graph", "salesforce", "notes"]
    bindings: Dict[str, GatewaySurfaceBinding] = {}
    for plugin in resolve_facade_plugins_for_included_surfaces(
        included_surfaces,
        facade_names=facade_names,
        default_only=default_only,
    ):
        for binding in plugin.gateway_surfaces:
            bindings.setdefault(binding.name, binding)
    ordered = [bindings[name] for name in preferred_order if name in bindings]
    extras = [
        bindings[name] for name in sorted(bindings) if name not in preferred_order
    ]
    return ordered + extras


def resolve_tool_operation_class(tool_name: str) -> str | None:
    normalized = tool_name.strip()
    for plugin in _PLUGINS.values():
        resolved = plugin.tool_operation_classes.get(normalized)
        if resolved:
            return resolved
    return None
