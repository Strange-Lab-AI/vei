from __future__ import annotations

from ._plugin_builtins import bootstrap_default_plugins
from ._plugin_registry import (
    get_facade_plugin,
    infer_tool_families_for_scenario,
    list_facade_plugins,
    list_runtime_facade_plugins,
    register_facade_plugin,
    resolve_facade_plugins_for_tool_families,
    resolve_gateway_surface_bindings,
    resolve_tool_operation_class,
)
from ._plugin_types import (
    FacadePlugin,
    FacadeRuntimeBinding,
    GatewaySurfaceBinding,
)

__all__ = [
    "FacadePlugin",
    "FacadeRuntimeBinding",
    "GatewaySurfaceBinding",
    "get_facade_plugin",
    "infer_tool_families_for_scenario",
    "list_facade_plugins",
    "list_runtime_facade_plugins",
    "register_facade_plugin",
    "resolve_facade_plugins_for_tool_families",
    "resolve_gateway_surface_bindings",
    "resolve_tool_operation_class",
]

bootstrap_default_plugins()
