from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Iterable, List

from vei.blueprint import FacadePlugin, list_runtime_facade_plugins
from vei.blueprint.api import FacadeRuntimeBinding

from ._catalog import build_alias_map, build_builtin_tool_specs
from .tool_registry import ToolSpec

if TYPE_CHECKING:
    from .core import Router


class RouterLifecycle:
    @staticmethod
    def bootstrap_facade_plugins(router: Router) -> None:
        for plugin in list_runtime_facade_plugins():
            component = (
                getattr(router, plugin.component_attr, None)
                if plugin.component_attr
                else None
            )
            if component is None and plugin.component_factory and plugin.component_attr:
                component = plugin.component_factory(router, router.scenario)
                setattr(router, plugin.component_attr, component)
            if plugin.component_attr and component is not None:
                router.facade_plugins[plugin.manifest.name] = FacadeRuntimeBinding(
                    plugin=plugin,
                    component=component,
                )
                if plugin.provider_factory is not None:
                    router.register_tool_provider(plugin.provider_factory(component))

    @staticmethod
    def event_targets(router: Router) -> List[str]:
        targets = ["tool"]
        for entry in router.facade_plugins.values():
            plugin: FacadePlugin = entry.plugin
            for target in plugin.event_targets:
                if target not in targets:
                    targets.append(target)
        return targets

    @staticmethod
    def register_tool_specs(router: Router, specs: Iterable[ToolSpec]) -> None:
        for spec in specs:
            try:
                router.registry.register(spec)
            except ValueError:
                continue

    @staticmethod
    def build_alias_map() -> Dict[str, str]:
        return build_alias_map()

    @staticmethod
    def register_alias_specs(router: Router, alias_map: Dict[str, str]) -> None:
        specs: List[ToolSpec] = []
        for alias_name, base_tool in alias_map.items():
            base = router.registry.get(base_tool)
            if base:
                specs.append(
                    ToolSpec(
                        name=alias_name,
                        description=f"Alias -> {base_tool}. {base.description}",
                        side_effects=base.side_effects,
                        permissions=base.permissions,
                        default_latency_ms=base.default_latency_ms,
                        latency_jitter_ms=base.latency_jitter_ms,
                        nominal_cost=base.nominal_cost,
                        returns=base.returns,
                        fault_probability=base.fault_probability,
                    )
                )
            else:
                specs.append(
                    ToolSpec(name=alias_name, description=f"Alias -> {base_tool}")
                )
        RouterLifecycle.register_tool_specs(router, specs)

    @staticmethod
    def seed_tool_registry(router: Router) -> None:
        RouterLifecycle.register_tool_specs(router, build_builtin_tool_specs())
