from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from .models import FacadeManifest

if TYPE_CHECKING:
    from vei.router import Router
    from vei.world.api import Scenario


SummaryBuilder = Callable[["Router", Any], str]
ActionMenuBuilder = Callable[["Router", Any], List[Dict[str, Any]]]
StateDumpHook = Callable[[Any], Dict[str, Any]]
StateRestoreHook = Callable[[Any, Dict[str, Any]], None]
ComponentFactory = Callable[["Router", "Scenario"], Any]
ProviderFactory = Callable[[Any], Any]
EventHandler = Callable[["Router", Any, Dict[str, Any]], Dict[str, Any]]
StudioPanelBuilder = Callable[[Dict[str, Dict[str, Any]], Dict[str, Any]], Any]
GatewayRouteRegistrar = Callable[[Any, Any], None]


@dataclass(frozen=True)
class GatewaySurfaceBinding:
    name: str
    title: str
    base_path: str
    auth_style: str = "bearer"


@dataclass(frozen=True)
class FacadePlugin:
    manifest: FacadeManifest
    tool_families: tuple[str, ...]
    tool_prefixes: tuple[str, ...]
    scenario_seed_fields: tuple[str, ...] = ()
    component_attr: Optional[str] = None
    focuses: tuple[str, ...] = ()
    event_targets: tuple[str, ...] = ()
    summary_builder: Optional[SummaryBuilder] = None
    action_menu_builder: Optional[ActionMenuBuilder] = None
    state_dump: Optional[StateDumpHook] = None
    state_restore: Optional[StateRestoreHook] = None
    component_factory: Optional[ComponentFactory] = None
    provider_factory: Optional[ProviderFactory] = None
    event_handler: Optional[EventHandler] = None
    version: str = "builtin"
    included_surface_aliases: tuple[str, ...] = ()
    studio_panel_builder: Optional[StudioPanelBuilder] = None
    gateway_surfaces: tuple[GatewaySurfaceBinding, ...] = ()
    gateway_route_registrar: Optional[GatewayRouteRegistrar] = None
    default_gateway_surface: bool = False
    tool_operation_classes: Dict[str, str] = field(default_factory=dict)

    def matches_tool_family(self, tool_family: str) -> bool:
        return tool_family.strip().lower() in self.tool_families

    def matches_focus(self, focus: str) -> bool:
        return focus.strip().lower() in self.focuses

    def matches_included_surface(self, surface: str) -> bool:
        normalized = surface.strip().lower()
        aliases = set(self.included_surface_aliases) | {self.manifest.name.lower()}
        return normalized in aliases

    def supports_scenario(self, scenario: "Scenario") -> bool:
        for field_name in self.scenario_seed_fields:
            if getattr(scenario, field_name, None):
                return True
        return False

    def state_component_name(self) -> str:
        for root in self.manifest.state_roots:
            normalized = str(root).strip()
            if not normalized.startswith("components."):
                continue
            component_name = normalized.removeprefix("components.").strip()
            if component_name:
                return component_name
        if self.component_attr:
            return self.component_attr
        return self.manifest.name


@dataclass(frozen=True)
class FacadeRuntimeBinding:
    plugin: FacadePlugin
    component: Any
