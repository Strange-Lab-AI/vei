from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, Iterable, List, Optional, TYPE_CHECKING

from .models import FacadeManifest

if TYPE_CHECKING:
    from vei.world.scenario import Scenario


SummaryBuilder = Callable[[Any, Any], str]
ActionMenuBuilder = Callable[[Any, Any], List[Dict[str, Any]]]
StateDumpHook = Callable[[Any], Dict[str, Any]]
StateRestoreHook = Callable[[Any, Dict[str, Any]], None]
ComponentFactory = Callable[[Any, "Scenario"], Any]
ProviderFactory = Callable[[Any], Any]
EventHandler = Callable[[Any, Any, Dict[str, Any]], Dict[str, Any]]


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

    def matches_tool_family(self, tool_family: str) -> bool:
        return tool_family.strip().lower() in self.tool_families

    def matches_focus(self, focus: str) -> bool:
        return focus.strip().lower() in self.focuses

    def supports_scenario(self, scenario: "Scenario") -> bool:
        for field_name in self.scenario_seed_fields:
            if getattr(scenario, field_name, None):
                return True
        return False


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


def _manifest(
    *,
    name: str,
    title: str,
    domain: str,
    router_module: str,
    description: str,
    surfaces: List[str],
    primary_tools: List[str],
    state_roots: List[str],
    tags: List[str],
) -> FacadeManifest:
    return FacadeManifest(
        name=name,
        title=title,
        domain=domain,  # type: ignore[arg-type]
        router_module=router_module,
        description=description,
        surfaces=surfaces,  # type: ignore[arg-type]
        primary_tools=primary_tools,
        state_roots=state_roots,
        tags=tags,
    )


def _spreadsheet_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router.spreadsheet import SpreadsheetSim

    return SpreadsheetSim(router.bus, scenario)


def _spreadsheet_provider_factory(component: Any) -> Any:
    from vei.router.spreadsheet import SpreadsheetToolProvider

    return SpreadsheetToolProvider(component)


def _spreadsheet_summary(router: Any, component: Any) -> str:
    return component.summary()


def _spreadsheet_action_menu(router: Any, component: Any) -> List[Dict[str, Any]]:
    return component.action_menu()


def _spreadsheet_dump(component: Any) -> Dict[str, Any]:
    return component.export_state()


def _spreadsheet_restore(component: Any, state: Dict[str, Any]) -> None:
    component.import_state(state)


def _bootstrap_default_plugins() -> None:
    builtins = [
        FacadePlugin(
            manifest=_manifest(
                name="slack",
                title="Slack",
                domain="comm_graph",
                router_module="vei.router.core",
                description="Chat and thread surface for agent collaboration and approvals.",
                surfaces=["mcp", "chat"],
                primary_tools=["slack.send_message", "slack.read_channel"],
                state_roots=["components.slack"],
                tags=["communication", "chat"],
            ),
            tool_families=("slack",),
            tool_prefixes=("slack.",),
            scenario_seed_fields=("slack_initial_message", "slack_channels"),
            component_attr="slack",
            focuses=("slack",),
            event_targets=("slack",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="mail",
                title="Mail",
                domain="comm_graph",
                router_module="vei.router.core",
                description="Email inbox and compose surface for threaded external communication.",
                surfaces=["mcp", "email"],
                primary_tools=["mail.compose", "mail.read_inbox"],
                state_roots=["components.mail"],
                tags=["communication", "email"],
            ),
            tool_families=("mail",),
            tool_prefixes=("mail.",),
            scenario_seed_fields=("vendor_reply_variants",),
            component_attr="mail",
            focuses=("mail",),
            event_targets=("mail",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="browser",
                title="Browser",
                domain="doc_graph",
                router_module="vei.router.core",
                description="Read-only admin and knowledge UI facade for browsing synthetic pages.",
                surfaces=["mcp", "ui"],
                primary_tools=["browser.read", "browser.click"],
                state_roots=["components.browser"],
                tags=["knowledge", "ui"],
            ),
            tool_families=("browser",),
            tool_prefixes=("browser.",),
            scenario_seed_fields=("browser_nodes",),
            component_attr="browser",
            focuses=("browser",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="docs",
                title="Docs",
                domain="doc_graph",
                router_module="vei.router.docs",
                description="Document and ACL surface for shared knowledge artifacts.",
                surfaces=["mcp", "ui"],
                primary_tools=["docs.read", "docs.write"],
                state_roots=["components.docs"],
                tags=["knowledge", "documents"],
            ),
            tool_families=("docs",),
            tool_prefixes=("docs.",),
            scenario_seed_fields=("documents",),
            component_attr="docs",
            focuses=("docs",),
            event_targets=("docs",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="calendar",
                title="Calendar",
                domain="comm_graph",
                router_module="vei.router.calendar",
                description="Calendar and meeting scheduling surface.",
                surfaces=["mcp", "ui"],
                primary_tools=["calendar.list_events", "calendar.create_event"],
                state_roots=["components.calendar"],
                tags=["communication", "calendar"],
            ),
            tool_families=("calendar",),
            tool_prefixes=("calendar.",),
            scenario_seed_fields=("calendar_events",),
            component_attr="calendar",
            focuses=("calendar",),
            event_targets=("calendar",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="tickets",
                title="Tickets",
                domain="work_graph",
                router_module="vei.router.tickets",
                description="Generic work-queue and ticket handling surface.",
                surfaces=["mcp", "ui"],
                primary_tools=["tickets.list", "tickets.update"],
                state_roots=["components.tickets"],
                tags=["work", "support"],
            ),
            tool_families=("tickets",),
            tool_prefixes=("tickets.",),
            scenario_seed_fields=("tickets",),
            component_attr="tickets",
            focuses=("tickets",),
            event_targets=("tickets",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="servicedesk",
                title="ServiceDesk",
                domain="work_graph",
                router_module="vei.router.servicedesk",
                description="Incident and request workflows with approvals and comments.",
                surfaces=["mcp", "ui"],
                primary_tools=[
                    "servicedesk.list_incidents",
                    "servicedesk.update_request",
                ],
                state_roots=["components.servicedesk"],
                tags=["work", "service-management"],
            ),
            tool_families=("servicedesk",),
            tool_prefixes=("servicedesk.",),
            scenario_seed_fields=("service_incidents", "service_requests"),
            component_attr="servicedesk",
            focuses=("servicedesk",),
            event_targets=("servicedesk",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="jira",
                title="Jira",
                domain="work_graph",
                router_module="vei.router.jira",
                description="Jira-style issue tracking facade for project and support work.",
                surfaces=["mcp", "ui"],
                primary_tools=["jira.list_issues", "jira.update_issue"],
                state_roots=["components.tickets"],
                tags=["work", "issues"],
            ),
            tool_families=("jira",),
            tool_prefixes=("jira.",),
            scenario_seed_fields=("tickets",),
            component_attr="tickets",
            focuses=("jira",),
            event_targets=("jira",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="identity",
                title="Identity",
                domain="identity_graph",
                router_module="vei.router.identity",
                description="Okta-style identity graph for users, groups, apps, and assignments.",
                surfaces=["mcp", "ui"],
                primary_tools=["okta.get_user", "okta.assign_app"],
                state_roots=["components.okta"],
                tags=["identity", "access"],
            ),
            tool_families=("okta", "identity"),
            tool_prefixes=("okta.",),
            scenario_seed_fields=(
                "identity_users",
                "identity_groups",
                "identity_applications",
            ),
            component_attr="okta",
            focuses=("okta",),
            event_targets=("okta",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="google_admin",
                title="Google Admin",
                domain="identity_graph",
                router_module="vei.router.google_admin",
                description="Google Workspace admin facade for OAuth apps, Drive sharing, and users.",
                surfaces=["mcp", "ui"],
                primary_tools=[
                    "google_admin.get_oauth_app",
                    "google_admin.preserve_oauth_evidence",
                    "google_admin.restrict_drive_share",
                ],
                state_roots=["components.google_admin"],
                tags=["identity", "admin"],
            ),
            tool_families=("google_admin",),
            tool_prefixes=("google_admin.",),
            scenario_seed_fields=("google_admin",),
            component_attr="google_admin",
            focuses=("google_admin",),
            event_targets=("google_admin",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="hris",
                title="HRIS",
                domain="identity_graph",
                router_module="vei.router.hris",
                description="HR and employee lifecycle facade for onboarding and status changes.",
                surfaces=["mcp", "ui"],
                primary_tools=["hris.get_employee", "hris.complete_onboarding"],
                state_roots=["components.hris"],
                tags=["identity", "people-ops"],
            ),
            tool_families=("hris",),
            tool_prefixes=("hris.",),
            scenario_seed_fields=("hris",),
            component_attr="hris",
            focuses=("hris",),
            event_targets=("hris",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="crm",
                title="CRM",
                domain="revenue_graph",
                router_module="vei.router.crm",
                description="Revenue ownership and opportunity management facade.",
                surfaces=["mcp", "ui"],
                primary_tools=["crm.get_deal", "crm.reassign_deal_owner"],
                state_roots=["components.crm"],
                tags=["revenue", "sales"],
            ),
            tool_families=("crm",),
            tool_prefixes=("crm.", "salesforce.", "hubspot."),
            scenario_seed_fields=("crm",),
            component_attr="crm",
            focuses=("crm",),
            event_targets=("crm",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="erp",
                title="ERP",
                domain="ops_graph",
                router_module="vei.router.erp",
                description="Back-office operations and procurement facade.",
                surfaces=["mcp", "ui"],
                primary_tools=["erp.get_order", "erp.update_order"],
                state_roots=["components.erp"],
                tags=["operations", "finance"],
            ),
            tool_families=("erp",),
            tool_prefixes=("erp.",),
            scenario_seed_fields=(),
            component_attr="erp",
            focuses=("erp",),
            event_targets=("erp",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="database",
                title="Database",
                domain="data_graph",
                router_module="vei.router.database",
                description="Deterministic tabular data facade for audit and query workflows.",
                surfaces=["mcp", "api"],
                primary_tools=["db.query", "db.upsert"],
                state_roots=["components.database"],
                tags=["data", "audit"],
            ),
            tool_families=("db", "database"),
            tool_prefixes=("db.",),
            scenario_seed_fields=("database_tables",),
            component_attr="database",
            focuses=("db",),
            event_targets=("db", "database"),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="siem",
                title="SIEM",
                domain="obs_graph",
                router_module="vei.router.siem",
                description="Security operations facade for alerts, evidence, and case state.",
                surfaces=["mcp", "ui"],
                primary_tools=["siem.preserve_evidence", "siem.update_case"],
                state_roots=["components.siem"],
                tags=["observability", "security"],
            ),
            tool_families=("siem",),
            tool_prefixes=("siem.",),
            scenario_seed_fields=("siem",),
            component_attr="siem",
            focuses=("siem",),
            event_targets=("siem",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="datadog",
                title="Datadog",
                domain="obs_graph",
                router_module="vei.router.datadog",
                description="Monitoring and service-health facade for production signals.",
                surfaces=["mcp", "ui"],
                primary_tools=["datadog.get_service", "datadog.update_service"],
                state_roots=["components.datadog"],
                tags=["observability", "reliability"],
            ),
            tool_families=("datadog",),
            tool_prefixes=("datadog.",),
            scenario_seed_fields=("datadog",),
            component_attr="datadog",
            focuses=("datadog",),
            event_targets=("datadog",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="pagerduty",
                title="PagerDuty",
                domain="obs_graph",
                router_module="vei.router.pagerduty",
                description="Incident paging and acknowledgement facade.",
                surfaces=["mcp", "ui"],
                primary_tools=["pagerduty.get_incident", "pagerduty.resolve_incident"],
                state_roots=["components.pagerduty"],
                tags=["observability", "incident-response"],
            ),
            tool_families=("pagerduty",),
            tool_prefixes=("pagerduty.",),
            scenario_seed_fields=("pagerduty",),
            component_attr="pagerduty",
            focuses=("pagerduty",),
            event_targets=("pagerduty",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="feature_flags",
                title="Feature Flags",
                domain="ops_graph",
                router_module="vei.router.feature_flags",
                description="Rollout and kill-switch control plane facade.",
                surfaces=["mcp", "ui"],
                primary_tools=["feature_flags.get_flag", "feature_flags.set_rollout"],
                state_roots=["components.feature_flags"],
                tags=["operations", "rollout"],
            ),
            tool_families=("feature_flags",),
            tool_prefixes=("feature_flags.",),
            scenario_seed_fields=("feature_flags",),
            component_attr="feature_flags",
            focuses=("feature_flags",),
            event_targets=("feature_flags",),
        ),
        FacadePlugin(
            manifest=_manifest(
                name="spreadsheet",
                title="Spreadsheet",
                domain="data_graph",
                router_module="vei.router.spreadsheet",
                description="Workbook and sheet surface for tabular operational analysis.",
                surfaces=["mcp", "ui"],
                primary_tools=[
                    "spreadsheet.list_workbooks",
                    "spreadsheet.read_sheet",
                    "spreadsheet.update_cell",
                ],
                state_roots=["components.spreadsheet"],
                tags=["data", "spreadsheet", "office"],
            ),
            tool_families=("spreadsheet",),
            tool_prefixes=("spreadsheet.",),
            scenario_seed_fields=("spreadsheets",),
            component_attr="spreadsheet",
            focuses=("spreadsheet",),
            event_targets=("spreadsheet",),
            summary_builder=_spreadsheet_summary,
            action_menu_builder=_spreadsheet_action_menu,
            state_dump=_spreadsheet_dump,
            state_restore=_spreadsheet_restore,
            component_factory=_spreadsheet_component_factory,
            provider_factory=_spreadsheet_provider_factory,
        ),
    ]
    for plugin in builtins:
        register_facade_plugin(plugin)


_bootstrap_default_plugins()
