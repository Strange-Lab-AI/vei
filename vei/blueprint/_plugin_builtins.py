from __future__ import annotations

import logging
from typing import Any, Dict, List, TYPE_CHECKING

from ._plugin_registry import list_facade_plugins, register_facade_plugin
from ._plugin_state_hooks import (
    dump_browser_state,
    dump_calendar_state,
    dump_crm_state,
    dump_database_state,
    dump_datadog_state,
    dump_docs_state,
    dump_erp_state,
    dump_feature_flags_state,
    dump_google_admin_state,
    dump_hris_state,
    dump_mail_state,
    dump_okta_state,
    dump_pagerduty_state,
    dump_servicedesk_state,
    dump_siem_state,
    dump_slack_state,
    dump_tickets_state,
    restore_browser_state,
    restore_calendar_state,
    restore_crm_state,
    restore_database_state,
    restore_datadog_state,
    restore_docs_state,
    restore_erp_state,
    restore_feature_flags_state,
    restore_google_admin_state,
    restore_hris_state,
    restore_mail_state,
    restore_okta_state,
    restore_pagerduty_state,
    restore_servicedesk_state,
    restore_siem_state,
    restore_slack_state,
    restore_tickets_state,
)
from ._plugin_types import FacadePlugin, GatewaySurfaceBinding
from .models import FacadeManifest

if TYPE_CHECKING:
    from vei.world.api import Scenario

logger = logging.getLogger(__name__)

_BOOTSTRAPPED = False


def bootstrap_default_plugins() -> None:
    global _BOOTSTRAPPED
    if _BOOTSTRAPPED or list_facade_plugins():
        _BOOTSTRAPPED = True
        return

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
            state_dump=dump_slack_state,
            state_restore=restore_slack_state,
            component_factory=_slack_component_factory,
            included_surface_aliases=("slack",),
            studio_panel_builder=_build_slack_panel,
            gateway_surfaces=(GatewaySurfaceBinding("slack", "Slack", "/slack/api"),),
            gateway_route_registrar=_register_slack_gateway_routes,
            default_gateway_surface=True,
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
            state_dump=dump_mail_state,
            state_restore=restore_mail_state,
            component_factory=_mail_component_factory,
            included_surface_aliases=("mail",),
            studio_panel_builder=_build_mail_panel,
            gateway_surfaces=(
                GatewaySurfaceBinding("graph", "Microsoft Graph", "/graph/v1.0"),
            ),
            gateway_route_registrar=_register_graph_gateway_routes,
            default_gateway_surface=True,
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
            state_dump=dump_browser_state,
            state_restore=restore_browser_state,
            component_factory=_browser_component_factory,
        ),
        FacadePlugin(
            manifest=_manifest(
                name="docs",
                title="Docs",
                domain="doc_graph",
                router_module="vei.router.docs",
                description="Document and ACL surface for shared knowledge artifacts.",
                surfaces=["mcp", "ui"],
                primary_tools=["docs.read", "docs.update"],
                state_roots=["components.docs"],
                tags=["knowledge", "documents"],
            ),
            tool_families=("docs",),
            tool_prefixes=("docs.",),
            scenario_seed_fields=("documents",),
            component_attr="docs",
            focuses=("docs",),
            event_targets=("docs",),
            state_dump=dump_docs_state,
            state_restore=restore_docs_state,
            component_factory=_docs_component_factory,
            included_surface_aliases=("docs",),
            studio_panel_builder=_build_docs_panel,
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
            state_dump=dump_calendar_state,
            state_restore=restore_calendar_state,
            component_factory=_calendar_component_factory,
            included_surface_aliases=("calendar",),
            gateway_surfaces=(
                GatewaySurfaceBinding("graph", "Microsoft Graph", "/graph/v1.0"),
            ),
            gateway_route_registrar=_register_graph_gateway_routes,
            default_gateway_surface=True,
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
            state_dump=dump_tickets_state,
            state_restore=restore_tickets_state,
            component_factory=_tickets_component_factory,
            included_surface_aliases=("tickets",),
            studio_panel_builder=_build_ticket_panel,
            gateway_surfaces=(
                GatewaySurfaceBinding("jira", "Jira", "/jira/rest/api/3"),
            ),
            gateway_route_registrar=_register_jira_gateway_routes,
            default_gateway_surface=True,
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
            state_dump=dump_servicedesk_state,
            state_restore=restore_servicedesk_state,
            component_factory=_servicedesk_component_factory,
            provider_factory=_servicedesk_provider_factory,
            included_surface_aliases=("approvals", "tickets"),
            studio_panel_builder=_build_approval_panel,
            gateway_surfaces=(
                GatewaySurfaceBinding("jira", "Jira", "/jira/rest/api/3"),
            ),
            gateway_route_registrar=_register_jira_gateway_routes,
            default_gateway_surface=True,
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
            state_dump=dump_tickets_state,
            state_restore=restore_tickets_state,
            component_factory=_tickets_component_factory,
            provider_factory=_jira_provider_factory,
            included_surface_aliases=("tickets", "approvals"),
            studio_panel_builder=_build_ticket_panel,
            gateway_surfaces=(
                GatewaySurfaceBinding("jira", "Jira", "/jira/rest/api/3"),
            ),
            gateway_route_registrar=_register_jira_gateway_routes,
            default_gateway_surface=True,
        ),
        FacadePlugin(
            manifest=_manifest(
                name="identity",
                title="Identity",
                domain="identity_graph",
                router_module="vei.router.identity",
                description="Okta-style identity graph for users, groups, apps, and assignments.",
                surfaces=["mcp", "ui"],
                primary_tools=["okta.get_user", "okta.assign_application"],
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
            state_dump=dump_okta_state,
            state_restore=restore_okta_state,
            component_factory=_okta_component_factory,
            provider_factory=_okta_provider_factory,
            included_surface_aliases=("identity",),
            gateway_surfaces=(
                GatewaySurfaceBinding("graph", "Microsoft Graph", "/graph/v1.0"),
            ),
            gateway_route_registrar=_register_graph_gateway_routes,
            default_gateway_surface=True,
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
            state_dump=dump_google_admin_state,
            state_restore=restore_google_admin_state,
            component_factory=_google_admin_component_factory,
            provider_factory=_google_admin_provider_factory,
            included_surface_aliases=("identity",),
            gateway_surfaces=(
                GatewaySurfaceBinding("graph", "Microsoft Graph", "/graph/v1.0"),
            ),
            gateway_route_registrar=_register_graph_gateway_routes,
            default_gateway_surface=True,
        ),
        FacadePlugin(
            manifest=_manifest(
                name="hris",
                title="HRIS",
                domain="identity_graph",
                router_module="vei.router.hris",
                description="HR and employee lifecycle facade for onboarding and status changes.",
                surfaces=["mcp", "ui"],
                primary_tools=["hris.get_employee", "hris.mark_onboarded"],
                state_roots=["components.hris"],
                tags=["identity", "people-ops"],
            ),
            tool_families=("hris",),
            tool_prefixes=("hris.",),
            scenario_seed_fields=("hris",),
            component_attr="hris",
            focuses=("hris",),
            event_targets=("hris",),
            state_dump=dump_hris_state,
            state_restore=restore_hris_state,
            component_factory=_hris_component_factory,
            provider_factory=_hris_provider_factory,
            included_surface_aliases=("identity",),
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
            state_dump=dump_crm_state,
            state_restore=restore_crm_state,
            component_factory=_crm_component_factory,
            included_surface_aliases=("crm",),
            studio_panel_builder=_build_revenue_panel,
            gateway_surfaces=(
                GatewaySurfaceBinding(
                    "salesforce",
                    "Salesforce",
                    "/salesforce/services/data/v60.0",
                ),
            ),
            gateway_route_registrar=_register_salesforce_gateway_routes,
            default_gateway_surface=True,
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
            state_dump=dump_erp_state,
            state_restore=restore_erp_state,
            component_factory=_erp_component_factory,
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
            state_dump=dump_database_state,
            state_restore=restore_database_state,
            component_factory=_database_component_factory,
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
            state_dump=dump_siem_state,
            state_restore=restore_siem_state,
            component_factory=_siem_component_factory,
            provider_factory=_siem_provider_factory,
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
            state_dump=dump_datadog_state,
            state_restore=restore_datadog_state,
            component_factory=_datadog_component_factory,
            provider_factory=_datadog_provider_factory,
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
            state_dump=dump_pagerduty_state,
            state_restore=restore_pagerduty_state,
            component_factory=_pagerduty_component_factory,
            provider_factory=_pagerduty_provider_factory,
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
            state_dump=dump_feature_flags_state,
            state_restore=restore_feature_flags_state,
            component_factory=_feature_flags_component_factory,
            provider_factory=_feature_flags_provider_factory,
        ),
        FacadePlugin(
            manifest=_manifest(
                name="notes",
                title="Notes",
                domain="doc_graph",
                router_module="vei.router.notes",
                description="Small note-taking facade used as a pluggable surface reference.",
                surfaces=["mcp", "ui", "api"],
                primary_tools=["notes.list_entries", "notes.create_entry"],
                state_roots=["components.notes"],
                tags=["knowledge", "notes", "example"],
            ),
            tool_families=("notes",),
            tool_prefixes=("notes.",),
            component_attr="notes",
            focuses=("notes",),
            event_targets=("notes",),
            summary_builder=_component_summary,
            action_menu_builder=_component_action_menu,
            state_dump=_component_dump,
            state_restore=_component_restore,
            component_factory=_notes_component_factory,
            provider_factory=_notes_provider_factory,
            included_surface_aliases=("notes",),
            studio_panel_builder=_build_notes_panel,
            gateway_surfaces=(GatewaySurfaceBinding("notes", "Notes", "/notes/api"),),
            gateway_route_registrar=_register_notes_gateway_routes,
            tool_operation_classes={
                "notes.create_entry": "write_safe",
                "notes.get_entry": "read",
                "notes.list_entries": "read",
                "notes.update_entry": "write_safe",
            },
        ),
        FacadePlugin(
            manifest=_manifest(
                name="knowledge",
                title="Knowledge",
                domain="knowledge_graph",
                router_module="vei.router.knowledge",
                description="Knowledge assets, freshness, citations, and composition surface.",
                surfaces=["mcp", "ui", "cli"],
                primary_tools=[
                    "knowledge.list_assets",
                    "knowledge.compose_artifact",
                    "knowledge.run_compaction",
                ],
                state_roots=["components.knowledge"],
                tags=["knowledge", "authoring", "citations"],
            ),
            tool_families=("knowledge",),
            tool_prefixes=("knowledge.",),
            scenario_seed_fields=("knowledge_graph",),
            component_attr="knowledge",
            focuses=("knowledge",),
            event_targets=("knowledge",),
            summary_builder=_component_summary,
            action_menu_builder=_component_action_menu,
            state_dump=_component_dump,
            state_restore=_component_restore,
            component_factory=_knowledge_component_factory,
            provider_factory=_knowledge_provider_factory,
            included_surface_aliases=("knowledge",),
            studio_panel_builder=_build_knowledge_panel,
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
        FacadePlugin(
            manifest=_manifest(
                name="property_ops",
                title="Property Operations",
                domain="property_graph",
                router_module="vei.router.property_ops",
                description="Property, lease, unit, vendor, and work-order domain surface.",
                surfaces=["mcp", "ui"],
                primary_tools=[
                    "property.list_overview",
                    "property.assign_vendor",
                    "property.update_lease_milestone",
                ],
                state_roots=["components.property_ops"],
                tags=["vertical", "real-estate", "operations"],
            ),
            tool_families=("property",),
            tool_prefixes=("property.",),
            scenario_seed_fields=("property_graph",),
            component_attr="property_ops",
            focuses=("property",),
            event_targets=("property_ops",),
            summary_builder=_component_summary,
            action_menu_builder=_component_action_menu,
            state_dump=_component_dump,
            state_restore=_component_restore,
            component_factory=_property_component_factory,
            provider_factory=_property_provider_factory,
            included_surface_aliases=("vertical",),
            studio_panel_builder=_build_property_panel,
        ),
        FacadePlugin(
            manifest=_manifest(
                name="campaign_ops",
                title="Campaign Operations",
                domain="campaign_graph",
                router_module="vei.router.campaign_ops",
                description="Campaign, creative, approval, and reporting domain surface.",
                surfaces=["mcp", "ui"],
                primary_tools=[
                    "campaign.list_overview",
                    "campaign.approve_creative",
                    "campaign.adjust_budget_pacing",
                ],
                state_roots=["components.campaign_ops"],
                tags=["vertical", "marketing", "campaigns"],
            ),
            tool_families=("campaign",),
            tool_prefixes=("campaign.",),
            scenario_seed_fields=("campaign_graph",),
            component_attr="campaign_ops",
            focuses=("campaign",),
            event_targets=("campaign_ops",),
            summary_builder=_component_summary,
            action_menu_builder=_component_action_menu,
            state_dump=_component_dump,
            state_restore=_component_restore,
            component_factory=_campaign_component_factory,
            provider_factory=_campaign_provider_factory,
            included_surface_aliases=("vertical",),
            studio_panel_builder=_build_campaign_panel,
        ),
        FacadePlugin(
            manifest=_manifest(
                name="inventory_ops",
                title="Inventory Operations",
                domain="inventory_graph",
                router_module="vei.router.inventory_ops",
                description="Capacity, quote, allocation, and fulfillment domain surface.",
                surfaces=["mcp", "ui"],
                primary_tools=[
                    "inventory.list_overview",
                    "inventory.allocate_capacity",
                    "inventory.revise_quote",
                ],
                state_roots=["components.inventory_ops"],
                tags=["vertical", "storage", "inventory"],
            ),
            tool_families=("inventory",),
            tool_prefixes=("inventory.",),
            scenario_seed_fields=("inventory_graph",),
            component_attr="inventory_ops",
            focuses=("inventory",),
            event_targets=("inventory_ops",),
            summary_builder=_component_summary,
            action_menu_builder=_component_action_menu,
            state_dump=_component_dump,
            state_restore=_component_restore,
            component_factory=_inventory_component_factory,
            provider_factory=_inventory_provider_factory,
            included_surface_aliases=("vertical",),
            studio_panel_builder=_build_inventory_panel,
        ),
        FacadePlugin(
            manifest=_manifest(
                name="service_ops",
                title="Service Operations",
                domain="ops_graph",
                router_module="vei.router.service_ops",
                description="Dispatch, billing hold, technician, and exception management surface.",
                surfaces=["mcp", "ui"],
                primary_tools=[
                    "service_ops.list_overview",
                    "service_ops.assign_dispatch",
                    "service_ops.hold_billing",
                ],
                state_roots=["components.service_ops"],
                tags=["vertical", "service", "operations"],
            ),
            tool_families=("service_ops",),
            tool_prefixes=("service_ops.",),
            scenario_seed_fields=("service_ops",),
            component_attr="service_ops",
            focuses=("service_ops",),
            event_targets=("service_ops",),
            summary_builder=_component_summary,
            action_menu_builder=_component_action_menu,
            state_dump=_component_dump,
            state_restore=_component_restore,
            component_factory=_service_ops_component_factory,
            provider_factory=_service_ops_provider_factory,
            included_surface_aliases=("vertical",),
            studio_panel_builder=_build_service_ops_panel,
        ),
    ]
    for plugin in builtins:
        register_facade_plugin(plugin)
    _BOOTSTRAPPED = True


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


def _slack_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import SlackSim

    return SlackSim(router.bus, scenario)


def _mail_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import MailSim

    return MailSim(router.bus, scenario)


def _browser_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import BrowserVirtual

    return BrowserVirtual(router.bus, scenario)


def _docs_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import DocsSim

    return DocsSim(scenario)


def _calendar_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import CalendarSim

    return CalendarSim(scenario)


def _tickets_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import TicketsSim

    return TicketsSim(scenario)


def _database_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import DatabaseSim

    return DatabaseSim(scenario)


def _erp_component_factory(router: Any, scenario: "Scenario") -> Any:
    try:
        from vei.router import ErpSim

        return ErpSim(router.bus, scenario)
    except Exception:
        logger.warning("ERP twin failed to initialise", exc_info=True)
        return None


def _crm_component_factory(router: Any, scenario: "Scenario") -> Any:
    try:
        from vei.router import CrmSim

        return CrmSim(router.bus, scenario)
    except Exception:
        logger.warning("CRM twin failed to initialise", exc_info=True)
        return None


def _okta_component_factory(router: Any, scenario: "Scenario") -> Any:
    try:
        from vei.router import OktaSim

        return OktaSim(scenario)
    except Exception:
        logger.warning("Okta twin failed to initialise", exc_info=True)
        return None


def _okta_provider_factory(component: Any) -> Any:
    from vei.router import OktaToolProvider

    return OktaToolProvider(component)


def _servicedesk_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import ServiceDeskSim

    return ServiceDeskSim(scenario)


def _servicedesk_provider_factory(component: Any) -> Any:
    from vei.router import ServiceDeskToolProvider

    return ServiceDeskToolProvider(component)


def _google_admin_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import GoogleAdminSim

    return GoogleAdminSim(scenario)


def _google_admin_provider_factory(component: Any) -> Any:
    from vei.router import GoogleAdminToolProvider

    return GoogleAdminToolProvider(component)


def _siem_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import SiemSim

    return SiemSim(scenario)


def _siem_provider_factory(component: Any) -> Any:
    from vei.router import SiemToolProvider

    return SiemToolProvider(component)


def _datadog_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import DatadogSim

    return DatadogSim(scenario)


def _datadog_provider_factory(component: Any) -> Any:
    from vei.router import DatadogToolProvider

    return DatadogToolProvider(component)


def _pagerduty_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import PagerDutySim

    return PagerDutySim(scenario)


def _pagerduty_provider_factory(component: Any) -> Any:
    from vei.router import PagerDutyToolProvider

    return PagerDutyToolProvider(component)


def _feature_flags_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import FeatureFlagSim

    return FeatureFlagSim(scenario)


def _feature_flags_provider_factory(component: Any) -> Any:
    from vei.router import FeatureFlagToolProvider

    return FeatureFlagToolProvider(component)


def _hris_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import HrisSim

    return HrisSim(scenario)


def _hris_provider_factory(component: Any) -> Any:
    from vei.router import HrisToolProvider

    return HrisToolProvider(component)


def _jira_provider_factory(component: Any) -> Any:
    from vei.router import JiraToolProvider

    return JiraToolProvider(component)


def _notes_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import NotesSim

    return NotesSim(router.bus, scenario)


def _knowledge_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import KnowledgeSim

    return KnowledgeSim(router, scenario)


def _knowledge_provider_factory(component: Any) -> Any:
    from vei.router import KnowledgeToolProvider

    return KnowledgeToolProvider(component)


def _notes_provider_factory(component: Any) -> Any:
    from vei.router import NotesToolProvider

    return NotesToolProvider(component)


def _spreadsheet_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import SpreadsheetSim

    return SpreadsheetSim(router.bus, scenario)


def _spreadsheet_provider_factory(component: Any) -> Any:
    from vei.router import SpreadsheetToolProvider

    return SpreadsheetToolProvider(component)


def _spreadsheet_summary(router: Any, component: Any) -> str:
    del router
    return component.summary()


def _spreadsheet_action_menu(router: Any, component: Any) -> List[Dict[str, Any]]:
    del router
    return component.action_menu()


def _spreadsheet_dump(component: Any) -> Dict[str, Any]:
    return component.export_state()


def _spreadsheet_restore(component: Any, state: Dict[str, Any]) -> None:
    component.import_state(state)


def _property_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import PropertyOpsSim

    return PropertyOpsSim(scenario)


def _property_provider_factory(component: Any) -> Any:
    from vei.router import PropertyOpsToolProvider

    return PropertyOpsToolProvider(component)


def _campaign_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import CampaignOpsSim

    return CampaignOpsSim(scenario)


def _campaign_provider_factory(component: Any) -> Any:
    from vei.router import CampaignOpsToolProvider

    return CampaignOpsToolProvider(component)


def _inventory_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import InventoryOpsSim

    return InventoryOpsSim(scenario)


def _inventory_provider_factory(component: Any) -> Any:
    from vei.router import InventoryOpsToolProvider

    return InventoryOpsToolProvider(component)


def _service_ops_component_factory(router: Any, scenario: "Scenario") -> Any:
    from vei.router import ServiceOpsSim

    return ServiceOpsSim(scenario)


def _service_ops_provider_factory(component: Any) -> Any:
    from vei.router import ServiceOpsToolProvider

    return ServiceOpsToolProvider(component)


def _component_summary(router: Any, component: Any) -> str:
    del router
    return component.summary()


def _component_action_menu(router: Any, component: Any) -> List[Dict[str, Any]]:
    del router
    return component.action_menu()


def _component_dump(component: Any) -> Dict[str, Any]:
    return component.export_state()


def _component_restore(component: Any, state: Dict[str, Any]) -> None:
    component.import_state(state)


def _register_slack_gateway_routes(app: Any, runtime: Any) -> None:
    from vei.twin.api import register_slack_gateway_routes

    register_slack_gateway_routes(app, runtime)


def _register_jira_gateway_routes(app: Any, runtime: Any) -> None:
    from vei.twin.api import register_jira_gateway_routes

    register_jira_gateway_routes(app, runtime)


def _register_graph_gateway_routes(app: Any, runtime: Any) -> None:
    from vei.twin.api import register_graph_gateway_routes

    register_graph_gateway_routes(app, runtime)


def _register_salesforce_gateway_routes(app: Any, runtime: Any) -> None:
    from vei.twin.api import register_salesforce_gateway_routes

    register_salesforce_gateway_routes(app, runtime)


def _register_notes_gateway_routes(app: Any, runtime: Any) -> None:
    from vei.twin.api import register_notes_gateway_routes

    register_notes_gateway_routes(app, runtime)


def _build_slack_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from vei.run.api import build_slack_surface_panel

    return build_slack_surface_panel(components, context)


def _build_mail_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from vei.run.api import build_mail_surface_panel

    return build_mail_surface_panel(components, context)


def _build_ticket_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from vei.run.api import build_ticket_surface_panel

    return build_ticket_surface_panel(components, context)


def _build_docs_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from vei.run.api import build_docs_surface_panel

    return build_docs_surface_panel(components, context)


def _build_approval_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from vei.run.api import build_approval_surface_panel

    return build_approval_surface_panel(components, context)


def _build_notes_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from vei.run.api import build_notes_surface_panel

    return build_notes_surface_panel(components, context)


def _build_knowledge_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from vei.run.api import build_knowledge_surface_panel

    return build_knowledge_surface_panel(components, context)


def _build_revenue_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from vei.run.api import build_revenue_surface_panel

    return build_revenue_surface_panel(components, context)


def _build_property_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from vei.run.api import build_property_surface_panel

    return build_property_surface_panel(components, context)


def _build_campaign_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from vei.run.api import build_campaign_surface_panel

    return build_campaign_surface_panel(components, context)


def _build_inventory_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from vei.run.api import build_inventory_surface_panel

    return build_inventory_surface_panel(components, context)


def _build_service_ops_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from vei.run.api import build_service_ops_surface_panel

    return build_service_ops_surface_panel(components, context)
