from __future__ import annotations

from typing import Any


def resolve_focus_for_tool(router: Any, tool: str) -> str:
    resolved = router.alias_map.get(tool, tool)
    for prefix in (
        "slack",
        "mail",
        "docs",
        "calendar",
        "tickets",
        "erp",
        "crm",
        "db",
        "browser",
        "okta",
        "servicedesk",
        "google_admin",
        "siem",
        "datadog",
        "pagerduty",
        "feature_flags",
        "hris",
        "jira",
    ):
        if resolved.startswith(f"{prefix}."):
            return prefix
    if tool.startswith("salesforce.") or tool.startswith("hubspot."):
        return "crm"
    plugin_focus = router._plugin_focus_for_tool(resolved)
    if plugin_focus:
        return plugin_focus
    return "browser"


def build_focus_summary(router: Any, focus: str) -> str:
    if focus == "browser":
        page = router.browser.read()
        return f"Browser: {page['title']} — {page['excerpt']}"
    if focus == "slack":
        channels = router.slack.list_channels()
        if not channels:
            return "Slack: no channels"
        channel = channels[0]
        opened = router.slack.open_channel(channel)
        latest = opened["messages"][-1]["text"] if opened["messages"] else ""
        return f"Slack {channel} latest: {latest}"
    if focus == "mail":
        messages = router.mail.list()
        if messages:
            return f"Mail: {messages[0]['subj']} from {messages[0]['from']}"
        return "Mail: INBOX empty"
    if focus == "docs":
        docs = router.docs.list()
        if not docs:
            return "Docs: empty library"
        return f"Docs: {len(docs)} available (latest: {docs[-1]['title']})"
    if focus == "calendar":
        events = router.calendar.list_events()
        if not events:
            return "Calendar: no scheduled events"
        soon = events[0]
        return f"Calendar: next {soon['title']} at {soon['start_ms']}"
    if focus == "tickets":
        tickets = router.tickets.list()
        if not tickets:
            return "Tickets: queue empty"
        open_count = sum(
            1 for ticket in tickets if ticket["status"].lower() != "closed"
        )
        return f"Tickets: {open_count} open of {len(tickets)}"
    if focus == "erp":
        purchase_orders = (
            len(getattr(router, "erp").pos) if getattr(router, "erp", None) else 0
        )
        invoices = (
            len(getattr(router, "erp").invoices) if getattr(router, "erp", None) else 0
        )
        return f"ERP: {purchase_orders} POs, {invoices} invoices"
    if focus == "crm":
        contacts = (
            len(getattr(router, "crm").contacts) if getattr(router, "crm", None) else 0
        )
        deals = len(getattr(router, "crm").deals) if getattr(router, "crm", None) else 0
        return f"CRM: {contacts} contacts, {deals} deals"
    if focus == "db":
        tables = router.database.list_tables()
        if not tables:
            return "DB: no tables"
        largest = max(tables, key=lambda item: int(item.get("row_count", 0)))
        return f"DB: {len(tables)} tables (largest: {largest['table']})"
    if focus == "okta":
        if not getattr(router, "okta", None):
            return "Okta: unavailable"
        users = router.okta.list_users(limit=1)
        total = int(users.get("total", users.get("count", 0)))
        suspended = router.okta.list_users(status="SUSPENDED", limit=1)
        suspended_total = int(suspended.get("total", suspended.get("count", 0)))
        return f"Okta: {total} users ({suspended_total} suspended)"
    if focus == "servicedesk":
        incidents = router.servicedesk.list_incidents(limit=1)
        request_rows = router.servicedesk.list_requests(limit=1)
        return (
            "ServiceDesk: "
            f"{incidents.get('total', incidents.get('count', 0))} incidents, "
            f"{request_rows.get('total', request_rows.get('count', 0))} requests"
        )
    if focus == "google_admin":
        apps = router.google_admin.list_oauth_apps(limit=1)
        shares = router.google_admin.list_drive_shares(limit=1)
        return (
            "Google Admin: "
            f"{apps.get('total', apps.get('count', 0))} OAuth apps, "
            f"{shares.get('total', shares.get('count', 0))} drive shares"
        )
    if focus == "siem":
        alerts = router.siem.list_alerts(limit=1)
        cases = router.siem.list_cases(limit=1)
        return (
            "SIEM: "
            f"{alerts.get('total', alerts.get('count', 0))} alerts, "
            f"{cases.get('total', cases.get('count', 0))} cases"
        )
    if focus == "datadog":
        services = router.datadog.list_services(limit=1)
        monitors = router.datadog.list_monitors(limit=1)
        return (
            "Datadog: "
            f"{services.get('total', services.get('count', 0))} services, "
            f"{monitors.get('total', monitors.get('count', 0))} monitors"
        )
    if focus == "pagerduty":
        incidents = router.pagerduty.list_incidents(limit=1)
        return (
            "PagerDuty: "
            f"{incidents.get('total', incidents.get('count', 0))} incidents"
        )
    if focus == "feature_flags":
        flags = router.feature_flags.list_flags(limit=1)
        return f"Feature Flags: {flags.get('total', flags.get('count', 0))} flags"
    if focus == "hris":
        employees = router.hris.list_employees(limit=1)
        return "HRIS: " f"{employees.get('total', employees.get('count', 0))} employees"
    if focus == "jira":
        issues = router.tickets.list(limit=1)
        total = (
            issues.get("total", issues.get("count", 0))
            if isinstance(issues, dict)
            else len(issues)
        )
        return f"Jira: {total} issues"
    plugin_summary = router._plugin_summary(focus)
    if plugin_summary is not None:
        return plugin_summary
    return ""


def build_action_menu(router: Any, focus: str) -> list[dict[str, Any]]:
    if focus == "browser":
        node_affordances = router.browser.nodes[router.browser.state]["affordances"]
        generic = [
            {"tool": "browser.read", "args_schema": {}},
            {
                "tool": "browser.find",
                "args_schema": {"query": "str", "top_k": "int?"},
            },
            {"tool": "browser.open", "args_schema": {"url": "str"}},
            {"tool": "browser.back", "args_schema": {}},
        ]
        return [*node_affordances, *generic]
    if focus == "slack":
        return [
            {
                "tool": "slack.send_message",
                "args_schema": {
                    "channel": "str",
                    "text": "str",
                    "thread_ts": "str?",
                },
            },
        ]
    if focus == "mail":
        return [
            {
                "tool": "mail.compose",
                "args_schema": {"to": "str", "subj": "str", "body_text": "str"},
            },
        ]
    if focus == "docs":
        return [
            {
                "tool": "docs.list",
                "args_schema": {
                    "query": "str?",
                    "tag": "str?",
                    "status": "str?",
                    "owner": "str?",
                    "limit": "int?",
                    "cursor": "str?",
                    "sort_by": "str?",
                    "sort_dir": "asc|desc?",
                },
            },
            {
                "tool": "docs.search",
                "args_schema": {"query": "str", "limit": "int?", "cursor": "str?"},
            },
            {"tool": "docs.read", "args_schema": {"doc_id": "str"}},
            {
                "tool": "docs.create",
                "args_schema": {
                    "title": "str",
                    "body": "str",
                    "tags": "[str]?",
                    "owner": "str?",
                    "status": "str?",
                },
            },
            {
                "tool": "docs.update",
                "args_schema": {
                    "doc_id": "str",
                    "title": "str?",
                    "body": "str?",
                    "tags": "[str]?",
                    "status": "str?",
                },
            },
        ]
    if focus == "calendar":
        return [
            {
                "tool": "calendar.list_events",
                "args_schema": {
                    "attendee": "str?",
                    "status": "str?",
                    "starts_after_ms": "int?",
                    "ends_before_ms": "int?",
                    "limit": "int?",
                    "cursor": "str?",
                    "sort_dir": "asc|desc?",
                },
            },
            {
                "tool": "calendar.create_event",
                "args_schema": {
                    "title": "str",
                    "start_ms": "int",
                    "end_ms": "int",
                    "attendees": "[str]?",
                    "location": "str?",
                    "description": "str?",
                    "organizer": "str?",
                    "status": "str?",
                },
            },
            {
                "tool": "calendar.accept",
                "args_schema": {"event_id": "str", "attendee": "str"},
            },
            {
                "tool": "calendar.decline",
                "args_schema": {"event_id": "str", "attendee": "str"},
            },
            {
                "tool": "calendar.update_event",
                "args_schema": {
                    "event_id": "str",
                    "title": "str?",
                    "start_ms": "int?",
                    "end_ms": "int?",
                    "attendees": "[str]?",
                    "location": "str?",
                    "description": "str?",
                    "status": "str?",
                },
            },
            {
                "tool": "calendar.cancel_event",
                "args_schema": {"event_id": "str", "reason": "str?"},
            },
        ]
    if focus == "tickets":
        return [
            {
                "tool": "tickets.list",
                "args_schema": {
                    "status": "str?",
                    "assignee": "str?",
                    "priority": "str?",
                    "query": "str?",
                    "limit": "int?",
                    "cursor": "str?",
                    "sort_by": "str?",
                    "sort_dir": "asc|desc?",
                },
            },
            {"tool": "tickets.get", "args_schema": {"ticket_id": "str"}},
            {
                "tool": "tickets.create",
                "args_schema": {
                    "title": "str",
                    "description": "str?",
                    "assignee": "str?",
                    "priority": "str?",
                    "severity": "str?",
                    "labels": "[str]?",
                },
            },
            {
                "tool": "tickets.update",
                "args_schema": {
                    "ticket_id": "str",
                    "description": "str?",
                    "assignee": "str?",
                    "priority": "str?",
                    "severity": "str?",
                    "labels": "[str]?",
                },
            },
            {
                "tool": "tickets.transition",
                "args_schema": {"ticket_id": "str", "status": "str"},
            },
            {
                "tool": "tickets.add_comment",
                "args_schema": {
                    "ticket_id": "str",
                    "body": "str",
                    "author": "str?",
                },
            },
        ]
    if focus == "erp" and getattr(router, "erp", None):
        return [
            {
                "tool": "erp.create_po",
                "args_schema": {
                    "vendor": "str",
                    "currency": "str",
                    "lines": "[{item_id,desc,qty,unit_price}]",
                },
            },
            {
                "tool": "erp.list_pos",
                "args_schema": {
                    "vendor": "str?",
                    "status": "str?",
                    "currency": "str?",
                    "limit": "int?",
                    "cursor": "str?",
                    "sort_by": "str?",
                    "sort_dir": "asc|desc?",
                },
            },
            {
                "tool": "erp.submit_invoice",
                "args_schema": {
                    "vendor": "str",
                    "po_id": "str",
                    "lines": "[{item_id,qty,unit_price}]",
                },
            },
            {
                "tool": "erp.match_three_way",
                "args_schema": {
                    "po_id": "str",
                    "invoice_id": "str",
                    "receipt_id": "str?",
                },
            },
        ]
    if focus == "crm" and getattr(router, "crm", None):
        return [
            {
                "tool": "crm.create_contact",
                "args_schema": {
                    "email": "str",
                    "first_name": "str?",
                    "last_name": "str?",
                    "do_not_contact": "bool?",
                },
            },
            {
                "tool": "crm.create_company",
                "args_schema": {"name": "str", "domain": "str?"},
            },
            {
                "tool": "crm.list_contacts",
                "args_schema": {
                    "query": "str?",
                    "company_id": "str?",
                    "do_not_contact": "bool?",
                    "limit": "int?",
                    "cursor": "str?",
                },
            },
            {
                "tool": "crm.list_companies",
                "args_schema": {
                    "query": "str?",
                    "domain": "str?",
                    "limit": "int?",
                    "cursor": "str?",
                },
            },
            {
                "tool": "crm.associate_contact_company",
                "args_schema": {"contact_id": "str", "company_id": "str"},
            },
            {
                "tool": "crm.create_deal",
                "args_schema": {
                    "name": "str",
                    "amount": "number",
                    "stage": "str?",
                    "contact_id": "str?",
                    "company_id": "str?",
                    "close_date": "str?",
                },
            },
            {
                "tool": "crm.update_deal_stage",
                "args_schema": {"id": "str", "stage": "str"},
            },
            {
                "tool": "crm.list_deals",
                "args_schema": {
                    "stage": "str?",
                    "company_id": "str?",
                    "min_amount": "number?",
                    "max_amount": "number?",
                    "limit": "int?",
                    "cursor": "str?",
                },
            },
            {
                "tool": "crm.reassign_deal_owner",
                "args_schema": {"id": "str", "owner": "str"},
            },
            {
                "tool": "crm.log_activity",
                "args_schema": {
                    "kind": "str",
                    "contact_id": "str?",
                    "deal_id": "str?",
                    "note": "str?",
                },
            },
        ]
    if focus == "db":
        return [
            {
                "tool": "db.list_tables",
                "args_schema": {
                    "query": "str?",
                    "limit": "int?",
                    "cursor": "str?",
                    "sort_by": "str?",
                    "sort_dir": "asc|desc?",
                },
            },
            {"tool": "db.describe_table", "args_schema": {"table": "str"}},
            {
                "tool": "db.query",
                "args_schema": {
                    "table": "str",
                    "filters": "object?",
                    "columns": "[str]?",
                    "limit": "int?",
                    "offset": "int?",
                    "cursor": "str?",
                    "sort_by": "str?",
                    "descending": "bool?",
                },
            },
            {
                "tool": "db.upsert",
                "args_schema": {"table": "str", "row": "object", "key": "str?"},
            },
        ]
    if focus == "okta":
        return [
            {
                "tool": "okta.list_users",
                "args_schema": {
                    "status": "str?",
                    "query": "str?",
                    "include_groups": "bool?",
                    "limit": "int?",
                    "cursor": "str?",
                },
            },
            {"tool": "okta.get_user", "args_schema": {"user_id": "str"}},
            {"tool": "okta.suspend_user", "args_schema": {"user_id": "str"}},
            {"tool": "okta.unsuspend_user", "args_schema": {"user_id": "str"}},
            {"tool": "okta.list_groups", "args_schema": {"query": "str?"}},
            {
                "tool": "okta.assign_group",
                "args_schema": {"user_id": "str", "group_id": "str"},
            },
        ]
    if focus == "servicedesk":
        return [
            {
                "tool": "servicedesk.list_incidents",
                "args_schema": {
                    "status": "str?",
                    "priority": "str?",
                    "query": "str?",
                    "limit": "int?",
                    "cursor": "str?",
                },
            },
            {
                "tool": "servicedesk.get_incident",
                "args_schema": {"incident_id": "str"},
            },
            {
                "tool": "servicedesk.update_incident",
                "args_schema": {
                    "incident_id": "str",
                    "status": "str?",
                    "assignee": "str?",
                    "comment": "str?",
                },
            },
            {
                "tool": "servicedesk.list_requests",
                "args_schema": {"status": "str?", "query": "str?"},
            },
            {
                "tool": "servicedesk.update_request",
                "args_schema": {
                    "request_id": "str",
                    "status": "str?",
                    "approval_stage": "str?",
                    "approval_status": "str?",
                    "comment": "str?",
                },
            },
        ]
    if focus == "google_admin":
        return [
            {
                "tool": "google_admin.list_oauth_apps",
                "args_schema": {
                    "status": "str?",
                    "risk_level": "str?",
                    "query": "str?",
                    "limit": "int?",
                    "cursor": "str?",
                },
            },
            {"tool": "google_admin.get_oauth_app", "args_schema": {"app_id": "str"}},
            {
                "tool": "google_admin.suspend_oauth_app",
                "args_schema": {"app_id": "str", "reason": "str?"},
            },
            {
                "tool": "google_admin.preserve_oauth_evidence",
                "args_schema": {"app_id": "str", "note": "str?"},
            },
            {
                "tool": "google_admin.list_drive_shares",
                "args_schema": {
                    "visibility": "str?",
                    "owner": "str?",
                    "query": "str?",
                    "limit": "int?",
                    "cursor": "str?",
                },
            },
            {
                "tool": "google_admin.restrict_drive_share",
                "args_schema": {
                    "doc_id": "str",
                    "visibility": "str?",
                    "note": "str?",
                },
            },
        ]
    if focus == "siem":
        return [
            {
                "tool": "siem.list_alerts",
                "args_schema": {
                    "status": "str?",
                    "severity": "str?",
                    "query": "str?",
                    "limit": "int?",
                    "cursor": "str?",
                },
            },
            {"tool": "siem.get_alert", "args_schema": {"alert_id": "str"}},
            {
                "tool": "siem.create_case",
                "args_schema": {
                    "title": "str",
                    "alert_id": "str?",
                    "severity": "str?",
                    "owner": "str?",
                },
            },
            {
                "tool": "siem.list_cases",
                "args_schema": {"status": "str?", "owner": "str?"},
            },
            {"tool": "siem.get_case", "args_schema": {"case_id": "str"}},
            {
                "tool": "siem.preserve_evidence",
                "args_schema": {
                    "alert_id": "str",
                    "case_id": "str?",
                    "note": "str?",
                },
            },
            {
                "tool": "siem.update_case",
                "args_schema": {
                    "case_id": "str",
                    "status": "str?",
                    "owner": "str?",
                    "customer_notification_required": "bool?",
                    "note": "str?",
                },
            },
        ]
    if focus == "datadog":
        return [
            {
                "tool": "datadog.list_services",
                "args_schema": {"status": "str?", "query": "str?"},
            },
            {"tool": "datadog.get_service", "args_schema": {"service_id": "str"}},
            {
                "tool": "datadog.update_service",
                "args_schema": {
                    "service_id": "str",
                    "status": "str?",
                    "note": "str?",
                },
            },
            {
                "tool": "datadog.list_monitors",
                "args_schema": {
                    "status": "str?",
                    "severity": "str?",
                    "service_id": "str?",
                },
            },
            {"tool": "datadog.get_monitor", "args_schema": {"monitor_id": "str"}},
            {
                "tool": "datadog.mute_monitor",
                "args_schema": {"monitor_id": "str", "reason": "str?"},
            },
        ]
    if focus == "pagerduty":
        return [
            {
                "tool": "pagerduty.list_incidents",
                "args_schema": {
                    "status": "str?",
                    "urgency": "str?",
                    "service_id": "str?",
                },
            },
            {
                "tool": "pagerduty.get_incident",
                "args_schema": {"incident_id": "str"},
            },
            {
                "tool": "pagerduty.ack_incident",
                "args_schema": {"incident_id": "str", "assignee": "str?"},
            },
            {
                "tool": "pagerduty.escalate_incident",
                "args_schema": {"incident_id": "str", "assignee": "str"},
            },
            {
                "tool": "pagerduty.resolve_incident",
                "args_schema": {"incident_id": "str", "note": "str?"},
            },
        ]
    if focus == "feature_flags":
        return [
            {
                "tool": "feature_flags.list_flags",
                "args_schema": {"service": "str?", "env": "str?", "limit": "int?"},
            },
            {"tool": "feature_flags.get_flag", "args_schema": {"flag_key": "str"}},
            {
                "tool": "feature_flags.set_flag",
                "args_schema": {
                    "flag_key": "str",
                    "enabled": "bool",
                    "env": "str?",
                    "reason": "str?",
                },
            },
            {
                "tool": "feature_flags.update_rollout",
                "args_schema": {
                    "flag_key": "str",
                    "rollout_pct": "int",
                    "env": "str?",
                    "reason": "str?",
                },
            },
        ]
    if focus == "hris":
        return [
            {
                "tool": "hris.list_employees",
                "args_schema": {
                    "status": "str?",
                    "cohort": "str?",
                    "query": "str?",
                    "limit": "int?",
                    "cursor": "str?",
                },
            },
            {"tool": "hris.get_employee", "args_schema": {"employee_id": "str"}},
            {
                "tool": "hris.resolve_identity",
                "args_schema": {
                    "employee_id": "str",
                    "corporate_email": "str?",
                    "manager": "str?",
                    "note": "str?",
                },
            },
            {
                "tool": "hris.mark_onboarded",
                "args_schema": {"employee_id": "str", "note": "str?"},
            },
        ]
    if focus == "jira":
        return [
            {
                "tool": "jira.list_issues",
                "args_schema": {"status": "str?", "assignee": "str?"},
            },
            {"tool": "jira.get_issue", "args_schema": {"issue_id": "str"}},
            {
                "tool": "jira.create_issue",
                "args_schema": {
                    "title": "str",
                    "description": "str?",
                    "assignee": "str?",
                },
            },
            {
                "tool": "jira.transition_issue",
                "args_schema": {"issue_id": "str", "status": "str"},
            },
            {
                "tool": "jira.add_comment",
                "args_schema": {"issue_id": "str", "body": "str", "author": "str?"},
            },
        ]
    plugin_action_menu = router._plugin_action_menu(focus)
    if plugin_action_menu is not None:
        return plugin_action_menu
    return []
