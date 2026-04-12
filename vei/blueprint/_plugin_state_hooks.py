from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Dict

from vei.identity.api import IdentityApplication, IdentityGroup, IdentityUser
from vei.world import CalendarEvent, Document, Ticket


def jsonable(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def dump_slack_state(component: Any) -> Dict[str, Any]:
    return {
        "channels": jsonable(component.channels),
        "budget_cap_usd": component.budget_cap_usd,
        "derail_prob": component.derail_prob,
    }


def restore_slack_state(component: Any, state: Dict[str, Any]) -> None:
    component.channels = jsonable(state.get("channels", {}))
    component.budget_cap_usd = int(
        state.get("budget_cap_usd", component.budget_cap_usd)
    )
    component.derail_prob = float(state.get("derail_prob", component.derail_prob))


def dump_mail_state(component: Any) -> Dict[str, Any]:
    return {
        "messages": jsonable(component.messages),
        "inbox": list(component.inbox),
        "counter": int(component.counter),
        "variants_override": jsonable(component._variants_override),
    }


def restore_mail_state(component: Any, state: Dict[str, Any]) -> None:
    component.messages = jsonable(state.get("messages", {}))
    component.inbox = list(state.get("inbox", []))
    component.counter = int(state.get("counter", component.counter))
    component._variants_override = jsonable(state.get("variants_override"))


def dump_browser_state(component: Any) -> Dict[str, Any]:
    return {"nodes": jsonable(component.nodes), "state": component.state}


def restore_browser_state(component: Any, state: Dict[str, Any]) -> None:
    component.nodes = jsonable(state.get("nodes", component.nodes))
    component.state = str(state.get("state", component.state))


def dump_docs_state(component: Any) -> Dict[str, Any]:
    return {
        "docs": {doc_id: jsonable(doc) for doc_id, doc in component.docs.items()},
        "metadata": jsonable(component.metadata),
        "clock_ms": int(component._clock_ms),
        "doc_seq": int(component._doc_seq),
    }


def restore_docs_state(component: Any, state: Dict[str, Any]) -> None:
    component.docs = {
        doc_id: Document(**payload)
        for doc_id, payload in jsonable(state.get("docs", {})).items()
    }
    component.metadata = jsonable(state.get("metadata", {}))
    component._clock_ms = int(state.get("clock_ms", component._clock_ms))
    component._doc_seq = int(state.get("doc_seq", component._doc_seq))


def dump_calendar_state(component: Any) -> Dict[str, Any]:
    return {
        "events": {
            event_id: jsonable(event) for event_id, event in component.events.items()
        },
        "responses": jsonable(component.responses),
        "metadata": jsonable(component.metadata),
        "clock_ms": int(component._clock_ms),
        "event_seq": int(component._event_seq),
    }


def restore_calendar_state(component: Any, state: Dict[str, Any]) -> None:
    component.events = {
        event_id: CalendarEvent(**payload)
        for event_id, payload in jsonable(state.get("events", {})).items()
    }
    component.responses = jsonable(state.get("responses", {}))
    component.metadata = jsonable(state.get("metadata", {}))
    component._clock_ms = int(state.get("clock_ms", component._clock_ms))
    component._event_seq = int(state.get("event_seq", component._event_seq))


def dump_tickets_state(component: Any) -> Dict[str, Any]:
    return {
        "tickets": {
            ticket_id: jsonable(ticket)
            for ticket_id, ticket in component.tickets.items()
        },
        "metadata": jsonable(component.metadata),
        "clock_ms": int(component._clock_ms),
        "ticket_seq": int(component._ticket_seq),
    }


def restore_tickets_state(component: Any, state: Dict[str, Any]) -> None:
    component.tickets = {
        ticket_id: Ticket(**payload)
        for ticket_id, payload in jsonable(state.get("tickets", {})).items()
    }
    component.metadata = jsonable(state.get("metadata", {}))
    component._clock_ms = int(state.get("clock_ms", component._clock_ms))
    component._ticket_seq = int(state.get("ticket_seq", component._ticket_seq))


def dump_database_state(component: Any) -> Dict[str, Any]:
    return {"tables": jsonable(component.tables)}


def restore_database_state(component: Any, state: Dict[str, Any]) -> None:
    component.tables = jsonable(state.get("tables", {}))


def dump_erp_state(component: Any) -> Dict[str, Any]:
    return {
        "available": component is not None,
        "pos": jsonable(getattr(component, "pos", {})),
        "invoices": jsonable(getattr(component, "invoices", {})),
        "receipts": jsonable(getattr(component, "receipts", {})),
        "po_seq": int(getattr(component, "_po_seq", 1)),
        "inv_seq": int(getattr(component, "_inv_seq", 1)),
        "rcpt_seq": int(getattr(component, "_rcpt_seq", 1)),
        "currency_default": getattr(component, "currency_default", "USD"),
        "error_rate": float(getattr(component, "error_rate", 0.0)),
    }


def restore_erp_state(component: Any, state: Dict[str, Any]) -> None:
    if not state.get("available", True):
        return
    component.pos = jsonable(state.get("pos", {}))
    component.invoices = jsonable(state.get("invoices", {}))
    component.receipts = jsonable(state.get("receipts", {}))
    component._po_seq = int(state.get("po_seq", component._po_seq))
    component._inv_seq = int(state.get("inv_seq", component._inv_seq))
    component._rcpt_seq = int(state.get("rcpt_seq", component._rcpt_seq))
    component.currency_default = str(
        state.get("currency_default", component.currency_default)
    )
    component.error_rate = float(state.get("error_rate", component.error_rate))


def dump_crm_state(component: Any) -> Dict[str, Any]:
    return {
        "available": component is not None,
        "contacts": jsonable(getattr(component, "contacts", {})),
        "companies": jsonable(getattr(component, "companies", {})),
        "deals": jsonable(getattr(component, "deals", {})),
        "activities": jsonable(getattr(component, "activities", [])),
        "contact_seq": int(getattr(component, "_c_seq", 1)),
        "company_seq": int(getattr(component, "_co_seq", 1)),
        "deal_seq": int(getattr(component, "_d_seq", 1)),
        "activity_seq": int(getattr(component, "_a_seq", 1)),
        "error_rate": float(getattr(component, "error_rate", 0.0)),
    }


def restore_crm_state(component: Any, state: Dict[str, Any]) -> None:
    if not state.get("available", True):
        return
    component.contacts = jsonable(state.get("contacts", {}))
    component.companies = jsonable(state.get("companies", {}))
    component.deals = jsonable(state.get("deals", {}))
    component.activities = jsonable(state.get("activities", []))
    component._c_seq = int(state.get("contact_seq", component._c_seq))
    component._co_seq = int(state.get("company_seq", component._co_seq))
    component._d_seq = int(state.get("deal_seq", component._d_seq))
    component._a_seq = int(state.get("activity_seq", component._a_seq))
    component.error_rate = float(state.get("error_rate", component.error_rate))


def dump_okta_state(component: Any) -> Dict[str, Any]:
    return {
        "available": component is not None,
        "users": {
            user_id: user.model_dump()
            for user_id, user in getattr(component, "users", {}).items()
        },
        "groups": {
            group_id: group.model_dump()
            for group_id, group in getattr(component, "groups", {}).items()
        },
        "apps": {
            app_id: app.model_dump()
            for app_id, app in getattr(component, "apps", {}).items()
        },
        "reset_seq": int(getattr(component, "_reset_seq", 1)),
    }


def restore_okta_state(component: Any, state: Dict[str, Any]) -> None:
    if not state.get("available", True):
        return
    component.users = {
        user_id: IdentityUser.model_validate(payload)
        for user_id, payload in jsonable(state.get("users", {})).items()
    }
    component.groups = {
        group_id: IdentityGroup.model_validate(payload)
        for group_id, payload in jsonable(state.get("groups", {})).items()
    }
    component.apps = {
        app_id: IdentityApplication.model_validate(payload)
        for app_id, payload in jsonable(state.get("apps", {})).items()
    }
    component._reset_seq = int(state.get("reset_seq", component._reset_seq))


def dump_servicedesk_state(component: Any) -> Dict[str, Any]:
    return {
        "available": component is not None,
        "incidents": jsonable(getattr(component, "incidents", {})),
        "requests": jsonable(getattr(component, "requests", {})),
    }


def restore_servicedesk_state(component: Any, state: Dict[str, Any]) -> None:
    if not state.get("available", True):
        return
    component.incidents = jsonable(state.get("incidents", {}))
    component.requests = jsonable(state.get("requests", {}))


def dump_google_admin_state(component: Any) -> Dict[str, Any]:
    return {
        "available": component is not None,
        "oauth_apps": jsonable(getattr(component, "oauth_apps", {})),
        "drive_shares": jsonable(getattr(component, "drive_shares", {})),
    }


def restore_google_admin_state(component: Any, state: Dict[str, Any]) -> None:
    if not state.get("available", True):
        return
    component.oauth_apps = jsonable(state.get("oauth_apps", {}))
    component.drive_shares = jsonable(state.get("drive_shares", {}))


def dump_siem_state(component: Any) -> Dict[str, Any]:
    return {
        "available": component is not None,
        "alerts": jsonable(getattr(component, "alerts", {})),
        "cases": jsonable(getattr(component, "cases", {})),
        "case_seq": int(getattr(component, "_case_seq", 1)),
    }


def restore_siem_state(component: Any, state: Dict[str, Any]) -> None:
    if not state.get("available", True):
        return
    component.alerts = jsonable(state.get("alerts", {}))
    component.cases = jsonable(state.get("cases", {}))
    component._case_seq = int(state.get("case_seq", component._case_seq))


def dump_datadog_state(component: Any) -> Dict[str, Any]:
    return {
        "available": component is not None,
        "services": jsonable(getattr(component, "services", {})),
        "monitors": jsonable(getattr(component, "monitors", {})),
    }


def restore_datadog_state(component: Any, state: Dict[str, Any]) -> None:
    if not state.get("available", True):
        return
    component.services = jsonable(state.get("services", {}))
    component.monitors = jsonable(state.get("monitors", {}))


def dump_pagerduty_state(component: Any) -> Dict[str, Any]:
    return {
        "available": component is not None,
        "incidents": jsonable(getattr(component, "incidents", {})),
    }


def restore_pagerduty_state(component: Any, state: Dict[str, Any]) -> None:
    if not state.get("available", True):
        return
    component.incidents = jsonable(state.get("incidents", {}))


def dump_feature_flags_state(component: Any) -> Dict[str, Any]:
    return {
        "available": component is not None,
        "flags": jsonable(getattr(component, "flags", {})),
    }


def restore_feature_flags_state(component: Any, state: Dict[str, Any]) -> None:
    if not state.get("available", True):
        return
    component.flags = jsonable(state.get("flags", {}))


def dump_hris_state(component: Any) -> Dict[str, Any]:
    return {
        "available": component is not None,
        "employees": jsonable(getattr(component, "employees", {})),
    }


def restore_hris_state(component: Any, state: Dict[str, Any]) -> None:
    if not state.get("available", True):
        return
    component.employees = jsonable(state.get("employees", {}))
