from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "BrowserVirtual",
    "CalendarSim",
    "__version__",
    "CrmSim",
    "DatabaseSim",
    "DatadogSim",
    "DatadogToolProvider",
    "DocsSim",
    "CampaignOpsSim",
    "CampaignOpsToolProvider",
    "ErpSim",
    "BusEvent",
    "EventBus",
    "FeatureFlagSim",
    "FeatureFlagToolProvider",
    "GoogleAdminSim",
    "GoogleAdminToolProvider",
    "HrisSim",
    "HrisToolProvider",
    "InventoryOpsSim",
    "InventoryOpsToolProvider",
    "JiraToolProvider",
    "KnowledgeSim",
    "KnowledgeToolProvider",
    "LinearCongruentialGenerator",
    "MailSim",
    "NotesSim",
    "NotesToolProvider",
    "OktaSim",
    "OktaToolProvider",
    "PagerDutySim",
    "PagerDutyToolProvider",
    "PropertyOpsSim",
    "PropertyOpsToolProvider",
    "Router",
    "ServiceOpsSim",
    "ServiceOpsToolProvider",
    "ServiceDeskSim",
    "ServiceDeskToolProvider",
    "SiemSim",
    "SiemToolProvider",
    "SlackSim",
    "SpreadsheetSim",
    "SpreadsheetToolProvider",
    "TicketsSim",
]

__version__ = "0.2.0a1"


def __getattr__(name: str) -> Any:  # pragma: no cover - thin import facade
    if name == "Router":
        module = import_module("vei.router.core")
        return getattr(module, name)
    if name in {"BusEvent", "EventBus", "LinearCongruentialGenerator"}:
        module = import_module("vei.router._event_bus")
        return getattr(module, name)
    if name in {"BrowserVirtual", "MailSim", "SlackSim"}:
        module = import_module("vei.router.sims")
        return getattr(module, name)
    if name == "DocsSim":
        module = import_module("vei.router.docs")
        return getattr(module, name)
    if name == "CalendarSim":
        module = import_module("vei.router.calendar")
        return getattr(module, name)
    if name in {"TicketsSim", "JiraToolProvider"}:
        module = import_module(
            "vei.router.jira" if name == "JiraToolProvider" else "vei.router.tickets"
        )
        return getattr(module, name)
    if name == "DatabaseSim":
        module = import_module("vei.router.database")
        return getattr(module, name)
    if name == "ErpSim":
        module = import_module("vei.router.erp")
        return getattr(module, name)
    if name == "CrmSim":
        module = import_module("vei.router.crm")
        return getattr(module, name)
    if name in {"OktaSim", "OktaToolProvider"}:
        module = import_module("vei.router.identity")
        return getattr(module, name)
    if name in {"ServiceDeskSim", "ServiceDeskToolProvider"}:
        module = import_module("vei.router.servicedesk")
        return getattr(module, name)
    if name in {"GoogleAdminSim", "GoogleAdminToolProvider"}:
        module = import_module("vei.router.google_admin")
        return getattr(module, name)
    if name in {"SiemSim", "SiemToolProvider"}:
        module = import_module("vei.router.siem")
        return getattr(module, name)
    if name in {"DatadogSim", "DatadogToolProvider"}:
        module = import_module("vei.router.datadog")
        return getattr(module, name)
    if name in {"PagerDutySim", "PagerDutyToolProvider"}:
        module = import_module("vei.router.pagerduty")
        return getattr(module, name)
    if name in {"FeatureFlagSim", "FeatureFlagToolProvider"}:
        module = import_module("vei.router.feature_flags")
        return getattr(module, name)
    if name in {"HrisSim", "HrisToolProvider"}:
        module = import_module("vei.router.hris")
        return getattr(module, name)
    if name in {"NotesSim", "NotesToolProvider"}:
        module = import_module("vei.router.notes")
        return getattr(module, name)
    if name in {"KnowledgeSim", "KnowledgeToolProvider"}:
        module = import_module("vei.router.knowledge")
        return getattr(module, name)
    if name in {"SpreadsheetSim", "SpreadsheetToolProvider"}:
        module = import_module("vei.router.spreadsheet")
        return getattr(module, name)
    if name in {"PropertyOpsSim", "PropertyOpsToolProvider"}:
        module = import_module("vei.router.property_ops")
        return getattr(module, name)
    if name in {"CampaignOpsSim", "CampaignOpsToolProvider"}:
        module = import_module("vei.router.campaign_ops")
        return getattr(module, name)
    if name in {"InventoryOpsSim", "InventoryOpsToolProvider"}:
        module = import_module("vei.router.inventory_ops")
        return getattr(module, name)
    if name in {"ServiceOpsSim", "ServiceOpsToolProvider"}:
        module = import_module("vei.router.service_ops")
        return getattr(module, name)
    raise AttributeError(name)
