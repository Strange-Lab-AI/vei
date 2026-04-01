from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "__version__",
    "CampaignOpsSim",
    "CampaignOpsToolProvider",
    "Event",
    "EventBus",
    "InventoryOpsSim",
    "InventoryOpsToolProvider",
    "LinearCongruentialGenerator",
    "PropertyOpsSim",
    "PropertyOpsToolProvider",
    "Router",
    "ServiceOpsSim",
    "ServiceOpsToolProvider",
    "SpreadsheetSim",
    "SpreadsheetToolProvider",
]

__version__ = "0.2.0a1"


def __getattr__(name: str) -> Any:  # pragma: no cover - thin import facade
    if name == "Router":
        module = import_module("vei.router.core")
        return getattr(module, name)
    if name in {"Event", "EventBus", "LinearCongruentialGenerator"}:
        module = import_module("vei.router._event_bus")
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
