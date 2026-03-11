from __future__ import annotations

from importlib import import_module

__all__ = [
    "CommGraphChannelView",
    "CommGraphView",
    "CapabilityGraphActionInput",
    "CapabilityGraphActionResult",
    "CapabilityGraphActionSchema",
    "CapabilityGraphPlan",
    "CapabilityGraphPlanStep",
    "DataGraphView",
    "DataWorkbookView",
    "DocGraphView",
    "DocumentView",
    "DriveShareView",
    "HrisEmployeeView",
    "IdentityApplicationView",
    "IdentityGraphView",
    "IdentityGroupView",
    "IdentityPolicyView",
    "IdentityUserView",
    "ObsGraphView",
    "ObsIncidentView",
    "ObsMonitorView",
    "ObsServiceView",
    "OpsFlagView",
    "OpsGraphView",
    "RevenueCompanyView",
    "RevenueContactView",
    "RevenueDealView",
    "RevenueGraphView",
    "RuntimeCapabilityGraphs",
    "ServiceRequestView",
    "WorkGraphView",
    "WorkItemView",
    "build_graph_action_plan",
    "build_runtime_capability_graphs",
    "get_runtime_capability_graph",
    "resolve_graph_action",
]


def __getattr__(name: str):  # pragma: no cover - thin import facade
    if name in {
        "build_graph_action_plan",
        "build_runtime_capability_graphs",
        "get_runtime_capability_graph",
        "resolve_graph_action",
    }:
        module = import_module("vei.capability_graph.api")
        return getattr(module, name)
    module = import_module("vei.capability_graph.models")
    if hasattr(module, name):
        return getattr(module, name)
    raise AttributeError(name)
