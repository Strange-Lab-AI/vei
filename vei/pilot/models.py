from __future__ import annotations

from vei.twin.models import (
    TwinActivityItem,
    TwinLaunchManifest,
    TwinLaunchRuntime,
    TwinLaunchSnippet,
    TwinLaunchStatus,
    TwinOutcomeSummary,
    TwinServiceName,
    TwinServiceRecord,
    TwinServiceState,
)

PilotServiceName = TwinServiceName
PilotServiceState = TwinServiceState
PilotSnippet = TwinLaunchSnippet
PilotServiceRecord = TwinServiceRecord
PilotManifest = TwinLaunchManifest
PilotRuntime = TwinLaunchRuntime
PilotActivityItem = TwinActivityItem
PilotOutcomeSummary = TwinOutcomeSummary
PilotStatus = TwinLaunchStatus

__all__ = [
    "PilotActivityItem",
    "PilotManifest",
    "PilotOutcomeSummary",
    "PilotRuntime",
    "PilotServiceName",
    "PilotServiceRecord",
    "PilotServiceState",
    "PilotSnippet",
    "PilotStatus",
]
