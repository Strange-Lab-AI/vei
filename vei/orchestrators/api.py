from __future__ import annotations

from typing import Protocol

from ._paperclip import (
    PaperclipOrchestratorClient,
    external_approval_id_for,
    external_agent_id_for,
    external_task_id_for,
    normalize_approval_id,
    normalize_agent_id,
    normalize_task_id,
)
from .models import (
    OrchestratorActivityItem,
    OrchestratorAgent,
    OrchestratorApproval,
    OrchestratorBudgetSummary,
    OrchestratorComment,
    OrchestratorCommandResult,
    OrchestratorConfig,
    OrchestratorIntegrationMode,
    OrchestratorProviderId,
    OrchestratorSnapshot,
    OrchestratorSummary,
    OrchestratorSyncCapabilities,
    OrchestratorSyncHealth,
    OrchestratorTask,
)


class OrchestratorClient(Protocol):
    def fetch_snapshot(self) -> OrchestratorSnapshot: ...

    def pause_agent(self, agent_id: str) -> OrchestratorCommandResult: ...

    def resume_agent(self, agent_id: str) -> OrchestratorCommandResult: ...

    def comment_on_task(self, task_id: str, body: str) -> OrchestratorCommandResult: ...

    def approve_approval(
        self,
        approval_id: str,
        *,
        decision_note: str | None = None,
    ) -> OrchestratorCommandResult: ...

    def reject_approval(
        self,
        approval_id: str,
        *,
        decision_note: str | None = None,
    ) -> OrchestratorCommandResult: ...

    def request_approval_revision(
        self,
        approval_id: str,
        *,
        decision_note: str | None = None,
    ) -> OrchestratorCommandResult: ...

    def sync_capabilities(self) -> OrchestratorSyncCapabilities: ...


def build_orchestrator_client(config: OrchestratorConfig) -> OrchestratorClient:
    if config.provider == "paperclip":
        return PaperclipOrchestratorClient(config)
    raise ValueError(f"unsupported orchestrator provider: {config.provider}")


__all__ = [
    "OrchestratorActivityItem",
    "OrchestratorAgent",
    "OrchestratorApproval",
    "OrchestratorBudgetSummary",
    "OrchestratorComment",
    "OrchestratorClient",
    "OrchestratorCommandResult",
    "OrchestratorConfig",
    "OrchestratorIntegrationMode",
    "OrchestratorProviderId",
    "OrchestratorSnapshot",
    "OrchestratorSummary",
    "OrchestratorSyncCapabilities",
    "OrchestratorSyncHealth",
    "OrchestratorTask",
    "PaperclipOrchestratorClient",
    "build_orchestrator_client",
    "external_approval_id_for",
    "external_agent_id_for",
    "external_task_id_for",
    "normalize_approval_id",
    "normalize_agent_id",
    "normalize_task_id",
]
