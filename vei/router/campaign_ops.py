from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from vei.world.api import Scenario

from .errors import MCPError
from .tool_providers import PrefixToolProvider
from .tool_registry import ToolSpec


def _normalize_records(value: Any, key_field: str) -> Dict[str, Dict[str, Any]]:
    if isinstance(value, dict):
        return {str(key): dict(payload) for key, payload in value.items()}
    records: Dict[str, Dict[str, Any]] = {}
    for item in value or []:
        payload = dict(item)
        record_id = str(payload.get(key_field) or payload.get("id") or "")
        if record_id:
            records[record_id] = payload
    return records


class CampaignOpsSim:
    """Deterministic campaign-operations twin for agency demos."""

    def __init__(self, scenario: Optional[Scenario] = None):
        seed = (scenario.campaign_graph or {}) if scenario else {}
        self.clients = _normalize_records(seed.get("clients"), "client_id")
        self.campaigns = _normalize_records(seed.get("campaigns"), "campaign_id")
        self.creatives = _normalize_records(seed.get("creatives"), "creative_id")
        self.approvals = _normalize_records(seed.get("approvals"), "approval_id")
        self.reports = _normalize_records(seed.get("reports"), "report_id")

    def export_state(self) -> Dict[str, Any]:
        return {
            "clients": self.clients,
            "campaigns": self.campaigns,
            "creatives": self.creatives,
            "approvals": self.approvals,
            "reports": self.reports,
        }

    def import_state(self, state: Dict[str, Any]) -> None:
        self.clients = _normalize_records(state.get("clients"), "client_id")
        self.campaigns = _normalize_records(state.get("campaigns"), "campaign_id")
        self.creatives = _normalize_records(state.get("creatives"), "creative_id")
        self.approvals = _normalize_records(state.get("approvals"), "approval_id")
        self.reports = _normalize_records(state.get("reports"), "report_id")

    def summary(self) -> str:
        return (
            f"{len(self.campaigns)} campaigns, {len(self.creatives)} creatives, "
            f"{len(self.approvals)} approvals, {len(self.reports)} reports"
        )

    def action_menu(self) -> List[Dict[str, Any]]:
        return [
            {"tool": "campaign.approve_creative", "label": "Approve Creative"},
            {"tool": "campaign.adjust_budget_pacing", "label": "Adjust Budget Pacing"},
            {"tool": "campaign.pause_channel_launch", "label": "Pause Launch"},
            {"tool": "campaign.publish_report_note", "label": "Publish Report Note"},
        ]

    def list_overview(self) -> Dict[str, Any]:
        return {
            "campaigns": list(self.campaigns.values()),
            "creatives": list(self.creatives.values()),
            "approvals": list(self.approvals.values()),
            "reports": list(self.reports.values()),
        }

    def approve_creative(self, creative_id: str, approval_id: str) -> Dict[str, Any]:
        creative = self._require(self.creatives, creative_id, "creative")
        approval = self._require(self.approvals, approval_id, "approval")
        creative["status"] = "approved"
        approval["status"] = "approved"
        return {
            "creative_id": creative_id,
            "approval_id": approval_id,
            "creative_status": "approved",
        }

    def adjust_budget_pacing(
        self, campaign_id: str, pacing_pct: float, note: Optional[str] = None
    ) -> Dict[str, Any]:
        campaign = self._require(self.campaigns, campaign_id, "campaign")
        campaign["pacing_pct"] = float(pacing_pct)
        if note:
            campaign["note"] = note
        return {"campaign_id": campaign_id, "pacing_pct": float(pacing_pct)}

    def pause_channel_launch(
        self, campaign_id: str, reason: Optional[str] = None
    ) -> Dict[str, Any]:
        campaign = self._require(self.campaigns, campaign_id, "campaign")
        campaign["status"] = "paused"
        if reason:
            campaign["pause_reason"] = reason
        return {"campaign_id": campaign_id, "status": "paused"}

    def publish_report_note(self, report_id: str, note: str) -> Dict[str, Any]:
        report = self._require(self.reports, report_id, "report")
        report["status"] = "refreshed"
        report["stale"] = False
        report["note"] = note
        return {"report_id": report_id, "status": "refreshed"}

    @staticmethod
    def _require(
        store: Dict[str, Dict[str, Any]], key: str, kind: str
    ) -> Dict[str, Any]:
        if key not in store:
            raise MCPError("campaign.not_found", f"Unknown {kind}: {key}")
        return store[key]


class CampaignOpsToolProvider(PrefixToolProvider):
    def __init__(self, sim: CampaignOpsSim):
        super().__init__("campaign", prefixes=("campaign.",))
        self.sim = sim
        self._specs: List[ToolSpec] = [
            ToolSpec(
                name="campaign.list_overview",
                description="List campaign execution overview records.",
                permissions=("campaign:read",),
                default_latency_ms=180,
                latency_jitter_ms=40,
            ),
            ToolSpec(
                name="campaign.approve_creative",
                description="Approve a pending creative and its approval record.",
                permissions=("campaign:write",),
                side_effects=("campaign_mutation",),
                default_latency_ms=230,
                latency_jitter_ms=60,
            ),
            ToolSpec(
                name="campaign.adjust_budget_pacing",
                description="Adjust budget pacing for a campaign.",
                permissions=("campaign:write",),
                side_effects=("campaign_mutation",),
                default_latency_ms=230,
                latency_jitter_ms=60,
            ),
            ToolSpec(
                name="campaign.pause_channel_launch",
                description="Pause a campaign launch before budget burns further.",
                permissions=("campaign:write",),
                side_effects=("campaign_mutation",),
                default_latency_ms=210,
                latency_jitter_ms=50,
            ),
            ToolSpec(
                name="campaign.publish_report_note",
                description="Refresh or annotate a campaign report artifact.",
                permissions=("campaign:write",),
                side_effects=("campaign_mutation",),
                default_latency_ms=190,
                latency_jitter_ms=40,
            ),
        ]
        self._handlers: Dict[str, Callable[..., Any]] = {
            "campaign.list_overview": self.sim.list_overview,
            "campaign.approve_creative": self.sim.approve_creative,
            "campaign.adjust_budget_pacing": self.sim.adjust_budget_pacing,
            "campaign.pause_channel_launch": self.sim.pause_channel_launch,
            "campaign.publish_report_note": self.sim.publish_report_note,
        }

    def specs(self) -> List[ToolSpec]:
        return list(self._specs)

    def call(self, tool: str, args: Dict[str, Any]) -> Any:
        handler = self._handlers.get(tool)
        if handler is None:
            raise MCPError("unknown_tool", f"No such tool: {tool}")
        try:
            return handler(**(args or {}))
        except TypeError as exc:
            raise MCPError("invalid_args", str(exc)) from exc
