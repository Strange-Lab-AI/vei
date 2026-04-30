from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI

from vei.ingest.api import agent_activity_ingest_status
from vei.provenance.api import (
    build_activity_graph,
    inspect_timeline,
    load_workspace_events,
)


def register_provenance_routes(app: FastAPI, workspace_root: Path) -> None:
    @app.get("/api/workspace/provenance/control")
    def provenance_control() -> dict:
        events = load_workspace_events(workspace_root)
        timeline = inspect_timeline(events)
        graph = build_activity_graph(events)
        return {
            "available": True,
            "event_count": timeline.event_count,
            "ingest": agent_activity_ingest_status(str(workspace_root)),
            "timeline": [item.model_dump(mode="json") for item in timeline.items[-20:]],
            "graph": graph.model_dump(mode="json"),
            "warnings": timeline.warnings + graph.warnings,
        }
