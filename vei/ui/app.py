from __future__ import annotations

from pathlib import Path

from .api import create_ui_app


def serve_ui(
    workspace_root: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 3010,
    skin: str = "studio",
) -> None:
    import uvicorn

    app = create_ui_app(workspace_root, skin=skin)
    uvicorn.run(app, host=host, port=port, log_level="warning")
