from __future__ import annotations

from pathlib import Path

from .api import create_ui_app

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency fallback

    def load_dotenv(*args: object, **kwargs: object) -> None:
        return None


def serve_ui(
    workspace_root: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 3010,
) -> None:
    import uvicorn

    load_dotenv(override=False)
    app = create_ui_app(workspace_root)
    uvicorn.run(app, host=host, port=port, log_level="warning")
