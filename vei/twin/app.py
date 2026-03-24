from __future__ import annotations

from pathlib import Path

from .gateway import create_twin_gateway_app


def serve_customer_twin(
    root: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 3020,
) -> None:
    import uvicorn

    app = create_twin_gateway_app(root)
    uvicorn.run(app, host=host, port=port, log_level="warning")
