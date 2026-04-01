from __future__ import annotations

from pathlib import Path

import typer

app = typer.Typer(add_completion=False, help="Serve the local VEI playback UI.")


@app.command("serve")
def serve(
    root: Path = typer.Option(Path("."), help="Workspace root directory"),
    host: str = typer.Option("127.0.0.1", help="Bind host"),
    port: int = typer.Option(3010, help="Bind port"),
) -> None:
    """Serve the local VEI playback UI for one workspace."""

    from vei.ui.app import serve_ui

    serve_ui(root, host=host, port=port)


if __name__ == "__main__":
    app()
