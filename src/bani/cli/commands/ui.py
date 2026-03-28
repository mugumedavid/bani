"""UI command — launch the Bani Web UI server (Section 20.5)."""

from __future__ import annotations

import typer


def ui(
    host: str = typer.Option(
        "127.0.0.1",
        "--host",
        help="Bind address. Use 0.0.0.0 to listen on all interfaces.",
    ),
    port: int = typer.Option(
        8910,
        "--port",
        help="Listen port.",
    ),
    projects_dir: str = typer.Option(
        "~/.bani/projects",
        "--projects-dir",
        help="Directory for BDL project files.",
    ),
) -> None:
    """Launch the Bani Web UI (FastAPI backend + React SPA)."""
    from bani.ui.server import BaniUIServer

    server = BaniUIServer(host=host, port=port, projects_dir=projects_dir)
    server.start()
