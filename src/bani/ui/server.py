"""Main FastAPI application for the Bani Web UI (Section 20).

Creates a FastAPI server that:
1. Serves REST endpoints calling the same application layer as CLI/SDK/MCP.
2. Streams real-time progress via WebSocket.
3. Serves the built React SPA as static files from ``ui/dist/``.
4. Requires a local auth token for all API endpoints.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from bani.ui.websocket import progress_websocket_handler

logger = logging.getLogger(__name__)


class BaniUIServer:
    """Wraps a FastAPI application for the Bani Web UI.

    Attributes:
        host: Bind address (default ``127.0.0.1`` for local-only access).
        port: Listen port (default ``8910``).
        auth_token: Randomly-generated Bearer token required for API access.
        app: The configured FastAPI application.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8910,
        projects_dir: str = "~/.bani/projects",
    ) -> None:
        """Initialise the server.

        Args:
            host: Bind address.
            port: Listen port.
            projects_dir: Directory for storing BDL project files.
        """
        self.host = host
        self.port = port
        self.auth_token = secrets.token_urlsafe(32)
        self.projects_dir = projects_dir
        self.app = self._create_app()

    def _create_app(self) -> FastAPI:
        """Build and configure the FastAPI application.

        Returns:
            A fully-configured FastAPI instance.
        """
        app = FastAPI(
            title="Bani",
            version="0.1.0",
            description="Bani Web UI backend",
        )

        # Store auth token and shared state on app.state
        app.state.auth_token = self.auth_token
        app.state.projects_dir = self.projects_dir
        app.state.migration_state: dict[str, Any] = {
            "running": False,
            "project_name": None,
            "tables_completed": 0,
            "tables_failed": 0,
            "total_rows_read": 0,
            "total_rows_written": 0,
            "error": None,
        }
        app.state.ws_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=1000
        )

        # CORS — allow same-origin only for production,
        # but allow all origins for local development.
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Health check (no auth required)
        @app.get("/api/health")
        async def health() -> dict[str, str]:
            return {"status": "ok"}

        # Mount API routes
        from bani.ui.routes import connectors, migration, projects, schema, settings

        app.include_router(projects.router, prefix="/api")
        app.include_router(migration.router, prefix="/api")
        app.include_router(schema.router, prefix="/api")
        app.include_router(connectors.router, prefix="/api")
        app.include_router(settings.router, prefix="/api")

        # WebSocket endpoint for progress streaming
        @app.websocket("/ws/progress")
        async def progress_ws(websocket: WebSocket) -> None:
            ws_queue: asyncio.Queue[dict[str, Any]] = app.state.ws_queue
            await progress_websocket_handler(websocket, ws_queue)

        # Serve static files (React SPA) — mounted LAST so API routes
        # take precedence.  If the dist directory doesn't exist, we skip
        # static file serving gracefully.
        self._mount_static(app)

        return app

    @staticmethod
    def _mount_static(app: FastAPI) -> None:
        """Attempt to mount the React SPA build directory as static files.

        Falls back gracefully if ``ui/dist/`` does not exist.

        Args:
            app: The FastAPI application.
        """
        # Look for ui/dist/ relative to the bani package
        package_dir = Path(__file__).resolve().parent
        dist_dir = package_dir / "dist"

        if dist_dir.is_dir():
            from fastapi.staticfiles import StaticFiles

            app.mount("/", StaticFiles(directory=str(dist_dir), html=True))
            logger.info("Serving SPA from %s", dist_dir)
        else:
            logger.info(
                "No ui/dist/ directory found at %s — SPA not served", dist_dir
            )

            # Provide a fallback root handler so the user gets a helpful message
            @app.get("/")
            async def root() -> JSONResponse:
                return JSONResponse(
                    content={
                        "message": "Bani API is running. "
                        "Build the React SPA into ui/dist/ to enable the Web UI.",
                        "api_docs": "/docs",
                    }
                )

    def start(self) -> None:
        """Start the server.

        Prints the URL and auth token to stdout, then blocks on
        ``uvicorn.run()``.
        """
        print(f"Bani UI: http://{self.host}:{self.port}")  # noqa: T201
        print(f"Auth token: {self.auth_token}")  # noqa: T201
        print(f"API docs: http://{self.host}:{self.port}/docs")  # noqa: T201
        uvicorn.run(self.app, host=self.host, port=self.port)
