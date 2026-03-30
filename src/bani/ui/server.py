"""Main FastAPI application for the Bani Web UI (Section 20).

Creates a FastAPI server that:
1. Serves REST endpoints calling the same application layer as CLI/SDK/MCP.
2. Streams real-time progress via Server-Sent Events (SSE).
3. Serves the built React SPA as static files from ``ui/dist/``.
4. Requires a local auth token for all API endpoints.
"""

from __future__ import annotations

import logging
import secrets
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from bani.ui.sse import SSEBroadcaster, sse_progress_endpoint

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
        @asynccontextmanager
        async def lifespan(app: FastAPI) -> AsyncIterator[None]:
            yield
            # Shutdown: cancel any running migration gracefully
            state = app.state.migration_state
            cancel_event = state.get("cancel_event")
            if isinstance(cancel_event, threading.Event) and state.get("running"):
                logger.info("Server shutting down — cancelling running migration")
                cancel_event.set()

        app = FastAPI(
            title="Bani",
            version="0.1.0",
            description="Bani Web UI backend",
            lifespan=lifespan,
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
        app.state.sse_broadcaster = SSEBroadcaster()

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

        # SSE endpoint for real-time progress streaming
        app.get("/api/migrate/progress")(sse_progress_endpoint)

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
        # Look for ui/dist/ in several locations:
        # 1. Repo root: <repo>/ui/dist/ (development)
        # 2. Relative to package: <package>/dist/ (bundled install)
        package_dir = Path(__file__).resolve().parent
        repo_root = package_dir.parent.parent.parent  # src/bani/ui -> repo root
        candidates = [
            repo_root / "ui" / "dist",   # development layout
            package_dir / "dist",         # bundled layout
        ]
        dist_dir = next((d for d in candidates if d.is_dir()), None)

        if dist_dir is not None:
            from fastapi.staticfiles import StaticFiles
            from fastapi.responses import FileResponse

            index_html = dist_dir / "index.html"

            # Serve static assets (JS, CSS, images) from dist/assets/
            app.mount(
                "/assets",
                StaticFiles(directory=str(dist_dir / "assets")),
                name="assets",
            )

            # Catch-all: serve index.html for any non-API path so that
            # React Router handles client-side routing on refresh.
            @app.get("/{full_path:path}")
            async def spa_fallback(full_path: str) -> FileResponse:
                return FileResponse(str(index_html))

            logger.info("Serving SPA from %s", dist_dir)
        else:
            logger.info("No ui/dist/ directory found — SPA not served")

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
