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

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from bani.ui.scheduler_registry import SchedulerRegistry
from bani.ui.sse import SSEBroadcaster, sse_progress_endpoint

logger = logging.getLogger(__name__)


def _init_oracle_thick_if_needed() -> None:
    """Scan connections.json for oracle_client_lib and init thick mode.

    Oracle thick mode must be activated before any thin mode connection
    is created. This is called at server startup so all subsequent
    Oracle connections use thick mode when Instant Client is available.
    """
    try:
        from bani.infra.connections import ConnectionRegistry

        for _key, conn in ConnectionRegistry.load().items():
            if conn.connector != "oracle":
                continue
            for opt_key, opt_val in conn.options:
                if opt_key == "oracle_client_lib" and opt_val:
                    from bani.connectors.oracle.connector import (
                        _init_thick_mode,
                    )

                    _init_thick_mode(opt_val)
                    return  # Only need to init once
    except Exception as exc:
        logger.warning("Oracle thick mode init skipped: %s", exc)


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
        import os

        self.host = host
        self.port = port
        self.auth_token = os.environ.get("BANI_AUTH_TOKEN") or secrets.token_urlsafe(32)
        self.projects_dir = projects_dir
        self.app = self._create_app()

    def _create_app(self) -> FastAPI:
        """Build and configure the FastAPI application.

        Returns:
            A fully-configured FastAPI instance.
        """
        projects_dir = self.projects_dir

        @asynccontextmanager
        async def lifespan(app: FastAPI) -> AsyncIterator[None]:
            # Pre-init Oracle thick mode if any connection needs it.
            # Must happen before any Oracle thin mode connection is made.
            _init_oracle_thick_if_needed()

            # Startup: start scheduler registry for cron-enabled projects
            registry = SchedulerRegistry(projects_dir)
            app.state.scheduler_registry = registry
            registry.scan_and_start_all()

            yield

            # Shutdown: stop all schedulers then cancel running migration
            registry.stop_all()
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
        app.state.migration_state = {
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
        from bani.ui.routes import (
            connections,
            connectors,
            migration,
            projects,
            schema,
            settings,
        )

        app.include_router(connections.router, prefix="/api")
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
            repo_root / "ui" / "dist",  # development layout
            package_dir / "dist",  # bundled layout
        ]
        dist_dir = next((d for d in candidates if d.is_dir()), None)

        if dist_dir is not None:
            from fastapi.responses import FileResponse
            from fastapi.staticfiles import StaticFiles

            index_html = dist_dir / "index.html"

            # Serve static assets (JS, CSS, images) from dist/assets/
            app.mount(
                "/assets",
                StaticFiles(directory=str(dist_dir / "assets")),
                name="assets",
            )

            # Catch-all: serve index.html for any non-API path so that
            # React Router handles client-side routing on refresh.
            @app.get(
                "/{full_path:path}",
                response_class=FileResponse,
                include_in_schema=False,
            )
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

        Prints the URL and auth token to stdout.  On localhost, opens the
        browser automatically with the token in the URL so the user
        doesn't need to copy-paste it.
        """
        # Use localhost for display — 0.0.0.0 is not a valid browser URL
        display_host = "localhost" if self.host == "0.0.0.0" else self.host
        url = f"http://{display_host}:{self.port}"
        token_url = f"{url}?token={self.auth_token}"

        print(f"Bani UI: {token_url}")
        print(f"API docs: {url}/docs")
        print(f"Token: {self.auth_token}")

        # Auto-open browser on localhost
        if self.host in ("127.0.0.1", "localhost", "0.0.0.0"):
            import webbrowser

            # Delay slightly so the server is ready when the browser opens
            timer = threading.Timer(1.5, webbrowser.open, args=[token_url])
            timer.daemon = True
            timer.start()

        uvicorn.run(self.app, host=self.host, port=self.port)
