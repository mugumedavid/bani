"""Tests for the migration routes."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from bani.ui.server import BaniUIServer


@pytest.fixture()
def _projects_dir(tmp_path: Path) -> str:
    d = tmp_path / "projects"
    d.mkdir()
    return str(d)


@pytest.fixture()
def _server(_projects_dir: str) -> BaniUIServer:
    return BaniUIServer(projects_dir=_projects_dir)


@pytest.fixture()
def _headers(_server: BaniUIServer) -> dict[str, str]:
    return {"Authorization": f"Bearer {_server.auth_token}"}


class TestMigrationRoutes:
    """Tests for migration start, status, and cancel endpoints."""

    @pytest.mark.anyio()
    async def test_start_migration_project_not_found(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Starting a migration for a non-existent project returns 404."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.post(
                "/api/migrate",
                json={"project_name": "nonexistent"},
                headers=_headers,
            )
        assert resp.status_code == 404

    @pytest.mark.anyio()
    async def test_start_migration_mocked(
        self,
        _server: BaniUIServer,
        _headers: dict[str, str],
        _projects_dir: str,
    ) -> None:
        """Start a migration with a mocked orchestrator."""
        # Create a minimal BDL project file
        Path(_projects_dir, "test.bdl").write_text("<bani/>")

        mock_result = MagicMock()
        mock_result.project_name = "test"
        mock_result.tables_completed = 5
        mock_result.tables_failed = 0
        mock_result.total_rows_read = 1000
        mock_result.total_rows_written = 1000
        mock_result.duration_seconds = 2.5
        mock_result.errors = ()

        mock_project = MagicMock()
        mock_project.run.return_value = mock_result

        with patch("bani.ui.routes.migration.asyncio") as mock_asyncio:
            # Make to_thread call the function directly
            async def fake_to_thread(fn: object, *args: object) -> object:
                # We need to mock Bani.load inside the function
                with patch("bani.sdk.bani.Bani.load", return_value=mock_project):
                    return fn()  # type: ignore[operator]

            mock_asyncio.to_thread = fake_to_thread
            mock_asyncio.QueueFull = type("QueueFull", (Exception,), {})

            transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
            async with AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as ac:
                resp = await ac.post(
                    "/api/migrate",
                    json={"project_name": "test"},
                    headers=_headers,
                )

        assert resp.status_code == 200
        data = resp.json()
        assert data["project_name"] == "test"
        assert data["tables_completed"] == 5

    @pytest.mark.anyio()
    async def test_get_status_idle(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Status returns idle when no migration is running."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.get("/api/migrate/status", headers=_headers)
        assert resp.status_code == 200
        assert resp.json()["running"] is False

    @pytest.mark.anyio()
    async def test_cancel_no_migration_returns_409(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Cancel when no migration is running returns 409."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.post("/api/migrate/cancel", headers=_headers)
        assert resp.status_code == 409
