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
        async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
            resp = await ac.post(
                "/api/migrate",
                json={"project_name": "nonexistent"},
                headers=_headers,
            )
        assert resp.status_code == 404

    @pytest.mark.anyio()
    async def test_start_migration_returns_202(
        self,
        _server: BaniUIServer,
        _headers: dict[str, str],
        _projects_dir: str,
    ) -> None:
        """Start a migration returns 202 Accepted immediately."""
        # Create a minimal BDL project file
        Path(_projects_dir, "test.bdl").write_text("<bani/>")

        # Patch threading.Thread so the background thread doesn't actually run
        with patch("bani.ui.routes.migration.threading") as mock_threading:
            mock_thread = MagicMock()
            mock_threading.Thread.return_value = mock_thread
            mock_threading.Event = MagicMock

            transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
            async with AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as ac:
                resp = await ac.post(
                    "/api/migrate",
                    json={"project_name": "test"},
                    headers=_headers,
                )

        assert resp.status_code == 202
        data = resp.json()
        assert data["status"] == "started"
        assert data["project_name"] == "test"
        mock_thread.start.assert_called_once()

    @pytest.mark.anyio()
    async def test_validate_migration_project_not_found(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Validating a non-existent project returns 404."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
            resp = await ac.post(
                "/api/migrate/validate",
                json={"project_name": "nonexistent"},
                headers=_headers,
            )
        assert resp.status_code == 404

    @pytest.mark.anyio()
    async def test_get_status_idle(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Status returns idle when no migration is running."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
            resp = await ac.get("/api/migrate/status", headers=_headers)
        assert resp.status_code == 200
        assert resp.json()["running"] is False

    @pytest.mark.anyio()
    async def test_cancel_no_migration_returns_409(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Cancel when no migration is running returns 409."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
            resp = await ac.post("/api/migrate/cancel", headers=_headers)
        assert resp.status_code == 409
