"""Tests for the projects CRUD routes."""

from __future__ import annotations

from pathlib import Path

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


class TestProjectCRUD:
    """Tests for project CRUD operations."""

    @pytest.mark.anyio()
    async def test_list_empty(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """List returns empty when no projects exist."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.get("/api/projects", headers=_headers)
        assert resp.status_code == 200
        assert resp.json() == []

    @pytest.mark.anyio()
    async def test_create_project(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Create a new project and verify it's returned."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.post(
                "/api/projects",
                json={"name": "test-proj", "content": "<bani/>"},
                headers=_headers,
            )
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "test-proj"
        assert data["content"] == "<bani/>"

    @pytest.mark.anyio()
    async def test_create_duplicate_returns_409(
        self, _server: BaniUIServer, _headers: dict[str, str], _projects_dir: str
    ) -> None:
        """Creating a project with an existing name returns 409."""
        Path(_projects_dir, "dup.bdl").write_text("<existing/>")
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.post(
                "/api/projects",
                json={"name": "dup", "content": "<new/>"},
                headers=_headers,
            )
        assert resp.status_code == 409

    @pytest.mark.anyio()
    async def test_get_project(
        self, _server: BaniUIServer, _headers: dict[str, str], _projects_dir: str
    ) -> None:
        """Read a project by name."""
        Path(_projects_dir, "myproj.bdl").write_text("<hello/>")
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.get("/api/projects/myproj", headers=_headers)
        assert resp.status_code == 200
        assert resp.json()["content"] == "<hello/>"

    @pytest.mark.anyio()
    async def test_get_not_found(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Get a non-existent project returns 404."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.get("/api/projects/nope", headers=_headers)
        assert resp.status_code == 404

    @pytest.mark.anyio()
    async def test_update_project(
        self, _server: BaniUIServer, _headers: dict[str, str], _projects_dir: str
    ) -> None:
        """Update an existing project's content."""
        Path(_projects_dir, "upd.bdl").write_text("<old/>")
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.put(
                "/api/projects/upd",
                json={"content": "<new/>"},
                headers=_headers,
            )
        assert resp.status_code == 200
        assert resp.json()["content"] == "<new/>"
        # Verify on disk
        assert Path(_projects_dir, "upd.bdl").read_text() == "<new/>"

    @pytest.mark.anyio()
    async def test_update_not_found(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Update a non-existent project returns 404."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.put(
                "/api/projects/nope",
                json={"content": "<x/>"},
                headers=_headers,
            )
        assert resp.status_code == 404

    @pytest.mark.anyio()
    async def test_delete_project(
        self, _server: BaniUIServer, _headers: dict[str, str], _projects_dir: str
    ) -> None:
        """Delete an existing project."""
        Path(_projects_dir, "del.bdl").write_text("<bye/>")
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.delete("/api/projects/del", headers=_headers)
        assert resp.status_code == 204
        assert not Path(_projects_dir, "del.bdl").exists()

    @pytest.mark.anyio()
    async def test_delete_not_found(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Delete a non-existent project returns 404."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.delete("/api/projects/nope", headers=_headers)
        assert resp.status_code == 404

    @pytest.mark.anyio()
    async def test_list_projects(
        self, _server: BaniUIServer, _headers: dict[str, str], _projects_dir: str
    ) -> None:
        """List returns all .bdl files in the projects directory."""
        Path(_projects_dir, "alpha.bdl").write_text("<a/>")
        Path(_projects_dir, "beta.bdl").write_text("<b/>")
        Path(_projects_dir, "not-bdl.txt").write_text("skip me")
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.get("/api/projects", headers=_headers)
        assert resp.status_code == 200
        names = [p["name"] for p in resp.json()]
        # Sorted newest first (beta created after alpha)
        assert names == ["beta", "alpha"]
