"""Tests for the connectors routes."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from bani.ui.server import BaniUIServer


@pytest.fixture()
def _server() -> BaniUIServer:
    return BaniUIServer()


@pytest.fixture()
def _headers(_server: BaniUIServer) -> dict[str, str]:
    return {"Authorization": f"Bearer {_server.auth_token}"}


class _FakeConnector:
    """Minimal fake connector class for testing."""

    __name__ = "FakeConnector"
    __module__ = "tests.fake"


class TestConnectorRoutes:
    """Tests for connector listing and detail endpoints."""

    @pytest.mark.anyio()
    async def test_list_connectors(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """List connectors returns discovered connectors."""
        fake_registry = {"sqlite": _FakeConnector, "mysql": _FakeConnector}
        with patch(
            "bani.connectors.registry.ConnectorRegistry.discover",
            return_value=fake_registry,
        ):
            transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
            async with AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as ac:
                resp = await ac.get("/api/connectors", headers=_headers)

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        names = [c["name"] for c in data]
        assert "mysql" in names
        assert "sqlite" in names

    @pytest.mark.anyio()
    async def test_get_connector(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Get a specific connector by name."""
        fake_registry = {"postgresql": _FakeConnector}
        with patch(
            "bani.connectors.registry.ConnectorRegistry.discover",
            return_value=fake_registry,
        ):
            transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
            async with AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as ac:
                resp = await ac.get("/api/connectors/postgresql", headers=_headers)

        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "postgresql"
        assert data["class_name"] == "_FakeConnector"

    @pytest.mark.anyio()
    async def test_get_unknown_connector_returns_404(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Get a non-existent connector returns 404."""
        with patch(
            "bani.connectors.registry.ConnectorRegistry.discover",
            return_value={},
        ):
            transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
            async with AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as ac:
                resp = await ac.get("/api/connectors/nonexistent", headers=_headers)

        assert resp.status_code == 404
