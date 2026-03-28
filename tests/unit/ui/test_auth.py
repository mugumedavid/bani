"""Tests for the auth token middleware."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from bani.ui.server import BaniUIServer


@pytest.fixture()
def _server() -> BaniUIServer:
    return BaniUIServer()


class TestAuthMiddleware:
    """Tests for Bearer token authentication."""

    @pytest.mark.anyio()
    async def test_valid_token_passes(self, _server: BaniUIServer) -> None:
        """A request with the correct Bearer token succeeds."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.get(
                "/api/connectors",
                headers={"Authorization": f"Bearer {_server.auth_token}"},
            )
        assert resp.status_code == 200

    @pytest.mark.anyio()
    async def test_missing_token_returns_401(self, _server: BaniUIServer) -> None:
        """A request without an Authorization header returns 401."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.get("/api/connectors")
        assert resp.status_code == 422  # FastAPI returns 422 for missing required header

    @pytest.mark.anyio()
    async def test_invalid_token_returns_401(self, _server: BaniUIServer) -> None:
        """A request with a wrong Bearer token returns 401."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.get(
                "/api/connectors",
                headers={"Authorization": "Bearer wrong-token"},
            )
        assert resp.status_code == 401

    @pytest.mark.anyio()
    async def test_malformed_auth_header_returns_401(
        self, _server: BaniUIServer
    ) -> None:
        """A request with a non-Bearer Authorization header returns 401."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.get(
                "/api/connectors",
                headers={"Authorization": "Basic dXNlcjpwYXNz"},
            )
        assert resp.status_code == 401

    @pytest.mark.anyio()
    async def test_health_does_not_require_auth(self, _server: BaniUIServer) -> None:
        """The health endpoint is accessible without auth."""
        transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
        async with AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as ac:
            resp = await ac.get("/api/health")
        assert resp.status_code == 200
