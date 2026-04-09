"""Tests for the BaniUIServer (FastAPI app creation and configuration)."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from bani.ui.server import BaniUIServer


class TestBaniUIServer:
    """Tests for BaniUIServer initialisation and app creation."""

    def test_creates_with_defaults(self) -> None:
        """Server creates with default host, port, and a random token."""
        server = BaniUIServer()
        assert server.host == "127.0.0.1"
        assert server.port == 8910
        assert len(server.auth_token) > 0
        assert server.app is not None

    def test_auth_token_is_random(self) -> None:
        """Each server instance gets a unique auth token."""
        server_a = BaniUIServer()
        server_b = BaniUIServer()
        assert server_a.auth_token != server_b.auth_token

    def test_custom_host_and_port(self) -> None:
        """Server respects custom host and port."""
        server = BaniUIServer(host="0.0.0.0", port=9999)
        assert server.host == "0.0.0.0"
        assert server.port == 9999

    def test_app_has_title_and_version(self) -> None:
        """FastAPI app has the expected title and version."""
        server = BaniUIServer()
        assert server.app.title == "Bani"
        assert server.app.version == "0.1.0"

    @pytest.mark.anyio()
    async def test_health_endpoint_no_auth(self) -> None:
        """Health endpoint does not require authentication."""
        server = BaniUIServer()
        transport = ASGITransport(app=server.app)  # type: ignore[arg-type]
        async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
            resp = await ac.get("/api/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

    @pytest.mark.anyio()
    async def test_root_responds(self) -> None:
        """Root returns either the SPA (200 HTML) or a fallback JSON message."""
        server = BaniUIServer()
        transport = ASGITransport(app=server.app)  # type: ignore[arg-type]
        async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
            resp = await ac.get("/")
        assert resp.status_code == 200

    @pytest.mark.anyio()
    async def test_app_state_has_migration_state(self) -> None:
        """App state includes migration_state dict."""
        server = BaniUIServer()
        assert hasattr(server.app.state, "migration_state")
        state = server.app.state.migration_state
        assert state["running"] is False

    @pytest.mark.anyio()
    async def test_app_state_has_sse_broadcaster(self) -> None:
        """App state includes an SSE broadcaster."""
        server = BaniUIServer()
        assert hasattr(server.app.state, "sse_broadcaster")
