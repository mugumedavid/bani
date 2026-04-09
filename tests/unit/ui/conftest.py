"""Shared fixtures for Bani Web UI unit tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Generator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from bani.ui.server import BaniUIServer


@pytest.fixture()
def tmp_projects_dir(tmp_path: Any) -> str:
    """Return a temporary directory path for BDL project files."""
    d = tmp_path / "projects"
    d.mkdir()
    return str(d)


@pytest.fixture()
def server(tmp_projects_dir: str) -> BaniUIServer:
    """Create a BaniUIServer instance with a temp projects directory."""
    srv = BaniUIServer(
        host="127.0.0.1",
        port=8910,
        projects_dir=tmp_projects_dir,
    )
    return srv


@pytest.fixture()
def auth_headers(server: BaniUIServer) -> dict[str, str]:
    """Return headers with the correct Bearer token."""
    return {"Authorization": f"Bearer {server.auth_token}"}


@pytest.fixture()
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture()
async def client(
    server: BaniUIServer, auth_headers: dict[str, str]
) -> AsyncGenerator[AsyncClient, None]:
    """Create an httpx AsyncClient wired to the FastAPI test app."""
    transport = ASGITransport(app=server.app)  # type: ignore[arg-type]
    async with AsyncClient(
        transport=transport,
        base_url="http://testserver",
        headers=auth_headers,
    ) as ac:
        yield ac
