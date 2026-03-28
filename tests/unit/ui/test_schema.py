"""Tests for the schema inspection route."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    TableDefinition,
)
from bani.ui.server import BaniUIServer


@pytest.fixture()
def _server() -> BaniUIServer:
    return BaniUIServer()


@pytest.fixture()
def _headers(_server: BaniUIServer) -> dict[str, str]:
    return {"Authorization": f"Bearer {_server.auth_token}"}


def _make_schema() -> DatabaseSchema:
    """Create a minimal DatabaseSchema for testing."""
    col = ColumnDefinition(
        name="id",
        data_type="INTEGER",
        nullable=False,
        is_auto_increment=True,
        arrow_type_str="int32",
    )
    table = TableDefinition(
        schema_name="public",
        table_name="users",
        columns=(col,),
        primary_key=("id",),
    )
    return DatabaseSchema(tables=(table,), source_dialect="postgresql")


class TestSchemaInspection:
    """Tests for the POST /api/schema/inspect endpoint."""

    @pytest.mark.anyio()
    async def test_inspect_with_mocked_connector(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Inspect returns the schema from a mocked SchemaInspector."""
        schema = _make_schema()
        with patch(
            "bani.sdk.schema_inspector.SchemaInspector.inspect",
            return_value=schema,
        ) as mock_inspect:
            transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
            async with AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as ac:
                resp = await ac.post(
                    "/api/schema/inspect",
                    json={
                        "dialect": "postgresql",
                        "host": "localhost",
                        "port": 5432,
                        "database": "testdb",
                    },
                    headers=_headers,
                )

        assert resp.status_code == 200
        data = resp.json()
        assert data["source_dialect"] == "postgresql"
        assert len(data["tables"]) == 1
        assert data["tables"][0]["table_name"] == "users"
        assert data["tables"][0]["columns"][0]["name"] == "id"

        mock_inspect.assert_called_once()

    @pytest.mark.anyio()
    async def test_inspect_unknown_dialect_returns_400(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Inspect with an unknown dialect returns 400."""
        with patch(
            "bani.sdk.schema_inspector.SchemaInspector.inspect",
            side_effect=ValueError("Connector 'nope' not found"),
        ):
            transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
            async with AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as ac:
                resp = await ac.post(
                    "/api/schema/inspect",
                    json={"dialect": "nope"},
                    headers=_headers,
                )

        assert resp.status_code == 400

    @pytest.mark.anyio()
    async def test_inspect_connection_failure_returns_500(
        self, _server: BaniUIServer, _headers: dict[str, str]
    ) -> None:
        """Inspect with a connection failure returns 500."""
        with patch(
            "bani.sdk.schema_inspector.SchemaInspector.inspect",
            side_effect=ConnectionError("cannot connect"),
        ):
            transport = ASGITransport(app=_server.app)  # type: ignore[arg-type]
            async with AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as ac:
                resp = await ac.post(
                    "/api/schema/inspect",
                    json={"dialect": "postgresql", "host": "badhost"},
                    headers=_headers,
                )

        assert resp.status_code == 500
