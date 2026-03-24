"""Tests for SchemaInspector."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from bani.domain.schema import ColumnDefinition, DatabaseSchema, TableDefinition
from bani.sdk.schema_inspector import SchemaInspector


class TestSchemaInspector:
    """Tests for SchemaInspector."""

    def test_inspect_with_mock_connector(self) -> None:
        """Test introspection with a mocked connector."""
        # Create mock connector
        mock_connector = MagicMock()

        # Create a sample schema to return
        sample_schema = DatabaseSchema(
            tables=(
                TableDefinition(
                    schema_name="public",
                    table_name="users",
                    columns=(
                        ColumnDefinition(
                            name="id",
                            data_type="INTEGER",
                            nullable=False,
                            ordinal_position=0,
                        ),
                        ColumnDefinition(
                            name="name",
                            data_type="VARCHAR(255)",
                            nullable=True,
                            ordinal_position=1,
                        ),
                    ),
                ),
            ),
            source_dialect="postgresql",
        )

        mock_connector.introspect_schema.return_value = sample_schema

        # Patch the registry to return our mock connector
        with patch(
            "bani.sdk.schema_inspector.ConnectorRegistry.get_source"
        ) as mock_get:
            mock_get.return_value = mock_connector

            result = SchemaInspector.inspect(
                "postgresql",
                host="localhost",
                port=5432,
                database="testdb",
                username_env="PG_USER",
                password_env="PG_PASS",
            )

            # Verify the result
            assert result.source_dialect == "postgresql"
            assert len(result.tables) == 1
            assert result.tables[0].schema_name == "public"
            assert result.tables[0].table_name == "users"
            assert len(result.tables[0].columns) == 2

            # Verify connector was called
            mock_get.assert_called_once()
            mock_connector.connect.assert_called_once()
            mock_connector.introspect_schema.assert_called_once()
            mock_connector.close.assert_called_once()

    def test_inspect_closes_on_error(self) -> None:
        """Test that connector is closed even on error."""
        mock_connector = MagicMock()
        mock_connector.introspect_schema.side_effect = RuntimeError("Connection failed")

        with patch(
            "bani.sdk.schema_inspector.ConnectorRegistry.get_source"
        ) as mock_get:
            mock_get.return_value = mock_connector

            try:
                SchemaInspector.inspect("postgresql", host="localhost")
            except RuntimeError:
                pass

            # Verify close was called despite the error
            mock_connector.close.assert_called_once()

    def test_inspect_passes_kwargs(self) -> None:
        """Test that additional kwargs are passed to connector."""
        mock_connector = MagicMock()
        mock_connector.introspect_schema.return_value = DatabaseSchema(
            tables=(), source_dialect="postgresql"
        )

        with patch(
            "bani.sdk.schema_inspector.ConnectorRegistry.get_source"
        ) as mock_get:
            mock_get.return_value = mock_connector

            SchemaInspector.inspect(
                "postgresql",
                host="localhost",
                port=5432,
                database="testdb",
                username_env="PG_USER",
                password_env="PG_PASS",
                ssl_mode="require",
                application_name="bani",
            )

            # Verify kwargs were passed
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs["ssl_mode"] == "require"
            assert call_kwargs["application_name"] == "bani"

    def test_inspect_no_connector_raises_error(self) -> None:
        """Test that KeyError is raised when connector not found."""
        with patch(
            "bani.sdk.schema_inspector.ConnectorRegistry.get_source"
        ) as mock_get:
            mock_get.side_effect = KeyError("No source connector for dialect 'unknown'")

            try:
                SchemaInspector.inspect("unknown")
                raise AssertionError("Should have raised KeyError")
            except KeyError:
                pass
