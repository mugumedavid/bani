"""Unit tests for MSSQL connector."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa  # type: ignore[import-untyped]
import pytest  # type: ignore[import-not-found]

from bani.connectors.mssql.connector import MSSQLConnector
from bani.domain.project import ConnectionConfig
from bani.domain.schema import ColumnDefinition, TableDefinition


class TestMSSQLConnector:
    """Tests for MSSQLConnector."""

    def test_connector_init(self) -> None:
        """Test connector initialization."""
        connector = MSSQLConnector()
        assert connector.connection is None
        assert connector._database == ""

    def test_resolve_env_var_with_env_prefix(self) -> None:
        """Test resolving environment variable with ${env:} prefix."""
        import os

        os.environ["TEST_VAR"] = "test_value"
        result = MSSQLConnector._resolve_env_var("${env:TEST_VAR}")
        assert result == "test_value"

    def test_resolve_env_var_without_prefix(self) -> None:
        """Test resolving environment variable without prefix."""
        import os

        os.environ["TEST_VAR2"] = "test_value2"
        result = MSSQLConnector._resolve_env_var("TEST_VAR2")
        assert result == "test_value2"

    def test_resolve_env_var_not_found(self) -> None:
        """Test resolving non-existent environment variable."""
        result = MSSQLConnector._resolve_env_var("NONEXISTENT_VAR_XYZ")
        assert result is None

    def test_resolve_env_var_empty_string(self) -> None:
        """Test resolving empty environment variable reference."""
        result = MSSQLConnector._resolve_env_var("")
        assert result is None

    @patch("pymssql.connect")
    def test_connect_basic(self, mock_connect: Any) -> None:
        """Test basic connection."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        connector = MSSQLConnector()
        config = ConnectionConfig(
            dialect="mssql",
            host="localhost",
            port=1433,
            database="testdb",
            username_env="",
            password_env="",
            encrypt=False,
        )

        connector.connect(config)

        assert connector.connection is not None
        assert connector._database == "testdb"
        mock_connect.assert_called_once()

    @patch("pymssql.connect")
    def test_connect_missing_host(self, mock_connect: Any) -> None:
        """Test connection fails without host."""
        connector = MSSQLConnector()
        config = ConnectionConfig(
            dialect="mssql",
            host="",
            port=1433,
            database="testdb",
            username_env="",
            password_env="",
            encrypt=False,
        )

        with pytest.raises(ValueError, match="requires 'host'"):
            connector.connect(config)

    @patch("pymssql.connect")
    def test_connect_missing_database(self, mock_connect: Any) -> None:
        """Test connection fails without database."""
        connector = MSSQLConnector()
        config = ConnectionConfig(
            dialect="mssql",
            host="localhost",
            port=1433,
            database="",
            username_env="",
            password_env="",
            encrypt=False,
        )

        with pytest.raises(ValueError, match="requires 'database'"):
            connector.connect(config)

    def test_disconnect_not_connected(self) -> None:
        """Test disconnect when not connected."""
        connector = MSSQLConnector()
        connector.disconnect()  # Should not raise

    @patch("pymssql.connect")
    def test_disconnect_connected(self, mock_connect: Any) -> None:
        """Test disconnect when connected."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        connector = MSSQLConnector()
        config = ConnectionConfig(
            dialect="mssql",
            host="localhost",
            port=1433,
            database="testdb",
            username_env="",
            password_env="",
            encrypt=False,
        )

        connector.connect(config)
        connector.disconnect()

        mock_connection.close.assert_called_once()
        assert connector.connection is None

    def test_introspect_schema_not_connected(self) -> None:
        """Test introspect_schema raises when not connected."""
        connector = MSSQLConnector()

        with pytest.raises(RuntimeError, match="not connected"):
            connector.introspect_schema()

    def test_read_table_not_connected(self) -> None:
        """Test read_table raises when not connected."""
        connector = MSSQLConnector()

        with pytest.raises(RuntimeError, match="not connected"):
            list(connector.read_table("test", "dbo"))

    def test_estimate_row_count_not_connected(self) -> None:
        """Test estimate_row_count raises when not connected."""
        connector = MSSQLConnector()

        with pytest.raises(RuntimeError, match="not connected"):
            connector.estimate_row_count("test", "dbo")

    def test_create_table_not_connected(self) -> None:
        """Test create_table raises when not connected."""
        connector = MSSQLConnector()
        table_def = TableDefinition(
            schema_name="dbo",
            table_name="test",
            columns=(
                ColumnDefinition(
                    name="id",
                    data_type="INT",
                    nullable=False,
                    ordinal_position=0,
                ),
            ),
        )

        with pytest.raises(RuntimeError, match="not connected"):
            connector.create_table(table_def)

    def test_write_batch_not_connected(self) -> None:
        """Test write_batch raises when not connected."""
        connector = MSSQLConnector()
        batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], names=["id"])

        with pytest.raises(RuntimeError, match="not connected"):
            connector.write_batch("test", "dbo", batch)

    def test_create_indexes_not_connected(self) -> None:
        """Test create_indexes raises when not connected."""
        connector = MSSQLConnector()

        with pytest.raises(RuntimeError, match="not connected"):
            connector.create_indexes("test", "dbo", ())

    def test_create_foreign_keys_not_connected(self) -> None:
        """Test create_foreign_keys raises when not connected."""
        connector = MSSQLConnector()

        with pytest.raises(RuntimeError, match="not connected"):
            connector.create_foreign_keys(())

    def test_execute_sql_not_connected(self) -> None:
        """Test execute_sql raises when not connected."""
        connector = MSSQLConnector()

        with pytest.raises(RuntimeError, match="not connected"):
            connector.execute_sql("SELECT 1")
