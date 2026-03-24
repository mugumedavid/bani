"""Unit tests for MySQL connector (no DB required)."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from bani.connectors.mysql.connector import MySQLConnector
from bani.domain.project import ConnectionConfig


class TestMySQLConnectorInit:
    """Tests for connector initialization."""

    def test_init_creates_connector(self) -> None:
        """Connector should initialize without errors."""
        connector = MySQLConnector()
        assert connector.connection is None
        assert connector._schema_reader is None
        assert connector._data_reader is None
        assert connector._data_writer is None
        assert connector._database == ""


class TestConnectionConfigResolution:
    """Tests for environment variable resolution."""

    def test_resolve_env_var_with_dollar_prefix(self) -> None:
        """Should resolve ${env:VAR} format."""
        with patch.dict(os.environ, {"TEST_VAR": "test_value"}):
            result = MySQLConnector._resolve_env_var("${env:TEST_VAR}")
            assert result == "test_value"

    def test_resolve_env_var_without_prefix(self) -> None:
        """Should resolve plain VAR_NAME format."""
        with patch.dict(os.environ, {"TEST_VAR": "test_value"}):
            result = MySQLConnector._resolve_env_var("TEST_VAR")
            assert result == "test_value"

    def test_resolve_env_var_not_set(self) -> None:
        """Should return None for unset variables."""
        with patch.dict(os.environ, {}, clear=False):
            result = MySQLConnector._resolve_env_var("UNDEFINED_VAR_XYZ")
            assert result is None

    def test_resolve_empty_string(self) -> None:
        """Should return None for empty string."""
        result = MySQLConnector._resolve_env_var("")
        assert result is None


class TestConnectValidation:
    """Tests for connection validation."""

    def test_connect_requires_host(self) -> None:
        """Should raise ValueError if host is missing."""
        connector = MySQLConnector()
        config = ConnectionConfig(
            dialect="mysql",
            host="",
            port=3306,
            database="test",
        )

        with pytest.raises(ValueError, match="requires 'host'"):
            connector.connect(config)

    def test_connect_requires_database(self) -> None:
        """Should raise ValueError if database is missing."""
        connector = MySQLConnector()
        config = ConnectionConfig(
            dialect="mysql",
            host="localhost",
            port=3306,
            database="",
        )

        with pytest.raises(ValueError, match="requires 'database'"):
            connector.connect(config)


class TestDisconnect:
    """Tests for disconnection."""

    def test_disconnect_clears_state(self) -> None:
        """Disconnect should clear connection and helper objects."""
        connector = MySQLConnector()
        # Mock connection
        connector.connection = MagicMock()
        connector._schema_reader = MagicMock()
        connector._data_reader = MagicMock()
        connector._data_writer = MagicMock()

        connector.disconnect()

        assert connector.connection is None
        assert connector._schema_reader is None
        assert connector._data_reader is None
        assert connector._data_writer is None


class TestIntrospectSchemaValidation:
    """Tests for schema introspection validation."""

    def test_introspect_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        connector = MySQLConnector()

        with pytest.raises(RuntimeError, match="not connected"):
            connector.introspect_schema()


class TestReadTableValidation:
    """Tests for read_table validation."""

    def test_read_table_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        connector = MySQLConnector()

        with pytest.raises(RuntimeError, match="not connected"):
            list(connector.read_table("test_table", "test_db"))


class TestEstimateRowCountValidation:
    """Tests for row count estimation validation."""

    def test_estimate_row_count_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        connector = MySQLConnector()

        with pytest.raises(RuntimeError, match="not connected"):
            connector.estimate_row_count("test_table", "test_db")


class TestCreateTableValidation:
    """Tests for table creation validation."""

    def test_create_table_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        from bani.domain.schema import ColumnDefinition, TableDefinition

        connector = MySQLConnector()
        table_def = TableDefinition(
            schema_name="test_db",
            table_name="test",
            columns=(ColumnDefinition(name="id", data_type="INT"),),
        )

        with pytest.raises(RuntimeError, match="not connected"):
            connector.create_table(table_def)


class TestWriteBatchValidation:
    """Tests for batch writing validation."""

    def test_write_batch_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        import pyarrow as pa

        connector = MySQLConnector()
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3])],
            names=["id"],
        )

        with pytest.raises(RuntimeError, match="not connected"):
            connector.write_batch("test_table", "test_db", batch)


class TestCreateIndexesValidation:
    """Tests for index creation validation."""

    def test_create_indexes_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        from bani.domain.schema import IndexDefinition

        connector = MySQLConnector()
        index = IndexDefinition(name="test_idx", columns=("id",))

        with pytest.raises(RuntimeError, match="not connected"):
            connector.create_indexes("test_table", "test_db", (index,))


class TestCreateForeignKeysValidation:
    """Tests for foreign key creation validation."""

    def test_create_foreign_keys_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        from bani.domain.schema import ForeignKeyDefinition

        connector = MySQLConnector()
        fk = ForeignKeyDefinition(
            name="fk_test",
            source_table="test_db.test1",
            source_columns=("id",),
            referenced_table="test_db.test2",
            referenced_columns=("id",),
        )

        with pytest.raises(RuntimeError, match="not connected"):
            connector.create_foreign_keys((fk,))


class TestExecuteSQLValidation:
    """Tests for SQL execution validation."""

    def test_execute_sql_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        connector = MySQLConnector()

        with pytest.raises(RuntimeError, match="not connected"):
            connector.execute_sql("SELECT 1")
