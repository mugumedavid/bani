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
        assert connector._data_reader is None  # type: ignore[attr-defined]  # private attr set in connect()
        assert connector._data_writer is None  # type: ignore[attr-defined]  # private attr set in connect()
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
        connector._data_reader = MagicMock()  # type: ignore[attr-defined]  # private attr set in connect()
        connector._data_writer = MagicMock()  # type: ignore[attr-defined]  # private attr set in connect()

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


class TestConnectWithCredentials:
    """Tests for actual connection with mocked pymysql."""

    @patch("bani.connectors.mysql.connector.pymysql.connect")
    def test_connect_success_minimal_config(
        self, mock_pymysql_connect: MagicMock
    ) -> None:
        """Should establish connection with minimal config."""
        mock_conn = MagicMock()
        mock_pymysql_connect.return_value = mock_conn

        connector = MySQLConnector()
        config = ConnectionConfig(
            dialect="mysql",
            host="localhost",
            port=3306,
            database="test_db",
        )

        connector.connect(config)

        assert connector.connection == mock_conn
        assert connector._database == "test_db"
        assert connector._schema_reader is not None
        assert connector._data_reader is not None  # type: ignore[attr-defined]  # private attr set in connect()
        assert connector._data_writer is not None  # type: ignore[attr-defined]  # private attr set in connect()
        mock_pymysql_connect.assert_called_once()

    @patch("bani.connectors.mysql.connector.pymysql.connect")
    def test_connect_with_credentials(self, mock_pymysql_connect: MagicMock) -> None:
        """Should pass username and password to connection."""
        mock_conn = MagicMock()
        mock_pymysql_connect.return_value = mock_conn

        with patch.dict(
            os.environ, {"MYSQL_USER": "testuser", "MYSQL_PASS": "testpass"}
        ):
            connector = MySQLConnector()
            config = ConnectionConfig(
                dialect="mysql",
                host="localhost",
                port=3306,
                database="test_db",
                username_env="MYSQL_USER",
                password_env="MYSQL_PASS",
            )

            connector.connect(config)

            call_kwargs = mock_pymysql_connect.call_args[1]
            assert call_kwargs["user"] == "testuser"
            assert call_kwargs["passwd"] == "testpass"

    @patch("bani.connectors.mysql.connector.pymysql.connect")
    def test_connect_with_default_port(self, mock_pymysql_connect: MagicMock) -> None:
        """Should use default port 3306 when port is 0."""
        mock_conn = MagicMock()
        mock_pymysql_connect.return_value = mock_conn

        connector = MySQLConnector()
        config = ConnectionConfig(
            dialect="mysql",
            host="localhost",
            port=0,
            database="test_db",
        )

        connector.connect(config)

        call_kwargs = mock_pymysql_connect.call_args[1]
        assert call_kwargs["port"] == 3306

    @patch("bani.connectors.mysql.connector.pymysql.connect")
    def test_connect_with_custom_port(self, mock_pymysql_connect: MagicMock) -> None:
        """Should use custom port when specified."""
        mock_conn = MagicMock()
        mock_pymysql_connect.return_value = mock_conn

        connector = MySQLConnector()
        config = ConnectionConfig(
            dialect="mysql",
            host="localhost",
            port=3307,
            database="test_db",
        )

        connector.connect(config)

        call_kwargs = mock_pymysql_connect.call_args[1]
        assert call_kwargs["port"] == 3307

    @patch("bani.connectors.mysql.connector.pymysql.connect")
    def test_connect_with_tls_encryption(self, mock_pymysql_connect: MagicMock) -> None:
        """Should enable TLS when encrypt flag is set."""
        mock_conn = MagicMock()
        mock_pymysql_connect.return_value = mock_conn

        connector = MySQLConnector()
        config = ConnectionConfig(
            dialect="mysql",
            host="localhost",
            port=3306,
            database="test_db",
            encrypt=True,
        )

        connector.connect(config)

        call_kwargs = mock_pymysql_connect.call_args[1]
        assert "ssl" in call_kwargs
        assert call_kwargs["ssl"] == {"ssl": True}

    @patch("bani.connectors.mysql.connector.pymysql.connect")
    def test_connect_sets_charset_utf8mb4(
        self, mock_pymysql_connect: MagicMock
    ) -> None:
        """Should set charset to utf8mb4."""
        mock_conn = MagicMock()
        mock_pymysql_connect.return_value = mock_conn

        connector = MySQLConnector()
        config = ConnectionConfig(
            dialect="mysql",
            host="localhost",
            port=3306,
            database="test_db",
        )

        connector.connect(config)

        call_kwargs = mock_pymysql_connect.call_args[1]
        assert call_kwargs["charset"] == "utf8mb4"
        assert call_kwargs["autocommit"] is True


class TestIntrospectSchemaDelegation:
    """Tests for schema introspection delegation."""

    def test_introspect_schema_delegates_to_reader(self) -> None:
        """Should delegate to _schema_reader."""
        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_reader = MagicMock()
        connector.connection = mock_conn
        connector._schema_reader = mock_reader

        from bani.domain.schema import DatabaseSchema

        mock_schema = DatabaseSchema(tables=(), source_dialect="mysql")
        mock_reader.read_schema.return_value = mock_schema

        result = connector.introspect_schema()

        assert result == mock_schema
        mock_reader.read_schema.assert_called_once()


class TestReadTableDelegation:
    """Tests for read_table delegation."""

    def test_read_table_delegates_to_reader(self) -> None:
        """Should delegate to _data_reader."""
        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_reader = MagicMock()
        connector.connection = mock_conn
        connector._data_reader = mock_reader  # type: ignore[attr-defined]  # private attr set in connect()

        import pyarrow as pa

        mock_batch = pa.RecordBatch.from_arrays([pa.array([1, 2])], names=["id"])
        mock_reader.read_table.return_value = iter([mock_batch])

        result = list(
            connector.read_table(
                "test_table",
                "test_db",
                columns=["id"],
                filter_sql="id > 0",
                batch_size=50,
            )
        )

        assert len(result) == 1
        mock_reader.read_table.assert_called_once_with(
            table_name="test_table",
            schema_name="test_db",
            columns=["id"],
            filter_sql="id > 0",
            batch_size=50,
        )


class TestEstimateRowCountDelegation:
    """Tests for row count estimation delegation."""

    def test_estimate_row_count_delegates_to_reader(self) -> None:
        """Should delegate to _data_reader."""
        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_reader = MagicMock()
        connector.connection = mock_conn
        connector._data_reader = mock_reader  # type: ignore[attr-defined]  # private attr set in connect()
        mock_reader.estimate_row_count.return_value = 1000

        result = connector.estimate_row_count("test_table", "test_db")

        assert result == 1000
        mock_reader.estimate_row_count.assert_called_once_with("test_table", "test_db")


class TestCreateTableExecution:
    """Tests for table creation with mocked execution."""

    def test_create_table_simple(self) -> None:
        """Should execute CREATE TABLE with columns."""
        from bani.domain.schema import ColumnDefinition, TableDefinition

        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        table_def = TableDefinition(
            schema_name="test_db",
            table_name="users",
            columns=(
                ColumnDefinition(name="id", data_type="INT", nullable=False),
                ColumnDefinition(name="name", data_type="VARCHAR(255)"),
            ),
        )

        connector.create_table(table_def)

        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "CREATE TABLE" in sql
        assert "`test_db`.`users`" in sql
        assert "`id` INT NOT NULL" in sql
        assert "`name` VARCHAR(255)" in sql

    def test_create_table_with_primary_key(self) -> None:
        """Should include PRIMARY KEY constraint."""
        from bani.domain.schema import ColumnDefinition, TableDefinition

        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        table_def = TableDefinition(
            schema_name="test_db",
            table_name="users",
            columns=(ColumnDefinition(name="id", data_type="INT", nullable=False),),
            primary_key=("id",),
        )

        connector.create_table(table_def)

        sql = mock_cursor.execute.call_args[0][0]
        assert "PRIMARY KEY (`id`)" in sql

    def test_create_table_with_check_constraints(self) -> None:
        """Should include CHECK constraints."""
        from bani.domain.schema import ColumnDefinition, TableDefinition

        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        table_def = TableDefinition(
            schema_name="test_db",
            table_name="users",
            columns=(ColumnDefinition(name="age", data_type="INT"),),
            check_constraints=("age >= 0", "age <= 150"),
        )

        connector.create_table(table_def)

        sql = mock_cursor.execute.call_args[0][0]
        assert "CHECK age >= 0" in sql
        assert "CHECK age <= 150" in sql

    def test_create_table_with_auto_increment(self) -> None:
        """Should include AUTO_INCREMENT for auto-increment columns."""
        from bani.domain.schema import ColumnDefinition, TableDefinition

        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        table_def = TableDefinition(
            schema_name="test_db",
            table_name="users",
            columns=(
                ColumnDefinition(name="id", data_type="INT", is_auto_increment=True),
            ),
        )

        connector.create_table(table_def)

        sql = mock_cursor.execute.call_args[0][0]
        assert "AUTO_INCREMENT" in sql

    def test_create_table_with_default_value(self) -> None:
        """Should include DEFAULT for columns with defaults."""
        from bani.domain.schema import ColumnDefinition, TableDefinition

        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        table_def = TableDefinition(
            schema_name="test_db",
            table_name="users",
            columns=(
                ColumnDefinition(
                    name="status",
                    data_type="VARCHAR(10)",
                    default_value="'active'",
                ),
            ),
        )

        connector.create_table(table_def)

        sql = mock_cursor.execute.call_args[0][0]
        assert "DEFAULT 'active'" in sql

    def test_create_table_sets_innodb_utf8mb4(self) -> None:
        """Should set InnoDB engine and utf8mb4 charset."""
        from bani.domain.schema import ColumnDefinition, TableDefinition

        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        table_def = TableDefinition(
            schema_name="test_db",
            table_name="users",
            columns=(ColumnDefinition(name="id", data_type="INT"),),
        )

        connector.create_table(table_def)

        sql = mock_cursor.execute.call_args[0][0]
        assert "ENGINE=InnoDB" in sql
        assert "DEFAULT CHARSET=utf8mb4" in sql
        assert "COLLATE=utf8mb4_unicode_ci" in sql


class TestWriteBatchDelegation:
    """Tests for batch writing delegation."""

    def test_write_batch_delegates_to_writer(self) -> None:
        """Should delegate to _data_writer."""
        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_writer = MagicMock()
        connector.connection = mock_conn
        connector._data_writer = mock_writer  # type: ignore[attr-defined]  # private attr set in connect()
        mock_writer.write_batch.return_value = 5

        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3, 4, 5])], names=["id"])

        result = connector.write_batch("test_table", "test_db", batch)

        assert result == 5
        mock_writer.write_batch.assert_called_once_with("test_table", "test_db", batch)


class TestCreateIndexesExecution:
    """Tests for index creation."""

    def test_create_indexes_single_index(self) -> None:
        """Should execute CREATE INDEX for each index."""
        from bani.domain.schema import IndexDefinition

        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        index = IndexDefinition(name="idx_name", columns=("name",))

        connector.create_indexes("users", "test_db", (index,))

        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "CREATE" in sql and "INDEX" in sql
        assert "`idx_name`" in sql
        assert "`test_db`.`users`" in sql
        assert "`name`" in sql

    def test_create_indexes_unique_index(self) -> None:
        """Should include UNIQUE keyword for unique indexes."""
        from bani.domain.schema import IndexDefinition

        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        index = IndexDefinition(name="uq_email", columns=("email",), is_unique=True)

        connector.create_indexes("users", "test_db", (index,))

        sql = mock_cursor.execute.call_args[0][0]
        assert "CREATE UNIQUE INDEX" in sql

    def test_create_indexes_composite_columns(self) -> None:
        """Should handle indexes with multiple columns."""
        from bani.domain.schema import IndexDefinition

        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        index = IndexDefinition(
            name="idx_composite", columns=("first_name", "last_name")
        )

        connector.create_indexes("users", "test_db", (index,))

        sql = mock_cursor.execute.call_args[0][0]
        assert "`first_name`, `last_name`" in sql

    def test_create_indexes_multiple_indexes(self) -> None:
        """Should execute CREATE INDEX for each index."""
        from bani.domain.schema import IndexDefinition

        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        indexes = (
            IndexDefinition(name="idx_name", columns=("name",)),
            IndexDefinition(name="idx_email", columns=("email",)),
        )

        connector.create_indexes("users", "test_db", indexes)

        assert mock_cursor.execute.call_count == 2


class TestCreateForeignKeysExecution:
    """Tests for foreign key creation."""

    def test_create_foreign_keys_simple(self) -> None:
        """Should execute ALTER TABLE ADD CONSTRAINT for FK."""
        from bani.domain.schema import ForeignKeyDefinition

        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn
        connector._database = "test_db"

        fk = ForeignKeyDefinition(
            name="fk_user_id",
            source_table="test_db.orders",
            source_columns=("user_id",),
            referenced_table="test_db.users",
            referenced_columns=("id",),
        )

        connector.create_foreign_keys((fk,))

        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "ALTER TABLE" in sql
        assert "`test_db`.`orders`" in sql
        assert "ADD CONSTRAINT `orders_fk_user_id`" in sql
        assert "FOREIGN KEY (`user_id`)" in sql
        assert "REFERENCES `test_db`.`users` (`id`)" in sql

    def test_create_foreign_keys_with_actions(self) -> None:
        """Should include ON DELETE and ON UPDATE actions."""
        from bani.domain.schema import ForeignKeyDefinition

        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn
        connector._database = "test_db"

        fk = ForeignKeyDefinition(
            name="fk_category",
            source_table="test_db.products",
            source_columns=("category_id",),
            referenced_table="test_db.categories",
            referenced_columns=("id",),
            on_delete="CASCADE",
            on_update="CASCADE",
        )

        connector.create_foreign_keys((fk,))

        sql = mock_cursor.execute.call_args[0][0]
        assert "ON DELETE CASCADE" in sql
        assert "ON UPDATE CASCADE" in sql

    def test_create_foreign_keys_defaults_to_current_db(self) -> None:
        """Should use current DB when FK table lacks schema prefix."""
        from bani.domain.schema import ForeignKeyDefinition

        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn
        connector._database = "mydb"

        fk = ForeignKeyDefinition(
            name="fk_test",
            source_table="orders",
            source_columns=("user_id",),
            referenced_table="users",
            referenced_columns=("id",),
        )

        connector.create_foreign_keys((fk,))

        sql = mock_cursor.execute.call_args[0][0]
        assert "`mydb`.`orders`" in sql
        assert "`mydb`.`users`" in sql

    def test_create_foreign_keys_composite_columns(self) -> None:
        """Should handle composite foreign keys."""
        from bani.domain.schema import ForeignKeyDefinition

        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn
        connector._database = "test_db"

        fk = ForeignKeyDefinition(
            name="fk_composite",
            source_table="test_db.order_items",
            source_columns=("order_id", "item_seq"),
            referenced_table="test_db.items",
            referenced_columns=("order_id", "sequence"),
        )

        connector.create_foreign_keys((fk,))

        sql = mock_cursor.execute.call_args[0][0]
        assert "`order_id`, `item_seq`" in sql
        assert "`order_id`, `sequence`" in sql


class TestExecuteSQLExecution:
    """Tests for arbitrary SQL execution."""

    def test_execute_sql_executes_statement(self) -> None:
        """Should execute the provided SQL statement."""
        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        connector.execute_sql("SELECT 1")

        mock_cursor.execute.assert_called_once_with("SELECT 1")

    def test_execute_sql_handles_complex_statements(self) -> None:
        """Should execute complex SQL statements."""
        connector = MySQLConnector()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        sql = "UPDATE users SET status='inactive' WHERE last_login < '2023-01-01'"
        connector.execute_sql(sql)

        mock_cursor.execute.assert_called_once_with(sql)
