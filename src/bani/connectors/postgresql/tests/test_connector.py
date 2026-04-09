"""Unit tests for PostgreSQL connector (no DB required)."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from bani.connectors.postgresql.connector import PostgreSQLConnector
from bani.domain.project import ConnectionConfig


class TestPostgreSQLConnectorInit:
    """Tests for connector initialization."""

    def test_init_creates_connector(self) -> None:
        """Connector should initialize without errors."""
        connector = PostgreSQLConnector()
        assert connector.connection is None
        assert connector._schema_reader is None
        assert connector._data_reader is None  # type: ignore[attr-defined]  # private attr set in connect()
        assert connector._data_writer is None  # type: ignore[attr-defined]  # private attr set in connect()


class TestConnectionConfigResolution:
    """Tests for environment variable resolution."""

    def test_resolve_env_var_with_dollar_prefix(self) -> None:
        """Should resolve ${env:VAR} format."""
        with patch.dict(os.environ, {"TEST_VAR": "test_value"}):
            result = PostgreSQLConnector._resolve_env_var("${env:TEST_VAR}")
            assert result == "test_value"

    def test_resolve_env_var_without_prefix(self) -> None:
        """Should resolve plain VAR_NAME format."""
        with patch.dict(os.environ, {"TEST_VAR": "test_value"}):
            result = PostgreSQLConnector._resolve_env_var("TEST_VAR")
            assert result == "test_value"

    def test_resolve_env_var_not_set(self) -> None:
        """Should return None for unset variables."""
        with patch.dict(os.environ, {}, clear=False):
            result = PostgreSQLConnector._resolve_env_var("UNDEFINED_VAR_XYZ")
            assert result is None

    def test_resolve_empty_string(self) -> None:
        """Should return None for empty string."""
        result = PostgreSQLConnector._resolve_env_var("")
        assert result is None


class TestConnectValidation:
    """Tests for connection validation."""

    def test_connect_requires_host(self) -> None:
        """Should raise ValueError if host is missing."""
        connector = PostgreSQLConnector()
        config = ConnectionConfig(
            dialect="postgresql",
            host="",
            port=5432,
            database="test",
        )

        with pytest.raises(ValueError, match="requires 'host'"):
            connector.connect(config)

    def test_connect_requires_database(self) -> None:
        """Should raise ValueError if database is missing."""
        connector = PostgreSQLConnector()
        config = ConnectionConfig(
            dialect="postgresql",
            host="localhost",
            port=5432,
            database="",
        )

        with pytest.raises(ValueError, match="requires 'database'"):
            connector.connect(config)


class TestDisconnect:
    """Tests for disconnection."""

    def test_disconnect_clears_state(self) -> None:
        """Disconnect should clear connection and helper objects."""
        connector = PostgreSQLConnector()
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
        connector = PostgreSQLConnector()

        with pytest.raises(RuntimeError, match="not connected"):
            connector.introspect_schema()


class TestReadTableValidation:
    """Tests for read_table validation."""

    def test_read_table_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        connector = PostgreSQLConnector()

        with pytest.raises(RuntimeError, match="not connected"):
            list(connector.read_table("test_table", "public"))


class TestEstimateRowCountValidation:
    """Tests for row count estimation validation."""

    def test_estimate_row_count_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        connector = PostgreSQLConnector()

        with pytest.raises(RuntimeError, match="not connected"):
            connector.estimate_row_count("test_table", "public")


class TestCreateTableValidation:
    """Tests for table creation validation."""

    def test_create_table_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        from bani.domain.schema import ColumnDefinition, TableDefinition

        connector = PostgreSQLConnector()
        table_def = TableDefinition(
            schema_name="public",
            table_name="test",
            columns=(ColumnDefinition(name="id", data_type="INTEGER"),),
        )

        with pytest.raises(RuntimeError, match="not connected"):
            connector.create_table(table_def)


class TestWriteBatchValidation:
    """Tests for batch writing validation."""

    def test_write_batch_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        import pyarrow as pa

        connector = PostgreSQLConnector()
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3])],
            names=["id"],
        )

        with pytest.raises(RuntimeError, match="not connected"):
            connector.write_batch("test_table", "public", batch)


class TestCreateIndexesValidation:
    """Tests for index creation validation."""

    def test_create_indexes_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        from bani.domain.schema import IndexDefinition

        connector = PostgreSQLConnector()
        index = IndexDefinition(name="test_idx", columns=("id",))

        with pytest.raises(RuntimeError, match="not connected"):
            connector.create_indexes("test_table", "public", (index,))


class TestCreateForeignKeysValidation:
    """Tests for foreign key creation validation."""

    def test_create_foreign_keys_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        from bani.domain.schema import ForeignKeyDefinition

        connector = PostgreSQLConnector()
        fk = ForeignKeyDefinition(
            name="fk_test",
            source_table="public.test1",
            source_columns=("id",),
            referenced_table="public.test2",
            referenced_columns=("id",),
        )

        with pytest.raises(RuntimeError, match="not connected"):
            connector.create_foreign_keys((fk,))


class TestExecuteSQLValidation:
    """Tests for SQL execution validation."""

    def test_execute_sql_requires_connection(self) -> None:
        """Should raise RuntimeError if not connected."""
        connector = PostgreSQLConnector()

        with pytest.raises(RuntimeError, match="not connected"):
            connector.execute_sql("SELECT 1")


class TestConnectWithCredentials:
    """Tests for connection with credential resolution."""

    def test_connect_resolves_username_env_var(self) -> None:
        """Should resolve username from environment variable."""
        connector = PostgreSQLConnector()
        config = ConnectionConfig(
            dialect="postgresql",
            host="localhost",
            port=5432,
            database="testdb",
            username_env="TEST_USER",
        )

        with patch.dict(os.environ, {"TEST_USER": "testuser"}):
            with patch(
                "bani.connectors.postgresql.connector.psycopg.connect"
            ) as mock_connect:
                mock_connect.return_value = MagicMock()
                connector.connect(config)

                # Check that psycopg.connect was called
                assert mock_connect.called

    def test_connect_resolves_password_env_var(self) -> None:
        """Should resolve password from environment variable."""
        connector = PostgreSQLConnector()
        config = ConnectionConfig(
            dialect="postgresql",
            host="localhost",
            port=5432,
            database="testdb",
            password_env="TEST_PASS",
        )

        with patch.dict(os.environ, {"TEST_PASS": "testpass"}):
            with patch(
                "bani.connectors.postgresql.connector.psycopg.connect"
            ) as mock_connect:
                mock_connect.return_value = MagicMock()
                connector.connect(config)

                assert mock_connect.called

    def test_connect_uses_default_port(self) -> None:
        """Should use port 5432 if port is 0."""
        connector = PostgreSQLConnector()
        config = ConnectionConfig(
            dialect="postgresql",
            host="localhost",
            port=0,
            database="testdb",
        )

        with patch(
            "bani.connectors.postgresql.connector.psycopg.connect"
        ) as mock_connect:
            mock_connect.return_value = MagicMock()
            connector.connect(config)

            call_args = mock_connect.call_args[0][0]
            assert "port=5432" in call_args

    def test_connect_uses_custom_port(self) -> None:
        """Should use custom port if provided."""
        connector = PostgreSQLConnector()
        config = ConnectionConfig(
            dialect="postgresql",
            host="localhost",
            port=5433,
            database="testdb",
        )

        with patch(
            "bani.connectors.postgresql.connector.psycopg.connect"
        ) as mock_connect:
            mock_connect.return_value = MagicMock()
            connector.connect(config)

            call_args = mock_connect.call_args[0][0]
            assert "port=5433" in call_args

    def test_connect_includes_ssl_when_encrypt_true(self) -> None:
        """Should include sslmode=prefer when encrypt is True."""
        connector = PostgreSQLConnector()
        config = ConnectionConfig(
            dialect="postgresql",
            host="localhost",
            port=5432,
            database="testdb",
            encrypt=True,
        )

        with patch(
            "bani.connectors.postgresql.connector.psycopg.connect"
        ) as mock_connect:
            mock_connect.return_value = MagicMock()
            connector.connect(config)

            call_args = mock_connect.call_args[0][0]
            assert "sslmode=prefer" in call_args

    def test_connect_includes_ssl_disable_when_encrypt_false(
        self,
    ) -> None:
        """Should include sslmode=disable when encrypt is False."""
        connector = PostgreSQLConnector()
        config = ConnectionConfig(
            dialect="postgresql",
            host="localhost",
            port=5432,
            database="testdb",
            encrypt=False,
        )

        with patch(
            "bani.connectors.postgresql.connector.psycopg.connect"
        ) as mock_connect:
            mock_connect.return_value = MagicMock()
            connector.connect(config)

            call_args = mock_connect.call_args[0][0]
            assert "sslmode=disable" in call_args

    def test_connect_initializes_helpers(self) -> None:
        """Should initialize schema/data reader/writer after connect."""
        connector = PostgreSQLConnector()
        config = ConnectionConfig(
            dialect="postgresql",
            host="localhost",
            port=5432,
            database="testdb",
        )

        with patch(
            "bani.connectors.postgresql.connector.psycopg.connect"
        ) as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            connector.connect(config)

            assert connector.connection is not None
            assert connector._schema_reader is not None
            assert connector._data_reader is not None  # type: ignore[attr-defined]  # private attr set in connect()
            assert connector._data_writer is not None  # type: ignore[attr-defined]  # private attr set in connect()


class TestIntrospectSchemaDelegation:
    """Tests for schema introspection delegation."""

    def test_introspect_schema_delegates_to_reader(self) -> None:
        """Should delegate to schema reader."""
        from bani.domain.schema import DatabaseSchema

        connector = PostgreSQLConnector()
        mock_conn = MagicMock()
        connector.connection = mock_conn
        mock_reader = MagicMock()
        connector._schema_reader = mock_reader

        expected_schema = DatabaseSchema(tables=(), source_dialect="postgresql")
        mock_reader.read_schema.return_value = expected_schema

        result = connector.introspect_schema()

        assert result is expected_schema
        mock_reader.read_schema.assert_called_once()


class TestReadTableDelegation:
    """Tests for table reading delegation."""

    def test_read_table_delegates_to_reader(self) -> None:
        """Should delegate to data reader."""
        connector = PostgreSQLConnector()
        mock_conn = MagicMock()
        connector.connection = mock_conn
        mock_reader = MagicMock()
        connector._data_reader = mock_reader  # type: ignore[attr-defined]  # private attr set in connect()

        mock_reader.read_table.return_value = iter([])

        list(connector.read_table("test", "public"))

        mock_reader.read_table.assert_called_once_with(
            table_name="test",
            schema_name="public",
            columns=None,
            filter_sql=None,
            batch_size=100_000,
        )

    def test_read_table_passes_through_parameters(self) -> None:
        """Should pass through all parameters to reader."""
        connector = PostgreSQLConnector()
        mock_conn = MagicMock()
        connector.connection = mock_conn
        mock_reader = MagicMock()
        connector._data_reader = mock_reader  # type: ignore[attr-defined]  # private attr set in connect()

        mock_reader.read_table.return_value = iter([])

        list(
            connector.read_table(
                "test",
                "public",
                columns=["id", "name"],
                filter_sql="id > 5",
                batch_size=1000,
            )
        )

        mock_reader.read_table.assert_called_once_with(
            table_name="test",
            schema_name="public",
            columns=["id", "name"],
            filter_sql="id > 5",
            batch_size=1000,
        )


class TestEstimateRowCountDelegation:
    """Tests for row count estimation delegation."""

    def test_estimate_row_count_delegates_to_reader(self) -> None:
        """Should delegate to data reader."""
        connector = PostgreSQLConnector()
        mock_conn = MagicMock()
        connector.connection = mock_conn
        mock_reader = MagicMock()
        connector._data_reader = mock_reader  # type: ignore[attr-defined]  # private attr set in connect()
        mock_reader.estimate_row_count.return_value = 12345

        result = connector.estimate_row_count("test", "public")

        assert result == 12345
        mock_reader.estimate_row_count.assert_called_once_with("test", "public")


class TestCreateTableDelegation:
    """Tests for table creation."""

    def test_create_table_builds_and_executes_sql(self) -> None:
        """Should build CREATE TABLE statement."""
        from bani.domain.schema import ColumnDefinition, TableDefinition

        connector = PostgreSQLConnector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        table_def = TableDefinition(
            schema_name="public",
            table_name="users",
            columns=(
                ColumnDefinition(name="id", data_type="INTEGER", ordinal_position=0),
                ColumnDefinition(name="name", data_type="TEXT", ordinal_position=1),
            ),
            primary_key=("id",),
        )

        connector.create_table(table_def)

        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0][0]
        assert "CREATE TABLE" in call_args
        assert "public" in call_args and "users" in call_args
        assert "id" in call_args
        assert "name" in call_args

    def test_create_table_with_not_null_constraint(self) -> None:
        """Should include NOT NULL constraints."""
        from bani.domain.schema import ColumnDefinition, TableDefinition

        connector = PostgreSQLConnector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        table_def = TableDefinition(
            schema_name="public",
            table_name="test",
            columns=(
                ColumnDefinition(
                    name="id",
                    data_type="INTEGER",
                    nullable=False,
                    ordinal_position=0,
                ),
            ),
        )

        connector.create_table(table_def)

        call_args = mock_cursor.execute.call_args[0][0]
        assert "NOT NULL" in call_args

    def test_create_table_with_default_value(self) -> None:
        """Should include DEFAULT clause."""
        from bani.domain.schema import ColumnDefinition, TableDefinition

        connector = PostgreSQLConnector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        table_def = TableDefinition(
            schema_name="public",
            table_name="test",
            columns=(
                ColumnDefinition(
                    name="status",
                    data_type="TEXT",
                    default_value="'active'",
                    ordinal_position=0,
                ),
            ),
        )

        connector.create_table(table_def)

        call_args = mock_cursor.execute.call_args[0][0]
        assert "DEFAULT 'active'" in call_args

    def test_create_table_with_check_constraints(self) -> None:
        """Should include CHECK constraints."""
        from bani.domain.schema import ColumnDefinition, TableDefinition

        connector = PostgreSQLConnector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        table_def = TableDefinition(
            schema_name="public",
            table_name="test",
            columns=(
                ColumnDefinition(name="age", data_type="INTEGER", ordinal_position=0),
            ),
            check_constraints=("(age >= 18)",),
        )

        connector.create_table(table_def)

        call_args = mock_cursor.execute.call_args[0][0]
        assert "CHECK (age >= 18)" in call_args


class TestWriteBatchDelegation:
    """Tests for batch writing delegation."""

    def test_write_batch_delegates_to_writer(self) -> None:
        """Should delegate to data writer."""
        import pyarrow as pa

        connector = PostgreSQLConnector()
        mock_conn = MagicMock()
        connector.connection = mock_conn
        mock_writer = MagicMock()
        connector._data_writer = mock_writer  # type: ignore[attr-defined]  # private attr set in connect()
        mock_writer.write_batch.return_value = 5

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3])],
            names=["id"],
        )

        result = connector.write_batch("test", "public", batch)

        assert result == 5
        mock_writer.write_batch.assert_called_once_with("test", "public", batch)


class TestCreateIndexesDelegation:
    """Tests for index creation."""

    def test_create_indexes_builds_and_executes_sql(self) -> None:
        """Should execute CREATE INDEX statements."""
        from bani.domain.schema import IndexDefinition

        connector = PostgreSQLConnector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        index = IndexDefinition(
            name="idx_test",
            columns=("col1", "col2"),
            is_unique=False,
        )

        connector.create_indexes("test_table", "public", (index,))

        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0][0]
        assert "CREATE" in call_args and "INDEX" in call_args
        assert "idx_test" in call_args
        assert "col1" in call_args

    def test_create_indexes_with_unique(self) -> None:
        """Should include UNIQUE keyword."""
        from bani.domain.schema import IndexDefinition

        connector = PostgreSQLConnector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        index = IndexDefinition(
            name="idx_unique",
            columns=("email",),
            is_unique=True,
        )

        connector.create_indexes("users", "public", (index,))

        call_args = mock_cursor.execute.call_args[0][0]
        assert "UNIQUE INDEX" in call_args

    def test_create_indexes_with_filter_expression(self) -> None:
        """Should include WHERE clause."""
        from bani.domain.schema import IndexDefinition

        connector = PostgreSQLConnector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        index = IndexDefinition(
            name="idx_active",
            columns=("status",),
            filter_expression="status = 'active'",
        )

        connector.create_indexes("users", "public", (index,))

        call_args = mock_cursor.execute.call_args[0][0]
        assert "WHERE status = 'active'" in call_args

    def test_create_multiple_indexes(self) -> None:
        """Should create multiple indexes."""
        from bani.domain.schema import IndexDefinition

        connector = PostgreSQLConnector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        indexes = (
            IndexDefinition(name="idx_1", columns=("col1",)),
            IndexDefinition(name="idx_2", columns=("col2",)),
        )

        connector.create_indexes("test_table", "public", indexes)

        assert mock_cursor.execute.call_count == 2


class TestCreateForeignKeysDelegation:
    """Tests for foreign key creation."""

    def test_create_foreign_keys_builds_alter_table(self) -> None:
        """Should execute ALTER TABLE statements."""
        from bani.domain.schema import ForeignKeyDefinition

        connector = PostgreSQLConnector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        fk = ForeignKeyDefinition(
            name="fk_test",
            source_table="public.posts",
            source_columns=("user_id",),
            referenced_table="public.users",
            referenced_columns=("id",),
            on_delete="CASCADE",
            on_update="NO ACTION",
        )

        connector.create_foreign_keys((fk,))

        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0][0]
        assert "ALTER TABLE" in call_args
        assert "FOREIGN KEY" in call_args
        assert "CASCADE" in call_args

    def test_create_foreign_keys_parses_fqn_with_schema(self) -> None:
        """Should parse fully qualified names."""
        from bani.domain.schema import ForeignKeyDefinition

        connector = PostgreSQLConnector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        fk = ForeignKeyDefinition(
            name="fk_test",
            source_table="app.posts",
            source_columns=("user_id",),
            referenced_table="app.users",
            referenced_columns=("id",),
        )

        connector.create_foreign_keys((fk,))

        call_args = mock_cursor.execute.call_args[0][0]
        assert "app.posts" in call_args or ("app" in call_args and "posts" in call_args)

    def test_create_multiple_foreign_keys(self) -> None:
        """Should create multiple foreign keys."""
        from bani.domain.schema import ForeignKeyDefinition

        connector = PostgreSQLConnector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        fks = (
            ForeignKeyDefinition(
                name="fk_1",
                source_table="public.t1",
                source_columns=("id",),
                referenced_table="public.t2",
                referenced_columns=("id",),
            ),
            ForeignKeyDefinition(
                name="fk_2",
                source_table="public.t3",
                source_columns=("id",),
                referenced_table="public.t4",
                referenced_columns=("id",),
            ),
        )

        connector.create_foreign_keys(fks)

        assert mock_cursor.execute.call_count == 2


class TestExecuteSQLDelegation:
    """Tests for SQL execution."""

    def test_execute_sql_executes_statement(self) -> None:
        """Should execute arbitrary SQL."""
        connector = PostgreSQLConnector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        connector.connection = mock_conn

        connector.execute_sql("DROP TABLE test")

        mock_cursor.execute.assert_called_once_with("DROP TABLE test")
