"""Unit tests for SQLite connector."""

from __future__ import annotations

from collections.abc import Generator

import pyarrow as pa
import pytest

from bani.connectors.sqlite.connector import SQLiteConnector
from bani.domain.project import ConnectionConfig
from bani.domain.schema import (
    ColumnDefinition,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


@pytest.fixture()
def config() -> ConnectionConfig:
    """In-memory SQLite connection config."""
    return ConnectionConfig(dialect="sqlite", database=":memory:")


@pytest.fixture()
def connector(config: ConnectionConfig) -> Generator[SQLiteConnector, None, None]:
    """Connected SQLite connector."""
    c = SQLiteConnector()
    c.connect(config)
    yield c
    c.disconnect()


class TestConnect:
    """Tests for connect/disconnect."""

    def test_connect_memory(self) -> None:
        connector = SQLiteConnector()
        config = ConnectionConfig(dialect="sqlite", database=":memory:")
        connector.connect(config)
        assert connector.connection is not None
        connector.disconnect()

    def test_connect_missing_database_raises(self) -> None:
        connector = SQLiteConnector()
        config = ConnectionConfig(dialect="sqlite", database="")
        with pytest.raises(ValueError, match="requires 'database'"):
            connector.connect(config)

    def test_disconnect_cleans_up(self) -> None:
        connector = SQLiteConnector()
        config = ConnectionConfig(dialect="sqlite", database=":memory:")
        connector.connect(config)
        connector.disconnect()
        assert connector.connection is None
        assert connector._schema_reader is None
        assert connector._data_reader is None
        assert connector._data_writer is None

    def test_disconnect_when_not_connected(self) -> None:
        connector = SQLiteConnector()
        connector.disconnect()  # Should not raise


class TestNotConnected:
    """Tests that methods raise when not connected."""

    def test_introspect_not_connected(self) -> None:
        connector = SQLiteConnector()
        with pytest.raises(RuntimeError, match="not connected"):
            connector.introspect_schema()

    def test_read_table_not_connected(self) -> None:
        connector = SQLiteConnector()
        with pytest.raises(RuntimeError, match="not connected"):
            connector.read_table("t", "main")

    def test_estimate_not_connected(self) -> None:
        connector = SQLiteConnector()
        with pytest.raises(RuntimeError, match="not connected"):
            connector.estimate_row_count("t", "main")

    def test_create_table_not_connected(self) -> None:
        connector = SQLiteConnector()
        td = TableDefinition(schema_name="main", table_name="t", columns=())
        with pytest.raises(RuntimeError, match="not connected"):
            connector.create_table(td)

    def test_write_batch_not_connected(self) -> None:
        connector = SQLiteConnector()
        batch = pa.RecordBatch.from_pydict({"a": [1]})
        with pytest.raises(RuntimeError, match="not connected"):
            connector.write_batch("t", "main", batch)

    def test_create_indexes_not_connected(self) -> None:
        connector = SQLiteConnector()
        with pytest.raises(RuntimeError, match="not connected"):
            connector.create_indexes("t", "main", ())

    def test_create_foreign_keys_not_connected(self) -> None:
        connector = SQLiteConnector()
        with pytest.raises(RuntimeError, match="not connected"):
            connector.create_foreign_keys(())

    def test_execute_sql_not_connected(self) -> None:
        connector = SQLiteConnector()
        with pytest.raises(RuntimeError, match="not connected"):
            connector.execute_sql("SELECT 1")


class TestCreateTable:
    """Tests for create_table."""

    def test_create_simple_table(self, connector: SQLiteConnector) -> None:
        table_def = TableDefinition(
            schema_name="main",
            table_name="users",
            columns=(
                ColumnDefinition(
                    name="id",
                    data_type="INTEGER",
                    nullable=False,
                    ordinal_position=0,
                    arrow_type_str="int64",
                ),
                ColumnDefinition(
                    name="name",
                    data_type="TEXT",
                    nullable=True,
                    ordinal_position=1,
                    arrow_type_str="string",
                ),
            ),
            primary_key=("id",),
        )
        connector.create_table(table_def)

        # Verify table exists
        assert connector.connection is not None
        cursor = connector.connection.cursor()
        cursor.execute("PRAGMA table_info('users')")
        cols = cursor.fetchall()
        assert len(cols) == 2
        assert cols[0][1] == "id"
        assert cols[1][1] == "name"

    def test_create_table_with_check_constraint(
        self, connector: SQLiteConnector
    ) -> None:
        table_def = TableDefinition(
            schema_name="main",
            table_name="products",
            columns=(
                ColumnDefinition(
                    name="id",
                    data_type="INTEGER",
                    nullable=False,
                    ordinal_position=0,
                    arrow_type_str="int64",
                ),
                ColumnDefinition(
                    name="price",
                    data_type="REAL",
                    nullable=False,
                    ordinal_position=1,
                    arrow_type_str="double",
                ),
            ),
            primary_key=("id",),
            check_constraints=("(price > 0)",),
        )
        connector.create_table(table_def)

        # Verify table exists
        assert connector.connection is not None
        cursor = connector.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='products'")
        assert cursor.fetchone()[0] == 1


class TestWriteAndRead:
    """Tests for writing and reading data."""

    def test_write_and_read_roundtrip(self, connector: SQLiteConnector) -> None:
        # Create table
        table_def = TableDefinition(
            schema_name="main",
            table_name="data",
            columns=(
                ColumnDefinition(
                    name="id",
                    data_type="INTEGER",
                    nullable=False,
                    ordinal_position=0,
                    arrow_type_str="int64",
                ),
                ColumnDefinition(
                    name="value",
                    data_type="TEXT",
                    nullable=True,
                    ordinal_position=1,
                    arrow_type_str="string",
                ),
            ),
            primary_key=("id",),
        )
        connector.create_table(table_def)

        # Write data
        batch = pa.RecordBatch.from_pydict(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "value": pa.array(["a", "b", "c"], type=pa.string()),
            }
        )
        rows_written = connector.write_batch("data", "main", batch)
        assert rows_written == 3

        # Read data back
        batches = list(connector.read_table("data", "main"))
        assert len(batches) == 1
        result = batches[0]
        assert result.num_rows == 3
        assert result.column("id").to_pylist() == [1, 2, 3]
        assert result.column("value").to_pylist() == ["a", "b", "c"]

    def test_write_empty_batch(self, connector: SQLiteConnector) -> None:
        table_def = TableDefinition(
            schema_name="main",
            table_name="empty",
            columns=(
                ColumnDefinition(
                    name="id",
                    data_type="INTEGER",
                    nullable=False,
                    ordinal_position=0,
                    arrow_type_str="int64",
                ),
            ),
            primary_key=("id",),
        )
        connector.create_table(table_def)

        batch = pa.RecordBatch.from_pydict(
            {
                "id": pa.array([], type=pa.int64()),
            }
        )
        rows_written = connector.write_batch("empty", "main", batch)
        assert rows_written == 0

    def test_write_with_nulls(self, connector: SQLiteConnector) -> None:
        table_def = TableDefinition(
            schema_name="main",
            table_name="nullable",
            columns=(
                ColumnDefinition(
                    name="id",
                    data_type="INTEGER",
                    nullable=False,
                    ordinal_position=0,
                    arrow_type_str="int64",
                ),
                ColumnDefinition(
                    name="value",
                    data_type="TEXT",
                    nullable=True,
                    ordinal_position=1,
                    arrow_type_str="string",
                ),
            ),
            primary_key=("id",),
        )
        connector.create_table(table_def)

        batch = pa.RecordBatch.from_pydict(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "value": pa.array(["hello", None], type=pa.string()),
            }
        )
        rows_written = connector.write_batch("nullable", "main", batch)
        assert rows_written == 2

        batches = list(connector.read_table("nullable", "main"))
        result = batches[0]
        assert result.column("value").to_pylist() == ["hello", None]

    def test_read_with_filter(self, connector: SQLiteConnector) -> None:
        table_def = TableDefinition(
            schema_name="main",
            table_name="filtered",
            columns=(
                ColumnDefinition(
                    name="id",
                    data_type="INTEGER",
                    nullable=False,
                    ordinal_position=0,
                    arrow_type_str="int64",
                ),
                ColumnDefinition(
                    name="name",
                    data_type="TEXT",
                    nullable=True,
                    ordinal_position=1,
                    arrow_type_str="string",
                ),
            ),
            primary_key=("id",),
        )
        connector.create_table(table_def)

        batch = pa.RecordBatch.from_pydict(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "name": pa.array(["alice", "bob", "charlie"], type=pa.string()),
            }
        )
        connector.write_batch("filtered", "main", batch)

        # Read with filter
        batches = list(connector.read_table("filtered", "main", filter_sql="id > 1"))
        assert len(batches) == 1
        assert batches[0].num_rows == 2

    def test_read_specific_columns(self, connector: SQLiteConnector) -> None:
        table_def = TableDefinition(
            schema_name="main",
            table_name="cols",
            columns=(
                ColumnDefinition(
                    name="id",
                    data_type="INTEGER",
                    nullable=False,
                    ordinal_position=0,
                    arrow_type_str="int64",
                ),
                ColumnDefinition(
                    name="a",
                    data_type="TEXT",
                    nullable=True,
                    ordinal_position=1,
                    arrow_type_str="string",
                ),
                ColumnDefinition(
                    name="b",
                    data_type="TEXT",
                    nullable=True,
                    ordinal_position=2,
                    arrow_type_str="string",
                ),
            ),
            primary_key=("id",),
        )
        connector.create_table(table_def)

        batch = pa.RecordBatch.from_pydict(
            {
                "id": pa.array([1], type=pa.int64()),
                "a": pa.array(["x"], type=pa.string()),
                "b": pa.array(["y"], type=pa.string()),
            }
        )
        connector.write_batch("cols", "main", batch)

        batches = list(connector.read_table("cols", "main", columns=["id", "b"]))
        assert batches[0].schema.names == ["id", "b"]


class TestEstimateRowCount:
    """Tests for estimate_row_count."""

    def test_estimate_with_count(self, connector: SQLiteConnector) -> None:
        assert connector.connection is not None
        connector.connection.execute("CREATE TABLE counting (id INTEGER)")
        connector.connection.execute("INSERT INTO counting VALUES (1)")
        connector.connection.execute("INSERT INTO counting VALUES (2)")
        connector.connection.commit()

        count = connector.estimate_row_count("counting", "main")
        assert count == 2


class TestIntrospectSchema:
    """Tests for introspect_schema."""

    def test_introspect_empty_database(self, connector: SQLiteConnector) -> None:
        schema = connector.introspect_schema()
        assert schema.source_dialect == "sqlite"
        assert len(schema.tables) == 0

    def test_introspect_with_table(self, connector: SQLiteConnector) -> None:
        assert connector.connection is not None
        connector.connection.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER DEFAULT 0
            )
        """)
        connector.connection.commit()

        schema = connector.introspect_schema()
        assert len(schema.tables) == 1

        table = schema.tables[0]
        assert table.table_name == "users"
        assert table.schema_name == "main"
        assert len(table.columns) == 4
        assert table.primary_key == ("id",)

        # Check INTEGER PRIMARY KEY is marked as auto_increment
        id_col = table.columns[0]
        assert id_col.name == "id"
        assert id_col.is_auto_increment is True
        assert id_col.arrow_type_str == "int64"

        name_col = table.columns[1]
        assert name_col.name == "name"
        assert name_col.nullable is False
        assert name_col.arrow_type_str == "string"

    def test_introspect_with_indexes(self, connector: SQLiteConnector) -> None:
        assert connector.connection is not None
        connector.connection.execute("""
            CREATE TABLE indexed (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT
            )
        """)
        connector.connection.execute("CREATE UNIQUE INDEX idx_email ON indexed(email)")
        connector.connection.execute("CREATE INDEX idx_name ON indexed(name)")
        connector.connection.commit()

        schema = connector.introspect_schema()
        table = schema.tables[0]

        assert len(table.indexes) == 2
        idx_names = {idx.name for idx in table.indexes}
        assert "idx_email" in idx_names
        assert "idx_name" in idx_names

        email_idx = next(idx for idx in table.indexes if idx.name == "idx_email")
        assert email_idx.is_unique is True

    def test_introspect_with_foreign_keys(self, connector: SQLiteConnector) -> None:
        assert connector.connection is not None
        connector.connection.execute("""
            CREATE TABLE parents (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        connector.connection.execute("""
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parents(id) ON DELETE CASCADE
            )
        """)
        connector.connection.commit()

        schema = connector.introspect_schema()
        children_table = schema.get_table("main", "children")
        assert children_table is not None
        assert len(children_table.foreign_keys) == 1

        fk = children_table.foreign_keys[0]
        assert fk.source_columns == ("parent_id",)
        assert fk.referenced_table == "main.parents"
        assert fk.referenced_columns == ("id",)
        assert fk.on_delete == "CASCADE"


class TestCreateIndexes:
    """Tests for create_indexes."""

    def test_create_unique_and_regular_indexes(
        self, connector: SQLiteConnector
    ) -> None:
        table_def = TableDefinition(
            schema_name="main",
            table_name="idx_test",
            columns=(
                ColumnDefinition(
                    name="id",
                    data_type="INTEGER",
                    nullable=False,
                    ordinal_position=0,
                    arrow_type_str="int64",
                ),
                ColumnDefinition(
                    name="email",
                    data_type="TEXT",
                    nullable=True,
                    ordinal_position=1,
                    arrow_type_str="string",
                ),
                ColumnDefinition(
                    name="name",
                    data_type="TEXT",
                    nullable=True,
                    ordinal_position=2,
                    arrow_type_str="string",
                ),
            ),
            primary_key=("id",),
        )
        connector.create_table(table_def)

        indexes = (
            IndexDefinition(
                name="idx_unique_email",
                columns=("email",),
                is_unique=True,
                is_clustered=False,
            ),
            IndexDefinition(
                name="idx_name",
                columns=("name",),
                is_unique=False,
                is_clustered=False,
            ),
        )
        connector.create_indexes("idx_test", "main", indexes)

        # Verify via introspection
        assert connector.connection is not None
        cursor = connector.connection.cursor()
        cursor.execute("PRAGMA index_list('idx_test')")
        idx_rows = cursor.fetchall()
        idx_names = {row[1] for row in idx_rows}
        assert "idx_unique_email" in idx_names
        assert "idx_name" in idx_names


class TestExecuteSql:
    """Tests for execute_sql."""

    def test_execute_arbitrary_sql(self, connector: SQLiteConnector) -> None:
        connector.execute_sql(
            "CREATE TABLE exec_test (id INTEGER PRIMARY KEY, val TEXT)"
        )

        assert connector.connection is not None
        cursor = connector.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='exec_test'")
        assert cursor.fetchone()[0] == 1


class TestForeignKeys:
    """Tests for create_foreign_keys (no-op in SQLite)."""

    def test_create_foreign_keys_is_noop(self, connector: SQLiteConnector) -> None:
        # Should not raise
        connector.create_foreign_keys(
            (
                ForeignKeyDefinition(
                    name="fk_test",
                    source_table="main.child",
                    source_columns=("parent_id",),
                    referenced_table="main.parent",
                    referenced_columns=("id",),
                ),
            )
        )
