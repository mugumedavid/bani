"""Unit tests for PostgreSQL schema reader (mocked DB interactions)."""

from __future__ import annotations

from unittest.mock import MagicMock, call

from bani.connectors.postgresql.schema_reader import PostgreSQLSchemaReader
from bani.domain.schema import ColumnDefinition, IndexDefinition, TableDefinition


class TestPostgreSQLSchemaReader:
    """Tests for schema reader with mocked connection."""

    def _make_mock_cursor(self) -> MagicMock:
        """Create a mock cursor."""
        return MagicMock()

    def _make_mock_connection(self) -> MagicMock:
        """Create a mock connection that returns a mock cursor."""
        mock_conn = MagicMock()
        mock_cursor = self._make_mock_cursor()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=None)
        return mock_conn

    def test_init_stores_connection(self) -> None:
        """Reader should store the connection."""
        mock_conn = self._make_mock_connection()
        reader = PostgreSQLSchemaReader(mock_conn)
        assert reader.connection is mock_conn

    def test_is_auto_increment_detects_serial(self) -> None:
        """Should detect serial/bigserial types as auto-increment."""
        assert PostgreSQLSchemaReader._is_auto_increment("serial") is True
        assert PostgreSQLSchemaReader._is_auto_increment("SERIAL") is True
        assert PostgreSQLSchemaReader._is_auto_increment("bigserial") is True
        assert PostgreSQLSchemaReader._is_auto_increment("smallserial") is True
        assert PostgreSQLSchemaReader._is_auto_increment("integer") is False

    def test_is_auto_increment_with_params(self) -> None:
        """Should detect auto-increment even with type parameters."""
        assert PostgreSQLSchemaReader._is_auto_increment("serial NOT NULL") is True
        assert (
            PostgreSQLSchemaReader._is_auto_increment("BIGSERIAL PRIMARY KEY") is True
        )

    def test_parse_pg_array_from_list(self) -> None:
        """Should handle Python list input."""
        result = PostgreSQLSchemaReader._parse_pg_array(["col1", "col2"])
        assert result == ("col1", "col2")

    def test_parse_pg_array_from_string(self) -> None:
        """Should handle PG array literal string."""
        result = PostgreSQLSchemaReader._parse_pg_array("{col1,col2}")
        assert result == ("col1", "col2")

    def test_parse_pg_array_empty(self) -> None:
        """Should handle empty array."""
        result = PostgreSQLSchemaReader._parse_pg_array("{}")
        assert result == ()

    def test_column_definition_creation(self) -> None:
        """Should create column definitions with proper attributes."""
        col = ColumnDefinition(
            name="id",
            data_type="INTEGER",
            nullable=False,
            is_auto_increment=True,
            ordinal_position=0,
        )

        assert col.name == "id"
        assert col.data_type == "INTEGER"
        assert col.nullable is False
        assert col.is_auto_increment is True
        assert col.ordinal_position == 0

    def test_index_definition_creation(self) -> None:
        """Should create index definitions with proper attributes."""
        index = IndexDefinition(
            name="idx_name",
            columns=("col1", "col2"),
            is_unique=True,
            is_clustered=False,
            filter_expression="active = true",
        )

        assert index.name == "idx_name"
        assert index.columns == ("col1", "col2")
        assert index.is_unique is True
        assert index.is_clustered is False
        assert index.filter_expression == "active = true"

    def test_table_definition_creation(self) -> None:
        """Should create table definitions with all metadata."""
        table = TableDefinition(
            schema_name="public",
            table_name="users",
            columns=(
                ColumnDefinition(name="id", data_type="INTEGER", ordinal_position=0),
                ColumnDefinition(name="name", data_type="TEXT", ordinal_position=1),
            ),
            primary_key=("id",),
            indexes=(),
            foreign_keys=(),
            check_constraints=(),
            row_count_estimate=100,
        )

        assert table.schema_name == "public"
        assert table.table_name == "users"
        assert len(table.columns) == 2
        assert table.primary_key == ("id",)
        assert table.fully_qualified_name == "public.users"

    def test_read_schema_returns_database_schema(self) -> None:
        """Should return DatabaseSchema with tables."""
        from unittest.mock import patch as mock_patch

        mock_conn = self._make_mock_connection()
        reader = PostgreSQLSchemaReader(mock_conn)

        with mock_patch.object(
            reader,
            "_read_tables",
            return_value=[
                TableDefinition(
                    schema_name="public",
                    table_name="users",
                    columns=(
                        ColumnDefinition(
                            name="id",
                            data_type="INTEGER",
                            ordinal_position=0,
                        ),
                    ),
                )
            ],
        ):
            result = reader.read_schema()

            assert result.source_dialect == "postgresql"
            assert len(result.tables) == 1
            assert result.tables[0].table_name == "users"

    def test_read_tables_assembles_from_bulk_queries(self) -> None:
        """Should issue 7 bulk queries and assemble TableDefinitions."""
        from unittest.mock import patch as mock_patch

        mock_conn = self._make_mock_connection()
        reader = PostgreSQLSchemaReader(mock_conn)

        table_key = ("public", "users")
        col = ColumnDefinition(
            name="id", data_type="integer", ordinal_position=0
        )

        with (
            mock_patch.object(
                reader, "_fetch_table_list", return_value=[table_key]
            ),
            mock_patch.object(
                reader, "_fetch_all_columns", return_value={table_key: [col]}
            ),
            mock_patch.object(
                reader, "_fetch_all_primary_keys", return_value={table_key: ["id"]}
            ),
            mock_patch.object(
                reader, "_fetch_all_indexes", return_value={}
            ),
            mock_patch.object(
                reader, "_fetch_all_foreign_keys", return_value={}
            ),
            mock_patch.object(
                reader, "_fetch_all_check_constraints", return_value={}
            ),
            mock_patch.object(
                reader, "_fetch_all_row_counts", return_value={table_key: 42}
            ),
        ):
            tables = reader._read_tables()

            assert len(tables) == 1
            t = tables[0]
            assert t.schema_name == "public"
            assert t.table_name == "users"
            assert t.columns == (col,)
            assert t.primary_key == ("id",)
            assert t.indexes == ()
            assert t.foreign_keys == ()
            assert t.check_constraints == ()
            assert t.row_count_estimate == 42

    def test_read_tables_empty_database(self) -> None:
        """Should return empty list when no tables exist."""
        from unittest.mock import patch as mock_patch

        mock_conn = self._make_mock_connection()
        reader = PostgreSQLSchemaReader(mock_conn)

        with mock_patch.object(reader, "_fetch_table_list", return_value=[]):
            tables = reader._read_tables()
            assert tables == []
