"""Unit tests for PostgreSQL schema reader (mocked DB interactions)."""

from __future__ import annotations

from unittest.mock import MagicMock

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
        mock_conn = self._make_mock_connection()
        reader = PostgreSQLSchemaReader(mock_conn)

        assert reader._is_auto_increment("serial") is True
        assert reader._is_auto_increment("SERIAL") is True
        assert reader._is_auto_increment("bigserial") is True
        assert reader._is_auto_increment("smallserial") is True
        assert reader._is_auto_increment("integer") is False

    def test_is_auto_increment_with_params(self) -> None:
        """Should detect auto-increment even with type parameters."""
        mock_conn = self._make_mock_connection()
        reader = PostgreSQLSchemaReader(mock_conn)

        assert reader._is_auto_increment("serial NOT NULL") is True
        assert reader._is_auto_increment("BIGSERIAL PRIMARY KEY") is True

    def test_extract_filter_expression_with_null(self) -> None:
        """Should return None when no filter expression exists."""
        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)
        result = reader._extract_filter_expression("test_idx", "public")

        assert result is None

    def test_extract_filter_expression_with_value(self) -> None:
        """Should extract filter expression when present."""
        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        mock_cursor.fetchall.return_value = [("active = true",)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)
        result = reader._extract_filter_expression("test_idx", "public")

        assert result == "active = true"

    def test_estimate_row_count_with_stats(self) -> None:
        """Should return row count from pg_stat_user_tables."""
        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        mock_cursor.fetchall.return_value = [("12345",)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)
        result = reader._estimate_row_count("test_table", "public")

        assert result == 12345

    def test_estimate_row_count_when_unavailable(self) -> None:
        """Should return None when stats are unavailable."""
        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)
        result = reader._estimate_row_count("test_table", "public")

        assert result is None

    def test_estimate_row_count_on_error(self) -> None:
        """Should return None if query raises exception."""
        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        mock_cursor.execute.side_effect = Exception("Connection error")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)
        result = reader._estimate_row_count("test_table", "public")

        assert result is None

    def test_column_definition_creation(self) -> None:
        """Should create column definitions with proper attributes."""
        # This is a basic integration test for the data structures
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
