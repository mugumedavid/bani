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

    def test_get_full_column_type_returns_formatted_type(self) -> None:
        """Should retrieve full column type from pg_catalog."""
        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        mock_cursor.fetchall.return_value = [("varchar(255)",)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)
        result = reader._get_full_column_type(
            "public", "users", "email", "character varying"
        )

        assert result == "varchar(255)"

    def test_get_full_column_type_fallback_to_base(self) -> None:
        """Should return base type if query returns no results."""
        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)
        result = reader._get_full_column_type("public", "users", "email", "text")

        assert result == "text"

    def test_read_columns_builds_definitions(self) -> None:
        """Should create column definitions from query results."""
        from unittest.mock import patch as mock_patch

        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        # Column name, data_type, is_nullable, default, ordinal
        mock_cursor.fetchall.return_value = [
            ("id", "integer", "NO", None, "1"),
            ("email", "text", "YES", None, "2"),
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)

        with mock_patch.object(
            reader,
            "_get_full_column_type",
            side_effect=lambda s, t, c, b: b,
        ):
            columns = reader._read_columns("public", "users")

            assert len(columns) == 2
            assert columns[0].name == "id"
            assert columns[0].nullable is False
            assert columns[1].name == "email"
            assert columns[1].nullable is True

    def test_read_columns_detects_auto_increment(self) -> None:
        """Should detect serial/bigserial as auto-increment."""
        from unittest.mock import patch as mock_patch

        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        mock_cursor.fetchall.return_value = [
            ("id", "integer", "NO", None, "1"),
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)

        with mock_patch.object(
            reader, "_get_full_column_type", return_value="bigserial"
        ):
            columns = reader._read_columns("public", "users")

            assert columns[0].is_auto_increment is True

    def test_read_primary_key_returns_columns(self) -> None:
        """Should read primary key columns in order."""
        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        mock_cursor.fetchall.return_value = [
            ("id",),
            ("user_id",),
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)
        result = reader._read_primary_key("public", "users")

        assert result == ["id", "user_id"]

    def test_read_primary_key_empty(self) -> None:
        """Should return empty list if no primary key."""
        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)
        result = reader._read_primary_key("public", "users")

        assert result == []

    def test_read_indexes_builds_definitions(self) -> None:
        """Should create index definitions from query results."""
        from unittest.mock import patch as mock_patch

        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        # name, is_unique, is_clustered, indexdef, columns
        mock_cursor.fetchall.return_value = [
            ("idx_email", False, False, "CREATE INDEX idx_email...", ["email"]),
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)

        with mock_patch.object(reader, "_extract_filter_expression", return_value=None):
            indexes = reader._read_indexes("public", "users")

            assert len(indexes) == 1
            assert indexes[0].name == "idx_email"
            assert indexes[0].columns == ("email",)
            assert indexes[0].is_unique is False

    def test_read_indexes_with_unique(self) -> None:
        """Should detect unique indexes."""
        from unittest.mock import patch as mock_patch

        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        mock_cursor.fetchall.return_value = [
            (
                "idx_username",
                True,
                False,
                "CREATE UNIQUE INDEX...",
                ["username"],
            ),
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)

        with mock_patch.object(reader, "_extract_filter_expression", return_value=None):
            indexes = reader._read_indexes("public", "users")

            assert indexes[0].is_unique is True

    def test_read_foreign_keys_builds_definitions(self) -> None:
        """Should create foreign key definitions."""
        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        # constraint_name, src_schema, src_table, src_cols, ref_schema,
        # ref_table, ref_cols, update_rule, delete_rule
        mock_cursor.fetchall.return_value = [
            (
                "fk_user_id",
                "public",
                "posts",
                ["user_id"],
                "public",
                "users",
                ["id"],
                "NO ACTION",
                "CASCADE",
            ),
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)
        fks = reader._read_foreign_keys("public", "posts")

        assert len(fks) == 1
        assert fks[0].name == "fk_user_id"
        assert fks[0].source_table == "public.posts"
        assert fks[0].referenced_table == "public.users"
        assert fks[0].on_delete == "CASCADE"
        assert fks[0].on_update == "NO ACTION"

    def test_read_check_constraints_extracts_conditions(self) -> None:
        """Should extract check constraints without CHECK keyword."""
        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        mock_cursor.fetchall.return_value = [
            ("CHECK (age >= 18)",),
            ("CHECK (status IN ('active', 'inactive'))",),
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)
        constraints = reader._read_check_constraints("public", "users")

        assert len(constraints) == 2
        assert constraints[0] == "(age >= 18)"
        assert constraints[1] == "(status IN ('active', 'inactive'))"

    def test_read_check_constraints_empty(self) -> None:
        """Should return empty list if no check constraints."""
        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)
        constraints = reader._read_check_constraints("public", "users")

        assert constraints == []

    def test_read_check_constraints_without_check_prefix(self) -> None:
        """Should handle constraints not prefixed with CHECK."""
        mock_conn = self._make_mock_connection()
        mock_cursor = self._make_mock_cursor()
        # Some systems might return without CHECK prefix
        mock_cursor.fetchall.return_value = [
            ("(age >= 18)",),
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        reader = PostgreSQLSchemaReader(mock_conn)
        constraints = reader._read_check_constraints("public", "users")

        assert len(constraints) == 1
        assert constraints[0] == "(age >= 18)"
