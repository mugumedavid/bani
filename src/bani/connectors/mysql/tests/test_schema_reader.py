"""Unit tests for MySQL schema reader (mocked DB)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from bani.connectors.mysql.schema_reader import MySQLSchemaReader


class FakeCursor:
    """A fake cursor that returns preset results for specific queries."""

    def __init__(self, results: dict[str, list[tuple[Any, ...]]]) -> None:
        """Initialize with query -> results mapping."""
        self._results = results
        self._current_results: list[tuple[Any, ...]] = []

    def execute(self, query: str, args: tuple[Any, ...] | None = None) -> None:
        """Store results for the matching query pattern."""
        for pattern, results in self._results.items():
            if pattern in query:
                self._current_results = results
                return
        self._current_results = []

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return preset results."""
        return self._current_results

    def fetchone(self) -> tuple[Any, ...] | None:
        """Return first result or None."""
        return self._current_results[0] if self._current_results else None

    def __enter__(self) -> FakeCursor:
        """Support context manager."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Support context manager."""
        pass


class TestMySQLSchemaReader:
    """Tests for MySQL schema reading."""

    def _make_connection(self, results: dict[str, list[tuple[Any, ...]]]) -> MagicMock:
        """Create a mock connection with preset cursor results."""
        connection = MagicMock()
        cursor = FakeCursor(results)
        connection.cursor.return_value = cursor
        return connection

    def test_read_schema_returns_database_schema(self) -> None:
        """Should return a DatabaseSchema with mysql dialect."""
        connection = self._make_connection({"information_schema.tables": []})
        reader = MySQLSchemaReader(connection, "test_db")

        schema = reader.read_schema()

        assert schema.source_dialect == "mysql"
        assert len(schema.tables) == 0

    def test_read_tables_with_columns(self) -> None:
        """Should read tables with their columns."""
        connection = self._make_connection(
            {
                "information_schema.tables": [
                    ("test_db", "users", 100),
                ],
                "information_schema.columns": [
                    ("test_db", "users", "id", "int", "NO", None, 1, "auto_increment"),
                    ("test_db", "users", "name", "varchar(255)", "YES", None, 2, ""),
                ],
                "constraint_name = 'PRIMARY'": [],
                "information_schema.statistics": [],
                "information_schema.referential_constraints": [],
                "check_constraints": [],
            }
        )
        reader = MySQLSchemaReader(connection, "test_db")

        schema = reader.read_schema()

        assert len(schema.tables) == 1
        table = schema.tables[0]
        assert table.table_name == "users"
        assert table.schema_name == "test_db"
        assert table.row_count_estimate == 100
        assert len(table.columns) == 2
        assert table.columns[0].name == "id"
        assert table.columns[0].is_auto_increment is True
        assert table.columns[0].nullable is False
        assert table.columns[1].name == "name"
        assert table.columns[1].nullable is True

    def test_read_primary_key(self) -> None:
        """Should read primary key columns."""
        connection = self._make_connection(
            {
                "information_schema.tables": [
                    ("test_db", "users", 50),
                ],
                "information_schema.columns": [
                    (
                        "test_db",
                        "users",
                        "id",
                        "int",
                        "NO",
                        None,
                        1,
                        "auto_increment",
                    ),
                ],
                "constraint_name = 'PRIMARY'": [
                    ("test_db", "users", "id"),
                ],
                "information_schema.statistics": [],
                "information_schema.referential_constraints": [],
                "check_constraints": [],
            }
        )

        reader = MySQLSchemaReader(connection, "test_db")
        schema = reader.read_schema()

        assert len(schema.tables) == 1
        assert schema.tables[0].primary_key == ("id",)

    def test_read_schema_handles_null_row_count(self) -> None:
        """Should handle NULL table_rows gracefully."""
        connection = self._make_connection(
            {
                "information_schema.tables": [
                    ("test_db", "empty_table", None),
                ],
                "information_schema.columns": [],
                "constraint_name = 'PRIMARY'": [],
                "information_schema.statistics": [],
                "information_schema.referential_constraints": [],
                "check_constraints": [],
            }
        )
        reader = MySQLSchemaReader(connection, "test_db")

        schema = reader.read_schema()

        assert len(schema.tables) == 1
        assert schema.tables[0].row_count_estimate is None
