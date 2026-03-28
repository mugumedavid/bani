"""Unit tests for MSSQL data reader."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa  # type: ignore[import-untyped]

from bani.connectors.mssql.data_reader import MSSQLDataReader


class TestMSSQLDataReader:
    """Tests for MSSQLDataReader."""

    def test_data_reader_init_default_driver(self) -> None:
        """Test data reader defaults to pymssql driver."""
        mock_connection = MagicMock()
        reader = MSSQLDataReader(mock_connection)

        assert reader.connection is mock_connection
        assert reader.type_mapper is not None
        assert reader._ph == "%s"

    def test_data_reader_init_pyodbc_driver(self) -> None:
        """Test data reader with pyodbc driver uses ? placeholder."""
        mock_connection = MagicMock()
        reader = MSSQLDataReader(mock_connection, driver="pyodbc")

        assert reader._ph == "?"

    def test_data_reader_init_pymssql_driver(self) -> None:
        """Test data reader with pymssql driver uses %s placeholder."""
        mock_connection = MagicMock()
        reader = MSSQLDataReader(mock_connection, driver="pymssql")

        assert reader._ph == "%s"

    def test_estimate_row_count_fallback_to_count(self) -> None:
        """Test estimate_row_count falls back to COUNT(*)."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (100,)
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        reader = MSSQLDataReader(mock_connection)
        count = reader.estimate_row_count("test_table", "dbo")

        assert count == 100

    def test_estimate_row_count_no_rows(self) -> None:
        """Test estimate_row_count returns 0 when table is empty."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0,)
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        reader = MSSQLDataReader(mock_connection)
        count = reader.estimate_row_count("test_table", "dbo")

        assert count == 0

    def test_make_record_batch(self) -> None:
        """Test _make_record_batch creates valid Arrow batch."""
        mock_connection = MagicMock()
        reader = MSSQLDataReader(mock_connection)

        rows = [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie"),
        ]
        col_names = ["id", "name"]
        arrow_types = [pa.int32(), pa.string()]

        batch = reader._make_record_batch(rows, col_names, arrow_types)

        assert batch.num_rows == 3
        assert batch.num_columns == 2
        assert batch.schema.names == col_names
        assert batch.column(0).to_pylist() == [1, 2, 3]
        assert batch.column(1).to_pylist() == ["Alice", "Bob", "Charlie"]

    def test_make_record_batch_with_nulls(self) -> None:
        """Test _make_record_batch handles NULL values."""
        mock_connection = MagicMock()
        reader = MSSQLDataReader(mock_connection)

        rows = [
            (1, "Alice"),
            (2, None),
            (3, "Charlie"),
        ]
        col_names = ["id", "name"]
        arrow_types = [pa.int32(), pa.string()]

        batch = reader._make_record_batch(rows, col_names, arrow_types)

        assert batch.num_rows == 3
        assert batch.column(1).to_pylist() == ["Alice", None, "Charlie"]

    def test_make_record_batch_empty(self) -> None:
        """Test _make_record_batch handles empty rows."""
        mock_connection = MagicMock()
        reader = MSSQLDataReader(mock_connection)

        rows: list[tuple[Any, ...]] = []
        col_names = ["id", "name"]
        arrow_types = [pa.int32(), pa.string()]

        batch = reader._make_record_batch(rows, col_names, arrow_types)

        assert batch.num_rows == 0
        assert batch.num_columns == 2
