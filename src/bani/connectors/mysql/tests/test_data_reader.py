"""Unit tests for MySQL data reader (mocked DB)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa

from bani.connectors.mysql.data_reader import MySQLDataReader


class TestMySQLDataReaderEstimate:
    """Tests for row count estimation."""

    def test_estimate_from_information_schema(self) -> None:
        """Should estimate row count from information_schema."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.fetchone.return_value = (1000,)
        connection.cursor.return_value = cursor

        reader = MySQLDataReader(connection)
        result = reader.estimate_row_count("users", "test_db")

        assert result == 1000

    def test_estimate_fallback_to_count(self) -> None:
        """Should fall back to COUNT(*) if information_schema returns None."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        # First call returns None (info_schema), second returns count
        cursor.fetchone.side_effect = [(None,), (500,)]
        connection.cursor.return_value = cursor

        reader = MySQLDataReader(connection)
        result = reader.estimate_row_count("users", "test_db")

        assert result == 500


class TestMySQLDataReaderBatch:
    """Tests for record batch creation."""

    def test_make_record_batch_creates_valid_batch(self) -> None:
        """Should create a valid Arrow RecordBatch from rows."""
        connection = MagicMock()
        reader = MySQLDataReader(connection)

        rows: list[tuple[Any, ...]] = [
            (1, "Alice"),
            (2, "Bob"),
        ]
        col_names = ["id", "name"]
        arrow_types: list[pa.DataType] = [pa.int64(), pa.string()]

        batch = reader._make_record_batch(rows, col_names, arrow_types)

        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_rows == 2
        assert batch.num_columns == 2
        assert batch.schema.names == ["id", "name"]

    def test_make_record_batch_handles_none_values(self) -> None:
        """Should handle None values in rows."""
        connection = MagicMock()
        reader = MySQLDataReader(connection)

        rows: list[tuple[Any, ...]] = [
            (1, None),
            (2, "Bob"),
        ]
        col_names = ["id", "name"]
        arrow_types: list[pa.DataType] = [pa.int64(), pa.string()]

        batch = reader._make_record_batch(rows, col_names, arrow_types)

        assert batch.num_rows == 2
        assert batch[1][0].as_py() is None
        assert batch[1][1].as_py() == "Bob"
