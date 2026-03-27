"""Unit tests for MSSQL data writer."""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa  # type: ignore[import-untyped]

from bani.connectors.mssql.data_writer import MSSQLDataWriter


class TestMSSQLDataWriter:
    """Tests for MSSQLDataWriter."""

    def test_data_writer_init(self) -> None:
        """Test data writer initialization."""
        mock_connection = MagicMock()
        writer = MSSQLDataWriter(mock_connection)

        assert writer.connection is mock_connection

    def test_write_batch_empty(self) -> None:
        """Test write_batch with empty batch returns 0."""
        mock_connection = MagicMock()
        writer = MSSQLDataWriter(mock_connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=pa.int32()), pa.array([], type=pa.string())],
            names=["id", "name"],
        )

        rows_written = writer.write_batch("test_table", "dbo", batch)
        assert rows_written == 0

    def test_write_batch_single_row(self) -> None:
        """Test write_batch with single row."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        writer = MSSQLDataWriter(mock_connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int32()), pa.array(["Alice"], type=pa.string())],
            names=["id", "name"],
        )

        rows_written = writer.write_batch("test_table", "dbo", batch)
        assert rows_written == 1

    def test_write_batch_multiple_rows(self) -> None:
        """Test write_batch with multiple rows."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        writer = MSSQLDataWriter(mock_connection)

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2, 3], type=pa.int32()),
                pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
            ],
            names=["id", "name"],
        )

        rows_written = writer.write_batch("test_table", "dbo", batch)
        assert rows_written == 3

    def test_write_batch_with_nulls(self) -> None:
        """Test write_batch handles NULL values."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        writer = MSSQLDataWriter(mock_connection)

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2, None], type=pa.int32()),
                pa.array(["Alice", None, "Charlie"], type=pa.string()),
            ],
            names=["id", "name"],
        )

        rows_written = writer.write_batch("test_table", "dbo", batch)
        assert rows_written == 3

        # Verify execute was called (multi-row INSERT, not executemany)
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert call_args is not None

    def test_write_batch_calls_commit(self) -> None:
        """Test write_batch calls commit."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        writer = MSSQLDataWriter(mock_connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int32())], names=["id"]
        )

        writer.write_batch("test_table", "dbo", batch)
        mock_connection.commit.assert_called_once()
