"""Unit tests for MSSQL data writer."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa

from bani.connectors.mssql.data_writer import MSSQLDataWriter


class TestMSSQLDataWriterPymssql:
    """Tests for MSSQLDataWriter using the pymssql driver."""

    def test_data_writer_init(self) -> None:
        """Test data writer initialization with pymssql driver."""
        mock_connection = MagicMock()
        writer = MSSQLDataWriter(mock_connection, driver="pymssql")

        assert writer.connection is mock_connection
        assert writer._driver == "pymssql"

    def test_data_writer_init_default_driver(self) -> None:
        """Test data writer defaults to pymssql driver."""
        mock_connection = MagicMock()
        writer = MSSQLDataWriter(mock_connection)

        assert writer._driver == "pymssql"

    def test_write_batch_empty(self) -> None:
        """Test write_batch with empty batch returns 0."""
        mock_connection = MagicMock()
        writer = MSSQLDataWriter(mock_connection, driver="pymssql")

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

        writer = MSSQLDataWriter(mock_connection, driver="pymssql")

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

        writer = MSSQLDataWriter(mock_connection, driver="pymssql")

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

        writer = MSSQLDataWriter(mock_connection, driver="pymssql")

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

        writer = MSSQLDataWriter(mock_connection, driver="pymssql")

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int32())], names=["id"]
        )

        writer.write_batch("test_table", "dbo", batch)
        mock_connection.commit.assert_called_once()


class TestMSSQLDataWriterPyodbc:
    """Tests for MSSQLDataWriter using the pyodbc driver."""

    def test_data_writer_init(self) -> None:
        """Test data writer initialization with pyodbc driver."""
        mock_connection = MagicMock()
        writer = MSSQLDataWriter(mock_connection, driver="pyodbc")

        assert writer.connection is mock_connection
        assert writer._driver == "pyodbc"

    def test_write_batch_empty(self) -> None:
        """Test write_batch with empty batch returns 0."""
        mock_connection = MagicMock()
        writer = MSSQLDataWriter(mock_connection, driver="pyodbc")

        batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=pa.int32()), pa.array([], type=pa.string())],
            names=["id", "name"],
        )

        rows_written = writer.write_batch("test_table", "dbo", batch)
        assert rows_written == 0

    def test_write_batch_single_row(self) -> None:
        """Test write_batch with single row uses fast_executemany."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        writer = MSSQLDataWriter(mock_connection, driver="pyodbc")

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int32()), pa.array(["Alice"], type=pa.string())],
            names=["id", "name"],
        )

        rows_written = writer.write_batch("test_table", "dbo", batch)
        assert rows_written == 1

        # Verify fast_executemany was enabled
        assert mock_cursor.fast_executemany is True

        # Verify executemany was called with ? placeholders
        mock_cursor.executemany.assert_called_once()
        call_args = mock_cursor.executemany.call_args
        assert call_args is not None
        sql: str = call_args[0][0]
        assert "?" in sql
        assert "%s" not in sql
        rows_arg: list[Any] = call_args[0][1]
        assert rows_arg == [(1, "Alice")]

        mock_cursor.close.assert_called_once()
        mock_connection.commit.assert_called_once()

    def test_write_batch_multiple_rows(self) -> None:
        """Test write_batch with multiple rows."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        writer = MSSQLDataWriter(mock_connection, driver="pyodbc")

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2, 3], type=pa.int32()),
                pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
            ],
            names=["id", "name"],
        )

        rows_written = writer.write_batch("test_table", "dbo", batch)
        assert rows_written == 3

        call_args = mock_cursor.executemany.call_args
        assert call_args is not None
        rows_arg = call_args[0][1]
        assert len(rows_arg) == 3
        assert rows_arg[0] == (1, "Alice")
        assert rows_arg[1] == (2, "Bob")
        assert rows_arg[2] == (3, "Charlie")

    def test_write_batch_with_nulls(self) -> None:
        """Test write_batch handles NULL values as None."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        writer = MSSQLDataWriter(mock_connection, driver="pyodbc")

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2, None], type=pa.int32()),
                pa.array(["Alice", None, "Charlie"], type=pa.string()),
            ],
            names=["id", "name"],
        )

        rows_written = writer.write_batch("test_table", "dbo", batch)
        assert rows_written == 3

        call_args = mock_cursor.executemany.call_args
        assert call_args is not None
        rows_arg = call_args[0][1]
        assert rows_arg[0] == (1, "Alice")
        assert rows_arg[1] == (2, None)
        assert rows_arg[2] == (None, "Charlie")

    def test_write_batch_json_values_converted(self) -> None:
        """Test that dict/list values are converted to JSON strings."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        _writer = MSSQLDataWriter(mock_connection, driver="pyodbc")

        # Arrow doesn't have a native dict type, but values come through
        # as Python dicts when read from JSON columns via .as_py()
        # Simulate this by using a string column with dict-like data.
        # The actual test is on _pyodbc_safe_value:
        from bani.connectors.mssql.data_writer import _pyodbc_safe_value

        assert _pyodbc_safe_value({"key": "val"}) == '{"key": "val"}'
        assert _pyodbc_safe_value([1, 2, 3]) == "[1, 2, 3]"
        assert _pyodbc_safe_value("hello") == "hello"
        assert _pyodbc_safe_value(42) == 42
        assert _pyodbc_safe_value(None) is None

    def test_write_batch_calls_commit(self) -> None:
        """Test write_batch calls commit."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        writer = MSSQLDataWriter(mock_connection, driver="pyodbc")

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int32())], names=["id"]
        )

        writer.write_batch("test_table", "dbo", batch)
        mock_connection.commit.assert_called_once()
