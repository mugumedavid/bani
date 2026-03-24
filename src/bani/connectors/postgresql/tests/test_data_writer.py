"""Unit tests for PostgreSQL data writer."""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa

from bani.connectors.postgresql.data_writer import PostgreSQLDataWriter


class TestPostgreSQLDataWriter:
    """Tests for PostgreSQL data writer."""

    def test_init_stores_connection(self) -> None:
        """Writer should store the connection."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)
        assert writer.connection is mock_conn

    def test_write_batch_empty_batch(self) -> None:
        """Should return 0 for empty batch."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=pa.int32())],
            names=["id"],
        )

        result = writer.write_batch("test_table", "public", batch)
        assert result == 0

    def test_scalar_to_csv_value_none(self) -> None:
        """Should handle None values."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        result = writer._scalar_to_csv_value(None)
        assert result == ""

    def test_scalar_to_csv_value_boolean(self) -> None:
        """Should convert booleans to 'true'/'false'."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        assert writer._scalar_to_csv_value(True) == "true"
        assert writer._scalar_to_csv_value(False) == "false"

    def test_scalar_to_csv_value_number(self) -> None:
        """Should convert numbers to strings."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        assert writer._scalar_to_csv_value(42) == "42"
        assert writer._scalar_to_csv_value(3.14) == "3.14"

    def test_scalar_to_csv_value_string_with_comma(self) -> None:
        """Should quote strings with commas."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        result = writer._scalar_to_csv_value("hello,world")
        assert result == '"hello,world"'

    def test_scalar_to_csv_value_string_with_quotes(self) -> None:
        """Should escape quotes in strings."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        result = writer._scalar_to_csv_value('say "hello"')
        assert result == '"say ""hello"""'

    def test_scalar_to_csv_value_bytes(self) -> None:
        """Should convert bytes to hex escape sequence."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        result = writer._scalar_to_csv_value(b"\x00\x01")
        assert result == "\\\\x0001"

    def test_scalar_to_sql_literal_none(self) -> None:
        """Should convert None to NULL."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        result = writer._scalar_to_sql_literal(None)
        assert result == "NULL"

    def test_scalar_to_sql_literal_boolean(self) -> None:
        """Should convert booleans to 'true'/'false'."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        assert writer._scalar_to_sql_literal(True) == "true"
        assert writer._scalar_to_sql_literal(False) == "false"

    def test_scalar_to_sql_literal_number(self) -> None:
        """Should convert numbers to strings."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        assert writer._scalar_to_sql_literal(42) == "42"
        assert writer._scalar_to_sql_literal(3.14) == "3.14"

    def test_scalar_to_sql_literal_string(self) -> None:
        """Should quote strings and escape single quotes."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        result = writer._scalar_to_sql_literal("hello")
        assert result == "'hello'"

    def test_scalar_to_sql_literal_string_with_quotes(self) -> None:
        """Should escape single quotes in strings."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        result = writer._scalar_to_sql_literal("it's")
        assert result == "'it''s'"

    def test_scalar_to_sql_literal_bytes(self) -> None:
        """Should convert bytes to hex escape sequence."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        result = writer._scalar_to_sql_literal(b"\x00\x01")
        assert result == "'\\\\x0001'"

    def test_scalar_to_csv_value_dict(self) -> None:
        """Should convert dicts to JSON string."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        result = writer._scalar_to_csv_value({"key": "value"})
        assert "key" in result
        assert "value" in result

    def test_scalar_to_sql_literal_dict(self) -> None:
        """Should convert dicts to quoted JSON string."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        result = writer._scalar_to_sql_literal({"key": "value"})
        assert result.startswith("'")
        assert result.endswith("'")
        assert "key" in result

    def test_batch_to_csv_empty_batch(self) -> None:
        """Should handle empty batch."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=pa.int32())],
            names=["id"],
        )

        result = writer._batch_to_csv(batch)
        assert result == b""

    def test_batch_to_csv_single_row(self) -> None:
        """Should convert batch with one row to CSV."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([42], type=pa.int32())],
            names=["id"],
        )

        result = writer._batch_to_csv(batch)
        assert result == b"42\n"

    def test_batch_to_csv_multiple_columns(self) -> None:
        """Should handle multiple columns in CSV."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2], type=pa.int32()),
                pa.array(["Alice", "Bob"], type=pa.string()),
            ],
            names=["id", "name"],
        )

        result = writer._batch_to_csv(batch)
        lines = result.decode("utf-8").strip().split("\n")
        assert len(lines) == 2
        assert "1" in lines[0] and "Alice" in lines[0]
        assert "2" in lines[1] and "Bob" in lines[1]

    def test_batch_to_csv_with_nulls(self) -> None:
        """Should represent NULLs as empty fields in CSV."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1, None], type=pa.int32()),
            ],
            names=["id"],
        )

        result = writer._batch_to_csv(batch)
        lines = result.decode("utf-8").split("\n")
        assert lines[0] == "1"
        assert lines[1] == ""

    def test_write_copy_builds_correct_sql(self) -> None:
        """Should build correct COPY command."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_copy = MagicMock()
        mock_copy.__enter__ = MagicMock(return_value=mock_copy)
        mock_copy.__exit__ = MagicMock(return_value=None)
        mock_cursor.copy.return_value = mock_copy
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor

        writer = PostgreSQLDataWriter(mock_conn)
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int32())],
            names=["id"],
        )

        result = writer._write_copy("test_table", "public", batch)

        assert result == 1
        # Verify copy command was called
        mock_copy.write.assert_called_once()

    def test_write_insert_single_row(self) -> None:
        """Should execute INSERT for single row."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor

        writer = PostgreSQLDataWriter(mock_conn)
        batch = pa.RecordBatch.from_arrays(
            [pa.array([42], type=pa.int32())],
            names=["id"],
        )

        result = writer._write_insert("test_table", "public", batch)

        assert result == 1
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0][0]
        assert "INSERT INTO" in call_args
        assert "42" in call_args

    def test_write_insert_multiple_rows(self) -> None:
        """Should execute INSERT for multiple rows."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor

        writer = PostgreSQLDataWriter(mock_conn)
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3], type=pa.int32())],
            names=["id"],
        )

        result = writer._write_insert("test_table", "public", batch)

        assert result == 3
        assert mock_cursor.execute.call_count == 3

    def test_write_insert_with_nulls(self) -> None:
        """Should use NULL for None values in INSERT."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor

        writer = PostgreSQLDataWriter(mock_conn)
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, None], type=pa.int32())],
            names=["id"],
        )

        result = writer._write_insert("test_table", "public", batch)

        assert result == 2
        calls = mock_cursor.execute.call_args_list
        # Second call should have NULL
        assert "NULL" in calls[1][0][0]

    def test_scalar_to_csv_value_newline(self) -> None:
        """Should quote strings with newlines."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        result = writer._scalar_to_csv_value("hello\nworld")
        assert result == '"hello\nworld"'

    def test_scalar_to_sql_literal_string_with_newline(self) -> None:
        """Should handle newlines in SQL literals."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        result = writer._scalar_to_sql_literal("hello\nworld")
        assert result.startswith("'")
        assert result.endswith("'")
        assert "\n" in result

    def test_write_batch_fallback_on_copy_error(self) -> None:
        """Should fall back to INSERT if COPY fails."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        # Make copy raise error
        mock_cursor.copy.side_effect = Exception("COPY failed")
        mock_conn.cursor.return_value = mock_cursor

        writer = PostgreSQLDataWriter(mock_conn)
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int32())],
            names=["id"],
        )

        # Should fall back to INSERT and succeed
        result = writer.write_batch("test_table", "public", batch)
        assert result == 1
