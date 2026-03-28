"""Unit tests for PostgreSQL data writer."""

from __future__ import annotations

from unittest.mock import MagicMock, call

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


class TestExtractRows:
    """Tests for _extract_rows helper."""

    def test_extract_simple_values(self) -> None:
        """Should extract int and string values."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2], type=pa.int32()),
                pa.array(["Alice", "Bob"], type=pa.string()),
            ],
            names=["id", "name"],
        )

        rows = writer._extract_rows(batch)
        assert len(rows) == 2
        assert rows[0] == [1, "Alice"]
        assert rows[1] == [2, "Bob"]

    def test_extract_nulls(self) -> None:
        """Should convert invalid scalars to None."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, None], type=pa.int32())],
            names=["id"],
        )

        rows = writer._extract_rows(batch)
        assert rows[0] == [1]
        assert rows[1] == [None]

    def test_extract_boolean(self) -> None:
        """Should preserve boolean values."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([True, False], type=pa.bool_())],
            names=["flag"],
        )

        rows = writer._extract_rows(batch)
        assert rows[0] == [True]
        assert rows[1] == [False]

    def test_extract_float(self) -> None:
        """Should preserve float values."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([3.14, 2.72], type=pa.float64())],
            names=["val"],
        )

        rows = writer._extract_rows(batch)
        assert rows[0] == [3.14]
        assert rows[1] == [2.72]

    def test_extract_bytes(self) -> None:
        """Should pass bytes through as-is."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([b"\x00\x01", b"\xff"], type=pa.binary())],
            names=["data"],
        )

        rows = writer._extract_rows(batch)
        assert rows[0] == [b"\x00\x01"]
        assert rows[1] == [b"\xff"]

    def test_extract_dict_becomes_json_string(self) -> None:
        """Should json.dumps() dict values for jsonb columns."""
        import json

        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        # Use a string column to hold JSON (Arrow doesn't have a native json type)
        # In practice, dicts come from as_py() on columns stored as JSON
        # We simulate this by creating an array of structs and extracting
        # We'll test _extract_rows behavior with a mock batch instead
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int32())],
            names=["id"],
        )

        # Override scalar to return dict
        rows = writer._extract_rows(batch)
        assert rows[0] == [1]

        # Direct test: create a batch where as_py() returns a dict
        # Use a struct array
        struct_arr = pa.array(
            [{"key": "value"}],
            type=pa.struct([("key", pa.string())]),
        )
        batch_with_struct = pa.RecordBatch.from_arrays(
            [struct_arr],
            names=["data"],
        )

        rows = writer._extract_rows(batch_with_struct)
        # Should be a JSON string, not a dict
        assert isinstance(rows[0][0], str)
        parsed = json.loads(rows[0][0])
        assert parsed == {"key": "value"}

    def test_extract_list_becomes_json_string(self) -> None:
        """Should json.dumps() list values for jsonb columns."""
        import json

        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        list_arr = pa.array(
            [[1, 2, 3]],
            type=pa.list_(pa.int64()),
        )
        batch = pa.RecordBatch.from_arrays(
            [list_arr],
            names=["tags"],
        )

        rows = writer._extract_rows(batch)
        assert isinstance(rows[0][0], str)
        parsed = json.loads(rows[0][0])
        assert parsed == [1, 2, 3]

    def test_extract_empty_batch(self) -> None:
        """Should return empty list for empty batch."""
        mock_conn = MagicMock()
        writer = PostgreSQLDataWriter(mock_conn)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=pa.int32())],
            names=["id"],
        )

        rows = writer._extract_rows(batch)
        assert rows == []


class TestWriteCopy:
    """Tests for COPY-based writing."""

    def test_write_copy_binary_uses_write_row(self) -> None:
        """Should call write_row() for each row in binary mode."""
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
            [pa.array([1, 2], type=pa.int32())],
            names=["id"],
        )

        result = writer._write_copy("test_table", "public", batch, binary=True)

        assert result == 2
        # Verify write_row was called for each row
        assert mock_copy.write_row.call_count == 2
        mock_copy.write_row.assert_any_call([1])
        mock_copy.write_row.assert_any_call([2])

    def test_write_copy_binary_sql_format(self) -> None:
        """Should use FORMAT BINARY in the SQL command."""
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

        writer._write_copy("test_table", "public", batch, binary=True)

        copy_sql = mock_cursor.copy.call_args[0][0]
        assert "FORMAT BINARY" in copy_sql
        assert '"public"."test_table"' in copy_sql

    def test_write_copy_text_sql_format(self) -> None:
        """Should not include FORMAT BINARY in text mode."""
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

        writer._write_copy("test_table", "public", batch, binary=False)

        copy_sql = mock_cursor.copy.call_args[0][0]
        assert "FORMAT BINARY" not in copy_sql
        assert "FROM STDIN" in copy_sql

    def test_write_copy_with_nulls(self) -> None:
        """Should pass None for NULL values in write_row."""
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
            [pa.array([1, None], type=pa.int32())],
            names=["id"],
        )

        writer._write_copy("test_table", "public", batch, binary=True)

        calls = mock_copy.write_row.call_args_list
        assert calls[0] == call([1])
        assert calls[1] == call([None])

    def test_write_copy_multiple_columns(self) -> None:
        """Should include all columns in each write_row call."""
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
            [
                pa.array([1], type=pa.int32()),
                pa.array(["Alice"], type=pa.string()),
            ],
            names=["id", "name"],
        )

        writer._write_copy("test_table", "public", batch, binary=True)

        mock_copy.write_row.assert_called_once_with([1, "Alice"])

    def test_write_copy_column_list_in_sql(self) -> None:
        """Should include quoted column names in the COPY command."""
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
            [
                pa.array([1], type=pa.int32()),
                pa.array(["x"], type=pa.string()),
            ],
            names=["my_id", "my_name"],
        )

        writer._write_copy("t", "s", batch, binary=False)

        copy_sql = mock_cursor.copy.call_args[0][0]
        assert '"my_id"' in copy_sql
        assert '"my_name"' in copy_sql


class TestWriteInsert:
    """Tests for multi-row INSERT fallback."""

    def test_write_insert_single_row(self) -> None:
        """Should execute parameterized INSERT for single row."""
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
        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]
        assert "INSERT INTO" in sql
        assert "%s" in sql
        assert params == [42]

    def test_write_insert_multiple_rows(self) -> None:
        """Should batch multiple rows into one INSERT."""
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
        # All 3 rows should be in a single INSERT (< 1000 row batch)
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]
        assert sql.count("%s") == 3
        assert params == [1, 2, 3]

    def test_write_insert_with_nulls(self) -> None:
        """Should pass None for NULL values via params."""
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
        params = mock_cursor.execute.call_args[0][1]
        assert params == [1, None]

    def test_write_insert_multiple_columns(self) -> None:
        """Should handle multi-column rows correctly."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor

        writer = PostgreSQLDataWriter(mock_conn)
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2], type=pa.int32()),
                pa.array(["a", "b"], type=pa.string()),
            ],
            names=["id", "name"],
        )

        result = writer._write_insert("test_table", "public", batch)

        assert result == 2
        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]
        # 2 rows x 2 cols = 4 placeholders
        assert sql.count("%s") == 4
        assert params == [1, "a", 2, "b"]


class TestWriteBatchFallback:
    """Tests for write_batch fallback behavior."""

    def test_write_batch_fallback_on_copy_error(self) -> None:
        """Should fall back to INSERT if all COPY modes fail."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        # Make copy raise error (both binary and text COPY will fail)
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

    def test_write_batch_fallback_binary_to_text(self) -> None:
        """Should fall back from binary COPY to text COPY."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_copy = MagicMock()
        mock_copy.__enter__ = MagicMock(return_value=mock_copy)
        mock_copy.__exit__ = MagicMock(return_value=None)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        call_count = 0

        def copy_side_effect(sql: str) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call (binary) fails
                raise Exception("Binary COPY not supported")
            # Second call (text) succeeds
            return mock_copy

        mock_cursor.copy.side_effect = copy_side_effect
        mock_conn.cursor.return_value = mock_cursor

        writer = PostgreSQLDataWriter(mock_conn)
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int32())],
            names=["id"],
        )

        result = writer.write_batch("test_table", "public", batch)
        assert result == 1
        # Text COPY should have called write_row
        mock_copy.write_row.assert_called_once_with([1])
