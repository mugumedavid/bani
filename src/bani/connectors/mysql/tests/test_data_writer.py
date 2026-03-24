"""Unit tests for MySQL data writer (mocked DB)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa

from bani.connectors.mysql.data_writer import MySQLDataWriter


class TestMySQLDataWriter:
    """Tests for MySQL data writing."""

    def test_write_empty_batch_returns_zero(self) -> None:
        """Should return 0 for empty batches."""
        connection = MagicMock()
        writer = MySQLDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=pa.int32())],
            names=["id"],
        )

        result = writer.write_batch("test_table", "test_db", batch)
        assert result == 0

    def test_write_batch_uses_executemany(self) -> None:
        """Should use executemany for multi-row INSERT."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        connection.cursor.return_value = cursor

        writer = MySQLDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
            names=["id", "name"],
        )

        result = writer.write_batch("test_table", "test_db", batch)

        assert result == 3
        cursor.executemany.assert_called_once()
        connection.commit.assert_called_once()

    def test_write_batch_handles_null_values(self) -> None:
        """Should handle NULL values in batch."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        connection.cursor.return_value = cursor

        writer = MySQLDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2]),
                pa.array(["Alice", None]),
            ],
            names=["id", "name"],
        )

        result = writer.write_batch("test_table", "test_db", batch)

        assert result == 2
        # Verify the data passed to executemany contains None
        call_args = cursor.executemany.call_args
        values = call_args[0][1]
        assert values[1][1] is None

    def test_write_batch_respects_insert_batch_size(self) -> None:
        """Should batch rows into chunks of INSERT_BATCH_SIZE."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        connection.cursor.return_value = cursor

        writer = MySQLDataWriter(connection)
        writer.INSERT_BATCH_SIZE = 2  # Small batch for testing

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3, 4, 5])],
            names=["id"],
        )

        result = writer.write_batch("test_table", "test_db", batch)

        assert result == 5
        # Should be 3 calls: 2 + 2 + 1
        assert cursor.executemany.call_count == 3
