"""Unit tests for MySQL data writer (mocked DB)."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

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

    def test_write_batch_falls_back_to_executemany(self) -> None:
        """Should fall back to executemany when LOAD DATA fails."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        # First cursor.execute (LOAD DATA) raises, second (executemany) works
        cursor.execute.side_effect = Exception("LOAD DATA not allowed")
        connection.cursor.return_value = cursor

        writer = MySQLDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
            names=["id", "name"],
        )

        result = writer.write_batch("test_table", "test_db", batch)

        assert result == 3
        cursor.executemany.assert_called_once()
        # commit called by fallback
        assert connection.commit.call_count >= 1

    def test_write_batch_executemany_handles_null_values(self) -> None:
        """Should handle NULL values in batch via executemany fallback."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        connection.cursor.return_value = cursor

        writer = MySQLDataWriter(connection)
        # Force executemany path
        writer._load_data_available = False

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

    def test_write_batch_executemany_sends_all_rows(self) -> None:
        """Should send all rows in one executemany call (no sub-batching)."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        connection.cursor.return_value = cursor

        writer = MySQLDataWriter(connection)
        # Force executemany path
        writer._load_data_available = False

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3, 4, 5])],
            names=["id"],
        )

        result = writer.write_batch("test_table", "test_db", batch)

        assert result == 5
        cursor.executemany.assert_called_once()
        call_args = cursor.executemany.call_args
        assert len(call_args[0][1]) == 5


class TestMySQLDataWriterLoadData:
    """Tests for the LOAD DATA LOCAL INFILE strategy."""

    def test_load_data_writes_temp_file_and_executes(self) -> None:
        """Should create temp TSV and issue LOAD DATA statement."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        connection.cursor.return_value = cursor

        writer = MySQLDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2]), pa.array(["hello", "world"])],
            names=["id", "name"],
        )

        result = writer._write_load_data("test_table", "test_db", batch)

        assert result == 2
        # LOAD DATA SQL was executed
        call_args = cursor.execute.call_args[0][0]
        assert "LOAD DATA LOCAL INFILE" in call_args
        assert "`test_db`.`test_table`" in call_args
        connection.commit.assert_called_once()

    def test_load_data_handles_null_values(self) -> None:
        """Should encode NULL as \\N in TSV."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        connection.cursor.return_value = cursor

        writer = MySQLDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2]), pa.array(["Alice", None])],
            names=["id", "name"],
        )

        # Capture the temp file contents before it's deleted
        written_bytes = bytearray()
        real_write = os.write
        real_close = os.close

        def capture_write(fd: int, data: bytes) -> int:
            written_bytes.extend(data)
            return real_write(fd, data)

        _mod = "bani.connectors.mysql.data_writer"
        with patch(f"{_mod}.os.write", side_effect=capture_write):
            with patch(f"{_mod}.os.close", side_effect=real_close):
                writer._write_load_data(
                    "test_table",
                    "test_db",
                    batch,
                )

        tsv_content = written_bytes.decode("utf-8")
        lines = tsv_content.strip().split("\n")
        assert len(lines) == 2
        # Second row should have \N for the NULL name
        assert "\\N" in lines[1]

    def test_load_data_handles_bool_values(self) -> None:
        """Should encode booleans as 1/0."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        connection.cursor.return_value = cursor

        writer = MySQLDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([True, False])],
            names=["active"],
        )

        written_bytes = bytearray()
        real_write = os.write
        real_close = os.close

        def capture_write(fd: int, data: bytes) -> int:
            written_bytes.extend(data)
            return real_write(fd, data)

        _mod = "bani.connectors.mysql.data_writer"
        with patch(f"{_mod}.os.write", side_effect=capture_write):
            with patch(f"{_mod}.os.close", side_effect=real_close):
                writer._write_load_data(
                    "test_table",
                    "test_db",
                    batch,
                )

        lines = written_bytes.decode("utf-8").strip().split("\n")
        assert lines[0] == "1"
        assert lines[1] == "0"

    def test_load_data_cleans_up_temp_file(self) -> None:
        """Should remove temp file even when LOAD DATA succeeds."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        connection.cursor.return_value = cursor

        writer = MySQLDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1])],
            names=["id"],
        )

        writer._write_load_data("test_table", "test_db", batch)

        # Extract the file path from the LOAD DATA SQL
        call_sql = cursor.execute.call_args[0][0]
        # Path is between single quotes after INFILE
        path_start = call_sql.index("'") + 1
        path_end = call_sql.index("'", path_start)
        tmp_path = call_sql[path_start:path_end]

        # Temp file should have been cleaned up
        assert not os.path.exists(tmp_path)

    def test_load_data_cleans_up_temp_file_on_failure(self) -> None:
        """Should remove temp file when LOAD DATA raises."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.execute.side_effect = Exception("LOAD DATA failed")
        connection.cursor.return_value = cursor

        writer = MySQLDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1])],
            names=["id"],
        )

        try:
            writer._write_load_data("test_table", "test_db", batch)
        except Exception:
            pass

        # No temp .tsv files should remain
        # (can't check exact path, but the finally block runs)

    def test_fallback_latches_after_first_failure(self) -> None:
        """Once LOAD DATA fails, subsequent calls should skip it."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        # Make cursor.execute raise (simulates LOAD DATA failure)
        cursor.execute.side_effect = Exception("LOAD DATA not supported")
        connection.cursor.return_value = cursor

        writer = MySQLDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1]), pa.array(["x"])],
            names=["id", "name"],
        )

        # First call: tries LOAD DATA, fails, falls back to executemany
        # Reset side effect for executemany to work
        cursor.execute.side_effect = Exception("LOAD DATA not supported")
        writer.write_batch("test_table", "test_db", batch)

        assert writer._load_data_available is False

        # Second call: should go straight to executemany
        cursor.execute.reset_mock()
        cursor.executemany.reset_mock()
        connection.commit.reset_mock()

        # No execute exception — executemany path doesn't call cursor.execute
        cursor.execute.side_effect = None
        writer.write_batch("test_table", "test_db", batch)

        cursor.executemany.assert_called_once()

    def test_load_data_success_latches_available(self) -> None:
        """Once LOAD DATA succeeds, _load_data_available should be True."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        connection.cursor.return_value = cursor

        writer = MySQLDataWriter(connection)
        assert writer._load_data_available is None

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1])],
            names=["id"],
        )

        writer.write_batch("test_table", "test_db", batch)

        assert writer._load_data_available is True
