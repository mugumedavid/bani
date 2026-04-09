"""Unit tests for Oracle data writer (mocked DB)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa

from bani.connectors.oracle.data_writer import OracleDataWriter


class TestOracleDataWriter:
    """Tests for Oracle data writing."""

    def test_write_empty_batch_returns_zero(self) -> None:
        """Should return 0 for empty batches."""
        connection = MagicMock()
        writer = OracleDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=pa.int32())],
            names=["id"],
        )

        result = writer.write_batch("test_table", "TEST_SCHEMA", batch)
        assert result == 0

    @patch("bani.connectors.oracle.data_writer.OracleDataWriter._arrow_to_input_sizes")
    def test_write_batch_uses_executemany(self, mock_sizes: MagicMock) -> None:
        """Should use executemany for batch INSERT."""
        mock_sizes.return_value = [None, None]

        connection = MagicMock()
        cursor = MagicMock()
        cursor.getbatcherrors.return_value = []
        connection.cursor.return_value = cursor

        writer = OracleDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
            names=["id", "name"],
        )

        result = writer.write_batch("test_table", "TEST_SCHEMA", batch)

        assert result == 3
        cursor.executemany.assert_called_once()
        # Verify batcherrors=True is passed
        call_kwargs = cursor.executemany.call_args
        assert call_kwargs[1].get("batcherrors") is True
        connection.commit.assert_called_once()
        cursor.close.assert_called_once()

    @patch("bani.connectors.oracle.data_writer.OracleDataWriter._arrow_to_input_sizes")
    def test_write_batch_handles_null_values(self, mock_sizes: MagicMock) -> None:
        """Should handle NULL values in batch."""
        mock_sizes.return_value = [None, None]

        connection = MagicMock()
        cursor = MagicMock()
        cursor.getbatcherrors.return_value = []
        connection.cursor.return_value = cursor

        writer = OracleDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2]),
                pa.array(["Alice", None]),
            ],
            names=["id", "name"],
        )

        result = writer.write_batch("test_table", "TEST_SCHEMA", batch)

        assert result == 2
        call_args = cursor.executemany.call_args
        values = call_args[0][1]
        assert values[1][1] is None

    @patch("bani.connectors.oracle.data_writer.OracleDataWriter._arrow_to_input_sizes")
    def test_write_batch_chunks_large_batches(self, mock_sizes: MagicMock) -> None:
        """Should chunk rows into CHUNK_SIZE groups."""
        mock_sizes.return_value = [None]

        connection = MagicMock()
        cursor = MagicMock()
        cursor.getbatcherrors.return_value = []
        connection.cursor.return_value = cursor

        writer = OracleDataWriter(connection)
        writer.CHUNK_SIZE = 2  # Small chunk for testing

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3, 4, 5])],
            names=["id"],
        )

        result = writer.write_batch("test_table", "TEST_SCHEMA", batch)

        assert result == 5
        # Should be 3 calls: 2 + 2 + 1
        assert cursor.executemany.call_count == 3
        connection.commit.assert_called_once()

    @patch("bani.connectors.oracle.data_writer.OracleDataWriter._arrow_to_input_sizes")
    def test_write_batch_setinputsizes_called_once(self, mock_sizes: MagicMock) -> None:
        """setinputsizes should be called once before the loop, not per chunk."""
        mock_sizes.return_value = [None]

        connection = MagicMock()
        cursor = MagicMock()
        cursor.getbatcherrors.return_value = []
        connection.cursor.return_value = cursor

        writer = OracleDataWriter(connection)
        writer.CHUNK_SIZE = 2

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3, 4, 5])],
            names=["id"],
        )

        writer.write_batch("test_table", "TEST_SCHEMA", batch)

        # setinputsizes called exactly once even though there are 3 chunks
        cursor.setinputsizes.assert_called_once()

    @patch("bani.connectors.oracle.data_writer.OracleDataWriter._arrow_to_input_sizes")
    def test_write_batch_deducts_batch_errors(self, mock_sizes: MagicMock) -> None:
        """Row count should exclude rows that failed with batch errors."""
        mock_sizes.return_value = [None, None]

        connection = MagicMock()
        cursor = MagicMock()

        # Simulate one batch error
        error = MagicMock()
        error.offset = 1
        error.message = "ORA-00001: unique constraint violated"
        cursor.getbatcherrors.return_value = [error]
        connection.cursor.return_value = cursor

        writer = OracleDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
            names=["id", "name"],
        )

        result = writer.write_batch("test_table", "TEST_SCHEMA", batch)

        # 3 rows sent, 1 error => 2 successful
        assert result == 2

    @patch("bani.connectors.oracle.data_writer.OracleDataWriter._arrow_to_input_sizes")
    def test_write_batch_logs_batch_errors(self, mock_sizes: MagicMock) -> None:
        """Batch errors should be logged as warnings."""
        mock_sizes.return_value = [None]

        connection = MagicMock()
        cursor = MagicMock()

        error = MagicMock()
        error.offset = 0
        error.message = "ORA-00001: unique constraint violated"
        cursor.getbatcherrors.return_value = [error]
        connection.cursor.return_value = cursor

        writer = OracleDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2])],
            names=["id"],
        )

        with patch("bani.connectors.oracle.data_writer.logger") as mock_logger:
            writer.write_batch("test_table", "TEST_SCHEMA", batch)
            mock_logger.warning.assert_called_once()

    @patch("bani.connectors.oracle.data_writer.OracleDataWriter._arrow_to_input_sizes")
    def test_write_batch_cursor_closed_on_error(self, mock_sizes: MagicMock) -> None:
        """Cursor should be closed even if executemany raises."""
        mock_sizes.return_value = [None]

        connection = MagicMock()
        cursor = MagicMock()
        cursor.executemany.side_effect = RuntimeError("connection lost")
        connection.cursor.return_value = cursor

        writer = OracleDataWriter(connection)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1])],
            names=["id"],
        )

        try:
            writer.write_batch("test_table", "TEST_SCHEMA", batch)
        except RuntimeError:
            pass

        cursor.close.assert_called_once()
