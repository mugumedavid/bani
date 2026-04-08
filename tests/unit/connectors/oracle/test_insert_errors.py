"""Tests for Oracle insert error collection.

Covers:
- Batch errors collected on data writer
- Errors propagated from writer to connector._insert_errors
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa

from bani.connectors.oracle.connector import OracleConnector
from bani.connectors.oracle.data_writer import OracleDataWriter


class TestOracleDataWriterErrors:
    """Tests for batch error collection in OracleDataWriter."""

    def test_batch_errors_collected(self) -> None:
        """Batch errors should be appended to writer.batch_errors."""
        conn = MagicMock()
        writer = OracleDataWriter(conn)

        cursor = MagicMock()
        conn.cursor.return_value = cursor

        # Simulate batch errors
        error1 = MagicMock()
        error1.offset = 5
        error1.message = "ORA-12899: value too large"
        error2 = MagicMock()
        error2.offset = 10
        error2.message = "ORA-12899: value too large"
        cursor.getbatcherrors.return_value = [error1, error2]

        batch = pa.record_batch(
            [pa.array([1, 2, 3])],
            names=["id"],
        )

        rows = writer.write_batch("test", "SA", batch)

        assert len(writer.batch_errors) == 2
        assert "ORA-12899" in writer.batch_errors[0]
        # 3 rows minus 2 errors = 1 written
        assert rows == 1

    def test_no_errors_empty_list(self) -> None:
        """No batch errors means empty list."""
        conn = MagicMock()
        writer = OracleDataWriter(conn)

        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.getbatcherrors.return_value = []

        batch = pa.record_batch(
            [pa.array([1, 2])],
            names=["id"],
        )

        rows = writer.write_batch("test", "SA", batch)

        assert writer.batch_errors == []
        assert rows == 2


class TestConnectorInsertErrorAggregation:
    """Tests that connector aggregates insert errors from writers."""

    @patch("bani.connectors.oracle.connector.oracledb")
    def test_insert_errors_collected_from_writer(
        self, mock_oracledb: MagicMock
    ) -> None:
        """Connector should collect batch errors from each write_batch call."""
        connector = OracleConnector()
        pool = MagicMock()
        conn = MagicMock()
        pool.acquire.return_value.__enter__ = MagicMock(return_value=conn)
        pool.acquire.return_value.__exit__ = MagicMock(return_value=False)
        connector._pool = pool
        connector._owner = "SA"

        batch = pa.record_batch(
            [pa.array([1]), pa.array(["x"])],
            names=["id", "name"],
        )

        with patch(
            "bani.connectors.oracle.connector.OracleDataWriter"
        ) as MockWriter:
            mock_writer = MockWriter.return_value
            mock_writer.write_batch.return_value = 0
            mock_writer.batch_errors = [
                "ORA-12899: value too large for column"
            ]

            connector.write_batch("test_table", "SA", batch)

        assert len(connector._insert_errors) == 1
        assert "test_table" in connector._insert_errors[0]
        assert "ORA-12899" in connector._insert_errors[0]
