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
