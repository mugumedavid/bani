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


class TestMySQLDataReaderInit:
    """Tests for data reader initialization."""

    def test_init_stores_connection(self) -> None:
        """Reader should store the connection and initialize mapper."""
        mock_conn = MagicMock()
        reader = MySQLDataReader(mock_conn)
        assert reader.connection is mock_conn
        assert reader.type_mapper is not None


class TestMySQLDataReaderReadTable:
    """Tests for read_table method."""

    def test_read_table_with_no_columns(self) -> None:
        """Should handle cursor with no description gracefully."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_cursor.description = None
        mock_conn.cursor.return_value = mock_cursor

        reader = MySQLDataReader(mock_conn)
        result = list(reader.read_table("test_table", "test_db"))

        assert result == []

    def test_read_table_single_batch(self) -> None:
        """Should read data in a single batch."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        mock_cursor.description = [
            ("id", 3, None, None, None, None, None, 0),
            ("name", 253, None, None, None, None, None, 0),
        ]
        mock_cursor.fetchmany.side_effect = [
            [(1, "Alice"), (2, "Bob")],
            [],
        ]
        mock_conn.cursor.return_value = mock_cursor

        reader = MySQLDataReader(mock_conn)
        batches = list(reader.read_table("test_table", "test_db", batch_size=100))

        assert len(batches) == 1
        assert batches[0].num_rows == 2
        assert batches[0].num_columns == 2

    def test_read_table_multiple_batches(self) -> None:
        """Should split data into multiple batches."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        mock_cursor.description = [
            ("id", 3, None, None, None, None, None, 0),
        ]
        mock_cursor.fetchmany.side_effect = [
            [(i,) for i in range(100)],
            [(i,) for i in range(100, 150)],
            [],
        ]
        mock_conn.cursor.return_value = mock_cursor

        reader = MySQLDataReader(mock_conn)
        batches = list(reader.read_table("test_table", "test_db", batch_size=100))

        assert len(batches) == 2
        assert batches[0].num_rows == 100
        assert batches[1].num_rows == 50

    def test_read_table_with_filter(self) -> None:
        """Should apply filter expression to query."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        mock_cursor.description = [
            ("id", 3, None, None, None, None, None, 0),
        ]
        mock_cursor.fetchmany.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        reader = MySQLDataReader(mock_conn)
        list(
            reader.read_table(
                "test_table",
                "test_db",
                filter_sql="id > 5",
            )
        )

        call_args = mock_cursor.execute.call_args[0][0]
        assert "WHERE id > 5" in call_args

    def test_read_table_with_specific_columns(self) -> None:
        """Should select specific columns."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        mock_cursor.description = [
            ("name", 253, None, None, None, None, None, 0),
        ]
        mock_cursor.fetchmany.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        reader = MySQLDataReader(mock_conn)
        list(
            reader.read_table(
                "test_table",
                "test_db",
                columns=["name"],
            )
        )

        call_args = mock_cursor.execute.call_args[0][0]
        assert "`name`" in call_args
        assert "FROM" in call_args

    def test_read_table_with_all_columns(self) -> None:
        """Should use * when no columns specified."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        mock_cursor.description = []
        mock_cursor.fetchmany.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        reader = MySQLDataReader(mock_conn)
        list(reader.read_table("test_table", "test_db"))

        call_args = mock_cursor.execute.call_args[0][0]
        assert "SELECT *" in call_args

    def test_read_table_cursor_closed_on_exit(self) -> None:
        """Should close cursor even if exception occurs."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_cursor.description = None
        mock_cursor.fetchmany.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        reader = MySQLDataReader(mock_conn)
        list(reader.read_table("test_table", "test_db"))

        mock_cursor.close.assert_called_once()

    def test_read_table_incomplete_batch_on_end(self) -> None:
        """Should yield incomplete batch when reader closes."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        mock_cursor.description = [
            ("id", 3, None, None, None, None, None, 0),
        ]
        mock_cursor.fetchmany.side_effect = [
            [(i,) for i in range(50)],
            [],
        ]
        mock_conn.cursor.return_value = mock_cursor

        reader = MySQLDataReader(mock_conn)
        batches = list(reader.read_table("test_table", "test_db", batch_size=100))

        assert len(batches) == 1
        assert batches[0].num_rows == 50


class TestMySQLDataReaderMakeBatchDetails:
    """Tests for _make_record_batch method with various types."""

    def test_make_record_batch_various_types(self) -> None:
        """Should handle various MySQL types."""
        connection = MagicMock()
        reader = MySQLDataReader(connection)

        rows: list[tuple[Any, ...]] = [
            (1, True, 3.14, "text"),
            (2, False, 2.71, "data"),
        ]
        col_names = ["id", "active", "price", "label"]
        col_types: list[pa.DataType] = [
            pa.int32(),
            pa.bool_(),
            pa.float64(),
            pa.string(),
        ]

        batch = reader._make_record_batch(rows, col_names, col_types)

        assert batch.num_rows == 2
        assert batch.num_columns == 4

    def test_make_record_batch_all_nulls(self) -> None:
        """Should handle columns with all None values."""
        connection = MagicMock()
        reader = MySQLDataReader(connection)

        rows: list[tuple[Any, ...]] = [
            (None,),
            (None,),
        ]
        col_names = ["value"]
        col_types: list[pa.DataType] = [pa.string()]

        batch = reader._make_record_batch(rows, col_names, col_types)

        assert batch.num_rows == 2
        assert batch[0][0].as_py() is None
        assert batch[0][1].as_py() is None

    def test_make_record_batch_single_row(self) -> None:
        """Should handle single row batches."""
        connection = MagicMock()
        reader = MySQLDataReader(connection)

        rows: list[tuple[Any, ...]] = [(42,)]
        col_names = ["id"]
        col_types: list[pa.DataType] = [pa.int64()]

        batch = reader._make_record_batch(rows, col_names, col_types)

        assert batch.num_rows == 1
        assert batch[0][0].as_py() == 42

    def test_make_record_batch_empty_rows(self) -> None:
        """Should handle empty rows list."""
        connection = MagicMock()
        reader = MySQLDataReader(connection)

        rows: list[tuple[Any, ...]] = []
        col_names = ["id", "name"]
        col_types: list[pa.DataType] = [pa.int64(), pa.string()]

        batch = reader._make_record_batch(rows, col_names, col_types)

        assert batch.num_rows == 0
        assert batch.num_columns == 2


class TestMySQLDataReaderEstimateDetails:
    """Tests for estimate_row_count method details."""

    def test_estimate_from_information_schema_with_null(self) -> None:
        """Should return 0 when information_schema has None row count."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.fetchone.side_effect = [(None,), (250,)]
        connection.cursor.return_value = cursor

        reader = MySQLDataReader(connection)
        result = reader.estimate_row_count("users", "test_db")

        assert result == 250

    def test_estimate_row_count_count_returns_none(self) -> None:
        """Should return 0 if COUNT(*) query returns None."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.fetchone.side_effect = [(None,), None]
        connection.cursor.return_value = cursor

        reader = MySQLDataReader(connection)
        result = reader.estimate_row_count("users", "test_db")

        assert result == 0

    def test_estimate_row_count_converts_to_int(self) -> None:
        """Should convert result to integer."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.fetchone.return_value = (12345,)
        connection.cursor.return_value = cursor

        reader = MySQLDataReader(connection)
        result = reader.estimate_row_count("users", "test_db")

        assert isinstance(result, int)
        assert result == 12345

    def test_estimate_queries_with_correct_params(self) -> None:
        """Should pass schema and table names to query."""
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.fetchone.return_value = (999,)
        connection.cursor.return_value = cursor

        reader = MySQLDataReader(connection)
        reader.estimate_row_count("my_table", "my_schema")

        call_args = cursor.execute.call_args
        query = call_args[0][0]
        params = call_args[0][1]

        assert "information_schema" in query
        assert params == ("my_schema", "my_table")
