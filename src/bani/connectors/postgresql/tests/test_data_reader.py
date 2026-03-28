"""Unit tests for PostgreSQL data reader (Arrow type mapping)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa

from bani.connectors.postgresql.data_reader import PostgreSQLDataReader
from bani.connectors.postgresql.type_mapper import PostgreSQLTypeMapper


def _make_ctx_cursor(cursor: MagicMock) -> MagicMock:
    """Add context-manager dunder methods to a mock cursor."""
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=None)
    return cursor


def _setup_conn_with_probe(
    col_meta: list[tuple[str, int]],
    *,
    data_fetchmany_side_effect: Any = None,
) -> tuple[MagicMock, MagicMock, MagicMock]:
    """Create a mock connection with separate probe and data cursors.

    The probe cursor (unnamed) is used for the LIMIT 0 metadata query.
    The data cursor (named) is used for the real SELECT with streaming.

    Returns:
        (connection, probe_cursor, data_cursor)
    """
    mock_conn = MagicMock()

    # Probe cursor (called without 'name' kwarg)
    probe_cursor = _make_ctx_cursor(MagicMock())
    probe_cursor.description = col_meta

    # Data cursor (called with name=...)
    data_cursor = _make_ctx_cursor(MagicMock())
    if data_fetchmany_side_effect is not None:
        data_cursor.fetchmany.side_effect = data_fetchmany_side_effect
    else:
        data_cursor.fetchmany.return_value = []

    def cursor_factory(**kwargs: Any) -> MagicMock:
        if "name" in kwargs:
            return data_cursor
        return probe_cursor

    # Also support positional-only call (no kwargs)
    def cursor_dispatch(*args: Any, **kwargs: Any) -> MagicMock:
        if kwargs.get("name"):
            return data_cursor
        return probe_cursor

    mock_conn.cursor.side_effect = cursor_dispatch
    return mock_conn, probe_cursor, data_cursor


class TestPostgreSQLDataReaderInit:
    """Tests for data reader initialization."""

    def test_init_stores_connection(self) -> None:
        """Reader should store the connection and initialize mapper."""
        mock_conn = MagicMock()
        reader = PostgreSQLDataReader(mock_conn)
        assert reader.connection is mock_conn
        assert reader.type_mapper is not None


class TestPostgreSQLDataReaderReadTable:
    """Tests for read_table method."""

    def test_read_table_with_no_columns(self) -> None:
        """Should handle cursor with no description gracefully."""
        mock_conn = MagicMock()
        probe_cursor = _make_ctx_cursor(MagicMock())
        probe_cursor.description = None
        mock_conn.cursor.return_value = probe_cursor

        reader = PostgreSQLDataReader(mock_conn)
        result = list(reader.read_table("test_table", "public"))

        assert result == []

    def test_read_table_single_batch(self) -> None:
        """Should read data in a single batch."""
        mock_conn, _probe, _data = _setup_conn_with_probe(
            [("id", 23), ("name", 25)],
            data_fetchmany_side_effect=[
                [(1, "Alice"), (2, "Bob")],
                [],
            ],
        )

        reader = PostgreSQLDataReader(mock_conn)
        batches = list(
            reader.read_table("test_table", "public", batch_size=100),
        )

        assert len(batches) == 1
        assert batches[0].num_rows == 2
        assert batches[0].num_columns == 2

    def test_read_table_multiple_batches(self) -> None:
        """Should split data into multiple batches."""
        mock_conn, _probe, _data = _setup_conn_with_probe(
            [("id", 23)],
            data_fetchmany_side_effect=[
                [(i,) for i in range(100)],
                [(i,) for i in range(100, 150)],
                [],
            ],
        )

        reader = PostgreSQLDataReader(mock_conn)
        batches = list(
            reader.read_table("test_table", "public", batch_size=100),
        )

        assert len(batches) == 2
        assert batches[0].num_rows == 100
        assert batches[1].num_rows == 50

    def test_read_table_with_filter(self) -> None:
        """Should apply filter expression to the data query."""
        mock_conn, _probe, data = _setup_conn_with_probe(
            [("id", 23)],
        )

        reader = PostgreSQLDataReader(mock_conn)
        list(
            reader.read_table(
                "test_table",
                "public",
                filter_sql="id > 5",
            )
        )

        # The data cursor should have the WHERE clause
        data_query = data.execute.call_args[0][0]
        assert "WHERE id > 5" in data_query

    def test_read_table_with_specific_columns(self) -> None:
        """Should select specific columns."""
        mock_conn, probe, _data = _setup_conn_with_probe(
            [("name", 25)],
        )

        reader = PostgreSQLDataReader(mock_conn)
        list(
            reader.read_table(
                "test_table",
                "public",
                columns=["name"],
            )
        )

        # Probe query should include the column
        probe_query = probe.execute.call_args[0][0]
        assert '"name"' in probe_query
        assert "LIMIT 0" in probe_query

    def test_read_table_probe_runs_limit_zero(self) -> None:
        """Should issue a LIMIT 0 probe before streaming."""
        mock_conn, probe, _data = _setup_conn_with_probe(
            [("id", 23), ("name", 25)],
        )

        reader = PostgreSQLDataReader(mock_conn)
        list(reader.read_table("test_table", "public"))

        # Verify probe query
        probe_query = probe.execute.call_args[0][0]
        assert "LIMIT 0" in probe_query
        assert '"public"."test_table"' in probe_query

    def test_read_table_casts_jsonb_to_text(self) -> None:
        """Should push ::text cast for jsonb columns (OID 3802)."""
        mock_conn, _probe, data = _setup_conn_with_probe(
            [("id", 23), ("data", 3802)],
            data_fetchmany_side_effect=[
                [(1, '{"key": "value"}')],
                [],
            ],
        )

        reader = PostgreSQLDataReader(mock_conn)
        batches = list(
            reader.read_table("test_table", "public", batch_size=100),
        )

        # The data query should have ::text cast for the jsonb column
        data_query = data.execute.call_args[0][0]
        assert '"data"::text AS "data"' in data_query
        # id should not be cast
        assert '"id"' in data_query
        assert '"id"::text' not in data_query

        assert len(batches) == 1
        assert batches[0].num_rows == 1

    def test_read_table_casts_json_to_text(self) -> None:
        """Should push ::text cast for json columns (OID 114)."""
        mock_conn, _probe, data = _setup_conn_with_probe(
            [("payload", 114)],
            data_fetchmany_side_effect=[
                [('["a","b"]',)],
                [],
            ],
        )

        reader = PostgreSQLDataReader(mock_conn)
        batches = list(
            reader.read_table("test_table", "public", batch_size=100),
        )

        data_query = data.execute.call_args[0][0]
        assert '"payload"::text AS "payload"' in data_query

        assert len(batches) == 1

    def test_read_table_casts_uuid_to_text(self) -> None:
        """Should push ::text cast for uuid columns (OID 2950)."""
        mock_conn, _probe, data = _setup_conn_with_probe(
            [("uid", 2950)],
            data_fetchmany_side_effect=[
                [("550e8400-e29b-41d4-a716-446655440000",)],
                [],
            ],
        )

        reader = PostgreSQLDataReader(mock_conn)
        batches = list(
            reader.read_table("test_table", "public", batch_size=100),
        )

        data_query = data.execute.call_args[0][0]
        assert '"uid"::text AS "uid"' in data_query

        assert batches[0].column("uid")[0].as_py() == (
            "550e8400-e29b-41d4-a716-446655440000"
        )

    def test_read_table_no_cast_for_regular_types(self) -> None:
        """Should not apply ::text cast for regular types."""
        mock_conn, _probe, data = _setup_conn_with_probe(
            [("id", 23), ("name", 25), ("active", 16)],
        )

        reader = PostgreSQLDataReader(mock_conn)
        list(reader.read_table("test_table", "public"))

        data_query = data.execute.call_args[0][0]
        assert "::text" not in data_query


class TestPostgreSQLDataReaderMakeBatch:
    """Tests for _make_record_batch method."""

    def test_make_record_batch_valid_data(self) -> None:
        """Should create valid batch from rows."""
        mock_conn = MagicMock()
        reader = PostgreSQLDataReader(mock_conn)

        rows: list[tuple[Any, ...]] = [(1, "Alice"), (2, "Bob")]
        col_names = ["id", "name"]
        col_types = [23, 25]  # int32, text

        batch = reader._make_record_batch(rows, col_names, col_types)

        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_rows == 2
        assert batch.num_columns == 2
        assert batch.schema.names == ["id", "name"]

    def test_make_record_batch_with_nulls(self) -> None:
        """Should handle None values in data."""
        mock_conn = MagicMock()
        reader = PostgreSQLDataReader(mock_conn)

        rows: list[tuple[Any, ...]] = [(1, None), (2, "Bob")]
        col_names = ["id", "name"]
        col_types = [23, 25]

        batch = reader._make_record_batch(rows, col_names, col_types)

        assert batch.num_rows == 2
        assert batch[1][0].as_py() is None

    def test_make_record_batch_various_types(self) -> None:
        """Should handle various PostgreSQL types."""
        mock_conn = MagicMock()
        reader = PostgreSQLDataReader(mock_conn)

        rows: list[tuple[Any, ...]] = [
            (1, True, 3.14, "text"),
            (2, False, 2.71, "data"),
        ]
        col_names = ["id", "active", "price", "label"]
        col_types = [23, 16, 701, 25]  # int, bool, float64, text

        batch = reader._make_record_batch(rows, col_names, col_types)

        assert batch.num_rows == 2
        assert batch.num_columns == 4

    def test_make_record_batch_string_col_with_text_oid(self) -> None:
        """After DB-level ::text cast, string data should pass through."""
        mock_conn = MagicMock()
        reader = PostgreSQLDataReader(mock_conn)

        # Simulates jsonb cast to text: OID 25 with plain string values
        rows: list[tuple[Any, ...]] = [
            ('{"key": "val"}',),
            (None,),
        ]
        col_names = ["data"]
        col_types = [25]  # text (post-cast OID)

        batch = reader._make_record_batch(rows, col_names, col_types)

        assert batch.num_rows == 2
        assert batch.column("data")[0].as_py() == '{"key": "val"}'
        assert batch.column("data")[1].as_py() is None


class TestPostgreSQLDataReaderEstimate:
    """Tests for estimate_row_count method."""

    def test_estimate_row_count_from_explain(self) -> None:
        """Should extract row count from EXPLAIN output."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        # Simulate EXPLAIN output with rows= indicator
        mock_cursor.fetchall.return_value = [
            ("Seq Scan on test_table (rows=12345 width=0)",)
        ]
        mock_conn.cursor.return_value = mock_cursor

        reader = PostgreSQLDataReader(mock_conn)
        result = reader.estimate_row_count("test_table", "public")

        assert result == 12345

    def test_estimate_row_count_fallback_to_count(self) -> None:
        """Should fall back to COUNT(*) if EXPLAIN lacks rows=."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        # First call: EXPLAIN without rows=, second: COUNT(*)
        mock_cursor.fetchall.side_effect = [
            [("Index Scan",)],
            [(5000,)],
        ]
        mock_conn.cursor.return_value = mock_cursor

        reader = PostgreSQLDataReader(mock_conn)
        result = reader.estimate_row_count("test_table", "public")

        assert result == 5000

    def test_estimate_row_count_empty_table(self) -> None:
        """Should return 0 for empty result set."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        reader = PostgreSQLDataReader(mock_conn)
        result = reader.estimate_row_count("test_table", "public")

        assert result == 0

    def test_estimate_row_count_with_spaces_in_explain(self) -> None:
        """Should handle EXPLAIN output with spaces."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)
        mock_cursor.fetchall.return_value = [
            ("  ->  Seq Scan on users (rows=999 width=100)",)
        ]
        mock_conn.cursor.return_value = mock_cursor

        reader = PostgreSQLDataReader(mock_conn)
        result = reader.estimate_row_count("users", "public")

        assert result == 999


class TestPostgreSQLTypeMapper:
    """Tests for PostgreSQL to Arrow type mapping."""

    def test_map_integer_types(self) -> None:
        """Should map PostgreSQL integer types to Arrow int types."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(16) == pa.bool_()  # boolean
        assert mapper.map_pg_type_oid(21) == pa.int16()  # smallint
        assert mapper.map_pg_type_oid(23) == pa.int32()  # integer
        assert mapper.map_pg_type_oid(20) == pa.int64()  # bigint

    def test_map_float_types(self) -> None:
        """Should map PostgreSQL float types to Arrow float types."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(700) == pa.float32()  # real
        assert mapper.map_pg_type_oid(701) == pa.float64()  # double precision

    def test_map_string_types(self) -> None:
        """Should map PostgreSQL string types to Arrow string."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(25) == pa.string()  # text
        assert mapper.map_pg_type_oid(1043) == pa.string()  # varchar
        assert mapper.map_pg_type_oid(1042) == pa.string()  # char

    def test_map_date_time_types(self) -> None:
        """Should map PostgreSQL date/time types to Arrow."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(1082) == pa.date32()  # date
        assert mapper.map_pg_type_oid(1083) == pa.time64("us")  # time
        assert mapper.map_pg_type_oid(1114) == pa.timestamp("us")  # timestamp
        assert mapper.map_pg_type_oid(1184) == pa.timestamp("us", tz="UTC")

    def test_map_json_types(self) -> None:
        """Should map PostgreSQL JSON types to Arrow string."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(114) == pa.string()  # json
        assert mapper.map_pg_type_oid(3802) == pa.string()  # jsonb

    def test_map_uuid_type(self) -> None:
        """Should map PostgreSQL UUID to Arrow string."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(2950) == pa.string()  # uuid

    def test_map_unknown_type(self) -> None:
        """Should default to string for unknown types."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(99999) == pa.string()

    def test_map_type_by_name_numeric(self) -> None:
        """Should map numeric type names."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("smallint") == pa.int16()
        assert mapper.map_pg_type_name("integer") == pa.int32()
        assert mapper.map_pg_type_name("int") == pa.int32()
        assert mapper.map_pg_type_name("bigint") == pa.int64()
        assert mapper.map_pg_type_name("real") == pa.float32()
        assert mapper.map_pg_type_name("double precision") == pa.float64()

    def test_map_type_by_name_string(self) -> None:
        """Should map string type names."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("text") == pa.string()
        assert mapper.map_pg_type_name("varchar") == pa.string()
        assert mapper.map_pg_type_name("char") == pa.string()

    def test_map_type_by_name_boolean(self) -> None:
        """Should map boolean type names."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("boolean") == pa.bool_()
        assert mapper.map_pg_type_name("bool") == pa.bool_()

    def test_map_type_by_name_case_insensitive(self) -> None:
        """Should handle case-insensitive type names."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("INTEGER") == pa.int32()
        assert mapper.map_pg_type_name("Integer") == pa.int32()
        assert mapper.map_pg_type_name("TEXT") == pa.string()

    def test_map_type_by_name_with_parameters(self) -> None:
        """Should strip parameters when mapping by name."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("varchar(255)") == pa.string()
        assert mapper.map_pg_type_name("numeric(10,2)") == pa.decimal128(38, 10)

    def test_map_type_by_name_with_spaces(self) -> None:
        """Should handle whitespace in type names."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("  double precision  ") == pa.float64()
        assert mapper.map_pg_type_name("double precision") == pa.float64()

    def test_map_serial_types(self) -> None:
        """Should map serial types to their base integer types."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("smallserial") == pa.int16()
        assert mapper.map_pg_type_name("serial") == pa.int32()
        assert mapper.map_pg_type_name("bigserial") == pa.int64()

    def test_map_date_time_type_names(self) -> None:
        """Should map date/time type names."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("date") == pa.date32()
        assert mapper.map_pg_type_name("time") == pa.time64("us")
        assert mapper.map_pg_type_name("timestamp") == pa.timestamp("us")
        assert mapper.map_pg_type_name("timestamptz") == pa.timestamp("us", tz="UTC")

    def test_map_json_type_names(self) -> None:
        """Should map JSON type names to string."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("json") == pa.string()
        assert mapper.map_pg_type_name("jsonb") == pa.string()

    def test_map_uuid_type_name(self) -> None:
        """Should map UUID type name to string."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("uuid") == pa.string()

    def test_map_bytea_type(self) -> None:
        """Should map bytea to Arrow binary."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(17) == pa.binary()  # bytea
        assert mapper.map_pg_type_name("bytea") == pa.binary()

    def test_map_unknown_type_by_name(self) -> None:
        """Should default to string for unknown type names."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("unknown_type_xyz") == pa.string()
