"""Tests for the data preview module."""

from __future__ import annotations

import datetime
import json
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pyarrow as pa

from bani.application.preview import (
    ColumnPreview,
    PreviewResult,
    TablePreview,
    _make_json_serializable,
    preview_source,
)
from bani.connectors.base import SourceConnector
from bani.domain.project import ConnectionConfig
from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    TableDefinition,
)


class MockSourceConnector(SourceConnector):
    """Mock source connector for preview tests."""

    def __init__(
        self,
        schema: DatabaseSchema,
        table_data: dict[str, list[pa.RecordBatch]] | None = None,
    ) -> None:
        """Initialize with a fixed schema and optional table data.

        Args:
            schema: The schema to return from introspect_schema().
            table_data: Mapping of table_name to list of record batches.
                If None, all tables return empty iterators.
        """
        self._schema = schema
        self._table_data = table_data or {}
        self.read_table_calls: list[dict[str, Any]] = []

    def connect(self, config: ConnectionConfig) -> None:
        """No-op."""

    def disconnect(self) -> None:
        """No-op."""

    def introspect_schema(self) -> DatabaseSchema:
        """Return the fixed schema."""
        return self._schema

    def read_table(
        self,
        table_name: str,
        schema_name: str,
        columns: list[str] | None = None,
        filter_sql: str | None = None,
        batch_size: int = 100_000,
    ) -> Iterator[pa.RecordBatch]:
        """Yield batches from pre-configured table data."""
        self.read_table_calls.append(
            {
                "table_name": table_name,
                "schema_name": schema_name,
                "columns": columns,
                "filter_sql": filter_sql,
                "batch_size": batch_size,
            }
        )
        batches = self._table_data.get(table_name, [])
        yield from batches

    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        """Return 0."""
        return 0


def _make_schema(
    tables: list[TableDefinition],
    dialect: str = "postgresql",
) -> DatabaseSchema:
    """Helper to create a DatabaseSchema."""
    return DatabaseSchema(tables=tuple(tables), source_dialect=dialect)


def _make_table(
    table_name: str,
    schema_name: str = "public",
    columns: tuple[ColumnDefinition, ...] = (),
    row_count_estimate: int | None = None,
) -> TableDefinition:
    """Helper to create a TableDefinition."""
    return TableDefinition(
        schema_name=schema_name,
        table_name=table_name,
        columns=columns,
        row_count_estimate=row_count_estimate,
    )


class TestMakeJsonSerializable:
    """Tests for the _make_json_serializable helper."""

    def test_none_passthrough(self) -> None:
        assert _make_json_serializable(None) is None

    def test_int_passthrough(self) -> None:
        assert _make_json_serializable(42) == 42

    def test_float_passthrough(self) -> None:
        assert _make_json_serializable(3.14) == 3.14

    def test_bool_passthrough(self) -> None:
        assert _make_json_serializable(True) is True

    def test_short_string_passthrough(self) -> None:
        assert _make_json_serializable("hello") == "hello"

    def test_long_string_truncated(self) -> None:
        long_text = "a" * 300
        result = _make_json_serializable(long_text)
        assert len(result) == 203  # 200 chars + "..."
        assert result.endswith("...")
        assert result[:200] == "a" * 200

    def test_string_at_limit_not_truncated(self) -> None:
        text = "x" * 200
        result = _make_json_serializable(text)
        assert result == text
        assert not result.endswith("...")

    def test_string_just_over_limit_truncated(self) -> None:
        text = "x" * 201
        result = _make_json_serializable(text)
        assert result == "x" * 200 + "..."

    def test_bytes_to_hex(self) -> None:
        data = b"\xde\xad\xbe\xef"
        result = _make_json_serializable(data)
        assert result == "deadbeef"

    def test_large_bytes_truncated(self) -> None:
        data = bytes(range(256)) * 2  # 512 bytes
        result = _make_json_serializable(data)
        expected_hex = data[:50].hex()
        assert result == expected_hex + "..."

    def test_bytes_at_limit_not_truncated(self) -> None:
        data = b"\xff" * 50
        result = _make_json_serializable(data)
        assert result == "ff" * 50
        assert not result.endswith("...")

    def test_bytearray_to_hex(self) -> None:
        data = bytearray(b"\x01\x02\x03")
        result = _make_json_serializable(data)
        assert result == "010203"

    def test_datetime_to_isoformat(self) -> None:
        dt = datetime.datetime(2024, 1, 15, 10, 30, 0, tzinfo=datetime.timezone.utc)
        result = _make_json_serializable(dt)
        assert result == "2024-01-15T10:30:00+00:00"

    def test_date_to_isoformat(self) -> None:
        d = datetime.date(2024, 6, 15)
        result = _make_json_serializable(d)
        assert result == "2024-06-15"

    def test_time_to_isoformat(self) -> None:
        t = datetime.time(14, 30, 0)
        result = _make_json_serializable(t)
        assert result == "14:30:00"

    def test_timedelta_to_string(self) -> None:
        td = datetime.timedelta(days=5, hours=3)
        result = _make_json_serializable(td)
        assert result == "5 days, 3:00:00"

    def test_decimal_to_string(self) -> None:
        d = Decimal("123.456")
        result = _make_json_serializable(d)
        assert result == "123.456"

    def test_list_recursive(self) -> None:
        data = [1, datetime.date(2024, 1, 1), None]
        result = _make_json_serializable(data)
        assert result == [1, "2024-01-01", None]

    def test_dict_recursive(self) -> None:
        data = {"key": Decimal("1.5"), "nested": b"\xab"}
        result = _make_json_serializable(data)
        assert result == {"key": "1.5", "nested": "ab"}

    def test_unknown_type_to_string(self) -> None:
        class Custom:
            def __str__(self) -> str:
                return "custom_repr"

        result = _make_json_serializable(Custom())
        assert result == "custom_repr"


class TestPreviewSource:
    """Tests for the preview_source function."""

    def test_empty_schema_returns_empty_result(self) -> None:
        schema = _make_schema([])
        source = MockSourceConnector(schema)
        result = preview_source(source)
        assert result == PreviewResult(tables=(), source_dialect="postgresql")

    def test_source_dialect_propagated(self) -> None:
        schema = _make_schema([], dialect="mysql")
        source = MockSourceConnector(schema)
        result = preview_source(source)
        assert result.source_dialect == "mysql"

    def test_empty_table_produces_empty_sample_rows(self) -> None:
        table = _make_table(
            "users",
            columns=(
                ColumnDefinition(name="id", data_type="INTEGER", nullable=False),
                ColumnDefinition(name="name", data_type="VARCHAR(100)", nullable=True),
            ),
        )
        schema = _make_schema([table])
        source = MockSourceConnector(schema)
        result = preview_source(source)

        assert len(result.tables) == 1
        tp = result.tables[0]
        assert tp.table_name == "users"
        assert tp.schema_name == "public"
        assert tp.sample_rows == ()
        assert len(tp.columns) == 2

    def test_column_metadata_populated(self) -> None:
        table = _make_table(
            "products",
            columns=(
                ColumnDefinition(
                    name="id",
                    data_type="INTEGER",
                    nullable=False,
                    arrow_type_str="int32",
                ),
                ColumnDefinition(
                    name="price",
                    data_type="DECIMAL(10,2)",
                    nullable=True,
                    arrow_type_str="decimal128(10, 2)",
                ),
                ColumnDefinition(
                    name="description",
                    data_type="TEXT",
                    nullable=True,
                ),
            ),
        )
        schema = _make_schema([table])
        source = MockSourceConnector(schema)
        result = preview_source(source)

        cols = result.tables[0].columns
        assert cols[0] == ColumnPreview(
            name="id", data_type="INTEGER", nullable=False, arrow_type="int32"
        )
        assert cols[1] == ColumnPreview(
            name="price",
            data_type="DECIMAL(10,2)",
            nullable=True,
            arrow_type="decimal128(10, 2)",
        )
        assert cols[2] == ColumnPreview(
            name="description", data_type="TEXT", nullable=True, arrow_type=""
        )

    def test_row_count_estimate_propagated(self) -> None:
        table = _make_table("events", row_count_estimate=42000)
        schema = _make_schema([table])
        source = MockSourceConnector(schema)
        result = preview_source(source)
        assert result.tables[0].row_count_estimate == 42000

    def test_row_count_estimate_none(self) -> None:
        table = _make_table("events")
        schema = _make_schema([table])
        source = MockSourceConnector(schema)
        result = preview_source(source)
        assert result.tables[0].row_count_estimate is None

    def test_sample_rows_with_various_types(self) -> None:
        table = _make_table(
            "mixed",
            columns=(
                ColumnDefinition(name="id", data_type="INTEGER", nullable=False),
                ColumnDefinition(name="name", data_type="VARCHAR(50)", nullable=True),
                ColumnDefinition(name="created", data_type="TIMESTAMP", nullable=True),
                ColumnDefinition(name="photo", data_type="BYTEA", nullable=True),
                ColumnDefinition(name="score", data_type="FLOAT", nullable=True),
            ),
        )
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2],
                "name": ["Alice", None],
                "created": [
                    datetime.datetime(2024, 1, 1, 12, 0, 0),
                    datetime.datetime(2024, 6, 15, 8, 30, 0),
                ],
                "photo": [b"\xde\xad", None],
                "score": [95.5, None],
            }
        )
        schema = _make_schema([table])
        source = MockSourceConnector(schema, table_data={"mixed": [batch]})
        result = preview_source(source)

        rows = result.tables[0].sample_rows
        assert len(rows) == 2

        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Alice"
        assert rows[0]["created"] == "2024-01-01T12:00:00"
        assert rows[0]["photo"] == "dead"
        assert rows[0]["score"] == 95.5

        assert rows[1]["id"] == 2
        assert rows[1]["name"] is None
        assert rows[1]["score"] is None
        assert rows[1]["photo"] is None

    def test_sample_size_parameter_passed_as_batch_size(self) -> None:
        table = _make_table("t1")
        schema = _make_schema([table])
        source = MockSourceConnector(schema)
        preview_source(source, sample_size=5)

        assert len(source.read_table_calls) == 1
        assert source.read_table_calls[0]["batch_size"] == 5

    def test_sample_size_default_is_ten(self) -> None:
        table = _make_table("t1")
        schema = _make_schema([table])
        source = MockSourceConnector(schema)
        preview_source(source)

        assert source.read_table_calls[0]["batch_size"] == 10

    def test_only_first_batch_consumed(self) -> None:
        table = _make_table(
            "big",
            columns=(
                ColumnDefinition(name="id", data_type="INTEGER", nullable=False),
            ),
        )
        batch1 = pa.RecordBatch.from_pydict({"id": [1, 2, 3]})
        batch2 = pa.RecordBatch.from_pydict({"id": [4, 5, 6]})
        schema = _make_schema([table])
        source = MockSourceConnector(
            schema, table_data={"big": [batch1, batch2]}
        )
        result = preview_source(source)

        rows = result.tables[0].sample_rows
        assert len(rows) == 3
        assert [r["id"] for r in rows] == [1, 2, 3]

    def test_large_text_values_truncated_in_rows(self) -> None:
        table = _make_table(
            "docs",
            columns=(
                ColumnDefinition(name="body", data_type="TEXT", nullable=True),
            ),
        )
        long_text = "x" * 500
        batch = pa.RecordBatch.from_pydict({"body": [long_text]})
        schema = _make_schema([table])
        source = MockSourceConnector(schema, table_data={"docs": [batch]})
        result = preview_source(source)

        body_val = result.tables[0].sample_rows[0]["body"]
        assert len(body_val) == 203
        assert body_val.endswith("...")

    def test_binary_values_shown_as_hex(self) -> None:
        table = _make_table(
            "files",
            columns=(
                ColumnDefinition(name="data", data_type="BYTEA", nullable=True),
            ),
        )
        batch = pa.RecordBatch.from_pydict({"data": [b"\xca\xfe\xba\xbe"]})
        schema = _make_schema([table])
        source = MockSourceConnector(schema, table_data={"files": [batch]})
        result = preview_source(source)

        assert result.tables[0].sample_rows[0]["data"] == "cafebabe"

    def test_large_binary_values_truncated(self) -> None:
        table = _make_table(
            "blobs",
            columns=(
                ColumnDefinition(name="blob", data_type="BYTEA", nullable=True),
            ),
        )
        big_blob = bytes(range(256)) * 2
        batch = pa.RecordBatch.from_pydict({"blob": [big_blob]})
        schema = _make_schema([table])
        source = MockSourceConnector(schema, table_data={"blobs": [batch]})
        result = preview_source(source)

        blob_val = result.tables[0].sample_rows[0]["blob"]
        assert blob_val.endswith("...")
        # First 50 bytes hex = 100 hex chars + "..."
        assert len(blob_val) == 103

    def test_table_filtering_by_simple_name(self) -> None:
        t1 = _make_table("users")
        t2 = _make_table("orders")
        schema = _make_schema([t1, t2])
        source = MockSourceConnector(schema)
        result = preview_source(source, tables=["users"])

        assert len(result.tables) == 1
        assert result.tables[0].table_name == "users"

    def test_table_filtering_by_fully_qualified_name(self) -> None:
        t1 = _make_table("users", schema_name="public")
        t2 = _make_table("orders", schema_name="sales")
        schema = _make_schema([t1, t2])
        source = MockSourceConnector(schema)
        result = preview_source(source, tables=["sales.orders"])

        assert len(result.tables) == 1
        assert result.tables[0].table_name == "orders"
        assert result.tables[0].schema_name == "sales"

    def test_table_filtering_no_match(self) -> None:
        t1 = _make_table("users")
        schema = _make_schema([t1])
        source = MockSourceConnector(schema)
        result = preview_source(source, tables=["nonexistent"])

        assert len(result.tables) == 0

    def test_table_filtering_none_returns_all(self) -> None:
        t1 = _make_table("users")
        t2 = _make_table("orders")
        schema = _make_schema([t1, t2])
        source = MockSourceConnector(schema)
        result = preview_source(source, tables=None)

        assert len(result.tables) == 2

    def test_all_null_rows_handled(self) -> None:
        table = _make_table(
            "sparse",
            columns=(
                ColumnDefinition(name="id", data_type="INTEGER", nullable=True),
                ColumnDefinition(name="val", data_type="TEXT", nullable=True),
            ),
        )
        batch = pa.RecordBatch.from_pydict(
            {
                "id": pa.array([None, None], type=pa.int32()),
                "val": pa.array([None, None], type=pa.string()),
            }
        )
        schema = _make_schema([table])
        source = MockSourceConnector(schema, table_data={"sparse": [batch]})
        result = preview_source(source)

        rows = result.tables[0].sample_rows
        assert len(rows) == 2
        assert rows[0] == {"id": None, "val": None}
        assert rows[1] == {"id": None, "val": None}

    def test_json_serializability_of_result(self) -> None:
        """All values in sample_rows must be JSON-serializable."""
        table = _make_table(
            "jsontest",
            columns=(
                ColumnDefinition(name="id", data_type="INTEGER", nullable=False),
                ColumnDefinition(name="ts", data_type="TIMESTAMP", nullable=True),
                ColumnDefinition(name="data", data_type="BYTEA", nullable=True),
                ColumnDefinition(name="amount", data_type="TEXT", nullable=True),
            ),
        )
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2],
                "ts": [
                    datetime.datetime(2024, 3, 15, 9, 0, 0),
                    None,
                ],
                "data": [b"\x01\x02\x03", None],
                "amount": ["100.50", None],
            }
        )
        schema = _make_schema([table])
        source = MockSourceConnector(schema, table_data={"jsontest": [batch]})
        result = preview_source(source)

        # This will raise if any value is not JSON-serializable
        for tp in result.tables:
            for row in tp.sample_rows:
                json.dumps(row)

    def test_multiple_tables_previewed(self) -> None:
        t1 = _make_table(
            "users",
            columns=(
                ColumnDefinition(name="id", data_type="INTEGER", nullable=False),
            ),
        )
        t2 = _make_table(
            "orders",
            columns=(
                ColumnDefinition(name="id", data_type="INTEGER", nullable=False),
            ),
        )
        batch1 = pa.RecordBatch.from_pydict({"id": [1]})
        batch2 = pa.RecordBatch.from_pydict({"id": [10, 20]})
        schema = _make_schema([t1, t2])
        source = MockSourceConnector(
            schema, table_data={"users": [batch1], "orders": [batch2]}
        )
        result = preview_source(source)

        assert len(result.tables) == 2
        assert result.tables[0].table_name == "users"
        assert len(result.tables[0].sample_rows) == 1
        assert result.tables[1].table_name == "orders"
        assert len(result.tables[1].sample_rows) == 2

    def test_result_dataclasses_are_frozen(self) -> None:
        schema = _make_schema([])
        source = MockSourceConnector(schema)
        result = preview_source(source)

        # PreviewResult is frozen
        try:
            result.source_dialect = "other"  # type: ignore[misc]
            raise AssertionError("Should have raised FrozenInstanceError")  # noqa: TRY301
        except AttributeError:
            pass

    def test_column_preview_frozen(self) -> None:
        col = ColumnPreview(name="x", data_type="INT", nullable=False)
        try:
            col.name = "y"  # type: ignore[misc]
            raise AssertionError("Should have raised FrozenInstanceError")  # noqa: TRY301
        except AttributeError:
            pass

    def test_table_preview_frozen(self) -> None:
        tp = TablePreview(
            table_name="t",
            schema_name="s",
            row_count_estimate=None,
            columns=(),
            sample_rows=(),
        )
        try:
            tp.table_name = "other"  # type: ignore[misc]
            raise AssertionError("Should have raised FrozenInstanceError")  # noqa: TRY301
        except AttributeError:
            pass

    def test_schema_name_in_table_preview(self) -> None:
        table = _make_table("users", schema_name="myschema")
        schema = _make_schema([table])
        source = MockSourceConnector(schema)
        result = preview_source(source)

        assert result.tables[0].schema_name == "myschema"

    def test_decimal_values_serialized_as_string(self) -> None:
        """Decimal values from Arrow should become strings in output."""
        table = _make_table(
            "money",
            columns=(
                ColumnDefinition(
                    name="amount",
                    data_type="DECIMAL(10,2)",
                    nullable=True,
                ),
            ),
        )
        # Arrow decimal128 produces Python Decimal objects via to_pydict
        batch = pa.RecordBatch.from_pydict(
            {
                "amount": pa.array(
                    [Decimal("123.45"), Decimal("0.01")],
                    type=pa.decimal128(10, 2),
                ),
            }
        )
        schema = _make_schema([table])
        source = MockSourceConnector(schema, table_data={"money": [batch]})
        result = preview_source(source)

        rows = result.tables[0].sample_rows
        assert rows[0]["amount"] == "123.45"
        assert rows[1]["amount"] == "0.01"
        # Verify JSON-serializable
        json.dumps(rows[0])
