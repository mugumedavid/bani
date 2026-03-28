"""Data preview functionality for source databases.

Provides structured preview of table schemas and sample data,
with proper serialization of all value types for display or JSON output.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from bani.connectors.base import SourceConnector

_TEXT_TRUNCATION_LIMIT = 200
_BINARY_TRUNCATION_LIMIT = 50


@dataclass(frozen=True)
class ColumnPreview:
    """Preview metadata for a single column.

    Attributes:
        name: Column name.
        data_type: Source database type string (e.g. ``"VARCHAR(255)"``).
        nullable: Whether the column allows NULL values.
        arrow_type: Arrow type string from introspection (e.g. ``"int32"``).
    """

    name: str
    data_type: str
    nullable: bool
    arrow_type: str = ""


@dataclass(frozen=True)
class TablePreview:
    """Preview of a single table including schema and sample data.

    Attributes:
        table_name: Name of the table.
        schema_name: Database schema (namespace) the table belongs to.
        row_count_estimate: Estimated row count, or ``None`` if unavailable.
        columns: Column metadata in ordinal order.
        sample_rows: Sample rows as JSON-serializable dictionaries.
    """

    table_name: str
    schema_name: str
    row_count_estimate: int | None
    columns: tuple[ColumnPreview, ...]
    sample_rows: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class PreviewResult:
    """Complete preview result for one or more tables.

    Attributes:
        tables: Previewed tables.
        source_dialect: Dialect identifier of the source database.
    """

    tables: tuple[TablePreview, ...]
    source_dialect: str


def _make_json_serializable(value: Any) -> Any:
    """Convert a value to a JSON-serializable form.

    Handles datetime objects, bytes, Decimal, and other non-serializable types.
    Large text values are truncated to 200 characters; binary values are
    converted to a hex representation of the first 50 bytes.

    Args:
        value: Any Python value from an Arrow batch.

    Returns:
        A JSON-serializable representation.
    """
    if value is None:
        return None

    if isinstance(value, bytes | bytearray):
        truncated = value[:_BINARY_TRUNCATION_LIMIT]
        hex_str = truncated.hex()
        if len(value) > _BINARY_TRUNCATION_LIMIT:
            return hex_str + "..."
        return hex_str

    if isinstance(value, str):
        if len(value) > _TEXT_TRUNCATION_LIMIT:
            return value[:_TEXT_TRUNCATION_LIMIT] + "..."
        return value

    if isinstance(value, datetime.datetime):
        return value.isoformat()

    if isinstance(value, datetime.date):
        return value.isoformat()

    if isinstance(value, datetime.time):
        return value.isoformat()

    if isinstance(value, datetime.timedelta):
        return str(value)

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, int | float | bool):
        return value

    if isinstance(value, list):
        return [_make_json_serializable(item) for item in value]

    if isinstance(value, dict):
        return {str(k): _make_json_serializable(v) for k, v in value.items()}

    # Fallback: convert to string
    return str(value)


def preview_source(
    source: SourceConnector,
    tables: list[str] | None = None,
    sample_size: int = 10,
) -> PreviewResult:
    """Preview data from the source database.

    Introspects the schema to get column metadata, then reads a small sample
    of rows from each table. All values in the returned sample rows are
    JSON-serializable.

    Args:
        source: Connected source connector.
        tables: Optional list of table names to preview. Names can be
            simple (``"users"``) or fully qualified (``"public.users"``).
            If ``None``, all tables are previewed.
        sample_size: Number of rows to sample per table.

    Returns:
        PreviewResult with column metadata and sample rows.
    """
    schema = source.introspect_schema()

    table_previews: list[TablePreview] = []

    for table_def in schema.tables:
        # Filter tables if a list was provided
        if tables is not None:
            fqn = table_def.fully_qualified_name
            if (
                table_def.table_name not in tables
                and fqn not in tables
            ):
                continue

        # Build column previews from introspected schema
        columns = tuple(
            ColumnPreview(
                name=col.name,
                data_type=col.data_type,
                nullable=col.nullable,
                arrow_type=col.arrow_type_str or "",
            )
            for col in table_def.columns
        )

        # Read sample rows
        sample_rows: list[dict[str, Any]] = []
        for batch in source.read_table(
            table_def.table_name,
            table_def.schema_name,
            batch_size=sample_size,
        ):
            batch_dict = batch.to_pydict()
            num_rows = batch.num_rows
            for i in range(num_rows):
                row = {
                    col_name: _make_json_serializable(batch_dict[col_name][i])
                    for col_name in batch.column_names
                }
                sample_rows.append(row)
            # Only take the first batch
            break

        table_previews.append(
            TablePreview(
                table_name=table_def.table_name,
                schema_name=table_def.schema_name,
                row_count_estimate=table_def.row_count_estimate,
                columns=columns,
                sample_rows=tuple(sample_rows),
            )
        )

    return PreviewResult(
        tables=tuple(table_previews),
        source_dialect=schema.source_dialect,
    )
