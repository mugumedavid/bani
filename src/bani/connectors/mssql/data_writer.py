"""MSSQL data writer using multi-row INSERT with inline values.

Writes Arrow batches to MSSQL tables using multi-row INSERT statements
with values formatted directly into the SQL string, bypassing pymssql's
slow per-parameter substitution.
"""

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pyarrow as pa  # type: ignore[import-untyped]

if TYPE_CHECKING:
    pass  # pymssql typing not needed since we use Any in __init__

# Max rows per INSERT statement. MSSQL allows 1000 rows per VALUES clause.
_MAX_ROWS_PER_INSERT = 1000


def _sql_literal(value: Any, is_binary: bool) -> str:
    """Format a Python value as a SQL literal for inline embedding.

    Args:
        value: The Python value (from Arrow .as_py()).
        is_binary: Whether the target column is binary.

    Returns:
        A SQL literal string safe for embedding in a statement.
    """
    if value is None:
        return "NULL"

    if is_binary:
        if isinstance(value, (bytes, bytearray)):
            return "0x" + value.hex()
        return "NULL"

    if isinstance(value, bool):
        return "1" if value else "0"

    if isinstance(value, int):
        return str(value)

    if isinstance(value, float):
        return repr(value)

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"N'{escaped}'"

    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return f"'{value!s}'"

    if isinstance(value, (bytes, bytearray)):
        return "0x" + value.hex()

    # Fallback: cast to string
    escaped = str(value).replace("'", "''")
    return f"N'{escaped}'"


class MSSQLDataWriter:
    """Writes Arrow batches to MSSQL tables.

    Formats values directly into multi-row INSERT statements to bypass
    pymssql's slow parameter substitution.
    """

    INSERT_BATCH_SIZE: int = 1000

    def __init__(self, connection: Any) -> None:
        """Initialize the data writer.

        Args:
            connection: An active pymssql connection.
        """
        self.connection = connection

    def write_batch(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write an Arrow batch to a table.

        Args:
            table_name: Name of the target table.
            schema_name: Schema containing the table.
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.

        Raises:
            Exception: If writing fails.
        """
        if batch.num_rows == 0:
            return 0

        return self._write_insert(table_name, schema_name, batch)

    def _write_insert(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write batch using multi-row INSERT with inline values.

        Builds ``INSERT ... VALUES (...), (...), ...`` with values
        formatted directly into the SQL string. No parameterised
        queries, no pymssql substitution overhead.

        Args:
            table_name: Name of the target table.
            schema_name: Schema containing the table.
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.
        """
        col_names = batch.schema.names
        num_cols = len(col_names)
        col_list = ", ".join(f"[{name}]" for name in col_names)

        is_binary = [
            pa.types.is_binary(batch.schema.field(i).type)
            or pa.types.is_large_binary(batch.schema.field(i).type)
            for i in range(num_cols)
        ]

        header = f"INSERT INTO [{schema_name}].[{table_name}] ({col_list}) VALUES "

        total_rows = 0

        with self.connection.cursor() as cur:
            for start in range(0, batch.num_rows, _MAX_ROWS_PER_INSERT):
                end = min(start + _MAX_ROWS_PER_INSERT, batch.num_rows)
                row_literals: list[str] = []

                for row_idx in range(start, end):
                    vals: list[str] = []
                    for col_idx in range(num_cols):
                        scalar = batch[col_idx][row_idx]
                        if not scalar.is_valid:
                            vals.append("NULL")
                        else:
                            vals.append(
                                _sql_literal(scalar.as_py(), is_binary[col_idx])
                            )
                    row_literals.append("(" + ", ".join(vals) + ")")

                sql = header + ", ".join(row_literals)
                cur.execute(sql)
                total_rows += end - start

        self.connection.commit()
        return total_rows
