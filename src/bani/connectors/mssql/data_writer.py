"""MSSQL data writer with dual-driver support.

When pyodbc is available, uses ``fast_executemany`` for high-throughput
writes via ODBC array parameter binding (all rows sent in one TDS
packet).  Falls back to pymssql's inline-value INSERT approach when
pyodbc is not available.
"""

from __future__ import annotations

import datetime
import json
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pyarrow as pa  # type: ignore[import-untyped]

if TYPE_CHECKING:
    pass  # driver typing not needed since we use Any in __init__

# Max rows per INSERT statement. MSSQL allows 1000 rows per VALUES clause.
_MAX_ROWS_PER_INSERT = 1000


def _sql_literal(value: Any, is_binary: bool) -> str:
    """Format a Python value as a SQL literal for inline embedding.

    Used only by the pymssql fallback path.

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


def _pyodbc_safe_value(value: Any) -> Any:
    """Convert a Python value to one that pyodbc can handle natively.

    pyodbc handles most types (str, int, float, Decimal, datetime,
    bytes, UUID) natively.  Dict and list values (from JSON columns)
    need conversion to str via json.dumps.

    Args:
        value: The Python value (from Arrow .as_py()).

    Returns:
        A value safe for pyodbc parameter binding.
    """
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


class MSSQLDataWriter:
    """Writes Arrow batches to MSSQL tables.

    Supports two write strategies:
    - **pyodbc** (fast path): Uses ``cursor.fast_executemany = True``
      for ODBC array parameter binding, sending all rows in a single
      TDS packet.
    - **pymssql** (fallback): Formats values directly into multi-row
      INSERT statements.
    """

    INSERT_BATCH_SIZE: int = 1000

    def __init__(self, connection: Any, driver: str = "pymssql") -> None:
        """Initialize the data writer.

        Args:
            connection: An active database connection (pyodbc or pymssql).
            driver: The driver name, either ``"pyodbc"`` or ``"pymssql"``.
        """
        self.connection = connection
        self._driver = driver

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

        if self._driver == "pyodbc":
            return self._write_pyodbc(table_name, schema_name, batch)
        return self._write_pymssql(table_name, schema_name, batch)

    # ------------------------------------------------------------------
    # pyodbc fast path (fast_executemany)
    # ------------------------------------------------------------------

    def _write_pyodbc(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write batch using pyodbc's fast_executemany.

        Uses ODBC array parameter binding to send all rows in a single
        TDS packet.  No 2100 parameter limit applies when
        ``fast_executemany`` is enabled.

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
        placeholders = ", ".join("?" for _ in col_names)
        insert_sql = (
            f"INSERT INTO [{schema_name}].[{table_name}] "
            f"({col_list}) VALUES ({placeholders})"
        )

        # Vectorized column extraction — one C-level to_pylist() per column
        columns = [batch.column(i).to_pylist() for i in range(num_cols)]

        # Post-process JSON columns (dict/list → str) for pyodbc binding
        for col_idx in range(num_cols):
            col = columns[col_idx]
            # Check first non-None value to decide if this column needs JSON coercion
            first_val = next((v for v in col if v is not None), None)
            if isinstance(first_val, (dict, list)):
                columns[col_idx] = [
                    json.dumps(v) if isinstance(v, (dict, list)) else v
                    for v in col
                ]

        rows = [tuple(row) for row in zip(*columns)]

        cursor = self.connection.cursor()
        try:
            cursor.fast_executemany = True
            cursor.executemany(insert_sql, rows)
        finally:
            cursor.close()

        self.connection.commit()
        return batch.num_rows

    # ------------------------------------------------------------------
    # pymssql fallback (inline INSERT)
    # ------------------------------------------------------------------

    def _write_pymssql(
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

        # Vectorized column extraction — one C-level to_pylist() per column
        columns = [batch.column(i).to_pylist() for i in range(num_cols)]

        total_rows = 0

        with self.connection.cursor() as cur:
            for start in range(0, batch.num_rows, _MAX_ROWS_PER_INSERT):
                end = min(start + _MAX_ROWS_PER_INSERT, batch.num_rows)
                row_literals: list[str] = []

                for row_idx in range(start, end):
                    vals: list[str] = []
                    for col_idx in range(num_cols):
                        value = columns[col_idx][row_idx]
                        if value is None:
                            vals.append("NULL")
                        else:
                            vals.append(
                                _sql_literal(value, is_binary[col_idx])
                            )
                    row_literals.append("(" + ", ".join(vals) + ")")

                sql = header + ", ".join(row_literals)
                cur.execute(sql)
                total_rows += end - start

        self.connection.commit()
        return total_rows
