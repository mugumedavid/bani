"""PostgreSQL data writer using COPY protocol for efficient bulk inserts.

Writes Arrow batches to PostgreSQL tables using the COPY FROM STDIN protocol
with psycopg3's write_row() for native serialization, with fallback to
multi-row INSERT batches if needed.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import psycopg

import pyarrow as pa

from bani.connectors.value_coercion import (
    DriverProfile,
    register_driver_profile,
)

# psycopg3 handles all common Python types natively — register a
# permissive profile so the test suite and any future callers work.
register_driver_profile("psycopg", DriverProfile())

logger = logging.getLogger(__name__)

_INSERT_BATCH_SIZE = 1000


class PostgreSQLDataWriter:
    """Writes Arrow batches to PostgreSQL tables.

    Prefers the efficient COPY protocol with write_row() but falls back
    to multi-row INSERT statements if necessary.
    """

    def __init__(self, connection: psycopg.Connection[tuple[Any, ...]]) -> None:
        """Initialize the data writer.

        Args:
            connection: An active psycopg connection.
        """
        self.connection = connection

    def write_batch(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write an Arrow batch to a table.

        Attempts COPY (binary, then text) for efficiency; falls back to
        multi-row INSERT if needed.

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

        # Try binary COPY first (fastest)
        try:
            return self._write_copy(table_name, schema_name, batch, binary=True)
        except Exception:
            logger.debug(
                "Binary COPY failed for %s.%s, falling back to text COPY",
                schema_name,
                table_name,
            )

        # Fall back to text COPY
        try:
            return self._write_copy(table_name, schema_name, batch, binary=False)
        except Exception:
            logger.debug(
                "Text COPY failed for %s.%s, falling back to INSERT",
                schema_name,
                table_name,
            )

        # Final fallback: multi-row INSERT
        return self._write_insert(table_name, schema_name, batch)

    def _extract_rows(self, batch: pa.RecordBatch) -> list[list[Any]]:
        """Extract rows from an Arrow batch as Python lists.

        Uses vectorized ``to_pylist()`` for efficient C-level column
        extraction, then post-processes JSON columns.  NULL values are
        automatically represented as ``None`` by ``to_pylist()``.

        Args:
            batch: Arrow RecordBatch.

        Returns:
            List of rows, each row a list of Python values.
        """
        num_cols = len(batch.schema)

        # Vectorized column extraction — one C-level to_pylist() per column
        columns = [batch.column(i).to_pylist() for i in range(num_cols)]

        # Post-process JSON columns (dict/list → str for psycopg jsonb/json)
        for col_idx in range(num_cols):
            col = columns[col_idx]
            first_val = next((v for v in col if v is not None), None)
            if isinstance(first_val, (dict, list)):
                columns[col_idx] = [
                    json.dumps(v) if isinstance(v, (dict, list)) else v
                    for v in col
                ]

        # Transpose columns to rows
        return [list(row) for row in zip(*columns)]

    def _write_copy(
        self,
        table_name: str,
        schema_name: str,
        batch: pa.RecordBatch,
        *,
        binary: bool,
    ) -> int:
        """Write batch using COPY FROM STDIN with write_row().

        Args:
            table_name: Name of the target table.
            schema_name: Schema containing the table.
            batch: Arrow RecordBatch to write.
            binary: If True, use FORMAT BINARY; otherwise plain text COPY.

        Returns:
            Number of rows written.

        Raises:
            Exception: If COPY fails.
        """
        col_names = batch.schema.names
        col_list = ", ".join(f'"{name}"' for name in col_names)
        fmt_clause = " (FORMAT BINARY)" if binary else ""
        copy_sql = (
            f'COPY "{schema_name}"."{table_name}" ({col_list}) '
            f"FROM STDIN{fmt_clause}"
        )

        rows = self._extract_rows(batch)

        with self.connection.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                for row in rows:
                    copy.write_row(row)

        return int(batch.num_rows)

    def _write_insert(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write batch using multi-row INSERT statements (fallback).

        Batches rows in groups of _INSERT_BATCH_SIZE using parameterized
        queries for safety and performance.

        Args:
            table_name: Name of the target table.
            schema_name: Schema containing the table.
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.

        Raises:
            Exception: If INSERT fails.
        """
        col_names = batch.schema.names
        col_list = ", ".join(f'"{name}"' for name in col_names)
        num_cols = len(col_names)

        rows = self._extract_rows(batch)
        total_rows = 0

        with self.connection.cursor() as cur:
            for chunk_start in range(0, len(rows), _INSERT_BATCH_SIZE):
                chunk = rows[chunk_start : chunk_start + _INSERT_BATCH_SIZE]

                # Build multi-row VALUES clause with placeholders
                single_row_ph = "(" + ", ".join(["%s"] * num_cols) + ")"
                values_clause = ", ".join([single_row_ph] * len(chunk))

                insert_sql = (
                    f'INSERT INTO "{schema_name}"."{table_name}" ({col_list}) '
                    f"VALUES {values_clause}"
                )

                # Flatten all row values into a single params list
                params: list[Any] = []
                for row in chunk:
                    params.extend(row)

                cur.execute(insert_sql, params)
                total_rows += len(chunk)

        return total_rows
