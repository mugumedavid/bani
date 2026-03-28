"""Oracle data writer using batch INSERT for efficient bulk inserts.

Writes Arrow batches to Oracle tables using ``executemany`` with
``batcherrors=True`` for partial-success semantics and 10 000-row
chunking to limit memory pressure.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from datetime import datetime, time

logger = logging.getLogger(__name__)

from bani.connectors.value_coercion import (
    DriverProfile,
    coerce_for_binding,
    register_driver_profile,
)


def _oracle_time_to_datetime(val: time) -> datetime:
    """Oracle has no TIME type; promote to TIMESTAMP."""
    return datetime(1970, 1, 1, val.hour, val.minute,
                    val.second, val.microsecond)


register_driver_profile("oracledb", DriverProfile(
    uuid=False,      # oracledb has no UUID support
    time=False,      # Oracle has no TIME type
    timedelta=False,
    list_ok=False,
    dict_ok=False,
    custom_coercions=(("time", _oracle_time_to_datetime),),
))

if TYPE_CHECKING:
    import oracledb


class OracleDataWriter:
    """Writes Arrow batches to Oracle tables.

    Uses ``executemany`` with ``batcherrors=True`` for efficient batch
    inserts that tolerate individual row failures.  Rows are chunked
    into groups of :pyattr:`CHUNK_SIZE` to limit memory pressure while
    keeping round-trip overhead low.
    """

    CHUNK_SIZE: int = 10_000

    def __init__(self, connection: oracledb.Connection) -> None:
        """Initialize the data writer.

        Args:
            connection: An active oracledb connection.
        """
        self.connection = connection

    def write_batch(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write an Arrow batch to a table.

        Uses executemany for efficiency.

        Args:
            table_name: Name of the target table.
            schema_name: Schema (owner) containing the table.
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
        """Write batch using chunked executemany with batch error handling.

        Rows are split into chunks of :pyattr:`CHUNK_SIZE` and inserted
        with ``batcherrors=True`` so that individual row failures do not
        abort the entire batch.

        Args:
            table_name: Name of the target table.
            schema_name: Schema (owner) containing the table.
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows successfully written.

        Raises:
            Exception: If INSERT fails catastrophically (connection
                error, invalid SQL, etc.).
        """
        col_names = batch.schema.names
        col_list = ", ".join(f'"{name}"' for name in col_names)
        placeholders = ", ".join([f":{i + 1}" for i in range(len(col_names))])

        insert_sql = (
            f'INSERT INTO "{schema_name}"."{table_name}" ({col_list}) '
            f"VALUES ({placeholders})"
        )

        # Vectorized column extraction — one C-level to_pylist() per column
        columns = [batch.column(i).to_pylist() for i in range(len(col_names))]

        # Apply driver-specific coercion per column
        for col_idx in range(len(col_names)):
            columns[col_idx] = [
                coerce_for_binding(v, "oracledb") if v is not None else None
                for v in columns[col_idx]
            ]

        # Transpose columns to rows
        all_values: list[tuple[Any, ...]] = [
            tuple(row) for row in zip(*columns)
        ]

        total_rows = 0
        cursor = self.connection.cursor()
        try:
            # Set input sizes ONCE before the loop so the cursor knows
            # the correct bind types for every chunk.
            input_sizes = self._arrow_to_input_sizes(batch.schema)
            if input_sizes:
                cursor.setinputsizes(*input_sizes)

            for i in range(0, len(all_values), self.CHUNK_SIZE):
                chunk = all_values[i : i + self.CHUNK_SIZE]
                cursor.executemany(insert_sql, chunk, batcherrors=True)

                errors = cursor.getbatcherrors()
                if errors:
                    for error in errors:
                        logger.warning(
                            "Oracle batch insert error at offset %d: %s",
                            error.offset,
                            error.message,
                        )

                total_rows += len(chunk) - len(errors)
        finally:
            cursor.close()

        self.connection.commit()
        return total_rows

    @staticmethod
    def _arrow_to_input_sizes(
        schema: pa.Schema,
    ) -> list[object]:
        """Map Arrow schema to oracledb input size hints."""
        import oracledb

        sizes: list[object] = []
        for i in range(len(schema)):
            t = schema.field(i).type
            if pa.types.is_float32(t):
                sizes.append(oracledb.DB_TYPE_BINARY_FLOAT)
            elif pa.types.is_float64(t):
                sizes.append(oracledb.DB_TYPE_BINARY_DOUBLE)
            elif pa.types.is_binary(t) or pa.types.is_large_binary(t):
                sizes.append(oracledb.DB_TYPE_RAW)
            else:
                sizes.append(None)  # let oracledb infer
        return sizes
