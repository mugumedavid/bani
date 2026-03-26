"""MSSQL data writer using batch INSERT for efficient bulk inserts.

Writes Arrow batches to MSSQL tables using multi-row INSERT statements
with configurable batch sizes for throughput.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa  # type: ignore[import-untyped]

from bani.connectors.value_coercion import (
    DriverProfile,
    coerce_for_binding,
    register_driver_profile,
)

register_driver_profile("pymssql", DriverProfile(
    decimal=False,   # pymssql doesn't adapt Decimal reliably
    uuid=False,
    time=False,      # pymssql time binding is unreliable
    timedelta=False,
    list_ok=False,
    dict_ok=False,
))

if TYPE_CHECKING:
    pass  # pymssql typing not needed since we use Any in __init__


class MSSQLDataWriter:
    """Writes Arrow batches to MSSQL tables.

    Uses multi-row INSERT statements for efficiency. Configurable batch
    size for throughput optimization.
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

        Uses multi-row INSERT for efficiency.

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
        """Write batch using multi-row INSERT statements.

        Batches rows into multi-row INSERT statements for throughput.

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
        col_list = ", ".join(f"[{name}]" for name in col_names)

        # Detect binary columns — pymssql cannot bind bytes reliably,
        # so we use CONVERT(VARBINARY(MAX), %s, 1) with a hex string.
        is_binary = [
            pa.types.is_binary(batch.schema.field(i).type)
            or pa.types.is_large_binary(batch.schema.field(i).type)
            for i in range(len(col_names))
        ]
        placeholders = ", ".join(
            "CONVERT(VARBINARY(MAX), %s, 1)" if b else "%s"
            for b in is_binary
        )
        total_rows = 0

        all_values: list[tuple[Any, ...]] = []
        for row_idx in range(batch.num_rows):
            row_values: list[Any] = []
            for col_idx in range(len(col_names)):
                column = batch[col_idx]
                value = column[row_idx]

                if not value.is_valid:
                    row_values.append(None)
                elif is_binary[col_idx]:
                    # Pass as "0x..." hex string for CONVERT(..., 1)
                    raw: bytes = value.as_py()
                    row_values.append("0x" + raw.hex())
                else:
                    row_values.append(
                        coerce_for_binding(value.as_py(), "pymssql")
                    )

            all_values.append(tuple(row_values))

        with self.connection.cursor() as cur:
            for i in range(0, len(all_values), self.INSERT_BATCH_SIZE):
                chunk = all_values[i : i + self.INSERT_BATCH_SIZE]
                insert_sql = (
                    f"INSERT INTO [{schema_name}].[{table_name}] "
                    f"({col_list}) VALUES ({placeholders})"
                )
                cur.executemany(insert_sql, chunk)
                total_rows += len(chunk)

        self.connection.commit()
        return total_rows
