"""MySQL data writer using batch INSERT for efficient bulk inserts.

Writes Arrow batches to MySQL tables using multi-row INSERT statements
with configurable batch sizes for throughput, with fallback to
single-row INSERT if needed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa

from bani.connectors.value_coercion import (
    DriverProfile,
    coerce_for_binding,
    register_driver_profile,
)

register_driver_profile("pymysql", DriverProfile(
    decimal=False,   # PyMySQL chokes on Decimal in executemany
    uuid=False,
    timedelta=False, # PyMySQL sends timedelta raw; MySQL TIME needs HH:MM:SS
    list_ok=False,
    dict_ok=False,
))

if TYPE_CHECKING:
    import pymysql


class MySQLDataWriter:
    """Writes Arrow batches to MySQL tables.

    Uses multi-row INSERT statements for efficiency. MySQL's
    max_allowed_packet setting determines the maximum size of a single
    INSERT statement, so rows are batched accordingly.
    """

    # Number of rows per multi-row INSERT statement
    INSERT_BATCH_SIZE: int = 1000

    def __init__(self, connection: pymysql.connections.Connection[Any]) -> None:
        """Initialize the data writer.

        Args:
            connection: An active PyMySQL connection.
        """
        self.connection = connection

    def write_batch(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write an Arrow batch to a table.

        Uses multi-row INSERT for efficiency.

        Args:
            table_name: Name of the target table.
            schema_name: Schema (database) containing the table.
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
            schema_name: Schema (database) containing the table.
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.

        Raises:
            Exception: If INSERT fails.
        """
        col_names = batch.schema.names
        col_list = ", ".join(f"`{name}`" for name in col_names)
        placeholders = ", ".join(["%s"] * len(col_names))
        total_rows = 0

        # Prepare all row values
        all_values: list[tuple[Any, ...]] = []
        for row_idx in range(batch.num_rows):
            row_values: list[Any] = []
            for col_idx in range(len(col_names)):
                column = batch[col_idx]
                value = column[row_idx]

                if not value.is_valid:
                    row_values.append(None)
                else:
                    row_values.append(
                        coerce_for_binding(value.as_py(), "pymysql")
                    )

            all_values.append(tuple(row_values))

        # Execute in batches
        with self.connection.cursor() as cur:
            for i in range(0, len(all_values), self.INSERT_BATCH_SIZE):
                chunk = all_values[i : i + self.INSERT_BATCH_SIZE]
                insert_sql = (
                    f"INSERT INTO `{schema_name}`.`{table_name}` "
                    f"({col_list}) VALUES ({placeholders})"
                )
                cur.executemany(insert_sql, chunk)
                total_rows += len(chunk)

        self.connection.commit()
        return total_rows
