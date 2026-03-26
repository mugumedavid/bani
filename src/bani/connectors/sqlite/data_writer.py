"""SQLite data writer using batch INSERT for efficient bulk inserts.

Writes Arrow batches to SQLite tables using cursor.executemany()
with explicit transactions for throughput.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import pyarrow as pa

from bani.connectors.value_coercion import (
    DriverProfile,
    coerce_for_binding,
    register_driver_profile,
)

register_driver_profile("sqlite3", DriverProfile(
    decimal=False,   # sqlite3 cannot bind Decimal
    uuid=False,      # sqlite3 cannot bind UUID
    date=False,      # sqlite3 stores as TEXT
    time=False,      # sqlite3 stores as TEXT
    timedelta=False,
    list_ok=False,
    dict_ok=False,
    bytes=True,      # sqlite3 handles bytes as BLOB
))


class SQLiteDataWriter:
    """Writes Arrow batches to SQLite tables.

    Uses executemany() within explicit transactions for bulk performance.
    SQLite does not support concurrent writes, so all writes are serialized.
    """

    # Number of rows per executemany batch
    INSERT_BATCH_SIZE: int = 1000

    def __init__(self, connection: sqlite3.Connection) -> None:
        """Initialize the data writer.

        Args:
            connection: An active sqlite3 connection.
        """
        self.connection = connection

    def write_batch(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write an Arrow batch to a table.

        Uses explicit transactions with executemany for efficiency.

        Args:
            table_name: Name of the target table.
            schema_name: Schema name (ignored for SQLite — always 'main').
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.

        Raises:
            Exception: If writing fails.
        """
        if batch.num_rows == 0:
            return 0

        return self._write_insert(table_name, batch)

    def _write_insert(self, table_name: str, batch: pa.RecordBatch) -> int:
        """Write batch using executemany with explicit transactions.

        Args:
            table_name: Name of the target table.
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.
        """
        col_names = batch.schema.names
        col_list = ", ".join(f'"{name}"' for name in col_names)
        placeholders = ", ".join(["?"] * len(col_names))
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
                        coerce_for_binding(value.as_py(), "sqlite3")
                    )

            all_values.append(tuple(row_values))

        # Execute in batches — sqlite3 auto-manages transactions
        cursor = self.connection.cursor()
        try:
            for i in range(0, len(all_values), self.INSERT_BATCH_SIZE):
                chunk = all_values[i : i + self.INSERT_BATCH_SIZE]
                insert_sql = (
                    f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders})'
                )
                cursor.executemany(insert_sql, chunk)
                total_rows += len(chunk)

            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

        return total_rows
