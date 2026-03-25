"""Oracle data writer using batch INSERT for efficient bulk inserts.

Writes Arrow batches to Oracle tables using executemany with
multi-row INSERT statements (or parameterized batch inserts).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa

if TYPE_CHECKING:
    import oracledb


class OracleDataWriter:
    """Writes Arrow batches to Oracle tables.

    Uses executemany for efficient batch inserts. Oracle's arraysize
    setting determines batch efficiency.
    """

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
        """Write batch using parameterized INSERT statements.

        Args:
            table_name: Name of the target table.
            schema_name: Schema (owner) containing the table.
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.

        Raises:
            Exception: If INSERT fails.
        """
        col_names = batch.schema.names
        col_list = ", ".join(f'"{name}"' for name in col_names)
        placeholders = ", ".join([f":{i + 1}" for i in range(len(col_names))])

        insert_sql = (
            f'INSERT INTO "{schema_name}"."{table_name}" ({col_list}) '
            f"VALUES ({placeholders})"
        )

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
                    row_values.append(value.as_py())

            all_values.append(tuple(row_values))

        # Execute using executemany for efficiency
        cursor = self.connection.cursor()
        try:
            cursor.executemany(insert_sql, all_values)
            # Oracle doesn't return rows affected in executemany,
            # so we track the number manually
            return len(all_values)
        finally:
            cursor.close()
