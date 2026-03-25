"""PostgreSQL data writer using COPY protocol for efficient bulk inserts.

Writes Arrow batches to PostgreSQL tables using the COPY FROM STDIN protocol
for maximum throughput, with fallback to INSERT batches if needed.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import psycopg

import pyarrow as pa


class PostgreSQLDataWriter:
    """Writes Arrow batches to PostgreSQL tables.

    Prefers the efficient COPY protocol but falls back to INSERT statements
    if necessary.
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

        Attempts to use COPY for efficiency; falls back to INSERT if needed.

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

        # Try COPY first (most efficient)
        try:
            return self._write_copy(table_name, schema_name, batch)
        except Exception:
            # Fall back to INSERT
            return self._write_insert(table_name, schema_name, batch)

    def _write_copy(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write batch using COPY FROM STDIN protocol.

        Args:
            table_name: Name of the target table.
            schema_name: Schema containing the table.
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.

        Raises:
            Exception: If COPY fails.
        """
        # Build the COPY command
        col_names = batch.schema.names
        col_list = ", ".join(f'"{name}"' for name in col_names)
        copy_sql = (
            f'COPY "{schema_name}"."{table_name}" ({col_list}) '
            "FROM STDIN WITH (FORMAT csv)"
        )

        # Convert batch to CSV format
        csv_data = self._batch_to_csv(batch)

        with self.connection.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                copy.write(csv_data)

        return int(batch.num_rows)

    def _batch_to_csv(self, batch: pa.RecordBatch) -> bytes:
        """Convert Arrow batch to CSV format for COPY.

        Args:
            batch: Arrow RecordBatch.

        Returns:
            CSV data as bytes.
        """
        output = io.StringIO()

        # Write each row as CSV
        for row_idx in range(batch.num_rows):
            row_values = []
            for col_idx, _col_name in enumerate(batch.schema.names):
                column = batch[col_idx]
                value = column[row_idx]

                # Convert value to CSV-safe string
                if value.is_valid == 0:  # NULL
                    row_values.append("")
                else:
                    scalar = value.as_py()
                    row_values.append(self._scalar_to_csv_value(scalar))

            output.write(",".join(row_values) + "\n")

        return output.getvalue().encode("utf-8")

    def _scalar_to_csv_value(self, value: Any) -> str:
        """Convert a Python scalar to CSV-safe string.

        Args:
            value: Python scalar value.

        Returns:
            CSV-safe string representation.
        """
        if value is None:
            return ""

        # Handle special types
        if isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, (list, dict)):
            # JSON types - convert to JSON string
            import json

            return json.dumps(value)
        elif isinstance(value, bytes):
            # Bytea - convert to escape sequence
            return f"\\\\x{value.hex()}"
        else:
            # For strings, escape quotes and backslashes
            str_val = str(value)
            if "," in str_val or '"' in str_val or "\n" in str_val:
                str_val = str_val.replace('"', '""')
                str_val = f'"{str_val}"'
            return str_val

    def _write_insert(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write batch using INSERT statements (fallback).

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
        total_rows = 0

        with self.connection.cursor() as cur:
            for row_idx in range(batch.num_rows):
                values = []
                for col_idx in range(len(col_names)):
                    column = batch[col_idx]
                    value = column[row_idx]

                    if value.is_valid == 0:  # NULL
                        values.append("NULL")
                    else:
                        scalar = value.as_py()
                        values.append(self._scalar_to_sql_literal(scalar))

                values_str = ", ".join(values)
                insert_sql = (
                    f'INSERT INTO "{schema_name}"."{table_name}" ({col_list}) '
                    f"VALUES ({values_str})"
                )

                cur.execute(insert_sql)
                total_rows += 1

        return total_rows

    def _scalar_to_sql_literal(self, value: Any) -> str:
        """Convert a Python scalar to SQL literal.

        Args:
            value: Python scalar value.

        Returns:
            SQL literal string (e.g., "'string'", "123", "true").
        """
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, bytes):
            return f"'\\\\x{value.hex()}'"
        elif isinstance(value, (list, dict)):
            import json

            json_str = json.dumps(value)
            # Escape single quotes for SQL
            json_str = json_str.replace("'", "''")
            return f"'{json_str}'"
        else:
            # String types
            str_val = str(value)
            # Escape single quotes
            str_val = str_val.replace("'", "''")
            return f"'{str_val}'"
