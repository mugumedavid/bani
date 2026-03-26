"""Oracle data writer using batch INSERT for efficient bulk inserts.

Writes Arrow batches to Oracle tables using executemany with
multi-row INSERT statements (or parameterized batch inserts).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa

from datetime import datetime, time

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
                    row_values.append(
                        coerce_for_binding(value.as_py(), "oracledb")
                    )

            all_values.append(tuple(row_values))

        # Execute using executemany for efficiency
        cursor = self.connection.cursor()
        try:
            # Tell oracledb the correct bind types based on Arrow schema
            # so it doesn't infer NUMBER for float columns (which can't
            # hold IEEE 754 extremes like 1.797E+308).
            input_sizes = self._arrow_to_input_sizes(batch.schema)
            if input_sizes:
                cursor.setinputsizes(*input_sizes)

            cursor.executemany(insert_sql, all_values)
            return len(all_values)
        finally:
            cursor.close()

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
