"""MSSQL data reader using server-side cursors and Arrow batches.

Reads table data efficiently using MSSQL server-side cursors and converts
rows to pyarrow.RecordBatch instances with proper type mappings.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import pyarrow as pa  # type: ignore[import-untyped]

if TYPE_CHECKING:
    pass  # pymssql typing not needed since we use Any in __init__

from bani.connectors.mssql.type_mapper import MSSQLTypeMapper


class MSSQLDataReader:
    """Reads data from MSSQL tables as Arrow batches.

    Uses server-side cursors for efficient streaming of large tables.
    Converts MSSQL types to Arrow types automatically.
    """

    def __init__(self, connection: Any) -> None:
        """Initialize the data reader.

        Args:
            connection: An active pymssql connection.
        """
        self.connection = connection
        self.type_mapper = MSSQLTypeMapper()

    def read_table(
        self,
        table_name: str,
        schema_name: str,
        columns: list[str] | None = None,
        filter_sql: str | None = None,
        batch_size: int = 100_000,
    ) -> Iterator[pa.RecordBatch]:
        """Read data from a table as Arrow batches.

        Uses a server-side cursor for memory-efficient streaming of large tables.

        Args:
            table_name: Name of the table to read from.
            schema_name: Schema containing the table.
            columns: Optional list of column names. If None, all columns.
            filter_sql: Optional WHERE clause (without WHERE keyword).
            batch_size: Number of rows per batch.

        Yields:
            pyarrow.RecordBatch instances.

        Raises:
            Exception: If reading fails.
        """
        col_list = "*" if columns is None else ", ".join(f"[{col}]" for col in columns)
        query = f"SELECT {col_list} FROM [{schema_name}].[{table_name}]"
        if filter_sql:
            query += f" WHERE {filter_sql}"

        cursor = self.connection.cursor()
        try:
            cursor.execute(query)

            if cursor.description is None:
                return

            col_names = [str(desc[0]) for desc in cursor.description]

            # pymssql's cursor.description[i][1] is a Python type
            # object (int, str, bytes…), not a SQL type name.  Query
            # INFORMATION_SCHEMA for the real MSSQL type names.
            mssql_types = self._get_column_types(schema_name, table_name, col_names)
            arrow_types = [self.type_mapper.map_mssql_type_name(t) for t in mssql_types]

            batch_rows: list[tuple[Any, ...]] = []

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    if batch_rows:
                        yield self._make_record_batch(
                            batch_rows, col_names, arrow_types
                        )
                    break

                batch_rows.extend(rows)

                if len(batch_rows) >= batch_size:
                    yield self._make_record_batch(batch_rows, col_names, arrow_types)
                    batch_rows = []
        finally:
            cursor.close()

    def _make_record_batch(
        self,
        rows: list[tuple[Any, ...]],
        col_names: list[str],
        arrow_types: list[pa.DataType],
    ) -> pa.RecordBatch:
        """Convert a list of rows to an Arrow RecordBatch.

        Args:
            rows: List of row tuples.
            col_names: Column names.
            arrow_types: Arrow types for each column.

        Returns:
            A pyarrow.RecordBatch.
        """
        columns_data: dict[str, list[Any]] = {name: [] for name in col_names}

        for row in rows:
            for i, value in enumerate(row):
                columns_data[col_names[i]].append(value)

        arrays = []
        fields = []

        for col_name, arrow_type in zip(col_names, arrow_types, strict=True):
            col_data = columns_data[col_name]
            arrow_array = pa.array(col_data, type=arrow_type)
            arrays.append(arrow_array)
            fields.append(pa.field(col_name, arrow_type))

        schema = pa.schema(fields)
        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    def _get_column_types(
        self,
        schema_name: str,
        table_name: str,
        col_names: list[str],
    ) -> list[str]:
        """Look up MSSQL data types for columns via INFORMATION_SCHEMA.

        Returns types in the same order as *col_names*.  Falls back to
        ``"nvarchar"`` for any column not found (safe default).
        """
        with self.connection.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME, DATA_TYPE "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (schema_name, table_name),
            )
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        type_map = {str(name): str(dtype) for name, dtype in rows}
        return [type_map.get(cn, "nvarchar") for cn in col_names]

    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        """Get an estimated row count for a table.

        Uses sys.dm_db_partition_stats for a fast estimate, falls back to COUNT(*).

        Args:
            table_name: Name of the table.
            schema_name: Schema containing the table.

        Returns:
            Estimated row count.

        Raises:
            Exception: If the query fails.
        """
        try:
            with self.connection.cursor() as cur:
                query = """
                    SELECT SUM(row_count)
                    FROM sys.dm_db_partition_stats
                    WHERE object_id = OBJECT_ID(%s + '.' + %s)
                    AND index_id <= 1
                """
                cur.execute(query, (schema_name, table_name))
                result: tuple[Any, ...] | None = cur.fetchone()
                if result and result[0] is not None:
                    return int(result[0])
        except Exception:
            pass

        with self.connection.cursor() as cur:
            query = f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]"
            cur.execute(query)
            count_result: tuple[Any, ...] | None = cur.fetchone()
            return int(count_result[0]) if count_result else 0
