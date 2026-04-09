"""MySQL data reader using server-side cursors and Arrow batches.

Reads table data efficiently using MySQL server-side cursors (SSCursor)
and converts rows to pyarrow.RecordBatch instances with proper type mappings.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import pyarrow as pa

if TYPE_CHECKING:
    import pymysql

from bani.connectors.mysql.type_mapper import MySQLTypeMapper


class MySQLDataReader:
    """Reads data from MySQL tables as Arrow batches.

    Uses server-side cursors (SSDictCursor/SSCursor) for efficient
    streaming of large tables. Converts MySQL types to Arrow types
    automatically, handling MySQL-specific quirks.
    """

    def __init__(self, connection: pymysql.connections.Connection[Any]) -> None:
        """Initialize the data reader.

        Args:
            connection: An active PyMySQL connection.
        """
        self.connection = connection
        self.type_mapper = MySQLTypeMapper()

    def read_table(
        self,
        table_name: str,
        schema_name: str,
        columns: list[str] | None = None,
        filter_sql: str | None = None,
        batch_size: int = 100_000,
    ) -> Iterator[pa.RecordBatch]:
        """Read data from a table as Arrow batches.

        Uses a server-side cursor (SSCursor) for memory-efficient streaming
        of large tables.

        Args:
            table_name: Name of the table to read from.
            schema_name: Schema (database) containing the table.
            columns: Optional list of column names. If None, all columns.
            filter_sql: Optional WHERE clause (without WHERE keyword).
            batch_size: Number of rows per batch.

        Yields:
            pyarrow.RecordBatch instances.

        Raises:
            Exception: If reading fails.
        """
        import pymysql.cursors

        # Build the SELECT query
        col_list = "*" if columns is None else ", ".join(f"`{col}`" for col in columns)
        query = f"SELECT {col_list} FROM `{schema_name}`.`{table_name}`"
        if filter_sql:
            query += f" WHERE {filter_sql}"

        # Use server-side cursor for memory efficiency
        cursor = self.connection.cursor(pymysql.cursors.SSCursor)
        try:
            cursor.execute(query)

            if cursor.description is None:
                return

            col_names = [str(desc[0]) for desc in cursor.description]
            col_type_codes = [int(desc[1]) for desc in cursor.description]
            # PyMySQL's cursor.description is 7-element (DB-API 2.0)
            # and does NOT include column flags. Read them from the
            # internal _result.fields which carries the full wire info.
            result_fields = getattr(getattr(cursor, "_result", None), "fields", None)
            if result_fields and len(result_fields) == len(col_names):
                col_flags = [int(f.flags) for f in result_fields]
                col_charsets = [int(f.charsetnr) for f in result_fields]
            else:
                col_flags = [0] * len(col_names)
                col_charsets = [0] * len(col_names)

            # Determine Arrow types for each column
            arrow_types = [
                self.type_mapper.map_mysql_type_code(tc, fl, cs)
                for tc, fl, cs in zip(
                    col_type_codes, col_flags, col_charsets, strict=True
                )
            ]

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

        Handles MySQL-specific value coercion (zero dates, timedelta, etc.).

        Args:
            rows: List of row tuples.
            col_names: Column names.
            arrow_types: Arrow types for each column.

        Returns:
            A pyarrow.RecordBatch.
        """
        # Organize data by column
        columns_data: dict[str, list[Any]] = {name: [] for name in col_names}

        for row in rows:
            for i, value in enumerate(row):
                coerced = self.type_mapper.coerce_value(value, arrow_types[i])
                columns_data[col_names[i]].append(coerced)

        # Convert each column to Arrow array
        arrays = []
        fields = []

        for col_name, arrow_type in zip(col_names, arrow_types, strict=True):
            col_data = columns_data[col_name]
            arrow_array = pa.array(col_data, type=arrow_type)
            arrays.append(arrow_array)
            fields.append(pa.field(col_name, arrow_type))

        schema = pa.schema(fields)
        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        """Get an estimated row count for a table.

        Uses information_schema.tables for a fast estimate.

        Args:
            table_name: Name of the table.
            schema_name: Schema (database) containing the table.

        Returns:
            Estimated row count.

        Raises:
            Exception: If the query fails.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT table_rows
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """
            cur.execute(query, (schema_name, table_name))
            result: tuple[Any, ...] | None = cur.fetchone()

            if result and result[0] is not None:
                return int(result[0])

        # Fallback: COUNT(*)
        with self.connection.cursor() as cur:
            query = f"SELECT COUNT(*) FROM `{schema_name}`.`{table_name}`"
            cur.execute(query)
            count_result: tuple[Any, ...] | None = cur.fetchone()
            return int(count_result[0]) if count_result else 0
