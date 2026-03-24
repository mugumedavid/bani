"""SQLite data reader using cursor.fetchmany and Arrow batches.

Reads table data from SQLite using the stdlib sqlite3 module and
converts rows to pyarrow.RecordBatch instances with proper type mappings.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from bani.connectors.sqlite.type_mapper import SQLiteTypeMapper


class SQLiteDataReader:
    """Reads data from SQLite tables as Arrow batches.

    Uses cursor.fetchmany() for batched reading. Since SQLite is an
    in-process database, there are no server-side cursors — but
    fetchmany() still provides memory-efficient batching.
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        """Initialize the data reader.

        Args:
            connection: An active sqlite3 connection.
        """
        self.connection = connection
        self.type_mapper = SQLiteTypeMapper()

    def read_table(
        self,
        table_name: str,
        schema_name: str,
        columns: list[str] | None = None,
        filter_sql: str | None = None,
        batch_size: int = 100_000,
    ) -> Iterator[pa.RecordBatch]:
        """Read data from a table as Arrow batches.

        Args:
            table_name: Name of the table to read from.
            schema_name: Schema name (ignored for SQLite — always 'main').
            columns: Optional list of column names. If None, all columns.
            filter_sql: Optional WHERE clause (without WHERE keyword).
            batch_size: Number of rows per batch.

        Yields:
            pyarrow.RecordBatch instances.

        Raises:
            Exception: If reading fails.
        """
        # Build the SELECT query
        if columns is not None:
            col_list = ", ".join(f'"{col}"' for col in columns)
        else:
            col_list = "*"

        query = f'SELECT {col_list} FROM "{table_name}"'
        if filter_sql:
            query += f" WHERE {filter_sql}"

        cursor = self.connection.cursor()
        cursor.execute(query)

        if cursor.description is None:
            return

        col_names = [str(desc[0]) for desc in cursor.description]

        # Get Arrow types from PRAGMA table_info
        arrow_types = self._get_arrow_types(table_name, col_names)

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            yield self._make_record_batch(rows, col_names, arrow_types)

    def _get_arrow_types(
        self, table_name: str, col_names: list[str]
    ) -> list[pa.DataType]:
        """Determine Arrow types for columns from PRAGMA table_info.

        Args:
            table_name: Name of the table.
            col_names: Column names to look up.

        Returns:
            List of Arrow data types.
        """
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        pragma_rows: list[tuple[Any, ...]] = cursor.fetchall()

        # Build a name→type map from PRAGMA
        col_type_map: dict[str, str] = {}
        for _cid, name, col_type, _notnull, _dflt_value, _pk in pragma_rows:
            col_type_map[name] = col_type if col_type else ""

        arrow_types: list[pa.DataType] = []
        for col_name in col_names:
            declared_type = col_type_map.get(col_name, "")
            arrow_types.append(self.type_mapper.map_sqlite_type_name(declared_type))

        return arrow_types

    def _make_record_batch(
        self,
        rows: list[tuple[Any, ...]],
        col_names: list[str],
        arrow_types: list[pa.DataType],
    ) -> pa.RecordBatch:
        """Convert a list of rows to an Arrow RecordBatch.

        Handles SQLite-specific value coercion.

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
        arrays: list[pa.Array] = []
        fields: list[pa.Field] = []

        for col_name, arrow_type in zip(col_names, arrow_types, strict=True):
            col_data = columns_data[col_name]
            arrow_array = pa.array(col_data, type=arrow_type)
            arrays.append(arrow_array)
            fields.append(pa.field(col_name, arrow_type))

        schema = pa.schema(fields)
        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        """Get an estimated row count for a table.

        Tries sqlite_stat1 first, falls back to COUNT(*).

        Args:
            table_name: Name of the table.
            schema_name: Schema name (ignored for SQLite).

        Returns:
            Estimated row count.
        """
        cursor = self.connection.cursor()

        # Try sqlite_stat1
        try:
            cursor.execute(
                "SELECT stat FROM sqlite_stat1 WHERE tbl = ? AND idx IS NULL",
                (table_name,),
            )
            row = cursor.fetchone()
            if row and row[0]:
                parts = str(row[0]).split()
                if parts:
                    return int(parts[0])
        except sqlite3.OperationalError:
            pass

        # Fallback: COUNT(*)
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        result = cursor.fetchone()
        return int(result[0]) if result else 0
