"""Oracle data reader using cursor batches and Arrow batches.

Reads table data efficiently using oracledb cursors with array fetching
and converts rows to pyarrow.RecordBatch instances with proper type mappings.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import pyarrow as pa

if TYPE_CHECKING:
    import oracledb

from bani.connectors.oracle.type_mapper import OracleTypeMapper


class OracleDataReader:
    """Reads data from Oracle tables as Arrow batches.

    Uses oracledb cursor with array fetching (fetchmany) for efficient
    streaming of large tables. Converts Oracle types to Arrow types
    automatically.
    """

    def __init__(self, connection: oracledb.Connection, owner: str) -> None:
        """Initialize the data reader.

        Args:
            connection: An active oracledb connection.
            owner: The schema owner (user) for schema-qualified identifiers.
        """
        self.connection = connection
        self.owner = owner.upper()
        self.type_mapper = OracleTypeMapper()

    def read_table(
        self,
        table_name: str,
        schema_name: str,
        columns: list[str] | None = None,
        filter_sql: str | None = None,
        batch_size: int = 100_000,
    ) -> Iterator[pa.RecordBatch]:
        """Read data from a table as Arrow batches.

        Uses cursor array fetching for memory-efficient streaming of large tables.

        Args:
            table_name: Name of the table to read from.
            schema_name: Schema (owner) containing the table.
            columns: Optional list of column names. If None, all columns.
            filter_sql: Optional WHERE clause (without WHERE keyword).
            batch_size: Number of rows per batch.

        Yields:
            pyarrow.RecordBatch instances.

        Raises:
            Exception: If reading fails.
        """
        # Build the SELECT query
        col_list = "*" if columns is None else ", ".join(f'"{col}"' for col in columns)
        query = f'SELECT {col_list} FROM "{schema_name}"."{table_name}"'
        if filter_sql:
            query += f" WHERE {filter_sql}"

        cursor = self.connection.cursor()
        try:
            # Set array size for efficient fetching
            cursor.arraysize = batch_size
            cursor.execute(query)

            if cursor.description is None:
                return

            col_names = [str(desc[0]) for desc in cursor.description]

            # oracledb's cursor.description[i][1] is a DB API type object
            # (e.g. DB_TYPE_NUMBER), not a type name string.  Query
            # ALL_TAB_COLUMNS for the real Oracle type names.
            oracle_types = self._get_column_types(schema_name, table_name, col_names)
            arrow_types = [
                self.type_mapper.map_oracle_type_name(t) for t in oracle_types
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

        Handles Oracle-specific value coercion.

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
                # Oracle returns CLOB/BLOB as LOB objects — read() to
                # materialize the content as str/bytes.
                if hasattr(value, "read"):
                    value = value.read()
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

    def _get_column_types(
        self,
        schema_name: str,
        table_name: str,
        col_names: list[str],
    ) -> list[str]:
        """Look up Oracle data types for columns via ALL_TAB_COLUMNS.

        Returns full type strings (e.g. ``NUMBER(10,2)``, ``VARCHAR2(255)``)
        in the same order as *col_names*.
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                "SELECT column_name, data_type, data_length, "
                "data_precision, data_scale "
                "FROM all_tab_columns "
                "WHERE owner = :owner AND table_name = :tbl",
                {"owner": schema_name, "tbl": table_name},
            )
            rows: list[tuple[Any, ...]] = list(cursor.fetchall())
        finally:
            cursor.close()

        type_map: dict[str, str] = {}
        for name, dtype, dlen, prec, scale in rows:
            if dtype == "NUMBER" and prec is not None:
                ts = f"NUMBER({prec},{scale})" if scale else f"NUMBER({prec})"
            elif dtype in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW") and dlen:
                ts = f"{dtype}({dlen})"
            else:
                ts = dtype
            type_map[str(name)] = ts

        return [type_map.get(cn, "VARCHAR2") for cn in col_names]

    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        """Get an estimated row count for a table.

        Uses all_tables.num_rows for a fast estimate.

        Args:
            table_name: Name of the table.
            schema_name: Schema (owner) containing the table.

        Returns:
            Estimated row count.

        Raises:
            Exception: If the query fails.
        """
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT num_rows
                FROM all_tables
                WHERE owner = :owner AND table_name = :table_name
            """
            cursor.execute(query, {"owner": schema_name, "table_name": table_name})
            result: tuple[Any, ...] | None = cursor.fetchone()

            if result and result[0] is not None:
                return int(result[0])
        finally:
            cursor.close()

        # Fallback: COUNT(*) — this may be slow on large tables
        cursor = self.connection.cursor()
        try:
            query = f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"'
            cursor.execute(query)
            count_result: tuple[Any, ...] | None = cursor.fetchone()
            return int(count_result[0]) if count_result else 0
        finally:
            cursor.close()
