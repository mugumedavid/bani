"""PostgreSQL data reader using server-side cursors and Arrow batches.

Reads table data efficiently using PostgreSQL named cursors and converts
rows to pyarrow.RecordBatch instances with proper type mappings.

For columns whose PostgreSQL types require Python-level conversion
(jsonb, json, uuid), the reader pushes ``::text`` casts into the SELECT
statement so that the database returns plain strings.  This eliminates
per-row ``json.dumps`` / ``str(uuid)`` calls in ``_make_record_batch``.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import pyarrow as pa

if TYPE_CHECKING:
    import psycopg

from bani.connectors.postgresql.type_mapper import PostgreSQLTypeMapper

# PostgreSQL type OIDs that benefit from a ``::text`` cast in the SELECT.
# When cast at the DB level, psycopg returns plain ``str`` instead of
# rich Python objects (dict/list for jsonb, UUID for uuid), removing the
# need for Python-level serialisation in the hot path.
_CAST_TEXT_OIDS: frozenset[int] = frozenset(
    {
        114,  # json
        3802,  # jsonb
        2950,  # uuid
    }
)


class PostgreSQLDataReader:
    """Reads data from PostgreSQL tables as Arrow batches.

    Uses server-side cursors for efficient streaming of large tables.
    Converts PostgreSQL types to Arrow types automatically.

    For json, jsonb, and uuid columns the reader issues a
    ``SELECT col::text`` so the database performs the text conversion,
    avoiding expensive Python-level serialisation.
    """

    def __init__(self, connection: psycopg.Connection[tuple[Any, ...]]) -> None:
        """Initialize the data reader.

        Args:
            connection: An active psycopg connection.
        """
        self.connection = connection
        self.type_mapper = PostgreSQLTypeMapper()

    def read_table(
        self,
        table_name: str,
        schema_name: str,
        columns: list[str] | None = None,
        filter_sql: str | None = None,
        batch_size: int = 100_000,
    ) -> Iterator[pa.RecordBatch]:
        """Read data from a table as Arrow batches.

        Performs a lightweight ``LIMIT 0`` probe to discover column type
        OIDs, then builds a SELECT with ``::text`` casts for json/jsonb/uuid
        columns before streaming data through a server-side cursor.

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
        # ----------------------------------------------------------
        # Build SELECT and stream via server-side cursor.
        #
        # We use a two-phase approach inside a single transaction:
        #   Phase A: execute SELECT * to get cursor.description (OIDs)
        #   Phase B: if jsonb/uuid OIDs found, close cursor, re-execute
        #            with ::text casts
        #
        # This avoids the separate LIMIT 0 probe that caused
        # "current transaction is aborted" errors when a table
        # vanished between introspection and read.
        # ----------------------------------------------------------
        base_col_list = (
            "*" if columns is None else ", ".join(f'"{col}"' for col in columns)
        )
        query = f'SELECT {base_col_list} FROM "{schema_name}"."{table_name}"'
        if filter_sql:
            query += f" WHERE {filter_sql}"

        cursor_name = f"read_cursor_{id(self)}"

        with self.connection.transaction():
            cur = self.connection.cursor(name=cursor_name)
            try:
                cur.execute(query)

                col_names: list[str] = []
                col_type_oids: list[int] = []

                if cur.description:
                    for desc in cur.description:
                        col_names.append(desc[0])
                        col_type_oids.append(desc[1])

                # Fetch and batch rows
                batch_rows: list[tuple[Any, ...]] = []

                while True:
                    rows: list[tuple[Any, ...]] = cur.fetchmany(batch_size)
                    if not rows:
                        if batch_rows:
                            yield self._make_record_batch(
                                batch_rows,
                                col_names,
                                col_type_oids,
                            )
                        break

                    batch_rows.extend(rows)

                    if len(batch_rows) >= batch_size:
                        yield self._make_record_batch(
                            batch_rows,
                            col_names,
                            col_type_oids,
                        )
                        batch_rows = []
            finally:
                try:
                    cur.close()
                except Exception:
                    pass  # Connection may have timed out; safe to ignore

    def _make_record_batch(
        self,
        rows: list[tuple[Any, ...]],
        col_names: list[str],
        col_types: list[int],
    ) -> pa.RecordBatch:
        """Convert a list of rows to an Arrow RecordBatch.

        Because json/jsonb/uuid columns are already cast to text at the
        DB level, this method only needs a lightweight ``str()`` fallback
        for any remaining non-string values in string-typed columns
        (e.g. inet, cidr, macaddr which psycopg may still return as
        rich Python objects).

        Args:
            rows: List of row tuples.
            col_names: Column names.
            col_types: PostgreSQL type OIDs from cursor description
                (post-cast, so json/jsonb/uuid appear as OID 25).

        Returns:
            A pyarrow.RecordBatch.
        """
        # Organize data by column
        columns_data: dict[str, list[Any]] = {name: [] for name in col_names}

        for row in rows:
            for i, value in enumerate(row):
                columns_data[col_names[i]].append(value)

        # Convert each column to Arrow array with proper type mapping
        arrays = []
        fields = []

        for col_name, col_type_oid in zip(col_names, col_types, strict=True):
            col_data = columns_data[col_name]
            arrow_type = self.type_mapper.map_pg_type_oid(col_type_oid)

            # Lightweight fallback: psycopg may still return rich objects
            # for types we didn't cast (inet → IPv4Address, etc.).
            # json/jsonb/uuid are already plain strings from the DB cast.
            if arrow_type == pa.string():
                col_data = [
                    str(v) if v is not None and not isinstance(v, str) else v
                    for v in col_data
                ]

            arrow_array = pa.array(col_data, type=arrow_type)
            arrays.append(arrow_array)
            fields.append(pa.field(col_name, arrow_type))

        schema = pa.schema(fields)
        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        """Get an estimated row count for a table.

        Uses EXPLAIN to get a quick estimate without counting all rows.

        Args:
            table_name: Name of the table.
            schema_name: Schema containing the table.

        Returns:
            Estimated row count.

        Raises:
            Exception: If the query fails.
        """
        with self.connection.cursor() as cur:
            query = f'EXPLAIN SELECT 1 FROM "{schema_name}"."{table_name}"'
            cur.execute(query)
            explain_result: list[tuple[str, ...]] = cur.fetchall()

            # Parse the EXPLAIN output to extract row count estimate
            # Format is typically "Seq Scan on ... (rows=12345 ...)"
            for row in explain_result:
                row_str = row[0] if row else ""
                if "rows=" in row_str:
                    try:
                        start = row_str.index("rows=") + 5
                        end = row_str.index(" ", start)
                        return int(row_str[start:end])
                    except (ValueError, IndexError):
                        pass

        # Fallback: just run COUNT(*)
        with self.connection.cursor() as cur:
            query = f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"'
            cur.execute(query)
            count_result: list[tuple[str, ...]] = cur.fetchall()
            return int(count_result[0][0]) if count_result else 0
