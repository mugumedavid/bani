"""MSSQL data reader using Arrow batches.

Reads table data and converts rows to pyarrow.RecordBatch instances
with proper type mappings. Uses fetchmany() for batch streaming.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import pyarrow as pa  # type: ignore[import-untyped]

if TYPE_CHECKING:
    pass  # pymssql typing not needed since we use Any in __init__

from bani.connectors.mssql.type_mapper import MSSQLTypeMapper

logger = logging.getLogger(__name__)


class MSSQLDataReader:
    """Reads data from MSSQL tables as Arrow batches.

    Uses fetchmany() for batch streaming. Column type metadata is
    queried before the data cursor opens to avoid MARS conflicts
    with pymssql.
    """

    # Reconnect pymssql after every page to reset FreeTDS internal state.
    # FreeTDS accumulates state that corrupts under Docker networking,
    # even after just a few pages. Fresh connection per page is the
    # only reliable approach.
    _PYMSSQL_RECONNECT_INTERVAL = 1  # pages

    def __init__(
        self,
        connection: Any,
        driver: str = "pymssql",
        reconnect_fn: Any | None = None,
    ) -> None:
        """Initialize the data reader.

        Args:
            connection: An active database connection (pyodbc or pymssql).
            driver: The driver name, either ``"pyodbc"`` or ``"pymssql"``.
            reconnect_fn: Optional callable that returns a fresh connection.
                Used by pymssql for periodic reconnection.
        """
        self.connection = connection
        self.type_mapper = MSSQLTypeMapper()
        self._driver = driver
        self._ph = "?" if driver == "pyodbc" else "%s"
        self._reconnect_fn = reconnect_fn

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
            schema_name: Schema containing the table.
            columns: Optional list of column names. If None, all columns.
            filter_sql: Optional WHERE clause (without WHERE keyword).
            batch_size: Number of rows per batch.

        Yields:
            pyarrow.RecordBatch instances.

        Raises:
            Exception: If reading fails.
        """
        # Query column types BEFORE opening the data cursor.
        col_info = self._get_all_column_types(schema_name, table_name)

        col_list = "*" if columns is None else ", ".join(f"[{col}]" for col in columns)

        # pymssql loads the full result set into memory on execute()
        # (no server-side cursors). For large tables this causes
        # FreeTDS to kill the connection ("DBPROCESS is dead").
        # Use SQL-level OFFSET/FETCH pagination instead so each
        # query only transfers `batch_size` rows over the wire.
        logger.info(
            "[MSSQL-READ] read_table: %s.%s driver=%s batch_size=%d conn=%s",
            schema_name, table_name, self._driver, batch_size, id(self.connection),
        )
        if self._driver == "pymssql":
            # Cap page size for pymssql to keep TDS packets small
            pymssql_page = min(batch_size, 10_000)
            yield from self._read_table_paginated(
                schema_name, table_name, col_list, filter_sql,
                pymssql_page, col_info,
            )
        else:
            yield from self._read_table_cursor(
                schema_name, table_name, col_list, filter_sql,
                batch_size, col_info,
            )

    def _read_table_cursor(
        self,
        schema_name: str,
        table_name: str,
        col_list: str,
        filter_sql: str | None,
        batch_size: int,
        col_info: dict[str, str],
    ) -> Iterator[pa.RecordBatch]:
        """Read using fetchmany() — works with pyodbc (true streaming)."""
        query = f"SELECT {col_list} FROM [{schema_name}].[{table_name}]"
        if filter_sql:
            query += f" WHERE {filter_sql}"

        logger.info(
            "[MSSQL-READ] cursor: %s.%s batch_size=%d conn=%s",
            schema_name, table_name, batch_size, id(self.connection),
        )
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            if cursor.description is None:
                logger.warning("[MSSQL-READ] cursor: no description for %s.%s", schema_name, table_name)
                return

            col_names = [str(desc[0]) for desc in cursor.description]
            mssql_types = [col_info.get(cn, "nvarchar") for cn in col_names]
            arrow_types = [self.type_mapper.map_mssql_type_name(t) for t in mssql_types]

            batch_num = 0
            total_rows = 0
            while True:
                try:
                    rows = cursor.fetchmany(batch_size)
                except Exception as exc:
                    logger.error(
                        "[MSSQL-READ] cursor fetchmany FAILED for %s.%s batch %d (total_rows=%d): %s: %s",
                        schema_name, table_name, batch_num, total_rows,
                        type(exc).__name__, exc,
                    )
                    raise
                if not rows:
                    break
                total_rows += len(rows)
                logger.info(
                    "[MSSQL-READ] cursor %s.%s batch %d: %d rows (total=%d)",
                    schema_name, table_name, batch_num, len(rows), total_rows,
                )
                yield self._make_record_batch(rows, col_names, arrow_types)
                batch_num += 1

            logger.info("[MSSQL-READ] cursor %s.%s: completed, total=%d batches=%d", schema_name, table_name, total_rows, batch_num)
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def _read_table_paginated(
        self,
        schema_name: str,
        table_name: str,
        col_list: str,
        filter_sql: str | None,
        batch_size: int,
        col_info: dict[str, str],
    ) -> Iterator[pa.RecordBatch]:
        """Read using OFFSET/FETCH pagination — for pymssql.

        pymssql loads the entire result set into memory on execute(),
        so we issue separate queries for each page of rows. This keeps
        the wire transfer small and prevents FreeTDS from dying.
        """
        logger.info(
            "[MSSQL-READ] paginated: %s.%s batch_size=%d conn=%s",
            schema_name, table_name, batch_size, id(self.connection),
        )
        # First, get column names from a LIMIT 0 query
        probe_query = (
            f"SELECT TOP 0 {col_list} FROM [{schema_name}].[{table_name}]"
        )
        logger.info("[MSSQL-READ] probe query: %s", probe_query)
        try:
            with self.connection.cursor() as cur:
                cur.execute(probe_query)
                if cur.description is None:
                    logger.warning("[MSSQL-READ] probe returned no description")
                    return
                col_names = [str(desc[0]) for desc in cur.description]
        except Exception as exc:
            logger.error("[MSSQL-READ] probe FAILED: %s: %s", type(exc).__name__, exc)
            raise

        mssql_types = [col_info.get(cn, "nvarchar") for cn in col_names]
        arrow_types = [self.type_mapper.map_mssql_type_name(t) for t in mssql_types]
        logger.info("[MSSQL-READ] %s.%s: %d columns resolved", schema_name, table_name, len(col_names))

        base = f"SELECT {col_list} FROM [{schema_name}].[{table_name}]"
        if filter_sql:
            base += f" WHERE {filter_sql}"
        base += " ORDER BY (SELECT NULL)"

        offset = 0
        page_num = 0
        while True:
            page_query = f"{base} OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
            logger.info(
                "[MSSQL-READ] %s.%s page %d: offset=%d batch_size=%d conn=%s",
                schema_name, table_name, page_num, offset, batch_size, id(self.connection),
            )
            rows = None
            last_exc = None
            # Try with current batch_size, then halve on failure (down to 100)
            attempt_size = batch_size
            while attempt_size >= 100:
                retry_query = f"{base} OFFSET {offset} ROWS FETCH NEXT {attempt_size} ROWS ONLY"
                try:
                    # Reconnect before retry if not the first attempt
                    if last_exc is not None and self._reconnect_fn is not None:
                        logger.info(
                            "[MSSQL-READ] %s.%s: retrying page %d with batch_size=%d",
                            schema_name, table_name, page_num, attempt_size,
                        )
                        try:
                            self.connection.close()
                        except Exception:
                            pass
                        self.connection = self._reconnect_fn()

                    with self.connection.cursor() as cur:
                        cur.execute(retry_query)
                        rows = cur.fetchall()
                    # If we had to reduce, stick with the smaller size
                    # for the rest of this table
                    if attempt_size < batch_size:
                        logger.info(
                            "[MSSQL-READ] %s.%s: reducing batch_size %d → %d for remaining pages",
                            schema_name, table_name, batch_size, attempt_size,
                        )
                        batch_size = attempt_size
                    break  # success
                except Exception as exc:
                    last_exc = exc
                    next_size = attempt_size // 2
                    if next_size >= 100:
                        logger.info(
                            "[MSSQL-READ] %s.%s page %d: batch_size %d too large, reducing to %d and retrying",
                            schema_name, table_name, page_num, attempt_size, next_size,
                        )
                    else:
                        logger.warning(
                            "[MSSQL-READ] %s.%s page %d: failed at offset=%d with smallest batch_size=%d: %s",
                            schema_name, table_name, page_num, offset, attempt_size, exc,
                        )
                    attempt_size = next_size

            if rows is None:
                logger.error(
                    "[MSSQL-READ] %s.%s page %d: all retries exhausted at offset=%d",
                    schema_name, table_name, page_num, offset,
                )
                raise last_exc  # type: ignore[misc]

            if not rows:
                logger.info("[MSSQL-READ] %s.%s: no more rows at offset=%d", schema_name, table_name, offset)
                break

            logger.info(
                "[MSSQL-READ] %s.%s page %d: got %d rows",
                schema_name, table_name, page_num, len(rows),
            )
            yield self._make_record_batch(rows, col_names, arrow_types)
            offset += len(rows)
            page_num += 1

            if len(rows) < attempt_size:
                logger.info("[MSSQL-READ] %s.%s: last page (%d < %d)", schema_name, table_name, len(rows), batch_size)
                break

            # Periodic reconnection for pymssql: reset FreeTDS internal
            # state before corruption builds up over many pages.
            if (
                self._reconnect_fn is not None
                and page_num % self._PYMSSQL_RECONNECT_INTERVAL == 0
            ):
                logger.info(
                    "[MSSQL-READ] %s.%s: reconnecting at page %d to reset FreeTDS state",
                    schema_name, table_name, page_num,
                )
                try:
                    self.connection.close()
                except Exception:
                    pass
                self.connection = self._reconnect_fn()
                logger.info(
                    "[MSSQL-READ] %s.%s: reconnected OK, new conn=%s",
                    schema_name, table_name, id(self.connection),
                )

        logger.info("[MSSQL-READ] %s.%s: completed, total offset=%d pages=%d", schema_name, table_name, offset, page_num)

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

    def _get_all_column_types(
        self,
        schema_name: str,
        table_name: str,
    ) -> dict[str, str]:
        """Look up MSSQL data types for all columns via INFORMATION_SCHEMA.

        Called before the data cursor is opened to avoid MARS conflicts.

        Returns:
            Dict mapping column name to MSSQL type name.
        """
        with self.connection.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME, DATA_TYPE "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = {self._ph} AND TABLE_NAME = {self._ph}",
                (schema_name, table_name),
            )
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        return {str(name): str(dtype) for name, dtype in rows}

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
                query = f"""
                    SELECT SUM(row_count)
                    FROM sys.dm_db_partition_stats
                    WHERE object_id = OBJECT_ID({self._ph} + '.' + {self._ph})
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
