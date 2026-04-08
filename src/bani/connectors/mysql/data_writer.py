"""MySQL data writer with LOAD DATA LOCAL INFILE and INSERT fallback.

Primary strategy: write Arrow batches via ``LOAD DATA LOCAL INFILE`` from
a temporary TSV file.  This bypasses SQL parsing and parameter binding,
giving significantly higher throughput on large batches.

Fallback strategy: if the server has ``local_infile`` disabled or the
LOAD DATA path fails for any reason, the writer falls back to PyMySQL's
``executemany`` which rewrites single-row INSERT templates into multi-row
``INSERT ... VALUES (...), (...), ...`` up to ``max_allowed_packet``.
"""

from __future__ import annotations

import io
import json
import logging
import os
import tempfile
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from bani.connectors.value_coercion import (
    DriverProfile,
    coerce_for_binding,
    register_driver_profile,
)

register_driver_profile(
    "pymysql",
    DriverProfile(
        decimal=False,  # PyMySQL chokes on Decimal in executemany
        uuid=False,
        timedelta=False,  # PyMySQL sends timedelta raw; MySQL TIME needs HH:MM:SS
        list_ok=False,
        dict_ok=False,
    ),
)

if TYPE_CHECKING:
    import pymysql

_log = logging.getLogger(__name__)


class MySQLDataWriter:
    """Writes Arrow batches to MySQL tables.

    Attempts ``LOAD DATA LOCAL INFILE`` first for maximum throughput,
    falling back to ``executemany`` if the server or connection does not
    support local infile.
    """

    def __init__(self, connection: pymysql.connections.Connection[Any]) -> None:
        """Initialize the data writer.

        Args:
            connection: An active PyMySQL connection.
        """
        self.connection = connection
        self._load_data_available: bool | None = None  # tri-state: unknown

    def write_batch(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write an Arrow batch to a table.

        Tries LOAD DATA LOCAL INFILE first; falls back to executemany
        on failure.

        Args:
            table_name: Name of the target table.
            schema_name: Schema (database) containing the table.
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.

        Raises:
            Exception: If both write strategies fail.
        """
        if batch.num_rows == 0:
            return 0

        # Fast path: LOAD DATA LOCAL INFILE
        if self._load_data_available is not False:
            try:
                rows = self._write_load_data(table_name, schema_name, batch)
                self._load_data_available = True
                return rows
            except Exception:
                if self._load_data_available is None:
                    _log.debug(
                        "LOAD DATA LOCAL INFILE unavailable, "
                        "falling back to executemany",
                        exc_info=True,
                    )
                    self._load_data_available = False
                else:
                    raise

        # Fallback: executemany INSERT
        return self._write_executemany(table_name, schema_name, batch)

    # ------------------------------------------------------------------
    # Primary strategy: LOAD DATA LOCAL INFILE
    # ------------------------------------------------------------------

    def _write_load_data(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write batch via LOAD DATA LOCAL INFILE from a temp TSV file.

        Converts Arrow columns to a tab-separated text buffer, writes it
        to a temporary file, and issues a LOAD DATA statement.  This
        bypasses SQL parsing entirely and is significantly faster than
        executemany for large batches.

        Args:
            table_name: Name of the target table.
            schema_name: Schema (database) containing the table.
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.

        Raises:
            Exception: If LOAD DATA fails (e.g. server has
                ``local_infile`` disabled).
        """
        col_names = batch.schema.names
        num_cols = len(col_names)
        # Strip column names — MySQL rejects trailing spaces
        col_list = ", ".join(f"`{name.strip()}`" for name in col_names)

        # Build TSV in memory using vectorized column extraction
        columns = [batch.column(i).to_pylist() for i in range(num_cols)]

        buf = io.BytesIO()
        for row_idx in range(batch.num_rows):
            vals: list[bytes] = []
            for col_idx in range(num_cols):
                val = columns[col_idx][row_idx]
                if val is None:
                    vals.append(b"\\N")
                else:
                    if isinstance(val, bool):
                        s = "1" if val else "0"
                    elif isinstance(val, (dict, list)):
                        s = json.dumps(val)
                    elif isinstance(val, bytes):
                        s = val.hex()
                    else:
                        s = str(val)
                    # Escape special characters for TSV format
                    s = (
                        s.replace("\\", "\\\\")
                        .replace("\t", "\\t")
                        .replace("\n", "\\n")
                        .replace("\r", "\\r")
                    )
                    vals.append(s.encode("utf-8"))
            buf.write(b"\t".join(vals) + b"\n")

        tsv_bytes = buf.getvalue()

        # Write to a temporary file for LOAD DATA LOCAL INFILE
        fd, tmp_path = tempfile.mkstemp(suffix=".tsv")
        try:
            os.write(fd, tsv_bytes)
            os.close(fd)

            load_sql = (
                f"LOAD DATA LOCAL INFILE '{tmp_path}' "
                f"INTO TABLE `{schema_name}`.`{table_name}` "
                f"CHARACTER SET utf8mb4 "
                f"FIELDS TERMINATED BY '\\t' "
                f"LINES TERMINATED BY '\\n' "
                f"({col_list})"
            )

            with self.connection.cursor() as cur:
                cur.execute(load_sql)

            self.connection.commit()
        finally:
            # Clean up temp file regardless of success/failure
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

        return int(batch.num_rows)

    # ------------------------------------------------------------------
    # Fallback strategy: executemany INSERT
    # ------------------------------------------------------------------

    def _write_executemany(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write batch using a single executemany call.

        PyMySQL's ``executemany`` automatically rewrites the INSERT
        template into multi-row statements up to the server's
        ``max_allowed_packet``, so we pass all rows at once and let
        the driver handle optimal splitting.

        Args:
            table_name: Name of the target table.
            schema_name: Schema (database) containing the table.
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.

        Raises:
            Exception: If INSERT fails.
        """
        col_names = batch.schema.names
        # Strip column names — MySQL rejects trailing spaces
        col_list = ", ".join(f"`{name.strip()}`" for name in col_names)
        placeholders = ", ".join(["%s"] * len(col_names))

        # Vectorized column extraction — one C-level to_pylist() per column
        columns = [batch.column(i).to_pylist() for i in range(len(col_names))]

        # Apply driver-specific coercion per column
        for col_idx in range(len(col_names)):
            columns[col_idx] = [
                coerce_for_binding(v, "pymysql") if v is not None else None
                for v in columns[col_idx]
            ]

        # Transpose columns to rows
        all_values: list[tuple[Any, ...]] = [
            tuple(row) for row in zip(*columns, strict=True)
        ]

        # Escape % in identifiers — PyMySQL's executemany uses
        # %-based string formatting internally, so literal % in
        # column names (e.g. "No and %") must be doubled.
        safe_col_list = col_list.replace("%", "%%")
        safe_table = table_name.replace("%", "%%")
        safe_schema = schema_name.replace("%", "%%")
        insert_sql = (
            f"INSERT INTO `{safe_schema}`.`{safe_table}` "
            f"({safe_col_list}) VALUES ({placeholders})"
        )

        # Single executemany call — PyMySQL splits internally
        with self.connection.cursor() as cur:
            cur.executemany(insert_sql, all_values)

        self.connection.commit()
        return len(all_values)
