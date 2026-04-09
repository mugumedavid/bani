"""MSSQL connector implementing both source and sink interfaces.

Prefers pyodbc with fast_executemany for high-throughput writes via ODBC
array parameter binding. Falls back to pymssql when the ODBC driver is
not available.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pymssql as pymssql_module

try:
    if os.environ.get("BANI_DISABLE_PYODBC"):
        raise ImportError("pyodbc disabled via BANI_DISABLE_PYODBC")
    # Ensure pyodbc can find the ODBC driver registry on macOS/Linux.
    # Homebrew installs odbcinst.ini to /usr/local/etc (Intel) or
    # /opt/homebrew/etc (Apple Silicon).
    if "ODBCSYSINI" not in os.environ:
        import platform as _plat

        # Apple Silicon first, then Intel, then system
        _candidates = ["/opt/homebrew/etc", "/usr/local/etc", "/etc"]
        if _plat.machine() != "arm64":
            _candidates = ["/usr/local/etc", "/opt/homebrew/etc", "/etc"]
        for _odbc_dir in _candidates:
            if os.path.isfile(os.path.join(_odbc_dir, "odbcinst.ini")):
                os.environ["ODBCSYSINI"] = _odbc_dir
                break
    import pyodbc as pyodbc_module
except ImportError:
    pyodbc_module = None

import re

from bani.connectors.base import SinkConnector, SourceConnector
from bani.connectors.default_translation import (
    DialectDefaultConfig,
    register_dialect_defaults,
    translate_default,
)
from bani.connectors.mssql.data_reader import MSSQLDataReader
from bani.connectors.mssql.data_writer import MSSQLDataWriter
from bani.connectors.mssql.schema_reader import MSSQLSchemaReader
from bani.connectors.mssql.type_mapper import MSSQLTypeMapper
from bani.connectors.pool import ConnectionPool
from bani.domain.project import ConnectionConfig
from bani.domain.schema import (
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)

register_dialect_defaults(
    "mssql",
    DialectDefaultConfig(
        timestamp_expression="GETDATE()",
        temporal_keywords=(
            "datetime",
            "date",
            "time",
            "smalldatetime",
            "datetime2",
            "datetimeoffset",
        ),
    ),
)

_log = logging.getLogger(__name__)

_CHAR_LENGTH_RE = re.compile(
    r"(?:n?var)?char(?:acter)?(?:2)?(?:\s+varying)?\s*\(\s*(\d+)\s*\)", re.IGNORECASE
)


def _extract_char_length(source_type: str) -> int | None:
    """Extract character length from a source type like varchar(255).

    Returns the length as an int, or None if the source type has no
    explicit length (e.g. ``text``, ``varchar`` without parens).
    """
    m = _CHAR_LENGTH_RE.search(source_type)
    return int(m.group(1)) if m else None


class MSSQLConnector(SourceConnector, SinkConnector):
    """MSSQL database connector.

    Implements both SourceConnector and SinkConnector to support reading
    from and writing to MSSQL databases. Uses pymssql for all database
    operations with UTF-8 charset by default.

    Supports SQL Server 2012+ via pymssql's broad compatibility.
    """

    # MSSQL writes are slower than other engines; smaller batches and
    # fewer workers prevent source connection timeouts during long writes.
    recommended_batch_size: int | None = 10_000
    recommended_parallel_workers: int | None = 2

    def __init__(self) -> None:
        """Initialize the MSSQL connector."""
        self.connection: Any = None
        self._schema_reader: MSSQLSchemaReader | None = None
        self._pool: ConnectionPool[Any] | None = None
        self._database: str = ""
        self._driver: str = "pymssql"

    def connect(self, config: ConnectionConfig, pool_size: int = 1) -> None:
        """Establish a connection to an MSSQL database.

        Tries pyodbc first (for fast_executemany support), then falls
        back to pymssql if the ODBC driver is not available.

        Args:
            config: Connection configuration with dialect="mssql".
            pool_size: Number of connections to create in the pool.

        Raises:
            ValueError: If required configuration is missing.
            Exception: If connection fails with both drivers.
        """
        if not config.host:
            raise ValueError("MSSQL connector requires 'host' in connection config")
        if not config.database:
            raise ValueError("MSSQL connector requires 'database' in connection config")

        username = self._resolve_env_var(config.username_env)
        password = self._resolve_env_var(config.password_env)

        port = config.port if config.port > 0 else 1433

        self._database = config.database
        self._config = config
        self._pool_size = pool_size

        # Determine which driver to use by testing a single connection
        driver = "pymssql"
        if pyodbc_module is not None:
            try:
                test_conn = self._connect_pyodbc(
                    config.host,
                    port,
                    config.database,
                    username,
                    password,
                )
                test_conn.close()
                driver = "pyodbc"
                _log.info("MSSQL: using pyodbc (fast_executemany)")
            except Exception as exc:
                _log.info(
                    "MSSQL: pyodbc connection failed (%s), falling back to pymssql",
                    exc,
                )

        self._driver = driver

        # Build pool factory based on chosen driver
        if driver == "pyodbc":

            def factory() -> Any:
                return self._connect_pyodbc(
                    config.host,
                    port,
                    config.database,
                    username,
                    password,
                )
        else:

            def factory() -> Any:
                return self._connect_pymssql(
                    config.host,
                    port,
                    config.database,
                    username,
                    password,
                    config.encrypt,
                )

        # pymssql/FreeTDS has known stability issues under concurrent
        # load — connections die with "DBPROCESS is dead".  Cap pool
        # to 1 for pymssql; pyodbc with MARS handles concurrency fine.
        effective_pool_size = pool_size if driver == "pyodbc" else 1
        if effective_pool_size < pool_size and pool_size > 1:
            _log.info(
                "MSSQL: pymssql driver — capping pool to 1 connection "
                "(pyodbc not available for full parallelism)"
            )
        self._pool_size = effective_pool_size

        self._pool = ConnectionPool(
            factory=factory,
            reset=lambda conn: conn.rollback(),
            close=lambda conn: conn.close(),
            size=effective_pool_size,
        )

        # Primary connection for backward compat and schema reads
        self.connection = self._pool.primary

        self._schema_reader = MSSQLSchemaReader(
            self.connection,
            self._database,
            reconnect_fn=self._pool._factory if self._driver == "pymssql" else None,
        )

    @staticmethod
    def _connect_pyodbc(
        host: str,
        port: int,
        database: str,
        username: str | None,
        password: str | None,
    ) -> Any:
        """Create a connection using pyodbc.

        Args:
            host: Server hostname.
            port: Server port.
            database: Database name.
            username: Optional username.
            password: Optional password.

        Returns:
            A pyodbc connection object.
        """
        parts = [
            "DRIVER={ODBC Driver 18 for SQL Server}",
            f"SERVER={host},{port}",
            f"DATABASE={database}",
            "TrustServerCertificate=yes",
            "MARS_Connection=yes",
        ]
        if username:
            parts.append(f"UID={username}")
        if password:
            parts.append(f"PWD={password}")

        conn_str = ";".join(parts)
        return pyodbc_module.connect(conn_str, autocommit=True)

    @staticmethod
    def _connect_pymssql(
        host: str,
        port: int,
        database: str,
        username: str | None,
        password: str | None,
        encrypt: bool,
    ) -> Any:
        """Create a connection using pymssql.

        Args:
            host: Server hostname.
            port: Server port.
            database: Database name.
            username: Optional username.
            password: Optional password.
            encrypt: Whether to use encrypted connection.

        Returns:
            A pymssql connection object.
        """
        connect_kwargs: dict[str, Any] = {
            "host": host,
            "port": port,
            "database": database,
            "charset": "UTF-8",
            "autocommit": True,
            "login_timeout": 60,
            "timeout": 0,  # No query timeout (long transfers)
            "tds_version": "7.3",  # TDS 7.3 for SQL Server 2008+
        }
        if username:
            connect_kwargs["user"] = username
        if password:
            connect_kwargs["password"] = password

        _log.info(
            "[MSSQL-CONN] pymssql connecting to %s:%d/%s timeout=%s tds=%s",
            host,
            port,
            database,
            connect_kwargs.get("timeout"),
            connect_kwargs.get("tds_version"),
        )
        conn = pymssql_module.connect(**connect_kwargs)
        _log.info("[MSSQL-CONN] pymssql connected OK: conn=%s", id(conn))

        try:
            with conn.cursor() as cur:
                cur.execute("SET TEXTSIZE 2147483647")
            _log.info("[MSSQL-CONN] SET TEXTSIZE OK on conn=%s", id(conn))
        except Exception as exc:
            _log.warning(
                "[MSSQL-CONN] SET TEXTSIZE failed on conn=%s: %s",
                id(conn),
                exc,
            )

        return conn

    def disconnect(self) -> None:
        """Close the database connection.

        Raises:
            Exception: If disconnection fails.
        """
        if self._pool is not None:
            self._pool.close_all()
            self._pool = None
        self.connection = None
        self._schema_reader = None

    def introspect_schema(self) -> DatabaseSchema:
        """Introspect the complete database schema.

        Reads all user tables and their metadata (columns, indexes,
        constraints, foreign keys) from the connected database.

        Returns:
            A DatabaseSchema object.

        Raises:
            RuntimeError: If not connected.
            Exception: If introspection fails.
        """
        if self.connection is None or self._schema_reader is None:
            raise RuntimeError("MSSQL connector is not connected")

        return self._schema_reader.read_schema()

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
            table_name: Name of the table.
            schema_name: Schema containing the table.
            columns: Optional list of column names.
            filter_sql: Optional WHERE clause (without WHERE keyword).
            batch_size: Number of rows per batch.

        Yields:
            pyarrow.RecordBatch instances.

        Raises:
            RuntimeError: If not connected.
            Exception: If reading fails.
        """
        if self._pool is None:
            raise RuntimeError("MSSQL connector is not connected")

        if self._driver == "pymssql":
            # pymssql: use a dedicated connection with periodic reconnect
            # to reset FreeTDS state on large tables.
            conn = self._pool._factory()
            try:
                reader = MSSQLDataReader(
                    conn,
                    self._driver,
                    reconnect_fn=self._pool._factory,
                )
                yield from reader.read_table(
                    table_name=table_name,
                    schema_name=schema_name,
                    columns=columns,
                    filter_sql=filter_sql,
                    batch_size=batch_size,
                )
            finally:
                try:
                    reader.connection.close()
                except Exception:
                    pass
        else:
            with self._pool.acquire() as conn:
                reader = MSSQLDataReader(conn, self._driver)
                yield from reader.read_table(
                    table_name=table_name,
                    schema_name=schema_name,
                    columns=columns,
                    filter_sql=filter_sql,
                    batch_size=batch_size,
                )

    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        """Get an estimated row count for a table.

        Args:
            table_name: Name of the table.
            schema_name: Schema containing the table.

        Returns:
            Estimated row count.

        Raises:
            RuntimeError: If not connected.
            Exception: If estimation fails.
        """
        if self._pool is None:
            raise RuntimeError("MSSQL connector is not connected")

        with self._pool.acquire() as conn:
            reader = MSSQLDataReader(conn, self._driver)
            return reader.estimate_row_count(table_name, schema_name)

    def create_table(self, table_def: TableDefinition) -> None:
        """Create a table in the database.

        Creates all columns, primary key, and check constraints.

        Args:
            table_def: TableDefinition describing the table.

        Raises:
            RuntimeError: If not connected.
            Exception: If table creation fails.
        """
        if self._pool is None:
            raise RuntimeError("MSSQL connector is not connected")
        if not table_def.columns:
            raise ValueError(f"Table {table_def.table_name} has no columns")

        col_defs = []
        for col in table_def.columns:
            if col.arrow_type_str:
                mssql_type = MSSQLTypeMapper.from_arrow_type(col.arrow_type_str)
                # Arrow 'string' loses varchar length → NVARCHAR(MAX).
                # Recover length from the source data_type when available
                # so that indexes on the column remain possible.
                if mssql_type == "NVARCHAR(MAX)" and col.data_type:
                    length = _extract_char_length(col.data_type)
                    # NVARCHAR max is 4000 chars; anything larger stays MAX
                    if length is not None and length <= 4000:
                        mssql_type = f"NVARCHAR({length})"
            else:
                mssql_type = col.data_type

            col_def = f"[{col.name}] {mssql_type}"

            if not col.nullable:
                col_def += " NOT NULL"

            if col.is_auto_increment:
                col_def += " IDENTITY(1,1)"
            elif col.default_value:
                translated = translate_default(col.default_value, "mssql", mssql_type)
                if translated is not None:
                    translated = self._normalize_default(translated, mssql_type)
                    col_def += f" DEFAULT {translated}"

            col_defs.append(col_def)

        if table_def.primary_key:
            pk_cols = ", ".join(f"[{col}]" for col in table_def.primary_key)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        for constraint in table_def.check_constraints:
            c = str(constraint)
            if "::" in c or "ARRAY[" in c or "ANY (" in c:
                continue
            col_defs.append(f"CHECK {c}")

        col_list = ", ".join(col_defs)

        # Drop FK constraints referencing this table, then drop the table
        schema = table_def.schema_name
        tname = table_def.table_name
        drop_fks_sql = f"""
            DECLARE @sql NVARCHAR(MAX) = N'';
            SELECT @sql += 'ALTER TABLE ' +
                QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.' +
                QUOTENAME(OBJECT_NAME(parent_object_id)) +
                ' DROP CONSTRAINT ' + QUOTENAME(name) + ';'
            FROM sys.foreign_keys
            WHERE referenced_object_id = OBJECT_ID('[{schema}].[{tname}]');
            EXEC sp_executesql @sql;
        """
        drop_sql = (
            f"IF OBJECT_ID('[{schema}].[{tname}]', 'U') "
            f"IS NOT NULL DROP TABLE [{schema}].[{tname}]"
        )
        create_sql = f"CREATE TABLE [{schema}].[{tname}] ({col_list})"

        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(drop_fks_sql)
                cur.execute(drop_sql)
                cur.execute(create_sql)

    def write_batch(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write an Arrow batch to a table.

        Args:
            table_name: Name of the target table.
            schema_name: Schema containing the table.
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.

        Raises:
            RuntimeError: If not connected.
            Exception: If writing fails.
        """
        if self._pool is None:
            raise RuntimeError("MSSQL connector is not connected")

        with self._pool.acquire() as conn:
            writer = MSSQLDataWriter(conn, self._driver)
            return writer.write_batch(table_name, schema_name, batch)

    @staticmethod
    def _normalize_default(raw_default: str, mssql_type: str) -> str:
        """Normalize a default value for MSSQL DDL.

        MySQL's information_schema returns bare string literals
        (e.g. ``pending``) that MSSQL interprets as column names.
        This method quotes them as ``'pending'``.
        """
        val = raw_default.strip()
        upper = val.upper()

        # Already quoted
        if val.startswith("'") and val.endswith("'"):
            return val

        # NULL
        if upper == "NULL":
            return val

        # Numeric literal
        stripped = val.lstrip("-")
        if stripped.replace(".", "", 1).isdigit():
            return val

        # SQL function call
        if "(" in val and ")" in val:
            return val

        # Boolean literals → MSSQL BIT uses 0/1
        if upper in ("TRUE", "FALSE"):
            return "1" if upper == "TRUE" else "0"

        # SQL keywords
        if upper in (
            "CURRENT_TIMESTAMP",
            "GETDATE",
            "NEWID",
            "DEFAULT",
        ):
            return val

        # Bare string → quote it
        escaped = val.replace("'", "''")
        return f"'{escaped}'"

    def create_indexes(
        self,
        table_name: str,
        schema_name: str,
        indexes: tuple[IndexDefinition, ...],
    ) -> None:
        """Create indexes on a table.

        Args:
            table_name: Name of the table.
            schema_name: Schema containing the table.
            indexes: Tuple of index definitions.

        Raises:
            RuntimeError: If not connected.
            Exception: If index creation fails.
        """
        if self._pool is None:
            raise RuntimeError("MSSQL connector is not connected")

        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                for index in indexes:
                    # MSSQL cannot index NVARCHAR(MAX) / VARBINARY(MAX).
                    # Narrow indexed MAX columns to a bounded length first.
                    self._narrow_max_columns_for_index(
                        cur, schema_name, table_name, index.columns
                    )

                    unique_kw = "UNIQUE" if index.is_unique else ""
                    col_list = ", ".join(f"[{col}]" for col in index.columns)

                    create_idx_sql = (
                        f"CREATE {unique_kw} INDEX [{index.name}] "
                        f"ON [{schema_name}].[{table_name}] ({col_list})"
                    )

                    # MSSQL treats NULLs as duplicates in unique indexes
                    # (unlike PostgreSQL). Add a WHERE filter to exclude
                    # NULLs so the unique constraint behaves like PG.
                    if index.is_unique:
                        not_null_filter = " AND ".join(
                            f"[{col}] IS NOT NULL" for col in index.columns
                        )
                        create_idx_sql += f" WHERE {not_null_filter}"

                    try:
                        cur.execute(create_idx_sql)
                    except Exception:
                        # Index may fail for other reasons (e.g. data
                        # incompatibility). Skip and continue.
                        pass

    def _narrow_max_columns_for_index(
        self,
        cur: Any,
        schema_name: str,
        table_name: str,
        columns: tuple[str, ...],
    ) -> None:
        """Alter NVARCHAR(MAX)/VARBINARY(MAX) columns to bounded lengths.

        MSSQL cannot use MAX-length columns as index keys.  When Arrow's
        ``string`` type loses the original varchar length, the column is
        created as NVARCHAR(MAX).  This method narrows only the columns
        that participate in the given index to NVARCHAR(4000) (MSSQL's
        largest indexable nvarchar length).
        """
        ph = "?" if self._driver == "pyodbc" else "%s"
        for col_name in columns:
            cur.execute(
                "SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = {ph} AND TABLE_NAME = {ph} "
                f"AND COLUMN_NAME = {ph}",
                (schema_name, table_name, col_name),
            )
            row = cur.fetchone()
            if row is None:
                continue
            dtype, max_len, nullable = row
            if dtype == "nvarchar" and max_len == -1:  # -1 means MAX
                null_kw = "NULL" if nullable == "YES" else "NOT NULL"
                cur.execute(
                    f"ALTER TABLE [{schema_name}].[{table_name}] "
                    f"ALTER COLUMN [{col_name}] NVARCHAR(4000) {null_kw}"
                )
            elif dtype == "varbinary" and max_len == -1:
                null_kw = "NULL" if nullable == "YES" else "NOT NULL"
                cur.execute(
                    f"ALTER TABLE [{schema_name}].[{table_name}] "
                    f"ALTER COLUMN [{col_name}] VARBINARY(900) {null_kw}"
                )

    def create_foreign_keys(self, fks: tuple[ForeignKeyDefinition, ...]) -> None:
        """Create foreign key constraints.

        Recovers from broken connections by reconnecting automatically.
        Gives up after 3 consecutive connection failures to avoid
        burning time on a persistently dead server.

        Args:
            fks: Tuple of foreign key definitions.

        Raises:
            RuntimeError: If not connected.
        """
        if self._pool is None:
            raise RuntimeError("MSSQL connector is not connected")

        consecutive_conn_failures = 0
        max_conn_failures = 3

        for fk in fks:
            if consecutive_conn_failures >= max_conn_failures:
                _log.warning(
                    "Aborting FK creation: %d consecutive connection failures. "
                    "Skipping remaining %d FKs.",
                    max_conn_failures,
                    len(fks),
                )
                break

            src_parts = fk.source_table.split(".")
            src_schema = src_parts[0] if len(src_parts) > 1 else self._database
            src_table = src_parts[-1]

            ref_parts = fk.referenced_table.split(".")
            ref_schema = ref_parts[0] if len(ref_parts) > 1 else self._database
            ref_table = ref_parts[-1]

            src_cols = ", ".join(f"[{col}]" for col in fk.source_columns)
            ref_cols = ", ".join(f"[{col}]" for col in fk.referenced_columns)

            unique_name = f"{src_table}_{fk.name}"[:128]

            on_delete = fk.on_delete or "NO ACTION"
            on_update = fk.on_update or "NO ACTION"

            alter_sql = (
                f"ALTER TABLE [{src_schema}].[{src_table}] "
                f"ADD CONSTRAINT [{unique_name}] "
                f"FOREIGN KEY ({src_cols}) "
                f"REFERENCES [{ref_schema}].[{ref_table}] "
                f"({ref_cols}) "
                f"ON DELETE {on_delete} ON UPDATE {on_update}"
            )

            try:
                with self._pool.acquire() as conn:
                    with conn.cursor() as cur:
                        cur.execute(alter_sql)
                consecutive_conn_failures = 0
            except Exception as exc:
                exc_str = str(exc)
                is_conn_error = (
                    "IMC06" in exc_str
                    or "connection is broken" in exc_str.lower()
                    or "communication link" in exc_str.lower()
                )
                if is_conn_error:
                    consecutive_conn_failures += 1
                    _log.warning(
                        "Connection broken during FK creation (%d/%d): %s",
                        consecutive_conn_failures,
                        max_conn_failures,
                        exc,
                    )
                    continue

                # 1785: cascade cycles — retry without cascade
                if "(1785)" in exc_str and (
                    on_delete != "NO ACTION" or on_update != "NO ACTION"
                ):
                    retry_sql = (
                        f"ALTER TABLE [{src_schema}].[{src_table}] "
                        f"ADD CONSTRAINT [{unique_name}] "
                        f"FOREIGN KEY ({src_cols}) "
                        f"REFERENCES [{ref_schema}].[{ref_table}] "
                        f"({ref_cols}) "
                        f"ON DELETE NO ACTION "
                        f"ON UPDATE NO ACTION"
                    )
                    try:
                        with self._pool.acquire() as conn:
                            with conn.cursor() as cur:
                                cur.execute(retry_sql)
                        _log.info(
                            "FK %s on %s.%s created with "
                            "NO ACTION (cascade would cycle)",
                            unique_name,
                            src_schema,
                            src_table,
                        )
                        continue
                    except Exception:
                        pass  # Fall through to skip

                # 1776: missing PK/unique — try creating a
                # unique index on the referenced columns first
                if "(1776)" in exc_str:
                    idx_name = f"UQ_{ref_table}_{'_'.join(fk.referenced_columns)}"[:128]
                    idx_cols = ", ".join(f"[{c}]" for c in fk.referenced_columns)
                    try:
                        with self._pool.acquire() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    f"CREATE UNIQUE INDEX "
                                    f"[{idx_name}] ON "
                                    f"[{ref_schema}].[{ref_table}] "
                                    f"({idx_cols})"
                                )
                                cur.execute(alter_sql)
                        _log.info(
                            "FK %s on %s.%s created after adding unique index on %s.%s",
                            unique_name,
                            src_schema,
                            src_table,
                            ref_schema,
                            ref_table,
                        )
                        continue
                    except Exception:
                        pass  # Fall through to skip

                # 1778: type mismatch — try aligning source column
                # types to match the referenced PK column types
                if "(1778)" in exc_str:
                    try:
                        with self._pool.acquire() as conn:
                            with conn.cursor() as cur:
                                for s_col, r_col in zip(
                                    fk.source_columns,
                                    fk.referenced_columns,
                                    strict=True,
                                ):
                                    cur.execute(
                                        "SELECT DATA_TYPE "
                                        "FROM INFORMATION_SCHEMA.COLUMNS "
                                        "WHERE TABLE_SCHEMA=%s "
                                        "AND TABLE_NAME=%s "
                                        "AND COLUMN_NAME=%s",
                                        (ref_schema, ref_table, r_col),
                                    )
                                    row = cur.fetchone()
                                    if row:
                                        cur.execute(
                                            f"ALTER TABLE "
                                            f"[{src_schema}].[{src_table}] "
                                            f"ALTER COLUMN [{s_col}] "
                                            f"{row[0]}"
                                        )
                                cur.execute(alter_sql)
                        _log.info(
                            "FK %s on %s.%s created after aligning column types",
                            unique_name,
                            src_schema,
                            src_table,
                        )
                        continue
                    except Exception:
                        pass  # Fall through to skip

                _log.warning(
                    "FK %s on %s.%s skipped: %s",
                    unique_name,
                    src_schema,
                    src_table,
                    exc_str[:120],
                )

    def execute_sql(self, sql_str: str) -> None:
        """Execute arbitrary SQL.

        Args:
            sql_str: SQL statement to execute.

        Raises:
            RuntimeError: If not connected.
            Exception: If execution fails.
        """
        if self._pool is None:
            raise RuntimeError("MSSQL connector is not connected")

        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_str)

    @staticmethod
    def _resolve_env_var(env_ref: str) -> str | None:
        """Resolve an environment variable reference.

        Format: ${env:VAR_NAME} or just VAR_NAME.

        Args:
            env_ref: Environment variable reference.

        Returns:
            The environment variable value, or None if not set.
        """
        if not env_ref:
            return None

        if env_ref.startswith("${env:") and env_ref.endswith("}"):
            var_name = env_ref[6:-1]
        else:
            var_name = env_ref

        return os.environ.get(var_name)
