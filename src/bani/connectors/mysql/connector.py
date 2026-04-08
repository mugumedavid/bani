"""MySQL connector implementing both source and sink interfaces.

Uses PyMySQL as the driver. Chosen over mysql-connector-python for its
pure-Python implementation (no C extension required), broad compatibility
with MySQL 5.x and 8.x, and its well-tested server-side cursor support.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pymysql

from bani.connectors.base import SinkConnector, SourceConnector
from bani.connectors.default_translation import (
    DialectDefaultConfig,
    register_dialect_defaults,
    translate_default,
)

register_dialect_defaults("mysql", DialectDefaultConfig(
    timestamp_expression="CURRENT_TIMESTAMP",
    temporal_keywords=("datetime", "timestamp", "date", "time"),
))
from bani.connectors.mysql.data_reader import MySQLDataReader
from bani.connectors.mysql.data_writer import MySQLDataWriter
from bani.connectors.mysql.schema_reader import MySQLSchemaReader
from bani.connectors.mysql.type_mapper import MySQLTypeMapper
from bani.connectors.pool import ConnectionPool
from bani.domain.project import ConnectionConfig
from bani.domain.schema import (
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)

logger = logging.getLogger(__name__)

_CHAR_LENGTH_RE = re.compile(
    r"(?:n?var)?char(?:acter)?(?:2)?(?:\s+varying)?\s*\(\s*(\d+)\s*\)", re.IGNORECASE
)


def _extract_char_length(source_type: str) -> int | None:
    """Extract character length from a source type like varchar(255)."""
    m = _CHAR_LENGTH_RE.search(source_type)
    return int(m.group(1)) if m else None


class MySQLConnector(SourceConnector, SinkConnector):
    """MySQL database connector.

    Implements both SourceConnector and SinkConnector to support reading
    from and writing to MySQL databases. Uses PyMySQL for all database
    operations with utf8mb4 charset by default.

    Supports MySQL 5.5+ through 9.0 via PyMySQL's broad compatibility.
    """

    def __init__(self) -> None:
        """Initialize the MySQL connector."""
        self.connection: pymysql.connections.Connection[Any] | None = None
        self._schema_reader: MySQLSchemaReader | None = None
        self._pool: ConnectionPool[pymysql.connections.Connection[Any]] | None = None
        self._database: str = ""

    def connect(self, config: ConnectionConfig, pool_size: int = 1) -> None:
        """Establish a connection to a MySQL database.

        Resolves credential environment variables and establishes the
        connection with utf8mb4 charset for full Unicode support.

        Args:
            config: Connection configuration with dialect="mysql".
            pool_size: Number of connections to create in the pool.

        Raises:
            ValueError: If required configuration is missing.
            pymysql.Error: If connection fails.
        """
        # Validate configuration
        if not config.host:
            raise ValueError("MySQL connector requires 'host' in connection config")
        if not config.database:
            raise ValueError("MySQL connector requires 'database' in connection config")

        # Resolve credentials from environment variables
        username = self._resolve_env_var(config.username_env)
        password = self._resolve_env_var(config.password_env)

        # Determine port
        port = config.port if config.port > 0 else 3306

        # Build connection kwargs
        connect_kwargs: dict[str, Any] = {
            "host": config.host,
            "port": port,
            "database": config.database,
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.Cursor,
            "autocommit": True,
            "local_infile": True,
        }

        if username:
            connect_kwargs["user"] = username

        if password:
            connect_kwargs["passwd"] = password

        # Handle TLS configuration
        if config.encrypt:
            connect_kwargs["ssl"] = {"ssl": True}
        else:
            connect_kwargs["ssl_disabled"] = True

        self._database = config.database
        self._config = config
        self._pool_size = pool_size

        # Create connection pool
        self._pool = ConnectionPool(
            factory=lambda: pymysql.connect(**connect_kwargs),
            reset=lambda conn: conn.rollback(),
            close=lambda conn: conn.close(),
            size=pool_size,
        )

        # Primary connection for backward compat and schema reads
        self.connection = self._pool.primary

        # Initialize schema reader on the primary connection
        self._schema_reader = MySQLSchemaReader(self.connection, self._database)

    def _schema(self, schema_name: str) -> str:
        """Default empty schema to the connected database name."""
        return schema_name or self._database

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
            raise RuntimeError("MySQL connector is not connected")

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
            schema_name: Schema (database) containing the table.
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
            raise RuntimeError("MySQL connector is not connected")

        with self._pool.acquire() as conn:
            reader = MySQLDataReader(conn)
            yield from reader.read_table(
                table_name=table_name,
                schema_name=self._schema(schema_name),
                columns=columns,
                filter_sql=filter_sql,
                batch_size=batch_size,
            )

    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        """Get an estimated row count for a table.

        Args:
            table_name: Name of the table.
            schema_name: Schema (database) containing the table.

        Returns:
            Estimated row count.

        Raises:
            RuntimeError: If not connected.
            Exception: If estimation fails.
        """
        if self._pool is None:
            raise RuntimeError("MySQL connector is not connected")

        with self._pool.acquire() as conn:
            reader = MySQLDataReader(conn)
            return reader.estimate_row_count(table_name, self._schema(schema_name))

    def create_table(self, table_def: TableDefinition) -> None:
        """Create a table in the database.

        Creates all columns, primary key, and check constraints.
        Uses InnoDB engine and utf8mb4 charset by default.

        Automatically handles MySQL version limitations by catching
        specific errors and retrying with workarounds:

        - **1071** (key too long): downgrades PK VARCHAR columns to
          VARCHAR(191) to fit the 767-byte InnoDB key limit.
        - **1118** (row too large): converts non-PK VARCHAR columns
          to TEXT to stay under the 65535-byte row limit.
        - **1067** (invalid default): strips problematic defaults.

        Args:
            table_def: TableDefinition describing the table.

        Raises:
            RuntimeError: If not connected.
            Exception: If table creation fails after retries.
        """
        if self._pool is None:
            raise RuntimeError("MySQL connector is not connected")
        if not table_def.columns:
            raise ValueError(f"Table {table_def.table_name} has no columns")

        self._create_table_impl(
            table_def,
            shorten_pk=False,
            compact_row=False,
            strip_defaults=False,
        )

    def _create_table_impl(
        self,
        table_def: TableDefinition,
        *,
        shorten_pk: bool,
        compact_row: bool,
        strip_defaults: bool,
    ) -> None:
        """Internal table creation with retry flags."""
        assert self._pool is not None

        pk_col_set = set(table_def.primary_key)

        # Build column definitions
        col_defs = []
        for col in table_def.columns:
            if col.arrow_type_str:
                mysql_type = MySQLTypeMapper.from_arrow_type(col.arrow_type_str)
                if mysql_type == "TEXT" and col.data_type:
                    length = _extract_char_length(col.data_type)
                    if length is not None and length <= 1000:
                        mysql_type = f"VARCHAR({length})"
            else:
                mysql_type = col.data_type

            # PK columns: TEXT→VARCHAR, and shorten if needed
            if col.name in pk_col_set and mysql_type in (
                "TEXT", "LONGTEXT", "MEDIUMTEXT",
            ):
                mysql_type = "VARCHAR(191)" if shorten_pk else "VARCHAR(255)"
            elif col.name in pk_col_set and shorten_pk:
                # Shorten existing VARCHAR PK columns to 191
                length = _extract_char_length(mysql_type)
                if length is not None and length > 191:
                    mysql_type = "VARCHAR(191)"

            # Row too large: convert non-PK VARCHAR to TEXT
            if compact_row and col.name not in pk_col_set:
                length = _extract_char_length(mysql_type)
                if length is not None and length > 255:
                    mysql_type = "TEXT"

            # Use LONGTEXT for large text to avoid 64KB TEXT limit
            if mysql_type == "TEXT" and col.data_type:
                dt = col.data_type.lower()
                if "text" in dt and "long" not in dt and "medium" not in dt:
                    mysql_type = "LONGTEXT"

            col_name = col.name.strip()
            col_def = f"`{col_name}` {mysql_type}"

            if not col.nullable:
                col_def += " NOT NULL"

            if col.is_auto_increment:
                col_def += " AUTO_INCREMENT"
            elif col.default_value and not strip_defaults:
                mu = mysql_type.upper()
                is_lob = any(
                    kw in mu
                    for kw in ("TEXT", "BLOB", "JSON", "GEOMETRY")
                )
                if not is_lob:
                    translated = translate_default(
                        col.default_value, "mysql", mysql_type
                    )
                    if translated is not None:
                        translated = self._normalize_default(
                            translated, mysql_type
                        )
                        col_def += f" DEFAULT {translated}"

            col_defs.append(col_def)

        # Add primary key if present
        if table_def.primary_key:
            pk_cols_sql = ", ".join(
                f"`{col}`" for col in table_def.primary_key
            )
            col_defs.append(f"PRIMARY KEY ({pk_cols_sql})")

        # Add check constraints
        for constraint in table_def.check_constraints:
            c = str(constraint)
            if "::" in c or "ARRAY[" in c or "ANY (" in c:
                continue
            col_defs.append(f"CHECK {c}")

        col_list = ", ".join(col_defs)

        schema = self._schema(table_def.schema_name)
        tname = table_def.table_name

        retry_kwargs: dict[str, bool] | None = None

        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute("SET FOREIGN_KEY_CHECKS=0")

                cur.execute(
                    "SELECT TABLE_NAME, CONSTRAINT_NAME "
                    "FROM information_schema.KEY_COLUMN_USAGE "
                    "WHERE REFERENCED_TABLE_SCHEMA = %s "
                    "AND REFERENCED_TABLE_NAME = %s",
                    (schema, tname),
                )
                for ref_table, fk_name in cur.fetchall():
                    try:
                        cur.execute(
                            f"ALTER TABLE `{schema}`.`{ref_table}` "
                            f"DROP FOREIGN KEY `{fk_name}`"
                        )
                    except Exception:
                        pass

                cur.execute(
                    f"DROP TABLE IF EXISTS `{schema}`.`{tname}`"
                )

                create_sql = (
                    f"CREATE TABLE `{schema}`.`{tname}` "
                    f"({col_list}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 "
                    f"COLLATE=utf8mb4_unicode_ci"
                )

                try:
                    cur.execute(create_sql)
                except Exception as exc:
                    code = getattr(exc, "args", (None,))[0]

                    if code == 1071 and not shorten_pk:
                        logger.warning(
                            "Table %s: key too long, retrying with "
                            "VARCHAR(191) PK columns",
                            tname,
                        )
                        retry_kwargs = {
                            "shorten_pk": True,
                            "compact_row": compact_row,
                            "strip_defaults": strip_defaults,
                        }
                    elif code == 1118 and not compact_row:
                        logger.warning(
                            "Table %s: row too large, retrying with "
                            "TEXT for large VARCHAR columns",
                            tname,
                        )
                        retry_kwargs = {
                            "shorten_pk": shorten_pk,
                            "compact_row": True,
                            "strip_defaults": strip_defaults,
                        }
                    elif code == 1067 and not strip_defaults:
                        logger.warning(
                            "Table %s: invalid default value, "
                            "retrying without defaults",
                            tname,
                        )
                        retry_kwargs = {
                            "shorten_pk": shorten_pk,
                            "compact_row": compact_row,
                            "strip_defaults": True,
                        }
                    else:
                        raise

                cur.execute("SET FOREIGN_KEY_CHECKS=1")

        if retry_kwargs is not None:
            self._create_table_impl(table_def, **retry_kwargs)

    @staticmethod
    def _normalize_default(raw_default: str, mysql_type: str) -> str:
        """Normalize a default value for MySQL DDL.

        Source databases may return bare string literals (e.g. ``pending``)
        that MySQL interprets as column names. This method quotes them.
        """
        val = raw_default.strip()
        upper = val.upper()

        if val.startswith("'") and val.endswith("'"):
            return val
        if upper == "NULL":
            return val
        stripped = val.lstrip("-")
        if stripped.replace(".", "", 1).isdigit():
            return val
        if "(" in val and ")" in val:
            return val
        if upper in (
            "CURRENT_TIMESTAMP", "TRUE", "FALSE",
            "CURRENT_DATE", "CURRENT_TIME",
        ):
            return val
        escaped = val.replace("'", "''")
        return f"'{escaped}'"

    def write_batch(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write an Arrow batch to a table.

        Args:
            table_name: Name of the target table.
            schema_name: Schema (database) containing the table.
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.

        Raises:
            RuntimeError: If not connected.
            Exception: If writing fails.
        """
        if self._pool is None:
            raise RuntimeError("MySQL connector is not connected")

        with self._pool.acquire() as conn:
            writer = MySQLDataWriter(conn)
            return writer.write_batch(table_name, self._schema(schema_name), batch)

    def create_indexes(
        self,
        table_name: str,
        schema_name: str,
        indexes: tuple[IndexDefinition, ...],
    ) -> None:
        """Create indexes on a table.

        Args:
            table_name: Name of the table.
            schema_name: Schema (database) containing the table.
            indexes: Tuple of index definitions.

        Raises:
            RuntimeError: If not connected.
            Exception: If index creation fails.
        """
        if self._pool is None:
            raise RuntimeError("MySQL connector is not connected")

        schema = self._schema(schema_name)
        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                # Query column types so we can add prefix lengths for TEXT/BLOB
                cur.execute(
                    "SELECT column_name, data_type "
                    "FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s",
                    (schema, table_name),
                )
                col_type_map: dict[str, str] = {
                    row[0]: row[1].upper() for row in cur.fetchall()
                }

                text_types = {"TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",
                              "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB"}

                for index in indexes:
                    unique_kw = "UNIQUE" if index.is_unique else ""
                    col_parts = []
                    for col in index.columns:
                        ctype = col_type_map.get(col, "")
                        if ctype in text_types:
                            col_parts.append(f"`{col}`(255)")
                        else:
                            col_parts.append(f"`{col}`")
                    col_list = ", ".join(col_parts)

                    create_idx_sql = (
                        f"CREATE {unique_kw} INDEX `{index.name}` "
                        f"ON `{schema}`.`{table_name}` ({col_list})"
                    )

                    try:
                        cur.execute(create_idx_sql)
                    except Exception as exc:
                        logging.getLogger(__name__).warning(
                            "Index %s on %s.%s skipped: %s",
                            index.name, schema, table_name, exc,
                        )

    def create_foreign_keys(self, fks: tuple[ForeignKeyDefinition, ...]) -> None:
        """Create foreign key constraints.

        Args:
            fks: Tuple of foreign key definitions.

        Raises:
            RuntimeError: If not connected.
            Exception: If FK creation fails.
        """
        if self._pool is None:
            raise RuntimeError("MySQL connector is not connected")

        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                for fk in fks:
                    # Parse source table FQN
                    src_parts = fk.source_table.split(".")
                    src_schema = src_parts[0] if len(src_parts) > 1 else self._database
                    src_table = src_parts[-1]

                    # Parse referenced table FQN
                    ref_parts = fk.referenced_table.split(".")
                    ref_schema = ref_parts[0] if len(ref_parts) > 1 else self._database
                    ref_table = ref_parts[-1]

                    # Build column lists
                    src_cols = ", ".join(f"`{col}`" for col in fk.source_columns)
                    ref_cols = ", ".join(f"`{col}`" for col in fk.referenced_columns)

                    # MySQL requires FK names to be unique per-database.
                    # Prefix with source table to avoid collisions.
                    unique_name = f"{src_table}_{fk.name}"[:64]

                    alter_sql = (
                        f"ALTER TABLE `{src_schema}`.`{src_table}` "
                        f"ADD CONSTRAINT `{unique_name}` FOREIGN KEY ({src_cols}) "
                        f"REFERENCES `{ref_schema}`.`{ref_table}` ({ref_cols}) "
                        f"ON DELETE {fk.on_delete} ON UPDATE {fk.on_update}"
                    )

                    try:
                        cur.execute(alter_sql)
                    except Exception:
                        pass  # Skip FKs that fail (type mismatch, missing PK, etc.)

    def execute_sql(self, sql_str: str) -> None:
        """Execute arbitrary SQL.

        Args:
            sql_str: SQL statement to execute.

        Raises:
            RuntimeError: If not connected.
            Exception: If execution fails.
        """
        if self._pool is None:
            raise RuntimeError("MySQL connector is not connected")

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

        # Handle ${env:VAR} format
        if env_ref.startswith("${env:") and env_ref.endswith("}"):
            var_name = env_ref[6:-1]
        else:
            var_name = env_ref

        return os.environ.get(var_name)
