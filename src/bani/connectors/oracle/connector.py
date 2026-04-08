"""Oracle connector implementing both source and sink interfaces.

Uses python-oracledb in THIN mode by default (pure Python, no Oracle Client
needed, supports Oracle 12c+).  When ``oracle_client_lib`` is provided via
connection options, the driver switches to THICK mode which supports Oracle
9.2+ (including 11g).
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from typing import Any

import pyarrow as pa

try:
    import oracledb
except ImportError as e:
    raise ImportError(
        "python-oracledb is not installed. Install it with: pip install oracledb"
    ) from e

from bani.connectors.base import SinkConnector, SourceConnector
from bani.connectors.default_translation import (
    DialectDefaultConfig,
    register_dialect_defaults,
    translate_default,
)

register_dialect_defaults("oracle", DialectDefaultConfig(
    timestamp_expression="SYSDATE",
    temporal_keywords=("date", "timestamp", "interval"),
))
from bani.connectors.oracle.data_reader import OracleDataReader
from bani.connectors.oracle.data_writer import OracleDataWriter
from bani.connectors.oracle.schema_reader import OracleSchemaReader
from bani.connectors.oracle.type_mapper import OracleTypeMapper
from bani.connectors.pool import ConnectionPool
from bani.domain.project import ConnectionConfig
from bani.domain.schema import (
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


_logger = logging.getLogger(__name__)

_thick_mode_initialised = False


def _init_thick_mode(lib_dir: str) -> None:
    """Activate oracledb thick mode (once per process).

    Thick mode enables connectivity to Oracle 9.2+ (including 11g)
    by loading the Oracle Instant Client shared libraries.
    """
    global _thick_mode_initialised  # noqa: PLW0603
    if _thick_mode_initialised:
        return
    oracledb.init_oracle_client(lib_dir=lib_dir)
    _thick_mode_initialised = True
    _logger.info("Oracle thick mode enabled (lib_dir=%s)", lib_dir)


class OracleConnector(SourceConnector, SinkConnector):
    """Oracle database connector.

    Implements both SourceConnector and SinkConnector to support reading
    from and writing to Oracle databases.  Uses python-oracledb in THIN
    mode by default (12c+).  Set the ``oracle_client_lib`` connection
    option to a path containing Oracle Instant Client to enable THICK
    mode (9.2+, including 11g).

    Connection options (passed via ``ConnectionConfig.extra``):
        service_name: Use a service name instead of SID.
        oracle_client_lib: Path to Oracle Instant Client libraries.
        ssl_cert_path: Path to SSL certificate.
    """

    def __init__(self) -> None:
        """Initialize the Oracle connector."""
        self.connection: oracledb.Connection | None = None
        self._schema_reader: OracleSchemaReader | None = None
        self._pool: ConnectionPool[oracledb.Connection] | None = None
        self._owner: str = ""

    def connect(self, config: ConnectionConfig, pool_size: int = 1) -> None:
        """Establish a connection to an Oracle database.

        Resolves credential environment variables and establishes the
        connection using python-oracledb in THIN mode.

        Args:
            config: Connection configuration with dialect="oracle".
                    Requires host, port, and either database (SID) or
                    service_name.
            pool_size: Number of connections to create in the pool.

        Raises:
            ValueError: If required configuration is missing.
            oracledb.DatabaseError: If connection fails.
        """
        # Validate configuration
        if not config.host:
            raise ValueError("Oracle connector requires 'host' in connection config")
        # Get optional settings from extra config
        service_name: str | None = None
        ssl_cert_path: str | None = None
        oracle_client_lib: str | None = None
        for key, value in config.extra:
            if key == "service_name":
                service_name = value
            elif key == "ssl_cert_path":
                ssl_cert_path = value
            elif key == "oracle_client_lib":
                oracle_client_lib = value

        # Switch to thick mode if oracle_client_lib is provided
        if oracle_client_lib:
            _init_thick_mode(oracle_client_lib)

        if not config.database and not service_name:
            raise ValueError(
                "Oracle connector requires 'database' or 'service_name' in "
                "connection config"
            )

        # Resolve credentials from environment variables
        username = self._resolve_env_var(config.username_env)
        password = self._resolve_env_var(config.password_env)

        # Determine port
        port = config.port if config.port > 0 else 1521

        # Build connection kwargs
        connect_kwargs: dict[str, Any] = {
            "host": config.host,
            "port": port,
            "user": username or "system",
            "password": password or "",
        }

        # Use service_name vs SID (database)
        if service_name:
            connect_kwargs["service_name"] = service_name
        elif config.database:
            connect_kwargs["sid"] = config.database

        self._config = config
        self._pool_size = pool_size

        # Create connection pool
        self._pool = ConnectionPool(
            factory=lambda: oracledb.connect(**connect_kwargs),
            reset=lambda conn: conn.rollback(),
            close=lambda conn: conn.close(),
            size=pool_size,
        )

        # Primary connection for backward compat and schema reads
        self.connection = self._pool.primary

        # Store the owner for schema-qualified identifiers
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT user FROM dual")
            result: tuple[Any, ...] | None = cursor.fetchone()
            self._owner = result[0] if result else (username or "system")
        finally:
            cursor.close()

        # Initialize schema reader on the primary connection
        self._schema_reader = OracleSchemaReader(self.connection, self._owner)

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
            raise RuntimeError("Oracle connector is not connected")

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
            schema_name: Schema (owner) containing the table.
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
            raise RuntimeError("Oracle connector is not connected")

        with self._pool.acquire() as conn:
            reader = OracleDataReader(conn, self._owner)
            yield from reader.read_table(
                table_name=table_name,
                schema_name=schema_name or self._owner,
                columns=columns,
                filter_sql=filter_sql,
                batch_size=batch_size,
            )

    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        """Get an estimated row count for a table.

        Args:
            table_name: Name of the table.
            schema_name: Schema (owner) containing the table.

        Returns:
            Estimated row count.

        Raises:
            RuntimeError: If not connected.
            Exception: If estimation fails.
        """
        if self._pool is None:
            raise RuntimeError("Oracle connector is not connected")

        with self._pool.acquire() as conn:
            reader = OracleDataReader(conn, self._owner)
            return reader.estimate_row_count(table_name, schema_name or self._owner)

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
            raise RuntimeError("Oracle connector is not connected")
        if not table_def.columns:
            raise ValueError(f"Table {table_def.table_name} has no columns")

        # Build column definitions
        pk_cols = set(table_def.primary_key)

        col_defs = []
        for col in table_def.columns:
            # Resolve target type via the canonical Arrow mapping layer
            # when arrow_type_str is available; fall back to raw data_type.
            if col.arrow_type_str:
                oracle_type = OracleTypeMapper.from_arrow_type(col.arrow_type_str)
            else:
                oracle_type = col.data_type

            # Oracle doesn't allow CLOB/BLOB in primary keys or unique indexes.
            # Downgrade to VARCHAR2(4000) for PK columns.
            if col.name in pk_cols and oracle_type in ("CLOB", "BLOB"):
                oracle_type = "VARCHAR2(4000)"

            col_def = f'"{col.name}" {oracle_type}'

            if col.is_auto_increment:
                col_def += " GENERATED BY DEFAULT AS IDENTITY"
            elif col.default_value:
                translated = translate_default(
                    col.default_value, "oracle", oracle_type
                )
                if translated is not None:
                    translated = self._normalize_default(translated, oracle_type)
                    col_def += f" DEFAULT {translated}"

            if not col.nullable and not col.is_auto_increment:
                col_def += " NOT NULL"

            col_defs.append(col_def)

        # Add primary key if present
        if table_def.primary_key:
            pk_cols = ", ".join(f'"{col}"' for col in table_def.primary_key)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        col_list = ", ".join(col_defs)

        # Use connected user as default schema if none specified
        schema = table_def.schema_name or self._owner
        fqn = f'"{schema}"."{table_def.table_name}"'

        # Drop existing table if present, then create
        drop_sql = f"DROP TABLE {fqn} CASCADE CONSTRAINTS PURGE"
        create_sql = f"CREATE TABLE {fqn} ({col_list})"

        with self._pool.acquire() as conn:
            cursor = conn.cursor()
            try:
                try:
                    cursor.execute(drop_sql)
                except Exception:
                    pass  # Table doesn't exist yet — that's fine
                cursor.execute(create_sql)
            finally:
                cursor.close()

    @staticmethod
    def _normalize_default(raw_default: str, oracle_type: str = "") -> str:
        """Quote bare string defaults for Oracle DDL."""
        val = raw_default.strip()
        upper = val.upper()

        # Boolean literals → 0/1 for NUMBER columns
        is_numeric = oracle_type.upper().startswith("NUMBER")
        if is_numeric and upper in ("TRUE", "FALSE", "T", "F"):
            return "1" if upper in ("TRUE", "T") else "0"
        if is_numeric and upper in ("0", "1"):
            return val

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
            "SYSDATE", "SYSTIMESTAMP", "CURRENT_TIMESTAMP",
            "CURRENT_DATE", "USER",
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
            schema_name: Schema (owner) containing the table.
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.

        Raises:
            RuntimeError: If not connected.
            Exception: If writing fails.
        """
        if self._pool is None:
            raise RuntimeError("Oracle connector is not connected")

        with self._pool.acquire() as conn:
            writer = OracleDataWriter(conn)
            return writer.write_batch(table_name, schema_name or self._owner, batch)

    def create_indexes(
        self,
        table_name: str,
        schema_name: str,
        indexes: tuple[IndexDefinition, ...],
    ) -> None:
        """Create indexes on a table.

        Args:
            table_name: Name of the table.
            schema_name: Schema (owner) containing the table.
            indexes: Tuple of index definitions.

        Raises:
            RuntimeError: If not connected.
            Exception: If index creation fails.
        """
        if self._pool is None:
            raise RuntimeError("Oracle connector is not connected")

        with self._pool.acquire() as conn:
            cursor = conn.cursor()
            try:
                for index in indexes:
                    unique_kw = "UNIQUE" if index.is_unique else ""
                    col_list = ", ".join(f'"{col}"' for col in index.columns)

                    schema = schema_name or self._owner
                    create_idx_sql = (
                        f"CREATE {unique_kw} INDEX "
                        f'"{index.name}" ON "{schema}"."{table_name}" '
                        f"({col_list})"
                    )

                    try:
                        cursor.execute(create_idx_sql)
                    except Exception as exc:
                        # ORA-01408: column list already indexed (PK covers it)
                        # ORA-00955: name already used (index name collision)
                        exc_str = str(exc)
                        if "ORA-01408" in exc_str or "ORA-00955" in exc_str:
                            continue
                        raise
            finally:
                cursor.close()

    def create_foreign_keys(self, fks: tuple[ForeignKeyDefinition, ...]) -> None:
        """Create foreign key constraints.

        Args:
            fks: Tuple of foreign key definitions.

        Raises:
            RuntimeError: If not connected.
            Exception: If FK creation fails.
        """
        if self._pool is None:
            raise RuntimeError("Oracle connector is not connected")

        with self._pool.acquire() as conn:
            cursor = conn.cursor()
            try:
                for fk in fks:
                    # Parse source table FQN
                    src_parts = fk.source_table.split(".")
                    src_schema = src_parts[0] if len(src_parts) > 1 else self._owner
                    src_table = src_parts[-1]

                    # Parse referenced table FQN
                    ref_parts = fk.referenced_table.split(".")
                    ref_schema = ref_parts[0] if len(ref_parts) > 1 else self._owner
                    ref_table = ref_parts[-1]

                    # Build column lists
                    src_cols = ", ".join(f'"{col}"' for col in fk.source_columns)
                    ref_cols = ", ".join(f'"{col}"' for col in fk.referenced_columns)

                    # Oracle requires constraint names to be unique per-schema.
                    # Prefix with source table to avoid collisions.
                    unique_name = f"{src_table}_{fk.name}"[:30]

                    alter_sql = (
                        f'ALTER TABLE "{src_schema}"."{src_table}" '
                        f'ADD CONSTRAINT "{unique_name}" FOREIGN KEY ({src_cols}) '
                        f'REFERENCES "{ref_schema}"."{ref_table}" ({ref_cols})'
                    )
                    if fk.on_delete and fk.on_delete.upper() in (
                        "CASCADE", "SET NULL",
                    ):
                        alter_sql += f" ON DELETE {fk.on_delete}"

                    try:
                        cursor.execute(alter_sql)
                    except Exception:
                        pass  # Skip FKs that fail (type mismatch, missing PK, etc.)
            finally:
                cursor.close()

    def execute_sql(self, sql_str: str) -> None:
        """Execute arbitrary SQL.

        Args:
            sql_str: SQL statement to execute.

        Raises:
            RuntimeError: If not connected.
            Exception: If execution fails.
        """
        if self._pool is None:
            raise RuntimeError("Oracle connector is not connected")

        with self._pool.acquire() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql_str)
            finally:
                cursor.close()

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
