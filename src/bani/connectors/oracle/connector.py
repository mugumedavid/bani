"""Oracle connector implementing both source and sink interfaces.

Uses python-oracledb in THIN mode by default (pure Python, no Oracle Client
needed, supports Oracle 12c+).  When ``oracle_client_lib`` is provided via
connection options, the driver switches to THICK mode which supports Oracle
9.2+ (including 11g).
"""

from __future__ import annotations

import hashlib
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

register_dialect_defaults(
    "oracle",
    DialectDefaultConfig(
        timestamp_expression="SYSDATE",
        temporal_keywords=("date", "timestamp", "interval"),
    ),
)

_logger = logging.getLogger(__name__)

_thick_mode_initialised = False


def _init_thick_mode(lib_dir: str) -> None:
    """Activate oracledb thick mode (once per process).

    Thick mode enables connectivity to Oracle 9.2+ (including 11g)
    by loading the Oracle Instant Client shared libraries.
    """
    global _thick_mode_initialised
    if _thick_mode_initialised:
        return
    oracledb.init_oracle_client(lib_dir=lib_dir)
    _thick_mode_initialised = True
    _logger.info("Oracle thick mode enabled (lib_dir=%s)", lib_dir)


def _shorten_identifier(name: str, max_len: int = 30) -> str:
    """Shorten an identifier to *max_len* chars using a hash suffix.

    If the name already fits, it is returned unchanged.  Otherwise it is
    truncated and a 4-character hex hash is appended to avoid collisions.

    Example: ``eventvisualization_categoryoptiongroupsetdimensions``
             → ``eventvisualization_cate_a3f2``
    """
    if len(name) <= max_len:
        return name
    suffix = hashlib.md5(name.encode()).hexdigest()[:4]
    # Leave room for underscore + 4-char hash
    return name[: max_len - 5] + "_" + suffix


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
        # Mapping of original→shortened identifiers, populated when
        # ORA-00972 (identifier too long) is caught during create_table.
        self._name_map: dict[str, str] = {}
        # Per-row insert errors collected across all write_batch calls.
        self._insert_errors: list[str] = []

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
        oracle_client_lib: str | None = None
        for key, value in config.extra:
            if key == "service_name":
                service_name = value
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

    def post_migration(self) -> None:
        """Run post-migration tasks.

        Gathers schema statistics so that ``row_count_estimate`` from
        subsequent introspection calls reflects the actual row counts.
        """
        if self._pool is None:
            return
        try:
            with self._pool.acquire() as conn:
                cursor = conn.cursor()
                try:
                    # Submit as a background job so we don't block
                    cursor.execute(
                        "DECLARE v_job NUMBER; BEGIN "
                        "DBMS_JOB.SUBMIT(v_job, "
                        f"'DBMS_STATS.GATHER_SCHEMA_STATS(''{self._owner}'');'"
                        "); COMMIT; END;"
                    )
                except Exception:
                    _logger.debug(
                        "DBMS_STATS background job skipped",
                        exc_info=True,
                    )
                finally:
                    cursor.close()
        except Exception:
            pass  # Best-effort — don't fail the migration

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
        Automatically handles Oracle limitations by catching specific
        ORA errors and retrying with workarounds:

        - **ORA-00972** (identifier too long): truncates table/column
          names to 30 chars with a hash suffix.
        - **ORA-02000** (missing ALWAYS keyword): strips identity column
          syntax (unsupported before Oracle 12c).

        Args:
            table_def: TableDefinition describing the table.

        Raises:
            RuntimeError: If not connected.
            Exception: If table creation fails after retries.
        """
        if self._pool is None:
            raise RuntimeError("Oracle connector is not connected")
        if not table_def.columns:
            raise ValueError(f"Table {table_def.table_name} has no columns")

        self._create_table_impl(
            table_def,
            shorten_names=False,
            use_identity=True,
        )

    def _create_table_impl(
        self,
        table_def: TableDefinition,
        *,
        shorten_names: bool,
        use_identity: bool,
    ) -> None:
        """Internal table creation with retry flags."""
        assert self._pool is not None

        # Resolve table name
        tbl_name = table_def.table_name
        if shorten_names:
            tbl_name = self._shorten(table_def.table_name)

        # Build column definitions
        pk_col_set = set(table_def.primary_key)

        col_defs = []
        for col in table_def.columns:
            if col.arrow_type_str:
                oracle_type = OracleTypeMapper.from_arrow_type(col.arrow_type_str)
            else:
                oracle_type = col.data_type

            if col.name in pk_col_set and oracle_type in ("CLOB", "BLOB"):
                # Use VARCHAR2(255) for PK columns to stay within Oracle's
                # max key length (6398 bytes).  VARCHAR2(4000) would exceed
                # the limit on composite keys.
                oracle_type = "VARCHAR2(255)"

            col_name = self._shorten(col.name) if shorten_names else col.name
            col_def = f'"{col_name}" {oracle_type}'

            if col.is_auto_increment and use_identity:
                col_def += " GENERATED BY DEFAULT AS IDENTITY"
            elif col.default_value:
                translated = translate_default(col.default_value, "oracle", oracle_type)
                if translated is not None:
                    translated = self._normalize_default(translated, oracle_type)
                    col_def += f" DEFAULT {translated}"

            if not col.nullable and not col.is_auto_increment:
                col_def += " NOT NULL"

            col_defs.append(col_def)

        # Add primary key
        if table_def.primary_key:
            pk_cols_sql = ", ".join(
                f'"{self._shorten(c) if shorten_names else c}"'
                for c in table_def.primary_key
            )
            col_defs.append(f"PRIMARY KEY ({pk_cols_sql})")

        col_list = ", ".join(col_defs)

        schema = table_def.schema_name or self._owner
        fqn = f'"{schema}"."{tbl_name}"'

        drop_sql = f"DROP TABLE {fqn} CASCADE CONSTRAINTS PURGE"
        create_sql = f"CREATE TABLE {fqn} ({col_list})"

        retry_kwargs: dict[str, bool] | None = None

        with self._pool.acquire() as conn:
            cursor = conn.cursor()
            try:
                try:
                    cursor.execute(drop_sql)
                except Exception:
                    pass  # Table doesn't exist yet — fine

                try:
                    cursor.execute(create_sql)
                except Exception as exc:
                    exc_str = str(exc)

                    # ORA-00972: identifier is too long → retry with
                    # shortened names
                    if "ORA-00972" in exc_str and not shorten_names:
                        _logger.warning(
                            "Table %s: identifier too long, retrying "
                            "with shortened names",
                            table_def.table_name,
                        )
                        retry_kwargs = {
                            "shorten_names": True,
                            "use_identity": use_identity,
                        }
                    # ORA-02000: missing ALWAYS keyword → retry without
                    # identity syntax (pre-12c)
                    elif "ORA-02000" in exc_str and use_identity:
                        _logger.warning(
                            "Table %s: IDENTITY not supported, "
                            "retrying without identity columns",
                            table_def.table_name,
                        )
                        retry_kwargs = {
                            "shorten_names": shorten_names,
                            "use_identity": False,
                        }
                    else:
                        raise
            finally:
                cursor.close()

        # Retry outside the connection/cursor scope
        if retry_kwargs is not None:
            self._create_table_impl(table_def, **retry_kwargs)

    def _shorten(self, name: str, max_len: int = 30) -> str:
        """Shorten an identifier and record the mapping."""
        short = _shorten_identifier(name, max_len)
        if short != name:
            self._name_map[name] = short
        return short

    def _resolve_name(self, name: str) -> str:
        """Return the shortened name if one exists, else the original."""
        return self._name_map.get(name, name)

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
            "SYSDATE",
            "SYSTIMESTAMP",
            "CURRENT_TIMESTAMP",
            "CURRENT_DATE",
            "USER",
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

        # Resolve shortened names if any were created during create_table
        resolved_table = self._resolve_name(table_name)
        if self._name_map:
            # Rename batch columns to match shortened table column names
            new_names = [self._resolve_name(n) for n in batch.schema.names]
            if new_names != batch.schema.names:
                batch = batch.rename_columns(new_names)

        with self._pool.acquire() as conn:
            writer = OracleDataWriter(conn)
            rows = writer.write_batch(
                resolved_table,
                schema_name or self._owner,
                batch,
            )
            if writer.batch_errors:
                for err in writer.batch_errors:
                    self._insert_errors.append(f"{table_name}: {err}")
            return rows

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
                    resolved_cols = [self._resolve_name(col) for col in index.columns]
                    col_list = ", ".join(f'"{col}"' for col in resolved_cols)

                    schema = schema_name or self._owner
                    resolved_table = self._resolve_name(table_name)
                    idx_name = _shorten_identifier(index.name, 30)
                    create_idx_sql = (
                        f"CREATE {unique_kw} INDEX "
                        f'"{idx_name}" ON '
                        f'"{schema}"."{resolved_table}" '
                        f"({col_list})"
                    )

                    try:
                        cursor.execute(create_idx_sql)
                    except Exception as exc:
                        exc_str = str(exc)
                        if any(
                            code in exc_str
                            for code in (
                                "ORA-01408",  # already indexed
                                "ORA-00955",  # name collision
                                "ORA-01450",  # key too long
                                "ORA-00972",  # identifier too long
                                "ORA-02327",  # cannot index LOB column
                            )
                        ):
                            _logger.warning(
                                "Index %s on %s skipped: %s",
                                index.name,
                                table_name,
                                exc_str[:80],
                            )
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
                    src_table = self._resolve_name(src_parts[-1])

                    # Parse referenced table FQN
                    ref_parts = fk.referenced_table.split(".")
                    ref_schema = ref_parts[0] if len(ref_parts) > 1 else self._owner
                    ref_table = self._resolve_name(ref_parts[-1])

                    # Build column lists with resolved names
                    src_cols = ", ".join(
                        f'"{self._resolve_name(col)}"' for col in fk.source_columns
                    )
                    ref_cols = ", ".join(
                        f'"{self._resolve_name(col)}"' for col in fk.referenced_columns
                    )

                    # Oracle requires constraint names to be unique per-schema.
                    unique_name = _shorten_identifier(
                        f"{src_table}_{fk.name}",
                        30,
                    )

                    alter_sql = (
                        f'ALTER TABLE "{src_schema}"."{src_table}" '
                        f'ADD CONSTRAINT "{unique_name}" FOREIGN KEY ({src_cols}) '
                        f'REFERENCES "{ref_schema}"."{ref_table}" ({ref_cols})'
                    )
                    if fk.on_delete and fk.on_delete.upper() in (
                        "CASCADE",
                        "SET NULL",
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
