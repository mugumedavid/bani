"""PostgreSQL connector implementing both source and sink interfaces."""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import psycopg
import pyarrow as pa

if TYPE_CHECKING:
    import psycopg.abc

from bani.connectors.base import SinkConnector, SourceConnector
from bani.connectors.default_translation import (
    DialectDefaultConfig,
    register_dialect_defaults,
    translate_default,
)

register_dialect_defaults("postgresql", DialectDefaultConfig(
    timestamp_expression="NOW()",
    temporal_keywords=("timestamp", "date", "time", "interval"),
))
from bani.connectors.postgresql.data_reader import PostgreSQLDataReader
from bani.connectors.postgresql.data_writer import PostgreSQLDataWriter
from bani.connectors.postgresql.schema_reader import PostgreSQLSchemaReader
from bani.connectors.postgresql.type_mapper import PostgreSQLTypeMapper
from bani.domain.project import ConnectionConfig
from bani.domain.schema import (
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


class PostgreSQLConnector(SourceConnector, SinkConnector):
    """PostgreSQL database connector.

    Implements both SourceConnector and SinkConnector to support reading
    from and writing to PostgreSQL databases. Uses psycopg3 for all
    database operations.
    """

    def __init__(self) -> None:
        """Initialize the PostgreSQL connector."""
        self.connection: psycopg.Connection[tuple[Any, ...]] | None = None
        self._schema_reader: PostgreSQLSchemaReader | None = None
        self._data_reader: PostgreSQLDataReader | None = None
        self._data_writer: PostgreSQLDataWriter | None = None

    def connect(self, config: ConnectionConfig) -> None:
        """Establish a connection to a PostgreSQL database.

        Resolves credential environment variables and establishes the
        connection. Validates that the connection works before returning.

        Args:
            config: Connection configuration with dialect="postgresql".

        Raises:
            ValueError: If required configuration is missing.
            psycopg.Error: If connection fails.
        """
        # Validate configuration
        if not config.host:
            raise ValueError(
                "PostgreSQL connector requires 'host' in connection config"
            )
        if not config.database:
            raise ValueError(
                "PostgreSQL connector requires 'database' in connection config"
            )

        # Resolve credentials from environment variables
        username = self._resolve_env_var(config.username_env)
        password = self._resolve_env_var(config.password_env)

        # Determine port
        port = config.port if config.port > 0 else 5432

        # Build connection string
        conninfo_parts = [
            f"host={config.host}",
            f"port={port}",
            f"dbname={config.database}",
        ]

        if username:
            conninfo_parts.append(f"user={username}")

        if password:
            conninfo_parts.append(f"password={password}")

        # Handle TLS configuration
        if config.encrypt:
            conninfo_parts.append("sslmode=prefer")
        else:
            conninfo_parts.append("sslmode=disable")

        conninfo = " ".join(conninfo_parts)

        # Establish connection
        self.connection = psycopg.connect(conninfo, autocommit=True)

        # Initialize helper objects
        self._schema_reader = PostgreSQLSchemaReader(self.connection)
        self._data_reader = PostgreSQLDataReader(self.connection)
        self._data_writer = PostgreSQLDataWriter(self.connection)

    def disconnect(self) -> None:
        """Close the database connection.

        Raises:
            Exception: If disconnection fails.
        """
        if self.connection is not None:
            self.connection.close()
            self.connection = None
            self._schema_reader = None
            self._data_reader = None
            self._data_writer = None

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
            raise RuntimeError("PostgreSQL connector is not connected")

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
        if self.connection is None or self._data_reader is None:
            raise RuntimeError("PostgreSQL connector is not connected")

        return self._data_reader.read_table(
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
        if self.connection is None or self._data_reader is None:
            raise RuntimeError("PostgreSQL connector is not connected")

        return self._data_reader.estimate_row_count(table_name, schema_name)

    def create_table(self, table_def: TableDefinition) -> None:
        """Create a table in the database.

        Creates all columns, primary key, and check constraints.
        Indexes and foreign keys are created separately.

        Args:
            table_def: TableDefinition describing the table.

        Raises:
            RuntimeError: If not connected.
            Exception: If table creation fails.
        """
        if self.connection is None:
            raise RuntimeError("PostgreSQL connector is not connected")
        if not table_def.columns:
            raise ValueError(f"Table {table_def.table_name} has no columns")

        # Build column definitions
        col_defs = []
        for col in table_def.columns:
            is_auto = col.is_auto_increment or (
                "auto_increment" in col.data_type.lower()
            )

            # Resolve target type via the canonical Arrow mapping layer
            # when arrow_type_str is available; fall back to raw data_type.
            if col.arrow_type_str:
                pg_type = PostgreSQLTypeMapper.from_arrow_type(col.arrow_type_str)
            else:
                pg_type = col.data_type

            # serial/bigserial implies NOT NULL and a sequence default.
            # Pick bigserial for int64 sources so FK columns (also bigint)
            # stay type-compatible.
            if is_auto and pg_type not in ("serial", "bigserial"):
                if col.arrow_type_str and "int64" in col.arrow_type_str:
                    pg_type = "bigserial"
                else:
                    pg_type = "serial"

            col_def = f'"{col.name}" {pg_type}'

            if pg_type not in ("serial", "bigserial"):
                if not col.nullable:
                    col_def += " NOT NULL"

                if col.default_value:
                    # First: shared cross-DB filter (drops non-portable defaults)
                    translated = translate_default(
                        col.default_value, "postgresql", pg_type
                    )
                    if translated is not None:
                        # Then: PG-specific quoting of bare string literals
                        default = self._normalize_default(translated, pg_type)
                        col_def += f" DEFAULT {default}"

            col_defs.append(col_def)

        # Add primary key if present
        if table_def.primary_key:
            pk_cols = ", ".join(f'"{col}"' for col in table_def.primary_key)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        # Add check constraints if present
        for constraint in table_def.check_constraints:
            col_defs.append(f"CHECK {constraint}")

        col_list = ", ".join(col_defs)

        # Drop existing table if present, then create
        drop_sql = (
            f'DROP TABLE IF EXISTS '
            f'"{table_def.schema_name}"."{table_def.table_name}" CASCADE'
        )
        create_sql = (
            f'CREATE TABLE "{table_def.schema_name}"."{table_def.table_name}" '
            f"({col_list})"
        )

        with self.connection.cursor() as cur:
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
        if self.connection is None or self._data_writer is None:
            raise RuntimeError("PostgreSQL connector is not connected")

        return self._data_writer.write_batch(table_name, schema_name, batch)

    def create_indexes(
        self, table_name: str, schema_name: str, indexes: tuple[IndexDefinition, ...]
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
        if self.connection is None:
            raise RuntimeError("PostgreSQL connector is not connected")

        with self.connection.cursor() as cur:
            for index in indexes:
                # Build CREATE INDEX statement
                unique_kw = "UNIQUE" if index.is_unique else ""
                col_list = ", ".join(f'"{col}"' for col in index.columns)

                create_idx_sql = (
                    f'CREATE {unique_kw} INDEX "{index.name}" '
                    f'ON "{schema_name}"."{table_name}" ({col_list})'
                )

                # Add filter expression if present
                if index.filter_expression:
                    create_idx_sql += f" WHERE {index.filter_expression}"

                cur.execute(create_idx_sql)

    def create_foreign_keys(self, fks: tuple[ForeignKeyDefinition, ...]) -> None:
        """Create foreign key constraints.

        Args:
            fks: Tuple of foreign key definitions.

        Raises:
            RuntimeError: If not connected.
            Exception: If FK creation fails.
        """
        if self.connection is None:
            raise RuntimeError("PostgreSQL connector is not connected")

        with self.connection.cursor() as cur:
            for fk in fks:
                # Parse source table FQN
                src_parts = fk.source_table.split(".")
                src_schema = src_parts[0] if len(src_parts) > 1 else "public"
                src_table = src_parts[-1]

                # Parse referenced table FQN
                ref_parts = fk.referenced_table.split(".")
                ref_schema = ref_parts[0] if len(ref_parts) > 1 else "public"
                ref_table = ref_parts[-1]

                # Build column lists
                src_cols = ", ".join(f'"{col}"' for col in fk.source_columns)
                ref_cols = ", ".join(f'"{col}"' for col in fk.referenced_columns)

                # Build ALTER TABLE statement
                alter_sql = (
                    f'ALTER TABLE "{src_schema}"."{src_table}" '
                    f'ADD CONSTRAINT "{fk.name}" FOREIGN KEY ({src_cols}) '
                    f'REFERENCES "{ref_schema}"."{ref_table}" ({ref_cols}) '
                    f"ON DELETE {fk.on_delete} ON UPDATE {fk.on_update}"
                )

                cur.execute(alter_sql)

    def execute_sql(self, sql_str: str) -> None:
        """Execute arbitrary SQL.

        Args:
            sql_str: SQL statement to execute.

        Raises:
            RuntimeError: If not connected.
            Exception: If execution fails.
        """
        if self.connection is None:
            raise RuntimeError("PostgreSQL connector is not connected")

        with self.connection.cursor() as cur:
            cur.execute(sql_str)

    @staticmethod
    def _normalize_default(raw_default: str, pg_type: str) -> str:
        """Normalize a column default value for PostgreSQL DDL.

        MySQL's INFORMATION_SCHEMA returns bare string literals
        (e.g. ``pending``) while PostgreSQL requires them quoted
        (``'pending'``).  Numeric literals, SQL functions, and
        already-quoted values are passed through unchanged.

        Args:
            raw_default: The raw default expression from introspection.
            pg_type: The resolved PostgreSQL column type.

        Returns:
            A PostgreSQL-safe default expression.
        """
        val = raw_default.strip()
        upper = val.upper()

        # MySQL CURRENT_TIMESTAMP → PG NOW()
        if upper in ("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()"):
            return "NOW()"

        # NULL literal
        if upper == "NULL":
            return "NULL"

        # Already a quoted string
        if val.startswith("'") and val.endswith("'"):
            return val

        # Numeric literal (int, float, negative)
        stripped = val.lstrip("-")
        if stripped.replace(".", "", 1).isdigit():
            return val

        # Boolean literals for boolean columns
        if pg_type == "boolean" and upper in (
            "TRUE",
            "FALSE",
            "0",
            "1",
        ):
            return "TRUE" if upper in ("TRUE", "1") else "FALSE"

        # SQL function call (contains parens) — pass through
        if "(" in val and ")" in val:
            return val

        # SQL keyword expressions (e.g. CURRENT_DATE, TRUE, FALSE)
        _SQL_KEYWORDS = {
            "CURRENT_DATE",
            "CURRENT_TIME",
            "CURRENT_USER",
            "LOCALTIME",
            "LOCALTIMESTAMP",
            "TRUE",
            "FALSE",
        }
        if upper in _SQL_KEYWORDS:
            return val

        # Everything else: treat as a string literal, quote it
        escaped = val.replace("'", "''")
        return f"'{escaped}'"

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
