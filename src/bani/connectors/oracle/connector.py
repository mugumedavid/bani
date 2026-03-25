"""Oracle connector implementing both source and sink interfaces.

Uses python-oracledb in THIN mode (pure Python, no Oracle Client needed).
Supports Oracle 12c+ with straightforward connection setup.
"""

from __future__ import annotations

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
from bani.connectors.oracle.data_reader import OracleDataReader
from bani.connectors.oracle.data_writer import OracleDataWriter
from bani.connectors.oracle.schema_reader import OracleSchemaReader
from bani.connectors.oracle.type_mapper import OracleTypeMapper
from bani.domain.project import ConnectionConfig
from bani.domain.schema import (
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


class OracleConnector(SourceConnector, SinkConnector):
    """Oracle database connector.

    Implements both SourceConnector and SinkConnector to support reading
    from and writing to Oracle databases. Uses python-oracledb in THIN mode
    (pure Python, no Oracle Client installation needed).

    Supports Oracle 12c+ with straightforward connection setup via host, port,
    and service_name or SID.
    """

    def __init__(self) -> None:
        """Initialize the Oracle connector."""
        self.connection: oracledb.Connection | None = None
        self._schema_reader: OracleSchemaReader | None = None
        self._data_reader: OracleDataReader | None = None
        self._data_writer: OracleDataWriter | None = None
        self._owner: str = ""

    def connect(self, config: ConnectionConfig) -> None:
        """Establish a connection to an Oracle database.

        Resolves credential environment variables and establishes the
        connection using python-oracledb in THIN mode.

        Args:
            config: Connection configuration with dialect="oracle".
                    Requires host, port, and either database (SID) or
                    service_name.

        Raises:
            ValueError: If required configuration is missing.
            oracledb.DatabaseError: If connection fails.
        """
        # Validate configuration
        if not config.host:
            raise ValueError("Oracle connector requires 'host' in connection config")
        # Get service_name from extra config if provided
        service_name: str | None = None
        ssl_cert_path: str | None = None
        for key, value in config.extra:
            if key == "service_name":
                service_name = value
            elif key == "ssl_cert_path":
                ssl_cert_path = value

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

        # Enable THIN mode explicitly (pure Python, no Oracle Client)
        # This is the default in python-oracledb 2.x, but be explicit
        connect_kwargs["thick"] = False

        # Handle TLS configuration if needed
        if config.encrypt:
            connect_kwargs["ssl"] = True
            if ssl_cert_path:
                connect_kwargs["ssl_cert_dir"] = ssl_cert_path

        # Establish connection
        self.connection = oracledb.connect(**connect_kwargs)

        # Store the owner for schema-qualified identifiers
        # Retrieve the actual user connected as (may differ from config)
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT user FROM dual")
            result: tuple[Any, ...] | None = cursor.fetchone()
            self._owner = result[0] if result else (username or "system")
        finally:
            cursor.close()

        # Initialize helper objects
        self._schema_reader = OracleSchemaReader(self.connection, self._owner)
        self._data_reader = OracleDataReader(self.connection, self._owner)
        self._data_writer = OracleDataWriter(self.connection)

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
        if self.connection is None or self._data_reader is None:
            raise RuntimeError("Oracle connector is not connected")

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
            schema_name: Schema (owner) containing the table.

        Returns:
            Estimated row count.

        Raises:
            RuntimeError: If not connected.
            Exception: If estimation fails.
        """
        if self.connection is None or self._data_reader is None:
            raise RuntimeError("Oracle connector is not connected")

        return self._data_reader.estimate_row_count(table_name, schema_name)

    def create_table(self, table_def: TableDefinition) -> None:
        """Create a table in the database.

        Creates all columns, primary key, and check constraints.

        Args:
            table_def: TableDefinition describing the table.

        Raises:
            RuntimeError: If not connected.
            Exception: If table creation fails.
        """
        if self.connection is None:
            raise RuntimeError("Oracle connector is not connected")

        # Build column definitions
        col_defs = []
        for col in table_def.columns:
            # Resolve target type via the canonical Arrow mapping layer
            # when arrow_type_str is available; fall back to raw data_type.
            if col.arrow_type_str:
                oracle_type = OracleTypeMapper.from_arrow_type(col.arrow_type_str)
            else:
                oracle_type = col.data_type

            col_def = f'"{col.name}" {oracle_type}'

            if not col.nullable:
                col_def += " NOT NULL"

            if col.default_value:
                col_def += f" DEFAULT {col.default_value}"

            col_defs.append(col_def)

        # Add primary key if present
        if table_def.primary_key:
            pk_cols = ", ".join(f'"{col}"' for col in table_def.primary_key)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        col_list = ", ".join(col_defs)

        # Build and execute CREATE TABLE
        create_sql = (
            f'CREATE TABLE "{table_def.schema_name}"."{table_def.table_name}" '
            f"({col_list})"
        )

        cursor = self.connection.cursor()
        try:
            cursor.execute(create_sql)
        finally:
            cursor.close()

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
        if self.connection is None or self._data_writer is None:
            raise RuntimeError("Oracle connector is not connected")

        return self._data_writer.write_batch(table_name, schema_name, batch)

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
        if self.connection is None:
            raise RuntimeError("Oracle connector is not connected")

        cursor = self.connection.cursor()
        try:
            for index in indexes:
                unique_kw = "UNIQUE" if index.is_unique else ""
                col_list = ", ".join(f'"{col}"' for col in index.columns)

                create_idx_sql = (
                    f"CREATE {unique_kw} INDEX "
                    f'"{index.name}" ON "{schema_name}"."{table_name}" '
                    f"({col_list})"
                )

                cursor.execute(create_idx_sql)
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
        if self.connection is None:
            raise RuntimeError("Oracle connector is not connected")

        cursor = self.connection.cursor()
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

                # Build ALTER TABLE statement
                alter_sql = (
                    f'ALTER TABLE "{src_schema}"."{src_table}" '
                    f'ADD CONSTRAINT "{fk.name}" FOREIGN KEY ({src_cols}) '
                    f'REFERENCES "{ref_schema}"."{ref_table}" ({ref_cols}) '
                    f"ON DELETE {fk.on_delete}"
                )

                cursor.execute(alter_sql)
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
        if self.connection is None:
            raise RuntimeError("Oracle connector is not connected")

        cursor = self.connection.cursor()
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
