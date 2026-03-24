"""MSSQL connector implementing both source and sink interfaces.

Uses pymssql as the driver. Chosen for its pure-Python implementation,
broad compatibility with SQL Server 2012+, and ease of installation
(no native library dependencies like ODBC/FreeTDS).
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pymssql as pymssql_module

from bani.connectors.base import SinkConnector, SourceConnector
from bani.connectors.mssql.data_reader import MSSQLDataReader
from bani.connectors.mssql.data_writer import MSSQLDataWriter
from bani.connectors.mssql.schema_reader import MSSQLSchemaReader
from bani.connectors.mssql.type_mapper import MSSQLTypeMapper
from bani.domain.project import ConnectionConfig
from bani.domain.schema import (
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


class MSSQLConnector(SourceConnector, SinkConnector):
    """MSSQL database connector.

    Implements both SourceConnector and SinkConnector to support reading
    from and writing to MSSQL databases. Uses pymssql for all database
    operations with UTF-8 charset by default.

    Supports SQL Server 2012+ via pymssql's broad compatibility.
    """

    def __init__(self) -> None:
        """Initialize the MSSQL connector."""
        self.connection: Any = None
        self._schema_reader: MSSQLSchemaReader | None = None
        self._data_reader: MSSQLDataReader | None = None
        self._data_writer: MSSQLDataWriter | None = None
        self._database: str = ""

    def connect(self, config: ConnectionConfig) -> None:
        """Establish a connection to an MSSQL database.

        Resolves credential environment variables and establishes the
        connection with UTF-8 charset.

        Args:
            config: Connection configuration with dialect="mssql".

        Raises:
            ValueError: If required configuration is missing.
            pymssql.Error: If connection fails.
        """
        if not config.host:
            raise ValueError("MSSQL connector requires 'host' in connection config")
        if not config.database:
            raise ValueError("MSSQL connector requires 'database' in connection config")

        username = self._resolve_env_var(config.username_env)
        password = self._resolve_env_var(config.password_env)

        port = config.port if config.port > 0 else 1433

        connect_kwargs: dict[str, Any] = {
            "host": config.host,
            "port": port,
            "database": config.database,
            "charset": "UTF-8",
            "autocommit": True,
        }

        if username:
            connect_kwargs["user"] = username

        if password:
            connect_kwargs["password"] = password

        if config.encrypt:
            connect_kwargs["login_timeout"] = 30

        self.connection = pymssql_module.connect(**connect_kwargs)
        self._database = config.database

        self._schema_reader = MSSQLSchemaReader(self.connection, self._database)
        self._data_reader = MSSQLDataReader(self.connection)
        self._data_writer = MSSQLDataWriter(self.connection)

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
        if self.connection is None or self._data_reader is None:
            raise RuntimeError("MSSQL connector is not connected")

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
            raise RuntimeError("MSSQL connector is not connected")

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
            raise RuntimeError("MSSQL connector is not connected")

        col_defs = []
        for col in table_def.columns:
            if col.arrow_type_str:
                mssql_type = MSSQLTypeMapper.from_arrow_type(col.arrow_type_str)
            else:
                mssql_type = col.data_type

            col_def = f"[{col.name}] {mssql_type}"

            if not col.nullable:
                col_def += " NOT NULL"

            if col.is_auto_increment:
                col_def += " IDENTITY(1,1)"
            elif col.default_value:
                col_def += f" DEFAULT {col.default_value}"

            col_defs.append(col_def)

        if table_def.primary_key:
            pk_cols = ", ".join(f"[{col}]" for col in table_def.primary_key)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        for constraint in table_def.check_constraints:
            col_defs.append(f"CHECK {constraint}")

        col_list = ", ".join(col_defs)

        create_sql = (
            f"CREATE TABLE [{table_def.schema_name}].[{table_def.table_name}] "
            f"({col_list})"
        )

        with self.connection.cursor() as cur:
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
            raise RuntimeError("MSSQL connector is not connected")

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
            schema_name: Schema containing the table.
            indexes: Tuple of index definitions.

        Raises:
            RuntimeError: If not connected.
            Exception: If index creation fails.
        """
        if self.connection is None:
            raise RuntimeError("MSSQL connector is not connected")

        with self.connection.cursor() as cur:
            for index in indexes:
                unique_kw = "UNIQUE" if index.is_unique else ""
                col_list = ", ".join(f"[{col}]" for col in index.columns)

                create_idx_sql = (
                    f"CREATE {unique_kw} INDEX [{index.name}] "
                    f"ON [{schema_name}].[{table_name}] ({col_list})"
                )

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
            raise RuntimeError("MSSQL connector is not connected")

        with self.connection.cursor() as cur:
            for fk in fks:
                src_parts = fk.source_table.split(".")
                src_schema = src_parts[0] if len(src_parts) > 1 else self._database
                src_table = src_parts[-1]

                ref_parts = fk.referenced_table.split(".")
                ref_schema = ref_parts[0] if len(ref_parts) > 1 else self._database
                ref_table = ref_parts[-1]

                src_cols = ", ".join(f"[{col}]" for col in fk.source_columns)
                ref_cols = ", ".join(f"[{col}]" for col in fk.referenced_columns)

                alter_sql = (
                    f"ALTER TABLE [{src_schema}].[{src_table}] "
                    f"ADD CONSTRAINT [{fk.name}] FOREIGN KEY ({src_cols}) "
                    f"REFERENCES [{ref_schema}].[{ref_table}] ({ref_cols}) "
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
            raise RuntimeError("MSSQL connector is not connected")

        with self.connection.cursor() as cur:
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
