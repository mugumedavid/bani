"""SQLite connector implementing both source and sink interfaces.

Uses the stdlib sqlite3 module as the driver. No additional pip
dependencies are required for basic functionality.

SQLite-specific notes:
- ConnectionConfig.database is the file path (e.g. '/path/to/data.db')
  or ':memory:' for in-memory databases.
- Foreign keys are OFF by default; this connector enables them on connect.
- 'main' is used as the default schema_name.
- No concurrent writes — SQLite uses file-level locking.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator

import pyarrow as pa

from bani.connectors.base import SinkConnector, SourceConnector
from bani.connectors.default_translation import (
    DialectDefaultConfig,
    register_dialect_defaults,
    translate_default,
)

register_dialect_defaults("sqlite", DialectDefaultConfig(
    timestamp_expression="CURRENT_TIMESTAMP",
    temporal_keywords=("date", "datetime", "timestamp", "time"),
    reject_function_calls=True,  # SQLite only accepts constant expressions
))
from bani.connectors.pool import ConnectionPool
from bani.connectors.sqlite.data_reader import SQLiteDataReader
from bani.connectors.sqlite.data_writer import SQLiteDataWriter
from bani.connectors.sqlite.schema_reader import SQLiteSchemaReader
from bani.connectors.sqlite.type_mapper import SQLiteTypeMapper
from bani.domain.project import ConnectionConfig
from bani.domain.schema import (
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


def _create_sqlite_connection(database: str) -> sqlite3.Connection:
    """Create and configure a SQLite connection.

    Args:
        database: File path or ':memory:'.

    Returns:
        A configured sqlite3.Connection.
    """
    conn = sqlite3.connect(database, check_same_thread=False)

    # Performance pragmas
    if database != ":memory:":
        conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -64000")
    conn.execute("PRAGMA foreign_keys = ON")

    return conn


class SQLiteConnector(SourceConnector, SinkConnector):
    """SQLite database connector.

    Implements both SourceConnector and SinkConnector to support reading
    from and writing to SQLite databases. Uses the stdlib sqlite3 module
    for all database operations.

    Supports file-based databases and in-memory databases (':memory:').
    """

    def __init__(self) -> None:
        """Initialize the SQLite connector."""
        self.connection: sqlite3.Connection | None = None
        self._schema_reader: SQLiteSchemaReader | None = None
        self._pool: ConnectionPool[sqlite3.Connection] | None = None
        self._database: str = ""

    def connect(self, config: ConnectionConfig, pool_size: int = 1) -> None:
        """Establish a connection to a SQLite database.

        Uses the 'database' field from config as the file path.
        Enables foreign keys and sets WAL journal mode for better
        concurrent read performance.

        Args:
            config: Connection configuration with dialect="sqlite".
                The 'database' field is the file path or ':memory:'.
            pool_size: Number of connections to create in the pool.

        Raises:
            ValueError: If required configuration is missing.
            sqlite3.Error: If connection fails.
        """
        if not config.database:
            raise ValueError(
                "SQLite connector requires 'database' in connection config "
                "(file path or ':memory:')"
            )

        self._database = config.database
        self._config = config
        self._pool_size = pool_size

        # Create connection pool
        self._pool = ConnectionPool(
            factory=lambda: _create_sqlite_connection(config.database),
            reset=lambda conn: conn.rollback(),
            close=lambda conn: conn.close(),
            size=pool_size,
        )

        # Primary connection for backward compat and schema reads
        self.connection = self._pool.primary

        # Initialize schema reader on the primary connection
        self._schema_reader = SQLiteSchemaReader(self.connection)

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
            raise RuntimeError("SQLite connector is not connected")

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
            schema_name: Schema name (ignored — always 'main').
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
            raise RuntimeError("SQLite connector is not connected")

        with self._pool.acquire() as conn:
            reader = SQLiteDataReader(conn)
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
            schema_name: Schema name (ignored — always 'main').

        Returns:
            Estimated row count.

        Raises:
            RuntimeError: If not connected.
            Exception: If estimation fails.
        """
        if self._pool is None:
            raise RuntimeError("SQLite connector is not connected")

        with self._pool.acquire() as conn:
            reader = SQLiteDataReader(conn)
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
            raise RuntimeError("SQLite connector is not connected")
        if not table_def.columns:
            raise ValueError(f"Table {table_def.table_name} has no columns")

        # Build column definitions
        col_defs: list[str] = []
        for col in table_def.columns:
            # Resolve target type via the canonical Arrow mapping layer
            if col.arrow_type_str:
                sqlite_type = SQLiteTypeMapper.from_arrow_type(col.arrow_type_str)
            else:
                sqlite_type = col.data_type

            col_def = f'"{col.name}" {sqlite_type}'

            if not col.nullable:
                col_def += " NOT NULL"

            if col.is_auto_increment:
                # SQLite AUTOINCREMENT only works with INTEGER PRIMARY KEY;
                # handled via PRIMARY KEY below, so skip default here.
                pass
            elif col.default_value:
                translated = translate_default(
                    col.default_value, "sqlite", sqlite_type
                )
                if translated is not None:
                    col_def += f" DEFAULT {translated}"

            col_defs.append(col_def)

        # Add primary key if present
        if table_def.primary_key:
            pk_cols = ", ".join(f'"{col}"' for col in table_def.primary_key)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        # Add check constraints — skip any with source-specific syntax
        for constraint in table_def.check_constraints:
            c = str(constraint)
            if "::" in c or "ARRAY[" in c or "ANY (" in c:
                continue
            col_defs.append(f"CHECK {c}")

        # SQLite foreign keys must be in CREATE TABLE (not ALTER TABLE)
        for fk in table_def.foreign_keys:
            ref_table = fk.referenced_table.split(".")[-1]  # strip schema
            src_cols = ", ".join(f'"{c}"' for c in fk.source_columns)
            ref_cols = ", ".join(f'"{c}"' for c in fk.referenced_columns)
            fk_clause = (
                f"FOREIGN KEY ({src_cols}) REFERENCES \"{ref_table}\" ({ref_cols})"
            )
            if fk.on_delete and fk.on_delete != "NO ACTION":
                fk_clause += f" ON DELETE {fk.on_delete}"
            if fk.on_update and fk.on_update != "NO ACTION":
                fk_clause += f" ON UPDATE {fk.on_update}"
            col_defs.append(fk_clause)

        col_list = ", ".join(col_defs)

        drop_sql = f'DROP TABLE IF EXISTS "{table_def.table_name}"'
        create_sql = f'CREATE TABLE "{table_def.table_name}" ({col_list})'

        with self._pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = OFF")
            cursor.execute(drop_sql)
            cursor.execute(create_sql)
            conn.commit()
            cursor.execute("PRAGMA foreign_keys = ON")

    def write_batch(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write an Arrow batch to a table.

        Args:
            table_name: Name of the target table.
            schema_name: Schema name (ignored — always 'main').
            batch: Arrow RecordBatch to write.

        Returns:
            Number of rows written.

        Raises:
            RuntimeError: If not connected.
            Exception: If writing fails.
        """
        if self._pool is None:
            raise RuntimeError("SQLite connector is not connected")

        with self._pool.acquire() as conn:
            writer = SQLiteDataWriter(conn)
            return writer.write_batch(table_name, schema_name, batch)

    def create_indexes(
        self,
        table_name: str,
        schema_name: str,
        indexes: tuple[IndexDefinition, ...],
    ) -> None:
        """Create indexes on a table.

        Args:
            table_name: Name of the table.
            schema_name: Schema name (ignored — always 'main').
            indexes: Tuple of index definitions.

        Raises:
            RuntimeError: If not connected.
            Exception: If index creation fails.
        """
        if self._pool is None:
            raise RuntimeError("SQLite connector is not connected")

        with self._pool.acquire() as conn:
            cursor = conn.cursor()
            for index in indexes:
                unique_kw = "UNIQUE" if index.is_unique else ""
                col_list = ", ".join(f'"{col}"' for col in index.columns)

                create_idx_sql = (
                    f'CREATE {unique_kw} INDEX "{index.name}" '
                    f'ON "{table_name}" ({col_list})'
                )

                cursor.execute(create_idx_sql)

            conn.commit()

    def create_foreign_keys(self, fks: tuple[ForeignKeyDefinition, ...]) -> None:
        """Create foreign key constraints.

        Note: SQLite does not support ALTER TABLE ADD CONSTRAINT for
        foreign keys. Foreign keys must be defined in the CREATE TABLE
        statement. This method is a no-op for SQLite — foreign keys
        should be included in create_table() or the table must be
        recreated.

        Args:
            fks: Tuple of foreign key definitions.

        Raises:
            RuntimeError: If not connected.
        """
        if self._pool is None:
            raise RuntimeError("SQLite connector is not connected")

        # SQLite does not support adding foreign keys after table creation.
        # Foreign keys must be part of the CREATE TABLE statement.
        # This is a known limitation of SQLite.

    def execute_sql(self, sql_str: str) -> None:
        """Execute arbitrary SQL.

        Args:
            sql_str: SQL statement to execute.

        Raises:
            RuntimeError: If not connected.
            Exception: If execution fails.
        """
        if self._pool is None:
            raise RuntimeError("SQLite connector is not connected")

        with self._pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(sql_str)
            conn.commit()
