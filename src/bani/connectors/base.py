"""Abstract base classes for database connectors (Section 6.1).

Defines SourceConnector and SinkConnector abstract base classes that all
database connectors must implement. These form the boundary between the
domain layer and connector implementations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

from bani.domain.project import ConnectionConfig
from bani.domain.schema import (
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


class SourceConnector(ABC):
    """Port: reads schema and data from a source database.

    A source connector is responsible for introspecting a source database's
    schema and reading data from its tables. It abstracts away database-specific
    details like SQL dialects and driver APIs.
    """

    @abstractmethod
    def connect(self, config: ConnectionConfig) -> None:
        """Establish a connection to the source database.

        Args:
            config: Connection configuration with resolved credentials.

        Raises:
            Exception: If the connection fails.
        """
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection to the source database.

        Raises:
            Exception: If disconnection fails.
        """
        ...

    @abstractmethod
    def introspect_schema(self) -> DatabaseSchema:
        """Introspect the complete schema of the source database.

        Reads all tables, columns, constraints, indexes, and other schema
        metadata and returns it as a DatabaseSchema object.

        Returns:
            A DatabaseSchema containing all tables and their metadata.

        Raises:
            Exception: If introspection fails.
        """
        ...

    @abstractmethod
    def read_table(
        self,
        table_name: str,
        schema_name: str,
        columns: list[str] | None = None,
        filter_sql: str | None = None,
        batch_size: int = 100_000,
    ) -> Iterator[pa.RecordBatch]:
        """Read data from a table as batches of Arrow records.

        Yields batches of data from the specified table. Uses server-side
        cursors for memory efficiency on large tables.

        Args:
            table_name: Name of the table to read from.
            schema_name: Schema (namespace) containing the table.
            columns: Optional list of column names to read. If None, all
                columns are read.
            filter_sql: Optional WHERE clause (without "WHERE" keyword) to
                filter rows.
            batch_size: Number of rows per batch.

        Yields:
            pyarrow.RecordBatch instances.

        Raises:
            Exception: If reading fails.
        """
        ...

    @abstractmethod
    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        """Get an estimated row count for a table.

        Returns a fast estimate based on schema statistics if available,
        or executes a COUNT(*) query if not.

        Args:
            table_name: Name of the table.
            schema_name: Schema containing the table.

        Returns:
            Estimated or exact row count.

        Raises:
            Exception: If the query fails.
        """
        ...


class SinkConnector(ABC):
    """Port: writes schema and data to a target database.

    A sink connector is responsible for creating tables and writing data
    to a target database. It abstracts away database-specific DDL and
    DML details.
    """

    @abstractmethod
    def connect(self, config: ConnectionConfig) -> None:
        """Establish a connection to the target database.

        Args:
            config: Connection configuration with resolved credentials.

        Raises:
            Exception: If the connection fails.
        """
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection to the target database.

        Raises:
            Exception: If disconnection fails.
        """
        ...

    @abstractmethod
    def create_table(self, table_def: TableDefinition) -> None:
        """Create a table in the target database.

        Creates a table with all columns and constraints as specified in
        the table definition. Primary key and check constraints are
        included in the CREATE TABLE statement.

        Args:
            table_def: TableDefinition describing the table to create.

        Raises:
            Exception: If table creation fails.
        """
        ...

    @abstractmethod
    def write_batch(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Write a batch of Arrow records to a table.

        Uses the most efficient method available (e.g., COPY for PostgreSQL).

        Args:
            table_name: Name of the target table.
            schema_name: Schema containing the table.
            batch: A pyarrow.RecordBatch to write.

        Returns:
            Number of rows written.

        Raises:
            Exception: If writing fails.
        """
        ...

    @abstractmethod
    def create_indexes(
        self, table_name: str, schema_name: str, indexes: tuple[IndexDefinition, ...]
    ) -> None:
        """Create indexes on a table.

        Args:
            table_name: Name of the table.
            schema_name: Schema containing the table.
            indexes: Tuple of index definitions to create.

        Raises:
            Exception: If index creation fails.
        """
        ...

    @abstractmethod
    def create_foreign_keys(self, fks: tuple[ForeignKeyDefinition, ...]) -> None:
        """Create foreign key constraints.

        Args:
            fks: Tuple of foreign key definitions to create.

        Raises:
            Exception: If creation fails.
        """
        ...

    @abstractmethod
    def execute_sql(self, sql: str) -> None:
        """Execute arbitrary SQL.

        Used for custom DDL or DML not covered by other methods.

        Args:
            sql: SQL statement to execute.

        Raises:
            Exception: If execution fails.
        """
        ...
