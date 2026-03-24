"""Abstract base classes for database connectors (Section 6.1).

Defines the ``SourceConnector`` and ``SinkConnector`` interfaces that all
concrete database connectors must implement. These form the contract between
the orchestrator and dialect-specific connection logic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

    from bani.domain.schema import DatabaseSchema, TableDefinition


class SourceConnector(ABC):
    """Abstract base for reading from a source database.

    Concrete implementations (MySQL, PostgreSQL, etc.) inherit from this
    and implement dialect-specific connection, introspection, and batch
    reading logic. All instances support the context manager protocol.
    """

    @abstractmethod
    def connect(self, **kwargs: Any) -> None:
        """Establish a connection to the source database.

        Args:
            **kwargs: Connector-specific connection parameters.

        Raises:
            SourceConnectionError: If connection fails.
        """

    @abstractmethod
    def close(self) -> None:
        """Close the connection to the source database.

        Safe to call multiple times.
        """

    @abstractmethod
    def introspect_schema(self) -> DatabaseSchema:
        """Introspect the source database schema.

        Returns:
            A DatabaseSchema object containing all tables, columns, indexes,
            and foreign keys discovered in the source database.

        Raises:
            IntrospectionError: If schema introspection fails.
        """

    @abstractmethod
    def read_batches(
        self,
        table: TableDefinition,
        batch_size: int = 100_000,
        filter_sql: str | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Read data from a source table in Arrow RecordBatch chunks.

        Args:
            table: The table to read from.
            batch_size: Number of rows per batch (default 100,000).
            filter_sql: Optional WHERE clause to filter rows (without "WHERE").

        Yields:
            Arrow RecordBatch objects.

        Raises:
            ReadError: If reading fails.
        """

    @abstractmethod
    def estimate_row_count(self, table: TableDefinition) -> int | None:
        """Estimate the number of rows in a table.

        Used for progress estimation and resource planning. Does not need to
        be exact, and may return None if unavailable.

        Args:
            table: The table to estimate row count for.

        Returns:
            Estimated row count, or None if unavailable.

        Raises:
            ReadError: If estimation fails critically.
        """

    def __enter__(self) -> SourceConnector:
        """Enter the context manager (no-op, already connected)."""
        return self

    def __exit__(self, *args: object) -> None:
        """Exit the context manager, closing the connection."""
        self.close()


class SinkConnector(ABC):
    """Abstract base for writing to a target database.

    Concrete implementations (MySQL, PostgreSQL, etc.) inherit from this
    and implement dialect-specific connection, table creation, and batch
    writing logic. All instances support the context manager protocol.
    """

    @abstractmethod
    def connect(self, **kwargs: Any) -> None:
        """Establish a connection to the target database.

        Args:
            **kwargs: Connector-specific connection parameters.

        Raises:
            TargetConnectionError: If connection fails.
        """

    @abstractmethod
    def close(self) -> None:
        """Close the connection to the target database.

        Safe to call multiple times.
        """

    @abstractmethod
    def create_table(self, table: TableDefinition) -> None:
        """Create a table in the target database based on a source table definition.

        Translates source column types to target dialect types and creates the
        table with all columns, nullability constraints, and defaults.

        Args:
            table: The table definition to create.

        Raises:
            WriteError: If table creation fails.
            SchemaTranslationError: If type translation fails.
        """

    @abstractmethod
    def create_index(self, table_name: str, index: Any) -> None:
        """Create an index on the target table.

        Args:
            table_name: Fully qualified name of the table.
            index: The IndexDefinition to create.

        Raises:
            WriteError: If index creation fails.
        """

    @abstractmethod
    def create_foreign_key(self, fk: Any) -> None:
        """Create a foreign key constraint on the target table.

        Args:
            fk: The ForeignKeyDefinition to create.

        Raises:
            WriteError: If foreign key creation fails.
        """

    @abstractmethod
    def write_batch(self, table_name: str, batch: pa.RecordBatch) -> int:
        """Write a batch of rows to the target table.

        Args:
            table_name: Fully qualified name of the table.
            batch: The Arrow RecordBatch to write.

        Returns:
            Number of rows successfully written.

        Raises:
            WriteError: If writing fails.
        """

    @abstractmethod
    def drop_table(self, table_name: str) -> None:
        """Drop a table from the target database.

        Should not raise an error if the table does not exist (idempotent).

        Args:
            table_name: Fully qualified name of the table.

        Raises:
            WriteError: If drop fails critically.
        """

    def __enter__(self) -> SinkConnector:
        """Enter the context manager (no-op, already connected)."""
        return self

    def __exit__(self, *args: object) -> None:
        """Exit the context manager, closing the connection."""
        self.close()
