"""Abstract base classes for database connectors (Section 6.1)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

    from bani.domain.schema import DatabaseSchema, TableDefinition


class SourceConnector(ABC):
    """Port: reads schema and data from a source database."""

    @abstractmethod
    def connect(self, **kwargs: Any) -> None:
        """Establish a connection to the source database."""

    @abstractmethod
    def close(self) -> None:
        """Release all resources."""

    @abstractmethod
    def introspect_schema(self) -> DatabaseSchema:
        """Return the full schema of the source database."""

    @abstractmethod
    def read_batches(
        self,
        table: TableDefinition,
        batch_size: int = 100_000,
        filter_sql: str | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Yield Arrow RecordBatches from the given table."""

    @abstractmethod
    def estimate_row_count(self, table: TableDefinition) -> int | None:
        """Return an estimated row count for the table, or None."""

    def __enter__(self) -> SourceConnector:
        """Context manager entry."""
        return self

    def __exit__(self, *args: object) -> None:
        """Context manager exit."""
        self.close()


class SinkConnector(ABC):
    """Port: writes schema and data to a target database."""

    @abstractmethod
    def connect(self, **kwargs: Any) -> None:
        """Establish a connection to the target database."""

    @abstractmethod
    def close(self) -> None:
        """Release all resources."""

    @abstractmethod
    def create_table(self, table: TableDefinition) -> None:
        """Create a table in the target from a definition."""

    @abstractmethod
    def create_index(self, table_name: str, index: Any) -> None:
        """Create an index on a table."""

    @abstractmethod
    def create_foreign_key(self, fk: Any) -> None:
        """Create a foreign key constraint."""

    @abstractmethod
    def write_batch(self, table_name: str, batch: pa.RecordBatch) -> int:
        """Write a RecordBatch to the target table. Returns rows written."""

    @abstractmethod
    def drop_table(self, table_name: str) -> None:
        """Drop a table if it exists."""

    def __enter__(self) -> SinkConnector:
        """Context manager entry."""
        return self

    def __exit__(self, *args: object) -> None:
        """Context manager exit."""
        self.close()
