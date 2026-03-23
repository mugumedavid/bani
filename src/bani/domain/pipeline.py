"""Arrow-based data pipeline interfaces (Section 7).

Defines the ``BatchTransferPipeline`` and ``TransformStep`` abstractions
that form the core data movement contract. Concrete implementations are
provided by the application layer; the domain layer defines only the
interfaces and data structures.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa


class TransferStatus(Enum):
    """Status of a table transfer within the pipeline."""

    PENDING = auto()
    IN_PROGRESS = auto()
    COMPLETED = auto()
    FAILED = auto()
    SKIPPED = auto()


@dataclass(frozen=True)
class BatchResult:
    """Result of processing a single batch.

    Attributes:
        batch_number: 0-based index of this batch.
        rows_read: Number of rows read from the source.
        rows_written: Number of rows written to the target.
        rows_quarantined: Number of rows sent to the quarantine table.
    """

    batch_number: int
    rows_read: int
    rows_written: int
    rows_quarantined: int = 0


@dataclass(frozen=True)
class TableTransferResult:
    """Aggregate result of transferring a single table.

    Attributes:
        table_name: Fully qualified table name.
        status: Final transfer status.
        total_rows_read: Total rows read across all batches.
        total_rows_written: Total rows written across all batches.
        total_rows_quarantined: Total rows quarantined across all batches.
        batch_count: Number of batches processed.
        error_message: Error message if the transfer failed.
    """

    table_name: str
    status: TransferStatus
    total_rows_read: int = 0
    total_rows_written: int = 0
    total_rows_quarantined: int = 0
    batch_count: int = 0
    error_message: str | None = None


class TransformStep(ABC):
    """Abstract base class for a data transformation step.

    A transform step receives an Arrow RecordBatch, applies a transformation,
    and returns a (possibly modified) RecordBatch. Transform steps are chained
    together in the pipeline.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name for this transform step."""

    @abstractmethod
    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Apply the transformation to a batch.

        Args:
            batch: The input Arrow RecordBatch.

        Returns:
            The transformed RecordBatch.

        Raises:
            TransformError: If the transformation fails.
        """


class BatchTransferPipeline(ABC):
    """Abstract base class for the batch transfer pipeline.

    The pipeline reads batches from a source, applies transforms, and writes
    to a target. Concrete implementations handle checkpointing, progress
    reporting, and error recovery.
    """

    @abstractmethod
    def add_transform(self, step: TransformStep) -> None:
        """Add a transform step to the pipeline.

        Args:
            step: The transform step to add.
        """

    @abstractmethod
    def execute_table(self, table_name: str) -> TableTransferResult:
        """Execute the pipeline for a single table.

        Args:
            table_name: Fully qualified name of the table to transfer.

        Returns:
            The result of the transfer.
        """

    @abstractmethod
    def execute_all(self) -> tuple[TableTransferResult, ...]:
        """Execute the pipeline for all tables in dependency order.

        Returns:
            A tuple of results, one per table.
        """
