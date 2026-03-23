"""Delta detection strategies for incremental sync (Section 13).

Defines the abstract ``DeltaDetector`` and concrete strategy interfaces for
timestamp-based, rowversion-based, and checksum-based change detection.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto


class ChangeType(Enum):
    """Type of change detected by a delta detector."""

    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()


@dataclass(frozen=True)
class SyncState:
    """Persisted state for incremental sync (Section 13.2).

    Stored in ``_bani_sync_state`` in the target database, keyed by
    ``(project_name, table_name)``.

    Attributes:
        project_name: The BDL project name.
        table_name: Fully qualified table name.
        last_sync_timestamp: Timestamp of the last successful sync.
        last_rowversion: Last rowversion value (database-specific).
        last_checksum: Last full-table checksum.
    """

    project_name: str
    table_name: str
    last_sync_timestamp: datetime | None = None
    last_rowversion: str | None = None
    last_checksum: str | None = None


class DeltaDetector(ABC):
    """Abstract base for incremental change detection strategies.

    Each concrete implementation corresponds to one of the strategies
    in Section 13.1 (timestamp, rowversion, checksum).
    """

    @property
    @abstractmethod
    def strategy_name(self) -> str:
        """Return the strategy identifier (e.g. ``"timestamp"``)."""

    @abstractmethod
    def detect_changes(
        self,
        table_name: str,
        sync_state: SyncState | None,
    ) -> DeltaResult:
        """Detect changes since the last sync.

        Args:
            table_name: Fully qualified table name to check.
            sync_state: Previous sync state, or ``None`` for first sync.

        Returns:
            A ``DeltaResult`` describing what changed.
        """


@dataclass(frozen=True)
class DeltaResult:
    """Result of a delta detection pass.

    Attributes:
        table_name: Fully qualified table name.
        strategy: Strategy used for detection.
        has_changes: Whether any changes were detected.
        estimated_change_count: Approximate number of changed rows.
        filter_expression: SQL expression to select only changed rows,
            suitable for use in a WHERE clause. ``None`` if full resync
            is needed.
        new_sync_state: Updated sync state to persist after a successful sync.
    """

    table_name: str
    strategy: str
    has_changes: bool
    estimated_change_count: int | None = None
    filter_expression: str | None = None
    new_sync_state: SyncState | None = None
