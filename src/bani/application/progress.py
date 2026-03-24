"""Progress tracking and event emission for migrations (Section 16.2, 18.2).

Provides typed event dataclasses and thread-safe progress tracking with
observer/callback pattern for monitoring migration execution.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class MigrationStarted:
    """Emitted when a migration begins."""

    timestamp: datetime
    project_name: str
    source_dialect: str
    target_dialect: str
    table_count: int


@dataclass(frozen=True)
class TableStarted:
    """Emitted when a table transfer begins."""

    timestamp: datetime
    table_name: str
    estimated_rows: int | None


@dataclass(frozen=True)
class BatchComplete:
    """Emitted after a batch is successfully written."""

    timestamp: datetime
    table_name: str
    batch_number: int
    rows_read: int
    rows_written: int


@dataclass(frozen=True)
class TableComplete:
    """Emitted when a table transfer completes."""

    timestamp: datetime
    table_name: str
    total_rows_read: int
    total_rows_written: int
    batch_count: int


@dataclass(frozen=True)
class MigrationComplete:
    """Emitted when the entire migration completes."""

    timestamp: datetime
    project_name: str
    tables_completed: int
    tables_failed: int
    total_rows_read: int
    total_rows_written: int
    duration_seconds: float


ProgressEvent = (
    MigrationStarted | TableStarted | BatchComplete | TableComplete | MigrationComplete
)


class ProgressTracker:
    """Thread-safe progress tracker with observer pattern.

    Allows multiple listeners to register callbacks for progress events.
    All event emission is thread-safe.
    """

    def __init__(self) -> None:
        """Initialize the tracker."""
        self._listeners: list[Callable[[ProgressEvent], None]] = []
        self._lock = Lock()

    def add_listener(self, callback: Callable[[ProgressEvent], None]) -> None:
        """Register a listener callback for progress events.

        Args:
            callback: Function to call with each emitted event.
        """
        with self._lock:
            self._listeners.append(callback)

    def emit(self, event: ProgressEvent) -> None:
        """Emit a progress event to all registered listeners.

        Thread-safe: acquisition of the lock protects listener list
        access but not the execution of listener callbacks themselves.

        Args:
            event: The progress event to emit.
        """
        with self._lock:
            listeners = list(self._listeners)

        for listener in listeners:
            try:
                listener(event)
            except Exception:
                # Silently ignore callback errors to prevent one bad listener
                # from affecting others.
                pass

    def migration_started(
        self,
        project_name: str,
        source_dialect: str,
        target_dialect: str,
        table_count: int,
    ) -> None:
        """Emit a migration start event."""
        event = MigrationStarted(
            timestamp=datetime.now(timezone.utc),
            project_name=project_name,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
            table_count=table_count,
        )
        self.emit(event)

    def table_started(self, table_name: str, estimated_rows: int | None = None) -> None:
        """Emit a table start event."""
        event = TableStarted(
            timestamp=datetime.now(timezone.utc),
            table_name=table_name,
            estimated_rows=estimated_rows,
        )
        self.emit(event)

    def batch_complete(
        self,
        table_name: str,
        batch_number: int,
        rows_read: int,
        rows_written: int,
    ) -> None:
        """Emit a batch completion event."""
        event = BatchComplete(
            timestamp=datetime.now(timezone.utc),
            table_name=table_name,
            batch_number=batch_number,
            rows_read=rows_read,
            rows_written=rows_written,
        )
        self.emit(event)

    def table_complete(
        self,
        table_name: str,
        total_rows_read: int,
        total_rows_written: int,
        batch_count: int,
    ) -> None:
        """Emit a table completion event."""
        event = TableComplete(
            timestamp=datetime.now(timezone.utc),
            table_name=table_name,
            total_rows_read=total_rows_read,
            total_rows_written=total_rows_written,
            batch_count=batch_count,
        )
        self.emit(event)

    def migration_complete(
        self,
        project_name: str,
        tables_completed: int,
        tables_failed: int,
        total_rows_read: int,
        total_rows_written: int,
        duration_seconds: float,
    ) -> None:
        """Emit a migration completion event."""
        event = MigrationComplete(
            timestamp=datetime.now(timezone.utc),
            project_name=project_name,
            tables_completed=tables_completed,
            tables_failed=tables_failed,
            total_rows_read=total_rows_read,
            total_rows_written=total_rows_written,
            duration_seconds=duration_seconds,
        )
        self.emit(event)
