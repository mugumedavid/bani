"""Tests for the ProgressTracker and progress events."""

from __future__ import annotations

import threading
from datetime import datetime, timezone

from bani.application.progress import (
    BatchComplete,
    MigrationComplete,
    MigrationStarted,
    ProgressTracker,
    TableComplete,
    TableStarted,
)


def test_progress_tracker_emits_migration_started() -> None:
    """Test that migration_started emits the correct event."""
    tracker = ProgressTracker()
    events: list[object] = []

    tracker.add_listener(lambda e: events.append(e))
    tracker.migration_started("test_proj", "postgresql", "mssql", 3)

    assert len(events) == 1
    event = events[0]
    assert isinstance(event, MigrationStarted)
    assert event.project_name == "test_proj"
    assert event.source_dialect == "postgresql"
    assert event.target_dialect == "mssql"
    assert event.table_count == 3


def test_progress_tracker_emits_table_started() -> None:
    """Test that table_started emits the correct event."""
    tracker = ProgressTracker()
    events: list[object] = []

    tracker.add_listener(lambda e: events.append(e))
    tracker.table_started("users", estimated_rows=1000)

    assert len(events) == 1
    event = events[0]
    assert isinstance(event, TableStarted)
    assert event.table_name == "users"
    assert event.estimated_rows == 1000


def test_progress_tracker_emits_batch_complete() -> None:
    """Test that batch_complete emits the correct event."""
    tracker = ProgressTracker()
    events: list[object] = []

    tracker.add_listener(lambda e: events.append(e))
    tracker.batch_complete("users", 0, 100, 100)

    assert len(events) == 1
    event = events[0]
    assert isinstance(event, BatchComplete)
    assert event.table_name == "users"
    assert event.batch_number == 0
    assert event.rows_read == 100
    assert event.rows_written == 100


def test_progress_tracker_emits_table_complete() -> None:
    """Test that table_complete emits the correct event."""
    tracker = ProgressTracker()
    events: list[object] = []

    tracker.add_listener(lambda e: events.append(e))
    tracker.table_complete("users", 1000, 1000, 10)

    assert len(events) == 1
    event = events[0]
    assert isinstance(event, TableComplete)
    assert event.table_name == "users"
    assert event.total_rows_read == 1000
    assert event.total_rows_written == 1000
    assert event.batch_count == 10


def test_progress_tracker_emits_migration_complete() -> None:
    """Test that migration_complete emits the correct event."""
    tracker = ProgressTracker()
    events: list[object] = []

    tracker.add_listener(lambda e: events.append(e))
    tracker.migration_complete("test_proj", 2, 0, 2000, 2000, 45.5)

    assert len(events) == 1
    event = events[0]
    assert isinstance(event, MigrationComplete)
    assert event.project_name == "test_proj"
    assert event.tables_completed == 2
    assert event.tables_failed == 0
    assert event.total_rows_read == 2000
    assert event.total_rows_written == 2000
    assert event.duration_seconds == 45.5


def test_progress_tracker_multiple_listeners() -> None:
    """Test that multiple listeners all receive events."""
    tracker = ProgressTracker()
    events1: list[object] = []
    events2: list[object] = []

    tracker.add_listener(lambda e: events1.append(e))
    tracker.add_listener(lambda e: events2.append(e))

    tracker.table_started("users", 100)

    assert len(events1) == 1
    assert len(events2) == 1
    assert events1[0] == events2[0]


def test_progress_tracker_thread_safe() -> None:
    """Test that the tracker is thread-safe."""
    tracker = ProgressTracker()
    events: list[object] = []
    lock = threading.Lock()

    def listener(e: object) -> None:
        with lock:
            events.append(e)

    tracker.add_listener(listener)

    def emit_events() -> None:
        for i in range(10):
            tracker.batch_complete("table", i, 100, 100)

    threads = [threading.Thread(target=emit_events) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 5 threads * 10 events each = 50 events
    assert len(events) == 50


def test_progress_events_have_timestamps() -> None:
    """Test that all events include timestamps."""
    tracker = ProgressTracker()
    events: list[object] = []

    tracker.add_listener(lambda e: events.append(e))

    before = datetime.now(timezone.utc)  # noqa: UP017
    tracker.migration_started("proj", "pg", "mssql", 1)
    after = datetime.now(timezone.utc)  # noqa: UP017

    event = events[0]
    assert isinstance(event, MigrationStarted)
    assert isinstance(event.timestamp, datetime)
    # timestamp should be in the range [before, after]
    assert event.timestamp >= before
    assert event.timestamp <= after


def test_listener_exception_does_not_break_other_listeners() -> None:
    """Test that an exception in one listener doesn't affect others."""
    tracker = ProgressTracker()
    events1: list[object] = []
    events2: list[object] = []

    def failing_listener(e: object) -> None:
        raise ValueError("Listener error")

    tracker.add_listener(failing_listener)
    tracker.add_listener(lambda e: events1.append(e))
    tracker.add_listener(lambda e: events2.append(e))

    # This should not raise despite the first listener failing
    tracker.table_started("users", 100)

    assert len(events1) == 1
    assert len(events2) == 1
