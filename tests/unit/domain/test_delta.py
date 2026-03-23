"""Tests for delta detection models."""

from __future__ import annotations

from datetime import datetime, timezone

from bani.domain.delta import ChangeType, DeltaResult, SyncState


class TestSyncState:
    """Tests for SyncState."""

    def test_defaults(self) -> None:
        state = SyncState(project_name="myproj", table_name="public.users")
        assert state.last_sync_timestamp is None
        assert state.last_rowversion is None
        assert state.last_checksum is None

    def test_with_timestamp(self) -> None:
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)  # noqa: UP017
        state = SyncState(
            project_name="myproj",
            table_name="public.users",
            last_sync_timestamp=ts,
        )
        assert state.last_sync_timestamp == ts

    def test_frozen(self) -> None:
        state = SyncState(project_name="a", table_name="b")
        try:
            state.project_name = "c"  # type: ignore[misc]
            raise AssertionError("Should be frozen")
        except AttributeError:
            pass


class TestDeltaResult:
    """Tests for DeltaResult."""

    def test_no_changes(self) -> None:
        result = DeltaResult(
            table_name="public.users",
            strategy="timestamp",
            has_changes=False,
        )
        assert result.estimated_change_count is None
        assert result.filter_expression is None

    def test_with_changes(self) -> None:
        result = DeltaResult(
            table_name="public.users",
            strategy="timestamp",
            has_changes=True,
            estimated_change_count=500,
            filter_expression="updated_at > '2025-01-01'",
        )
        assert result.has_changes is True
        assert result.estimated_change_count == 500


class TestChangeType:
    """Tests for ChangeType enum."""

    def test_members(self) -> None:
        assert ChangeType.INSERT is not None
        assert ChangeType.UPDATE is not None
        assert ChangeType.DELETE is not None
