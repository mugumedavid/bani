"""Tests for pipeline interfaces and data structures."""

from __future__ import annotations

from bani.domain.pipeline import (
    BatchResult,
    TableTransferResult,
    TransferStatus,
)


class TestBatchResult:
    """Tests for BatchResult."""

    def test_defaults(self) -> None:
        result = BatchResult(batch_number=0, rows_read=1000, rows_written=998)
        assert result.rows_quarantined == 0

    def test_with_quarantine(self) -> None:
        result = BatchResult(
            batch_number=3,
            rows_read=1000,
            rows_written=995,
            rows_quarantined=5,
        )
        assert result.rows_quarantined == 5


class TestTableTransferResult:
    """Tests for TableTransferResult."""

    def test_successful(self) -> None:
        result = TableTransferResult(
            table_name="public.users",
            status=TransferStatus.COMPLETED,
            total_rows_read=50000,
            total_rows_written=50000,
            batch_count=5,
        )
        assert result.error_message is None
        assert result.total_rows_quarantined == 0

    def test_failed(self) -> None:
        result = TableTransferResult(
            table_name="public.orders",
            status=TransferStatus.FAILED,
            total_rows_read=10000,
            total_rows_written=8000,
            total_rows_quarantined=200,
            batch_count=2,
            error_message="Connection lost",
        )
        assert result.status == TransferStatus.FAILED
        assert result.error_message == "Connection lost"


class TestTransferStatus:
    """Tests for TransferStatus enum."""

    def test_all_statuses(self) -> None:
        assert TransferStatus.PENDING is not None
        assert TransferStatus.IN_PROGRESS is not None
        assert TransferStatus.COMPLETED is not None
        assert TransferStatus.FAILED is not None
        assert TransferStatus.SKIPPED is not None
