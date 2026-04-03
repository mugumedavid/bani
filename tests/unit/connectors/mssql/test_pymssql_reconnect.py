"""Tests for pymssql periodic reconnection in MSSQL data reader.

Ensures that:
- pymssql reader reconnects every N pages to reset FreeTDS state
- pyodbc reader does NOT reconnect (no reconnect_fn passed)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from bani.connectors.mssql.data_reader import MSSQLDataReader


def _make_cursor_mock(rows_per_page: int, total_rows: int) -> MagicMock:
    """Create a mock cursor that returns pages of rows."""
    pages = []
    remaining = total_rows
    while remaining > 0:
        page_size = min(rows_per_page, remaining)
        pages.append([(i,) for i in range(page_size)])
        remaining -= page_size
    pages.append([])  # empty page signals end

    cursor = MagicMock()
    cursor.description = [("id", int, None, None, None, None, None)]
    cursor.fetchall = MagicMock(side_effect=pages)
    cursor.execute = MagicMock()
    return cursor


class TestPymssqlReconnect:
    """Tests for periodic reconnection with pymssql driver."""

    def test_reconnect_called_at_interval(self) -> None:
        """pymssql reader should reconnect every _PYMSSQL_RECONNECT_INTERVAL pages."""
        batch_size = 100
        # 12 pages — should trigger 2 reconnects (at page 5 and 10)
        total_rows = batch_size * 12

        conn = MagicMock()
        new_conn = MagicMock()
        reconnect_fn = MagicMock(return_value=new_conn)

        # Set up cursor mocks for both connections
        pages = []
        remaining = total_rows
        while remaining > 0:
            page_size = min(batch_size, remaining)
            pages.append([(i,) for i in range(page_size)])
            remaining -= page_size
        pages.append([])  # end

        cursor_mock = MagicMock()
        cursor_mock.__enter__ = MagicMock(return_value=cursor_mock)
        cursor_mock.__exit__ = MagicMock(return_value=False)
        cursor_mock.description = [("id", int, None, None, None, None, None)]
        cursor_mock.fetchall = MagicMock(side_effect=pages)

        conn.cursor.return_value = cursor_mock
        new_conn.cursor.return_value = cursor_mock

        reader = MSSQLDataReader(conn, driver="pymssql", reconnect_fn=reconnect_fn)

        # Mock _get_all_column_types
        with patch.object(reader, "_get_all_column_types", return_value={"id": "int"}):
            batches = list(reader.read_table("test_table", "dbo", batch_size=batch_size))

        assert len(batches) == 12
        # Reconnects after every full page (interval=1)
        assert reconnect_fn.call_count >= 11

    def test_no_reconnect_for_pyodbc(self) -> None:
        """pyodbc reader should NOT receive a reconnect_fn."""
        conn = MagicMock()
        cursor_mock = MagicMock()
        cursor_mock.description = [("id", int, None, None, None, None, None)]
        cursor_mock.fetchmany = MagicMock(side_effect=[
            [(1,), (2,)],
            [],
        ])
        conn.cursor.return_value = cursor_mock

        reader = MSSQLDataReader(conn, driver="pyodbc", reconnect_fn=None)

        with patch.object(reader, "_get_all_column_types", return_value={"id": "int"}):
            batches = list(reader.read_table("test_table", "dbo", batch_size=100))

        assert len(batches) == 1
        # Connection should NOT have been closed/reconnected
        conn.close.assert_not_called()

    def test_reconnect_fn_is_none_for_pyodbc(self) -> None:
        """Verify reconnect_fn defaults to None."""
        conn = MagicMock()
        reader = MSSQLDataReader(conn, driver="pyodbc")
        assert reader._reconnect_fn is None

    def test_reconnect_fn_set_for_pymssql(self) -> None:
        """Verify reconnect_fn is stored when provided."""
        conn = MagicMock()
        factory = MagicMock()
        reader = MSSQLDataReader(conn, driver="pymssql", reconnect_fn=factory)
        assert reader._reconnect_fn is factory

    def test_single_page_no_reconnect(self) -> None:
        """A single-page table should not reconnect (no next page)."""
        batch_size = 100
        total_rows = 50  # Less than one page

        conn = MagicMock()
        reconnect_fn = MagicMock()

        pages = []
        remaining = total_rows
        while remaining > 0:
            page_size = min(batch_size, remaining)
            pages.append([(i,) for i in range(page_size)])
            remaining -= page_size
        pages.append([])

        cursor_mock = MagicMock()
        cursor_mock.__enter__ = MagicMock(return_value=cursor_mock)
        cursor_mock.__exit__ = MagicMock(return_value=False)
        cursor_mock.description = [("id", int, None, None, None, None, None)]
        cursor_mock.fetchall = MagicMock(side_effect=pages)

        conn.cursor.return_value = cursor_mock

        reader = MSSQLDataReader(conn, driver="pymssql", reconnect_fn=reconnect_fn)

        with patch.object(reader, "_get_all_column_types", return_value={"id": "int"}):
            batches = list(reader.read_table("test_table", "dbo", batch_size=batch_size))

        assert len(batches) == 1
        reconnect_fn.assert_not_called()
        conn.close.assert_not_called()
