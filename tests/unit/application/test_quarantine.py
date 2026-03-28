"""Tests for QuarantineManager (failed-row isolation)."""

from __future__ import annotations

import pyarrow as pa  # type: ignore[import-untyped]

from bani.application.quarantine import (
    QuarantineManager,
    _CREATE_TABLE_SQL_VARIANTS,
    _QUARANTINE_TABLE,
)
from bani.connectors.base import SinkConnector
from bani.domain.project import ConnectionConfig
from bani.domain.schema import ForeignKeyDefinition, IndexDefinition, TableDefinition


class MockSink(SinkConnector):
    """Minimal mock sink that records execute_sql calls."""

    def __init__(self, *, fail_create: bool = False, fail_insert: bool = False) -> None:
        """Initialize the mock sink.

        Args:
            fail_create: If True, all execute_sql calls for CREATE TABLE raise.
            fail_insert: If True, all execute_sql calls for INSERT raise.
        """
        self._fail_create = fail_create
        self._fail_insert = fail_insert
        self.executed_sql: list[str] = []

    def connect(self, config: ConnectionConfig) -> None:
        """No-op."""

    def disconnect(self) -> None:
        """No-op."""

    def create_table(self, table_def: TableDefinition) -> None:
        """No-op."""

    def write_batch(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """No-op, returns 0."""
        return 0

    def create_indexes(
        self, table_name: str, schema_name: str, indexes: tuple[IndexDefinition, ...]
    ) -> None:
        """No-op."""

    def create_foreign_keys(self, fks: tuple[ForeignKeyDefinition, ...]) -> None:
        """No-op."""

    def execute_sql(self, sql: str) -> None:
        """Record SQL and optionally raise."""
        if self._fail_create and "CREATE" in sql:
            raise RuntimeError("CREATE failed")
        if self._fail_insert and "INSERT" in sql:
            raise RuntimeError("INSERT failed")
        self.executed_sql.append(sql)


class TestEnsureTableExists:
    """Tests for QuarantineManager.ensure_table_exists()."""

    def test_creates_quarantine_table(self) -> None:
        """ensure_table_exists() should execute a CREATE TABLE variant."""
        sink = MockSink()
        qm = QuarantineManager()

        qm.ensure_table_exists(sink)

        assert len(sink.executed_sql) == 1
        assert _QUARANTINE_TABLE in sink.executed_sql[0]

    def test_tries_next_variant_on_failure(self) -> None:
        """If the first CREATE variant fails, it should try the next."""
        call_count = 0
        fail_count = 0

        class PartialFailSink(MockSink):
            """Sink that fails on the first N CREATE attempts."""

            def execute_sql(self, sql: str) -> None:
                nonlocal call_count, fail_count
                call_count += 1
                if "CREATE" in sql and fail_count < 2:
                    fail_count += 1
                    raise RuntimeError("dialect mismatch")
                self.executed_sql.append(sql)

        sink = PartialFailSink()
        qm = QuarantineManager()
        qm.ensure_table_exists(sink)

        # Should have tried at least 3 times (2 failures + 1 success)
        assert call_count >= 3
        assert len(sink.executed_sql) == 1  # One successful variant

    def test_skips_if_already_ensured(self) -> None:
        """ensure_table_exists() should be idempotent after first success."""
        sink = MockSink()
        qm = QuarantineManager()

        qm.ensure_table_exists(sink)
        qm.ensure_table_exists(sink)

        # Only one CREATE executed
        assert len(sink.executed_sql) == 1

    def test_does_not_crash_if_all_variants_fail(self) -> None:
        """If all DDL variants fail, it should log but not raise."""
        sink = MockSink(fail_create=True)
        qm = QuarantineManager()

        # Should not raise
        qm.ensure_table_exists(sink)
        assert len(sink.executed_sql) == 0


class TestQuarantineRow:
    """Tests for QuarantineManager.quarantine_row()."""

    def test_inserts_row_data(self) -> None:
        """quarantine_row() should INSERT a row into the quarantine table."""
        sink = MockSink()
        qm = QuarantineManager()

        qm.quarantine_row(
            sink,
            project_name="proj",
            table_name="public.users",
            row_offset=42,
            row_data_dict={"id": 1, "name": "Alice"},
            error_msg="constraint violation",
        )

        # Should have CREATE + INSERT
        assert len(sink.executed_sql) == 2
        insert_sql = sink.executed_sql[1]
        assert "INSERT INTO" in insert_sql
        assert _QUARANTINE_TABLE in insert_sql
        assert "proj" in insert_sql
        assert "public.users" in insert_sql
        assert "42" in insert_sql
        assert "constraint violation" in insert_sql
        assert "Alice" in insert_sql

    def test_json_serialises_row_data(self) -> None:
        """Row data dict should be JSON-serialised in the INSERT."""
        sink = MockSink()
        qm = QuarantineManager()

        qm.quarantine_row(
            sink,
            project_name="proj",
            table_name="t1",
            row_offset=0,
            row_data_dict={"key": "value", "num": 123},
            error_msg="err",
        )

        insert_sql = sink.executed_sql[1]
        # JSON should contain key and value
        assert '"key"' in insert_sql
        assert '"value"' in insert_sql

    def test_accepts_string_row_data(self) -> None:
        """quarantine_row() should accept pre-serialised string data."""
        sink = MockSink()
        qm = QuarantineManager()

        qm.quarantine_row(
            sink,
            project_name="proj",
            table_name="t1",
            row_offset=0,
            row_data_dict='{"pre": "serialised"}',
            error_msg="err",
        )

        insert_sql = sink.executed_sql[1]
        assert "pre" in insert_sql

    def test_handles_none_offset(self) -> None:
        """quarantine_row() should handle None row_offset."""
        sink = MockSink()
        qm = QuarantineManager()

        qm.quarantine_row(
            sink,
            project_name="proj",
            table_name="t1",
            row_offset=None,
            row_data_dict={"id": 1},
            error_msg="err",
        )

        insert_sql = sink.executed_sql[1]
        assert "NULL" in insert_sql

    def test_escapes_single_quotes(self) -> None:
        """Single quotes in values should be escaped."""
        sink = MockSink()
        qm = QuarantineManager()

        qm.quarantine_row(
            sink,
            project_name="proj's",
            table_name="t1",
            row_offset=0,
            row_data_dict={"name": "O'Brien"},
            error_msg="can't write",
        )

        insert_sql = sink.executed_sql[1]
        # Double single quotes for escaping
        assert "proj''s" in insert_sql
        assert "can''t write" in insert_sql

    def test_does_not_crash_on_insert_failure(self) -> None:
        """quarantine_row() should not raise if the INSERT fails."""
        sink = MockSink(fail_insert=True)
        qm = QuarantineManager()
        # Manually set ensured so it tries the INSERT
        qm._table_ensured = True

        # Should not raise
        qm.quarantine_row(
            sink, "proj", "t1", 0, {"id": 1}, "err"
        )

    def test_truncates_large_payloads(self) -> None:
        """Very large row data should be truncated."""
        sink = MockSink()
        qm = QuarantineManager()

        large_data = {"data": "x" * 2_000_000}
        qm.quarantine_row(
            sink, "proj", "t1", 0, large_data, "err"
        )

        insert_sql = sink.executed_sql[1]
        assert "truncated" in insert_sql


class TestGetQuarantinedRows:
    """Tests for QuarantineManager.get_quarantined_rows()."""

    def test_returns_empty_list(self) -> None:
        """get_quarantined_rows() returns empty list (execute_sql has no results)."""
        sink = MockSink()
        qm = QuarantineManager()

        result = qm.get_quarantined_rows(sink, "proj")
        assert result == []

    def test_accepts_table_name_filter(self) -> None:
        """get_quarantined_rows() should accept an optional table_name."""
        sink = MockSink()
        qm = QuarantineManager()

        result = qm.get_quarantined_rows(sink, "proj", table_name="t1")
        assert result == []


class TestClear:
    """Tests for QuarantineManager.clear()."""

    def test_deletes_rows_for_project(self) -> None:
        """clear() should execute a DELETE statement for the project."""
        sink = MockSink()
        qm = QuarantineManager()

        qm.clear(sink, "proj")

        assert len(sink.executed_sql) == 1
        sql = sink.executed_sql[0]
        assert "DELETE FROM" in sql
        assert _QUARANTINE_TABLE in sql
        assert "proj" in sql

    def test_does_not_crash_on_delete_failure(self) -> None:
        """clear() should not raise if the DELETE fails."""
        class FailDeleteSink(MockSink):
            """Sink that fails on DELETE."""

            def execute_sql(self, sql: str) -> None:
                if "DELETE" in sql:
                    raise RuntimeError("delete failed")
                self.executed_sql.append(sql)

        sink = FailDeleteSink()
        qm = QuarantineManager()

        # Should not raise
        qm.clear(sink, "proj")

    def test_escapes_project_name(self) -> None:
        """clear() should escape single quotes in the project name."""
        sink = MockSink()
        qm = QuarantineManager()

        qm.clear(sink, "proj's test")

        sql = sink.executed_sql[0]
        assert "proj''s test" in sql


class TestDDLVariants:
    """Tests for DDL variant coverage."""

    def test_all_variants_reference_quarantine_table(self) -> None:
        """Each DDL variant should reference the quarantine table name."""
        for variant in _CREATE_TABLE_SQL_VARIANTS:
            assert _QUARANTINE_TABLE in variant

    def test_at_least_four_variants(self) -> None:
        """Should have at least 4 DDL variants (PG, MySQL, MSSQL, SQLite)."""
        assert len(_CREATE_TABLE_SQL_VARIANTS) >= 4
