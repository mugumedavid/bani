"""Tests for the incremental sync engine (Section 13)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from bani.application.sync_engine import (
    IncrementalSyncEngine,
    SyncResult,
    SyncStateManager,
    SyncStateRow,
)
from bani.connectors.base import SinkConnector, SourceConnector
from bani.domain.project import ConnectionConfig, SyncConfig, SyncStrategy
from bani.domain.schema import (
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


# ---------------------------------------------------------------------------
# Mock connectors
# ---------------------------------------------------------------------------


class MockSourceConnector(SourceConnector):
    """Mock source connector that yields configurable batches."""

    def __init__(
        self,
        schema: DatabaseSchema | None = None,
        table_data: dict[str, list[pa.RecordBatch]] | None = None,
    ) -> None:
        self._schema = schema or DatabaseSchema(tables=(), source_dialect="postgresql")
        self._table_data: dict[str, list[pa.RecordBatch]] = table_data or {}
        self._connected = False
        self.read_table_calls: list[dict[str, Any]] = []

    def connect(self, config: ConnectionConfig) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def introspect_schema(self) -> DatabaseSchema:
        return self._schema

    def read_table(
        self,
        table_name: str,
        schema_name: str,
        columns: list[str] | None = None,
        filter_sql: str | None = None,
        batch_size: int = 100_000,
    ) -> Iterator[pa.RecordBatch]:
        self.read_table_calls.append(
            {
                "table_name": table_name,
                "schema_name": schema_name,
                "columns": columns,
                "filter_sql": filter_sql,
                "batch_size": batch_size,
            }
        )
        key = f"{schema_name}.{table_name}"
        yield from self._table_data.get(key, [])

    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        return 0


class MockSinkConnector(SinkConnector):
    """Mock sink connector that records all operations."""

    def __init__(self) -> None:
        self._connected = False
        self.executed_sql: list[str] = []
        self.batches_written: list[tuple[str, str, int]] = []
        self.created_tables: list[str] = []

    def connect(self, config: ConnectionConfig) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def create_table(self, table_def: TableDefinition) -> None:
        self.created_tables.append(table_def.fully_qualified_name)

    def write_batch(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        rows = batch.num_rows
        self.batches_written.append((table_name, schema_name, rows))
        return rows

    def create_indexes(
        self,
        table_name: str,
        schema_name: str,
        indexes: tuple[IndexDefinition, ...],
    ) -> None:
        pass

    def create_foreign_keys(self, fks: tuple[ForeignKeyDefinition, ...]) -> None:
        pass

    def execute_sql(self, sql: str) -> None:
        self.executed_sql.append(sql)


class MockCombinedConnector(MockSourceConnector, MockSinkConnector):  # type: ignore[misc]
    """A connector that implements both Source and Sink for the target DB.

    Used so that the same object can be passed as sink + target_reader.
    """

    def __init__(
        self,
        table_data: dict[str, list[pa.RecordBatch]] | None = None,
    ) -> None:
        # Initialise both parents explicitly.
        MockSourceConnector.__init__(self, table_data=table_data)
        MockSinkConnector.__init__(self)

    # read_table comes from MockSourceConnector; write_batch/execute_sql from
    # MockSinkConnector. No additional overrides needed.

    def connect(self, config: ConnectionConfig) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_batch(
    columns: dict[str, list[Any]],
    schema: pa.Schema | None = None,
) -> pa.RecordBatch:
    """Build a RecordBatch from a column dict."""
    if schema is not None:
        return pa.RecordBatch.from_pydict(columns, schema=schema)
    return pa.RecordBatch.from_pydict(columns)


def _sync_config(
    strategy: SyncStrategy = SyncStrategy.FULL,
    tracking_columns: tuple[tuple[str, str], ...] = (),
) -> SyncConfig:
    return SyncConfig(enabled=True, strategy=strategy, tracking_columns=tracking_columns)


# ---------------------------------------------------------------------------
# SyncStateManager tests
# ---------------------------------------------------------------------------


class TestSyncStateManager:
    """Tests for SyncStateManager."""

    def test_ensure_table_creates_ddl(self) -> None:
        """First call should issue CREATE TABLE DDL."""
        sink = MockSinkConnector()
        reader = MockSourceConnector()
        mgr = SyncStateManager(sink, reader)

        mgr._ensure_table()

        assert len(sink.executed_sql) == 1
        assert "CREATE TABLE" in sink.executed_sql[0]
        assert "_bani_sync_state" in sink.executed_sql[0]

    def test_ensure_table_only_once(self) -> None:
        """DDL should only run once regardless of how many calls."""
        sink = MockSinkConnector()
        reader = MockSourceConnector()
        mgr = SyncStateManager(sink, reader)

        mgr._ensure_table()
        mgr._ensure_table()
        mgr._ensure_table()

        assert len(sink.executed_sql) == 1

    def test_read_state_returns_none_for_empty_table(self) -> None:
        """read_state returns None when no batches are yielded."""
        sink = MockSinkConnector()
        reader = MockSourceConnector(table_data={})
        mgr = SyncStateManager(sink, reader)

        result = mgr.read_state("proj", "public.users")
        assert result is None

    def test_read_state_returns_row(self) -> None:
        """read_state returns a SyncStateRow when data exists."""
        state_batch = _make_batch(
            {
                "project_name": ["proj"],
                "table_name": ["public.users"],
                "last_timestamp": ["2025-06-01T00:00:00"],
                "last_rowversion": [42],
                "last_sync_at": ["2025-06-01T12:00:00"],
            }
        )
        reader = MockSourceConnector(
            table_data={"public._bani_sync_state": [state_batch]}
        )
        sink = MockSinkConnector()
        mgr = SyncStateManager(sink, reader)

        row = mgr.read_state("proj", "public.users")
        assert row is not None
        assert isinstance(row, SyncStateRow)
        assert row.project_name == "proj"
        assert row.table_name == "public.users"
        assert row.last_timestamp == "2025-06-01T00:00:00"
        assert row.last_rowversion == 42

    def test_update_state_issues_sql(self) -> None:
        """update_state writes DELETE+INSERT SQL."""
        sink = MockSinkConnector()
        reader = MockSourceConnector()
        mgr = SyncStateManager(sink, reader)

        mgr.update_state(
            "proj", "public.users", last_timestamp="2025-06-01T00:00:00"
        )

        # One for CREATE TABLE, one for DELETE+INSERT.
        assert len(sink.executed_sql) == 2
        upsert_sql = sink.executed_sql[1]
        assert "DELETE FROM _bani_sync_state" in upsert_sql
        assert "INSERT INTO _bani_sync_state" in upsert_sql
        assert "2025-06-01T00:00:00" in upsert_sql

    def test_update_state_with_rowversion(self) -> None:
        """update_state stores rowversion as integer."""
        sink = MockSinkConnector()
        reader = MockSourceConnector()
        mgr = SyncStateManager(sink, reader)

        mgr.update_state("proj", "public.users", last_rowversion=99)

        upsert_sql = sink.executed_sql[1]
        assert "99" in upsert_sql

    def test_clear_state(self) -> None:
        """clear_state deletes all rows for the project."""
        sink = MockSinkConnector()
        reader = MockSourceConnector()
        mgr = SyncStateManager(sink, reader)

        mgr.clear_state("proj")

        assert len(sink.executed_sql) == 2  # CREATE TABLE + DELETE
        assert "DELETE FROM _bani_sync_state" in sink.executed_sql[1]
        assert "project_name = 'proj'" in sink.executed_sql[1]

    def test_read_state_handles_reader_exception(self) -> None:
        """read_state returns None if the reader raises an exception."""

        class FailingReader(MockSourceConnector):
            def read_table(
                self,
                table_name: str,
                schema_name: str,
                columns: list[str] | None = None,
                filter_sql: str | None = None,
                batch_size: int = 100_000,
            ) -> Iterator[pa.RecordBatch]:
                raise RuntimeError("connection lost")
                yield  # pragma: no cover — make this a generator

        sink = MockSinkConnector()
        reader = FailingReader()
        mgr = SyncStateManager(sink, reader)

        result = mgr.read_state("proj", "public.users")
        assert result is None


# ---------------------------------------------------------------------------
# IncrementalSyncEngine — FULL strategy
# ---------------------------------------------------------------------------


class TestFullSync:
    """Tests for SyncStrategy.FULL."""

    def test_full_sync_transfers_all_rows(self) -> None:
        """FULL strategy deletes target data, then inserts all source rows."""
        batch = _make_batch({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        source = MockSourceConnector(table_data={"public.users": [batch]})
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.FULL),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table("users", "public", primary_key_columns=("id",))

        assert result.rows_inserted == 3
        assert result.rows_updated == 0
        assert result.rows_deleted == 0
        assert result.is_full_sync is True
        assert result.strategy == "full"

    def test_full_sync_empty_table(self) -> None:
        """FULL strategy with no source rows inserts nothing."""
        source = MockSourceConnector(table_data={})
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.FULL),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table("users", "public", primary_key_columns=("id",))

        assert result.rows_inserted == 0
        assert result.is_full_sync is True

    def test_full_sync_updates_state(self) -> None:
        """FULL strategy updates sync state after completion."""
        source = MockSourceConnector(table_data={})
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.FULL),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        engine.sync_table("users", "public", primary_key_columns=("id",))

        # Should have DDL (CREATE TABLE) + DELETE target + UPDATE state.
        state_sql = [s for s in target.executed_sql if "INSERT INTO _bani_sync_state" in s]
        assert len(state_sql) == 1


# ---------------------------------------------------------------------------
# IncrementalSyncEngine — TIMESTAMP strategy
# ---------------------------------------------------------------------------


class TestTimestampSync:
    """Tests for SyncStrategy.TIMESTAMP."""

    def test_first_sync_reads_all_rows(self) -> None:
        """First timestamp sync (no prior state) reads everything."""
        batch = _make_batch(
            {"id": [1, 2], "name": ["a", "b"], "updated_at": ["2025-06-01", "2025-06-02"]}
        )
        source = MockSourceConnector(table_data={"public.users": [batch]})
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(
                SyncStrategy.TIMESTAMP,
                tracking_columns=(("users", "updated_at"),),
            ),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table("users", "public", primary_key_columns=("id",))

        assert result.is_full_sync is True
        assert result.rows_inserted == 2

        # No filter_sql should have been applied.
        assert source.read_table_calls[0]["filter_sql"] is None

    def test_incremental_sync_applies_filter(self) -> None:
        """Subsequent sync applies WHERE tracking_col > last_timestamp."""
        # Seed the state table with a prior timestamp.
        state_batch = _make_batch(
            {
                "project_name": ["proj"],
                "table_name": ["public.users"],
                "last_timestamp": ["2025-06-01"],
                "last_rowversion": [None],
                "last_sync_at": ["2025-06-01T12:00:00"],
            }
        )
        delta_batch = _make_batch(
            {"id": [3], "name": ["c"], "updated_at": ["2025-06-02"]}
        )
        target = MockCombinedConnector(
            table_data={
                "public._bani_sync_state": [state_batch],
                "public.users": [],
            }
        )
        source = MockSourceConnector(table_data={"public.users": [delta_batch]})

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(
                SyncStrategy.TIMESTAMP,
                tracking_columns=(("users", "updated_at"),),
            ),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table("users", "public", primary_key_columns=("id",))

        assert result.is_full_sync is False
        # The read_table call should include a filter.
        call = source.read_table_calls[0]
        assert call["filter_sql"] is not None
        assert "updated_at" in call["filter_sql"]
        assert "2025-06-01" in call["filter_sql"]

    def test_no_tracking_column_raises(self) -> None:
        """Missing tracking column raises ConfigurationError."""
        source = MockSourceConnector()
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(
                SyncStrategy.TIMESTAMP,
                tracking_columns=(),  # no columns configured
            ),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        try:
            engine.sync_table("users", "public", primary_key_columns=("id",))
            raise AssertionError("Should have raised ConfigurationError")
        except Exception as exc:
            assert "tracking column" in str(exc).lower()

    def test_wildcard_tracking_column(self) -> None:
        """A wildcard '*' tracking column matches any table."""
        batch = _make_batch(
            {"id": [1], "name": ["a"], "modified": ["2025-06-01"]}
        )
        source = MockSourceConnector(table_data={"public.orders": [batch]})
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(
                SyncStrategy.TIMESTAMP,
                tracking_columns=(("*", "modified"),),
            ),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table("orders", "public", primary_key_columns=("id",))
        assert result.rows_inserted == 1

    def test_timestamp_state_is_updated(self) -> None:
        """After sync, the state table stores the max timestamp seen."""
        batch = _make_batch(
            {"id": [1, 2], "name": ["a", "b"], "updated_at": ["2025-06-01", "2025-06-02"]}
        )
        source = MockSourceConnector(table_data={"public.users": [batch]})
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(
                SyncStrategy.TIMESTAMP,
                tracking_columns=(("users", "updated_at"),),
            ),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        engine.sync_table("users", "public", primary_key_columns=("id",))

        state_sql = [s for s in target.executed_sql if "INSERT INTO _bani_sync_state" in s]
        assert len(state_sql) == 1
        assert "2025-06-02" in state_sql[0]


# ---------------------------------------------------------------------------
# IncrementalSyncEngine — ROWVERSION strategy
# ---------------------------------------------------------------------------


class TestRowversionSync:
    """Tests for SyncStrategy.ROWVERSION."""

    def test_first_rowversion_sync_reads_all(self) -> None:
        """First rowversion sync reads all rows (no prior state)."""
        batch = _make_batch({"id": [1, 2], "name": ["a", "b"], "xmin": [100, 200]})
        source = MockSourceConnector(table_data={"public.users": [batch]})
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.ROWVERSION),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
            source_dialect="postgresql",
        )

        result = engine.sync_table("users", "public", primary_key_columns=("id",))

        assert result.is_full_sync is True
        assert result.rows_inserted == 2

    def test_incremental_rowversion_applies_filter(self) -> None:
        """Subsequent rowversion sync filters by xmin > last value."""
        state_batch = _make_batch(
            {
                "project_name": ["proj"],
                "table_name": ["public.users"],
                "last_timestamp": [None],
                "last_rowversion": [150],
                "last_sync_at": ["2025-06-01T12:00:00"],
            }
        )
        delta_batch = _make_batch({"id": [3], "name": ["c"], "xmin": [200]})

        target = MockCombinedConnector(
            table_data={"public._bani_sync_state": [state_batch]}
        )
        source = MockSourceConnector(table_data={"public.users": [delta_batch]})

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.ROWVERSION),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
            source_dialect="postgresql",
        )

        result = engine.sync_table("users", "public", primary_key_columns=("id",))

        assert result.is_full_sync is False
        call = source.read_table_calls[0]
        assert call["filter_sql"] is not None
        assert "xmin" in call["filter_sql"]
        assert "150" in call["filter_sql"]

    def test_unsupported_dialect_raises(self) -> None:
        """Rowversion on SQLite raises ConfigurationError."""
        source = MockSourceConnector()
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.ROWVERSION),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
            source_dialect="sqlite",
        )

        try:
            engine.sync_table("users", "public", primary_key_columns=("id",))
            raise AssertionError("Should have raised ConfigurationError")
        except Exception as exc:
            assert "not supported" in str(exc).lower()

    def test_mysql_unsupported(self) -> None:
        """Rowversion on MySQL is not supported."""
        source = MockSourceConnector()
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.ROWVERSION),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
            source_dialect="mysql",
        )

        try:
            engine.sync_table("users", "public", primary_key_columns=("id",))
            raise AssertionError("Should have raised ConfigurationError")
        except Exception as exc:
            assert "not supported" in str(exc).lower()

    def test_mssql_rowversion_column(self) -> None:
        """MSSQL uses 'rowversion' as the change-tracking column."""
        batch = _make_batch(
            {"id": [1], "name": ["a"], "rowversion": [500]}
        )
        source = MockSourceConnector(table_data={"dbo.users": [batch]})
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.ROWVERSION),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
            source_dialect="mssql",
        )

        result = engine.sync_table("users", "dbo", primary_key_columns=("id",))
        assert result.rows_inserted == 1

    def test_oracle_ora_rowscn(self) -> None:
        """Oracle uses ORA_ROWSCN as the change-tracking column."""
        batch = _make_batch(
            {"id": [1], "name": ["a"], "ORA_ROWSCN": [12345]}
        )
        source = MockSourceConnector(table_data={"hr.users": [batch]})
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.ROWVERSION),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
            source_dialect="oracle",
        )

        result = engine.sync_table("users", "hr", primary_key_columns=("id",))
        assert result.rows_inserted == 1

    def test_rowversion_state_is_updated(self) -> None:
        """After sync, the state table stores the max rowversion seen."""
        batch = _make_batch({"id": [1, 2], "name": ["a", "b"], "xmin": [100, 200]})
        source = MockSourceConnector(table_data={"public.users": [batch]})
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.ROWVERSION),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
            source_dialect="postgresql",
        )

        engine.sync_table("users", "public", primary_key_columns=("id",))

        state_sql = [s for s in target.executed_sql if "INSERT INTO _bani_sync_state" in s]
        assert len(state_sql) == 1
        assert "200" in state_sql[0]


# ---------------------------------------------------------------------------
# IncrementalSyncEngine — CHECKSUM strategy
# ---------------------------------------------------------------------------


class TestChecksumSync:
    """Tests for SyncStrategy.CHECKSUM."""

    def test_checksum_inserts_new_rows(self) -> None:
        """Rows in source but not target are inserted."""
        source_batch = _make_batch({"id": [1, 2], "name": ["a", "b"]})
        source = MockSourceConnector(table_data={"public.users": [source_batch]})

        # Target has no rows.
        target = MockCombinedConnector(table_data={"public.users": []})

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.CHECKSUM),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table("users", "public", primary_key_columns=("id",))

        assert result.rows_inserted == 2
        assert result.rows_updated == 0
        assert result.rows_deleted == 0

    def test_checksum_detects_updates(self) -> None:
        """Rows that exist in both but differ are updated."""
        source_batch = _make_batch({"id": [1], "name": ["new_name"]})
        target_batch = _make_batch({"id": [1], "name": ["old_name"]})

        source = MockSourceConnector(table_data={"public.users": [source_batch]})
        target = MockCombinedConnector(
            table_data={"public.users": [target_batch]}
        )

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.CHECKSUM),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table("users", "public", primary_key_columns=("id",))

        assert result.rows_updated == 1
        assert result.rows_inserted == 0

    def test_checksum_detects_deletes(self) -> None:
        """Rows in target but not source are deleted."""
        source_batch = _make_batch({"id": [1], "name": ["a"]})
        target_batch = _make_batch({"id": [1, 2], "name": ["a", "b"]})

        source = MockSourceConnector(table_data={"public.users": [source_batch]})
        target = MockCombinedConnector(
            table_data={"public.users": [target_batch]}
        )

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.CHECKSUM),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table("users", "public", primary_key_columns=("id",))

        assert result.rows_deleted == 1

    def test_checksum_no_changes(self) -> None:
        """Identical source and target produce zero changes."""
        batch = _make_batch({"id": [1, 2], "name": ["a", "b"]})
        source = MockSourceConnector(table_data={"public.users": [batch]})
        target = MockCombinedConnector(table_data={"public.users": [batch]})

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.CHECKSUM),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table("users", "public", primary_key_columns=("id",))

        assert result.rows_inserted == 0
        assert result.rows_updated == 0
        assert result.rows_deleted == 0

    def test_checksum_requires_pk(self) -> None:
        """Checksum strategy requires primary key columns."""
        source = MockSourceConnector()
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.CHECKSUM),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        try:
            engine.sync_table("users", "public", primary_key_columns=())
            raise AssertionError("Should have raised ConfigurationError")
        except Exception as exc:
            assert "primary key" in str(exc).lower()

    def test_checksum_composite_pk(self) -> None:
        """Checksum strategy works with composite primary keys."""
        source_batch = _make_batch(
            {"tenant_id": [1, 1], "user_id": [10, 20], "name": ["a", "b"]}
        )
        target_batch = _make_batch(
            {"tenant_id": [1], "user_id": [10], "name": ["a"]}
        )

        source = MockSourceConnector(table_data={"public.memberships": [source_batch]})
        target = MockCombinedConnector(
            table_data={"public.memberships": [target_batch]}
        )

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.CHECKSUM),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table(
            "memberships", "public", primary_key_columns=("tenant_id", "user_id")
        )

        assert result.rows_inserted == 1  # (1, 20) is new
        assert result.rows_updated == 0
        assert result.rows_deleted == 0

    def test_checksum_empty_source_deletes_all_target(self) -> None:
        """Empty source table means all target rows should be deleted."""
        target_batch = _make_batch({"id": [1, 2], "name": ["a", "b"]})

        source = MockSourceConnector(table_data={"public.users": []})
        target = MockCombinedConnector(
            table_data={"public.users": [target_batch]}
        )

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.CHECKSUM),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table("users", "public", primary_key_columns=("id",))

        assert result.rows_deleted == 2
        assert result.rows_inserted == 0


# ---------------------------------------------------------------------------
# SyncResult
# ---------------------------------------------------------------------------


class TestSyncResult:
    """Tests for SyncResult dataclass."""

    def test_sync_result_fields(self) -> None:
        result = SyncResult(
            table_name="public.users",
            strategy="timestamp",
            rows_inserted=10,
            rows_updated=5,
            rows_deleted=2,
            is_full_sync=False,
            duration_seconds=1.234,
        )
        assert result.table_name == "public.users"
        assert result.strategy == "timestamp"
        assert result.rows_inserted == 10
        assert result.rows_updated == 5
        assert result.rows_deleted == 2
        assert result.is_full_sync is False

    def test_sync_result_is_frozen(self) -> None:
        result = SyncResult(
            table_name="t",
            strategy="full",
            rows_inserted=0,
            rows_updated=0,
            rows_deleted=0,
            is_full_sync=True,
            duration_seconds=0.0,
        )
        try:
            result.rows_inserted = 99  # type: ignore[misc]
            raise AssertionError("Should be frozen")
        except AttributeError:
            pass


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge cases and error scenarios."""

    def test_multiple_batches(self) -> None:
        """Engine handles multiple batches from source."""
        batch1 = _make_batch({"id": [1, 2], "name": ["a", "b"]})
        batch2 = _make_batch({"id": [3, 4], "name": ["c", "d"]})
        source = MockSourceConnector(
            table_data={"public.users": [batch1, batch2]}
        )
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.FULL),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table("users", "public", primary_key_columns=("id",))
        assert result.rows_inserted == 4

    def test_duration_is_positive(self) -> None:
        """SyncResult duration should be non-negative."""
        source = MockSourceConnector(table_data={})
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.FULL),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table("users", "public", primary_key_columns=("id",))
        assert result.duration_seconds >= 0.0

    def test_state_manager_escapes_single_quotes(self) -> None:
        """Project or table names with single quotes are escaped."""
        sink = MockSinkConnector()
        reader = MockSourceConnector()
        mgr = SyncStateManager(sink, reader)

        mgr.update_state("proj'ect", "public.use'rs")

        upsert_sql = sink.executed_sql[1]
        assert "proj''ect" in upsert_sql
        assert "use''rs" in upsert_sql

    def test_full_sync_with_no_pk(self) -> None:
        """FULL strategy works even without primary keys."""
        batch = _make_batch({"col1": [1, 2], "col2": ["a", "b"]})
        source = MockSourceConnector(table_data={"public.logs": [batch]})
        target = MockCombinedConnector()

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(SyncStrategy.FULL),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table("logs", "public", primary_key_columns=())
        assert result.rows_inserted == 2

    def test_timestamp_sync_no_pk_inserts_only(self) -> None:
        """Timestamp sync without PK columns does insert-only (no upsert)."""
        state_batch = _make_batch(
            {
                "project_name": ["proj"],
                "table_name": ["public.logs"],
                "last_timestamp": ["2025-01-01"],
                "last_rowversion": [None],
                "last_sync_at": ["2025-01-01T00:00:00"],
            }
        )
        delta_batch = _make_batch(
            {"msg": ["hello"], "ts": ["2025-06-01"]}
        )
        target = MockCombinedConnector(
            table_data={"public._bani_sync_state": [state_batch]}
        )
        source = MockSourceConnector(table_data={"public.logs": [delta_batch]})

        engine = IncrementalSyncEngine(
            sync_config=_sync_config(
                SyncStrategy.TIMESTAMP,
                tracking_columns=(("logs", "ts"),),
            ),
            project_name="proj",
            source=source,
            sink=target,
            target_reader=target,
        )

        result = engine.sync_table("logs", "public", primary_key_columns=())

        # Without PK, incremental inserts rather than upserts.
        assert result.is_full_sync is False
        # Rows go through the insert path.
        assert len(target.batches_written) > 0
