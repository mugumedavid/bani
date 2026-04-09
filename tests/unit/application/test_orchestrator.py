"""Tests for the MigrationOrchestrator."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa

from bani.application.checkpoint import CheckpointManager
from bani.application.orchestrator import (
    _CHUNK_ROW_THRESHOLD,
    _NUMERIC_ARROW_PREFIXES,
    MigrationOrchestrator,
)
from bani.application.progress import ProgressTracker
from bani.application.quarantine import QuarantineManager
from bani.connectors.base import SinkConnector, SourceConnector
from bani.domain.project import (
    ConnectionConfig,
    ErrorHandlingStrategy,
    ProjectModel,
    ProjectOptions,
)
from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    TableDefinition,
)


class MockSourceConnector(SourceConnector):
    """Mock source connector for testing."""

    def __init__(self, schema: DatabaseSchema) -> None:
        """Initialize with a fixed schema."""
        self._schema = schema
        self._connected = False
        self.connection: Any = None
        self.read_table_calls: list[dict[str, Any]] = []

    def connect(self, config: ConnectionConfig) -> None:
        """Mark as connected."""
        self._connected = True

    def disconnect(self) -> None:
        """Mark as disconnected."""
        self._connected = False

    def introspect_schema(self) -> DatabaseSchema:
        """Return the fixed schema."""
        return self._schema

    def read_table(
        self,
        table_name: str,
        schema_name: str,
        columns: list[str] | None = None,
        filter_sql: str | None = None,
        batch_size: int = 100_000,
    ) -> Iterator[pa.RecordBatch]:
        """Yield empty batches (no actual data)."""
        self.read_table_calls.append(
            {
                "table_name": table_name,
                "schema_name": schema_name,
                "columns": columns,
                "filter_sql": filter_sql,
                "batch_size": batch_size,
            }
        )
        # For testing, we'll just not yield anything
        return iter([])

    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        """Return the row count estimate from the schema."""
        for table in self._schema.tables:
            if table.table_name == table_name and table.schema_name == schema_name:
                return table.row_count_estimate or 0
        return 0


class MockSinkConnector(SinkConnector):
    """Mock sink connector for testing."""

    def __init__(self) -> None:
        """Initialize the mock."""
        self._connected = False
        self.created_tables: list[str] = []
        self.created_indexes: list[tuple[str, tuple[Any, ...]]] = []
        self.created_fks: list[tuple[Any, ...]] = []
        self.executed_sql: list[str] = []
        self.batches_written: list[tuple[str, int]] = []

    def connect(self, config: ConnectionConfig) -> None:
        """Mark as connected."""
        self._connected = True

    def disconnect(self) -> None:
        """Mark as disconnected."""
        self._connected = False

    def create_table(self, table_def: TableDefinition) -> None:
        """Record table creation."""
        self.created_tables.append(table_def.fully_qualified_name)

    def create_indexes(
        self, table_name: str, schema_name: str, indexes: tuple[Any, ...]
    ) -> None:
        """Record index creation."""
        self.created_indexes.append((table_name, indexes))

    def create_foreign_keys(self, fks: tuple[Any, ...]) -> None:
        """Record FK creation."""
        self.created_fks.append(fks)

    def write_batch(
        self, table_name: str, schema_name: str, batch: pa.RecordBatch
    ) -> int:
        """Record batch write."""
        rows = len(batch)
        self.batches_written.append((table_name, rows))
        return rows

    def execute_sql(self, sql: str) -> None:
        """Record SQL execution."""
        self.executed_sql.append(sql)


def create_test_schema() -> DatabaseSchema:
    """Create a simple test schema."""
    users_table = TableDefinition(
        schema_name="public",
        table_name="users",
        columns=(
            ColumnDefinition("id", "INT", nullable=False, ordinal_position=0),
            ColumnDefinition("name", "VARCHAR(255)", nullable=True, ordinal_position=1),
        ),
        primary_key=("id",),
        row_count_estimate=100,
    )

    return DatabaseSchema(
        tables=(users_table,),
        source_dialect="postgresql",
    )


def create_test_project() -> ProjectModel:
    """Create a simple test project."""
    return ProjectModel(
        name="test_migration",
        source=ConnectionConfig(dialect="postgresql"),
        target=ConnectionConfig(dialect="mssql"),
        options=ProjectOptions(batch_size=50_000, parallel_workers=2),
    )


def test_orchestrator_initializes() -> None:
    """Test that the orchestrator initializes correctly."""
    project = create_test_project()
    source = MockSourceConnector(create_test_schema())
    sink = MockSinkConnector()
    tracker = ProgressTracker()

    orch = MigrationOrchestrator(project, source, sink, tracker)

    assert orch.project == project
    assert orch.source == source
    assert orch.sink == sink
    assert orch.tracker == tracker


def test_orchestrator_creates_target_schema() -> None:
    """Test that the orchestrator creates the target schema."""
    project = create_test_project()
    schema = create_test_schema()
    source = MockSourceConnector(schema)
    sink = MockSinkConnector()

    orch = MigrationOrchestrator(project, source, sink)
    result = orch.execute()

    # Schema remapper changes public→dbo for PG→MSSQL
    assert "dbo.users" in sink.created_tables
    assert result.tables_completed == 1


def test_orchestrator_creates_schema_only_if_requested() -> None:
    """Test that schema creation respects the create_target_schema option."""
    project = ProjectModel(
        name="test_migration",
        source=ConnectionConfig(dialect="postgresql"),
        target=ConnectionConfig(dialect="mssql"),
        options=ProjectOptions(create_target_schema=False),
    )
    schema = create_test_schema()
    source = MockSourceConnector(schema)
    sink = MockSinkConnector()

    orch = MigrationOrchestrator(project, source, sink)
    orch.execute()

    # Should NOT have created the table
    assert len(sink.created_tables) == 0


def test_orchestrator_drops_tables_if_requested() -> None:
    """Test that the orchestrator drops tables before creation if requested."""
    project = ProjectModel(
        name="test_migration",
        source=ConnectionConfig(dialect="postgresql"),
        target=ConnectionConfig(dialect="mssql"),
        options=ProjectOptions(
            create_target_schema=True,
            drop_target_tables_first=True,
        ),
    )
    schema = create_test_schema()
    source = MockSourceConnector(schema)
    sink = MockSinkConnector()

    orch = MigrationOrchestrator(project, source, sink)
    orch.execute()

    # Should have executed drop SQL and then created the table
    assert any("DROP TABLE" in sql for sql in sink.executed_sql)
    assert "dbo.users" in sink.created_tables


def test_orchestrator_respects_batch_size() -> None:
    """Test that the orchestrator uses the configured batch size."""
    project = create_test_project()
    schema = create_test_schema()
    source = MockSourceConnector(schema)
    sink = MockSinkConnector()

    orch = MigrationOrchestrator(project, source, sink)

    # Verify the batch size is set correctly
    assert orch.options.batch_size == 50_000
    assert orch.options.parallel_workers == 2


def test_orchestrator_returns_migration_result() -> None:
    """Test that the orchestrator returns a complete MigrationResult."""
    project = create_test_project()
    schema = create_test_schema()
    source = MockSourceConnector(schema)
    sink = MockSinkConnector()

    orch = MigrationOrchestrator(project, source, sink)
    result = orch.execute()

    assert result.project_name == "test_migration"
    assert result.tables_completed >= 0
    assert result.tables_failed >= 0
    assert result.duration_seconds >= 0
    assert isinstance(result.errors, tuple)


def test_orchestrator_emits_progress_events() -> None:
    """Test that the orchestrator emits progress events."""
    project = create_test_project()
    schema = create_test_schema()
    source = MockSourceConnector(schema)
    sink = MockSinkConnector()
    tracker = ProgressTracker()

    events: list[object] = []
    tracker.add_listener(lambda e: events.append(e))

    orch = MigrationOrchestrator(project, source, sink, tracker)
    orch.execute()

    # Should have at least migration_started and migration_complete events
    assert len(events) >= 2


def test_orchestrator_uses_default_progress_tracker() -> None:
    """Test that the orchestrator creates a default tracker if not provided."""
    project = create_test_project()
    schema = create_test_schema()
    source = MockSourceConnector(schema)
    sink = MockSinkConnector()

    orch = MigrationOrchestrator(project, source, sink)

    # Should have a default tracker
    assert orch.tracker is not None
    assert isinstance(orch.tracker, ProgressTracker)


def test_orchestrator_uses_default_project_options() -> None:
    """Test that the orchestrator uses default options if not provided."""
    project = ProjectModel(
        name="test_migration",
        source=ConnectionConfig(dialect="postgresql"),
        target=ConnectionConfig(dialect="mssql"),
    )
    schema = create_test_schema()
    source = MockSourceConnector(schema)
    sink = MockSinkConnector()

    orch = MigrationOrchestrator(project, source, sink)

    # Should use defaults
    assert orch.options.batch_size == 100_000
    assert orch.options.parallel_workers == 4


# ---------------------------------------------------------------------------
# Chunk-level parallelism tests
# ---------------------------------------------------------------------------


def _make_large_table(
    row_count: int = 100_000,
    pk_arrow_type: str = "int64",
    pk_columns: tuple[str, ...] = ("id",),
) -> TableDefinition:
    """Helper: create a TableDefinition that qualifies for chunking."""
    cols = [
        ColumnDefinition(
            "id",
            "BIGINT",
            nullable=False,
            ordinal_position=0,
            arrow_type_str=pk_arrow_type,
        ),
        ColumnDefinition(
            "name",
            "VARCHAR(255)",
            nullable=True,
            ordinal_position=1,
            arrow_type_str="string",
        ),
    ]
    return TableDefinition(
        schema_name="public",
        table_name="big_table",
        columns=tuple(cols),
        primary_key=pk_columns,
        row_count_estimate=row_count,
    )


def _make_mock_connection(min_val: int, max_val: int) -> MagicMock:
    """Create a mock database connection that returns a PK range."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (min_val, max_val)
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


class TestShouldChunk:
    """Tests for _should_chunk eligibility."""

    def test_qualifies_single_int_pk_above_threshold(self) -> None:
        """Table with single int PK and large row count qualifies."""
        table = _make_large_table(row_count=100_000, pk_arrow_type="int64")
        project = create_test_project()
        source = MockSourceConnector(
            DatabaseSchema(tables=(table,), source_dialect="postgresql")
        )
        sink = MockSinkConnector()
        orch = MigrationOrchestrator(project, source, sink)

        assert orch._should_chunk(table) is True

    def test_qualifies_uint_pk(self) -> None:
        """Table with unsigned integer PK qualifies."""
        table = _make_large_table(row_count=60_000, pk_arrow_type="uint32")
        project = create_test_project()
        source = MockSourceConnector(
            DatabaseSchema(tables=(table,), source_dialect="mysql")
        )
        sink = MockSinkConnector()
        orch = MigrationOrchestrator(project, source, sink)

        assert orch._should_chunk(table) is True

    def test_rejects_composite_pk(self) -> None:
        """Table with composite PK does not qualify."""
        table = _make_large_table(row_count=100_000, pk_columns=("id", "tenant_id"))
        project = create_test_project()
        source = MockSourceConnector(
            DatabaseSchema(tables=(table,), source_dialect="postgresql")
        )
        sink = MockSinkConnector()
        orch = MigrationOrchestrator(project, source, sink)

        assert orch._should_chunk(table) is False

    def test_rejects_no_pk(self) -> None:
        """Table with no primary key does not qualify."""
        table = _make_large_table(row_count=100_000, pk_columns=())
        project = create_test_project()
        source = MockSourceConnector(
            DatabaseSchema(tables=(table,), source_dialect="postgresql")
        )
        sink = MockSinkConnector()
        orch = MigrationOrchestrator(project, source, sink)

        assert orch._should_chunk(table) is False

    def test_rejects_below_threshold(self) -> None:
        """Table below the row threshold does not qualify."""
        table = _make_large_table(row_count=10_000)
        project = create_test_project()
        source = MockSourceConnector(
            DatabaseSchema(tables=(table,), source_dialect="postgresql")
        )
        sink = MockSinkConnector()
        orch = MigrationOrchestrator(project, source, sink)

        assert orch._should_chunk(table) is False

    def test_rejects_none_row_count(self) -> None:
        """Table with None row_count_estimate does not qualify."""
        table = TableDefinition(
            schema_name="public",
            table_name="big_table",
            columns=(
                ColumnDefinition(
                    "id",
                    "BIGINT",
                    nullable=False,
                    arrow_type_str="int64",
                ),
            ),
            primary_key=("id",),
            row_count_estimate=None,
        )
        project = create_test_project()
        source = MockSourceConnector(
            DatabaseSchema(tables=(table,), source_dialect="postgresql")
        )
        sink = MockSinkConnector()
        orch = MigrationOrchestrator(project, source, sink)

        assert orch._should_chunk(table) is False

    def test_rejects_string_pk(self) -> None:
        """Table with a string PK does not qualify."""
        table = _make_large_table(row_count=100_000, pk_arrow_type="string")
        project = create_test_project()
        source = MockSourceConnector(
            DatabaseSchema(tables=(table,), source_dialect="postgresql")
        )
        sink = MockSinkConnector()
        orch = MigrationOrchestrator(project, source, sink)

        assert orch._should_chunk(table) is False

    def test_rejects_pk_column_not_found(self) -> None:
        """Table whose PK column is missing from columns list does not qualify."""
        table = TableDefinition(
            schema_name="public",
            table_name="big_table",
            columns=(ColumnDefinition("name", "VARCHAR", arrow_type_str="string"),),
            primary_key=("missing_col",),
            row_count_estimate=100_000,
        )
        project = create_test_project()
        source = MockSourceConnector(
            DatabaseSchema(tables=(table,), source_dialect="postgresql")
        )
        sink = MockSinkConnector()
        orch = MigrationOrchestrator(project, source, sink)

        assert orch._should_chunk(table) is False


class TestGetPkRange:
    """Tests for _get_pk_range."""

    def test_returns_range_on_success(self) -> None:
        """Should return (min, max) tuple from the cursor result."""
        table = _make_large_table()
        schema = DatabaseSchema(tables=(table,), source_dialect="postgresql")
        source = MockSourceConnector(schema)
        source.connection = _make_mock_connection(1, 100_000)
        sink = MockSinkConnector()
        project = create_test_project()
        orch = MigrationOrchestrator(project, source, sink)

        result = orch._get_pk_range(table, "id")

        assert result == (1, 100_000)

    def test_returns_none_when_no_connection(self) -> None:
        """Should return None if the source has no connection attribute."""
        table = _make_large_table()
        schema = DatabaseSchema(tables=(table,), source_dialect="postgresql")
        source = MockSourceConnector(schema)
        # source.connection is None by default
        sink = MockSinkConnector()
        project = create_test_project()
        orch = MigrationOrchestrator(project, source, sink)

        assert orch._get_pk_range(table, "id") is None

    def test_returns_none_on_query_failure(self) -> None:
        """Should return None and not raise if the query fails."""
        table = _make_large_table()
        schema = DatabaseSchema(tables=(table,), source_dialect="postgresql")
        source = MockSourceConnector(schema)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = RuntimeError("connection lost")
        mock_conn.cursor.return_value = mock_cursor
        source.connection = mock_conn
        sink = MockSinkConnector()
        project = create_test_project()
        orch = MigrationOrchestrator(project, source, sink)

        assert orch._get_pk_range(table, "id") is None

    def test_returns_none_when_min_is_null(self) -> None:
        """Should return None if the table is empty (MIN/MAX return NULL)."""
        table = _make_large_table()
        schema = DatabaseSchema(tables=(table,), source_dialect="postgresql")
        source = MockSourceConnector(schema)
        source.connection = _make_mock_connection(0, 0)
        # Override fetchone to return NULLs
        mock_cursor = source.connection.cursor.return_value
        mock_cursor.fetchone.return_value = (None, None)
        sink = MockSinkConnector()
        project = create_test_project()
        orch = MigrationOrchestrator(project, source, sink)

        assert orch._get_pk_range(table, "id") is None

    def test_builds_correct_sql_with_schema(self) -> None:
        """Should include schema in the FROM clause."""
        table = _make_large_table()
        schema = DatabaseSchema(tables=(table,), source_dialect="postgresql")
        source = MockSourceConnector(schema)
        source.connection = _make_mock_connection(1, 500)
        sink = MockSinkConnector()
        project = create_test_project()
        orch = MigrationOrchestrator(project, source, sink)

        orch._get_pk_range(table, "id")

        mock_cursor = source.connection.cursor.return_value
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert '"public"."big_table"' in executed_sql
        assert 'MIN("id")' in executed_sql
        assert 'MAX("id")' in executed_sql

    def test_builds_correct_sql_without_schema(self) -> None:
        """Should omit schema when schema_name is empty."""
        table = TableDefinition(
            schema_name="",
            table_name="big_table",
            columns=(
                ColumnDefinition(
                    "id",
                    "BIGINT",
                    nullable=False,
                    arrow_type_str="int64",
                ),
            ),
            primary_key=("id",),
            row_count_estimate=100_000,
        )
        schema = DatabaseSchema(tables=(table,), source_dialect="sqlite")
        source = MockSourceConnector(schema)
        source.connection = _make_mock_connection(1, 500)
        sink = MockSinkConnector()
        project = create_test_project()
        orch = MigrationOrchestrator(project, source, sink)

        orch._get_pk_range(table, "id")

        mock_cursor = source.connection.cursor.return_value
        executed_sql = mock_cursor.execute.call_args[0][0]
        # Should not have schema prefix
        assert executed_sql.startswith('SELECT MIN("id"), MAX("id") FROM "big_table"')


class TestTransferTableChunked:
    """Tests for the chunked transfer path."""

    def test_chunked_transfer_splits_into_ranges(self) -> None:
        """Chunked transfer should call read_table with filter_sql for each chunk."""
        table = _make_large_table(row_count=100_000)
        schema = DatabaseSchema(tables=(table,), source_dialect="postgresql")
        source = MockSourceConnector(schema)
        source.connection = _make_mock_connection(1, 100_000)
        sink = MockSinkConnector()
        project = ProjectModel(
            name="test",
            source=ConnectionConfig(dialect="postgresql"),
            target=ConnectionConfig(dialect="mssql"),
            options=ProjectOptions(parallel_workers=2),
        )
        orch = MigrationOrchestrator(project, source, sink)

        result = orch._transfer_table_chunked(table)

        assert result.success is True
        # With 2 workers (min(2, 4) = 2 chunks), should have 2 read_table calls
        assert len(source.read_table_calls) == 2
        # Each call should have a filter_sql with PK range
        for call in source.read_table_calls:
            assert call["filter_sql"] is not None
            assert '"id" >=' in call["filter_sql"]
            assert '"id" <=' in call["filter_sql"]

    def test_chunked_transfer_covers_full_range(self) -> None:
        """All chunks together should cover the entire PK range."""
        table = _make_large_table(row_count=100_000)
        schema = DatabaseSchema(tables=(table,), source_dialect="postgresql")
        source = MockSourceConnector(schema)
        source.connection = _make_mock_connection(1, 100)
        sink = MockSinkConnector()
        project = ProjectModel(
            name="test",
            source=ConnectionConfig(dialect="postgresql"),
            target=ConnectionConfig(dialect="mssql"),
            options=ProjectOptions(parallel_workers=4),
        )
        orch = MigrationOrchestrator(project, source, sink)

        orch._transfer_table_chunked(table)

        # Parse ranges from filter_sql
        ranges: list[tuple[int, int]] = []
        for call in source.read_table_calls:
            parts = call["filter_sql"].split(" AND ")
            start = int(parts[0].split(">= ")[1])
            end = int(parts[1].split("<= ")[1])
            ranges.append((start, end))

        ranges.sort()
        # First chunk should start at 1
        assert ranges[0][0] == 1
        # Last chunk should end at 100
        assert ranges[-1][1] == 100
        # Chunks should be contiguous (no gaps)
        for i in range(1, len(ranges)):
            assert ranges[i][0] == ranges[i - 1][1] + 1

    def test_chunked_fallback_when_pk_range_fails(self) -> None:
        """Should fall back to sequential when PK range query fails."""
        table = _make_large_table(row_count=100_000)
        schema = DatabaseSchema(tables=(table,), source_dialect="postgresql")
        source = MockSourceConnector(schema)
        # connection exists but query will fail
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = RuntimeError("query failed")
        mock_conn.cursor.return_value = mock_cursor
        source.connection = mock_conn
        sink = MockSinkConnector()
        project = create_test_project()
        orch = MigrationOrchestrator(project, source, sink)

        result = orch._transfer_table_chunked(table)

        # Should succeed (empty table via sequential fallback)
        assert result.success is True
        # Sequential path does NOT use filter_sql
        if source.read_table_calls:
            assert source.read_table_calls[0]["filter_sql"] is None

    def test_chunked_transfer_with_data(self) -> None:
        """Chunked transfer should correctly aggregate rows across chunks."""
        table = _make_large_table(row_count=100_000)
        schema = DatabaseSchema(tables=(table,), source_dialect="postgresql")

        # Create a source that yields actual Arrow batches when filter_sql is set
        batch_1 = pa.record_batch(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
            names=["id", "name"],
        )
        batch_2 = pa.record_batch(
            [pa.array([4, 5]), pa.array(["d", "e"])],
            names=["id", "name"],
        )

        call_count = 0
        call_lock = threading.Lock()

        class DataSourceConnector(MockSourceConnector):
            """Source that yields batches based on filter_sql."""

            def read_table(
                self,
                table_name: str,
                schema_name: str,
                columns: list[str] | None = None,
                filter_sql: str | None = None,
                batch_size: int = 100_000,
            ) -> Iterator[pa.RecordBatch]:
                """Yield different batches per chunk."""
                self.read_table_calls.append(
                    {
                        "table_name": table_name,
                        "schema_name": schema_name,
                        "columns": columns,
                        "filter_sql": filter_sql,
                        "batch_size": batch_size,
                    }
                )
                nonlocal call_count
                with call_lock:
                    current = call_count
                    call_count += 1
                if current == 0:
                    yield batch_1
                else:
                    yield batch_2

        source = DataSourceConnector(schema)
        source.connection = _make_mock_connection(1, 100_000)
        sink = MockSinkConnector()
        project = ProjectModel(
            name="test",
            source=ConnectionConfig(dialect="postgresql"),
            target=ConnectionConfig(dialect="mssql"),
            options=ProjectOptions(parallel_workers=2),
        )
        orch = MigrationOrchestrator(project, source, sink)

        result = orch._transfer_table_chunked(table)

        assert result.success is True
        assert result.rows_read == 5  # 3 + 2
        assert result.rows_written == 5

    def test_transfer_table_dispatches_to_chunked(self) -> None:
        """_transfer_table should delegate to chunked path for qualifying tables."""
        table = _make_large_table(row_count=100_000, pk_arrow_type="int64")
        schema = DatabaseSchema(tables=(table,), source_dialect="postgresql")
        source = MockSourceConnector(schema)
        source.connection = _make_mock_connection(1, 100_000)
        sink = MockSinkConnector()
        project = ProjectModel(
            name="test",
            source=ConnectionConfig(dialect="postgresql"),
            target=ConnectionConfig(dialect="mssql"),
            options=ProjectOptions(parallel_workers=2),
        )
        orch = MigrationOrchestrator(project, source, sink)

        result = orch._transfer_table(table)

        assert result.success is True
        # Should have used filter_sql (chunked path), not plain read (sequential)
        assert len(source.read_table_calls) == 2
        for call in source.read_table_calls:
            assert call["filter_sql"] is not None

    def test_transfer_table_uses_sequential_for_small_table(self) -> None:
        """_transfer_table should use sequential path for small tables."""
        table = _make_large_table(row_count=1_000, pk_arrow_type="int64")
        schema = DatabaseSchema(tables=(table,), source_dialect="postgresql")
        source = MockSourceConnector(schema)
        source.connection = _make_mock_connection(1, 1_000)
        sink = MockSinkConnector()
        project = create_test_project()
        orch = MigrationOrchestrator(project, source, sink)

        result = orch._transfer_table(table)

        assert result.success is True
        # Sequential path: one call without filter_sql
        if source.read_table_calls:
            assert source.read_table_calls[0]["filter_sql"] is None

    def test_chunk_count_capped_at_four(self) -> None:
        """Number of chunks should be min(parallel_workers, 4)."""
        table = _make_large_table(row_count=1_000_000)
        schema = DatabaseSchema(tables=(table,), source_dialect="postgresql")
        source = MockSourceConnector(schema)
        source.connection = _make_mock_connection(1, 1_000_000)
        sink = MockSinkConnector()
        project = ProjectModel(
            name="test",
            source=ConnectionConfig(dialect="postgresql"),
            target=ConnectionConfig(dialect="mssql"),
            options=ProjectOptions(parallel_workers=8),
        )
        orch = MigrationOrchestrator(project, source, sink)

        orch._transfer_table_chunked(table)

        # min(8, 4) = 4 chunks
        assert len(source.read_table_calls) == 4


class TestConstants:
    """Verify module-level constants are exposed and correct."""

    def test_chunk_row_threshold(self) -> None:
        """Threshold should be 50_000."""
        assert _CHUNK_ROW_THRESHOLD == 50_000

    def test_numeric_arrow_prefixes(self) -> None:
        """Prefixes should cover int and uint."""
        assert "int" in _NUMERIC_ARROW_PREFIXES
        assert "uint" in _NUMERIC_ARROW_PREFIXES


# ---------------------------------------------------------------------------
# Resumability (checkpoint + quarantine) integration tests
# ---------------------------------------------------------------------------


def _create_multi_table_schema() -> DatabaseSchema:
    """Create a schema with multiple tables for resume testing."""
    t1 = TableDefinition(
        schema_name="public",
        table_name="users",
        columns=(
            ColumnDefinition("id", "INT", nullable=False, ordinal_position=0),
            ColumnDefinition("name", "VARCHAR(255)", nullable=True, ordinal_position=1),
        ),
        primary_key=("id",),
        row_count_estimate=100,
    )
    t2 = TableDefinition(
        schema_name="public",
        table_name="orders",
        columns=(
            ColumnDefinition("id", "INT", nullable=False, ordinal_position=0),
            ColumnDefinition("user_id", "INT", nullable=True, ordinal_position=1),
        ),
        primary_key=("id",),
        row_count_estimate=200,
    )
    return DatabaseSchema(tables=(t1, t2), source_dialect="postgresql")


class TestResumeExecution:
    """Tests for execute(resume=True) checkpoint flow."""

    def test_execute_clears_checkpoint_on_success(self, tmp_path: Path) -> None:
        """execute() should clear the checkpoint after full success."""
        project = create_test_project()
        schema = create_test_schema()
        source = MockSourceConnector(schema)
        sink = MockSinkConnector()
        ckpt = CheckpointManager(base_dir=tmp_path)

        orch = MigrationOrchestrator(project, source, sink, checkpoint=ckpt)
        result = orch.execute()

        assert result.tables_failed == 0
        # Checkpoint is cleaned up after full success
        assert ckpt.load(project.name) is None

    def test_resume_skips_completed_tables(self, tmp_path: Path) -> None:
        """resume=True should skip tables marked as completed in checkpoint."""
        project = create_test_project()
        schema = _create_multi_table_schema()
        source = MockSourceConnector(schema)
        sink = MockSinkConnector()
        ckpt = CheckpointManager(base_dir=tmp_path)

        # Pre-create a checkpoint where users is completed
        # Note: PG→MSSQL remaps public→dbo, so checkpoint uses dbo names
        project_hash = ckpt.compute_hash(project)
        ckpt.create(project.name, project_hash, ("dbo.users", "dbo.orders"))
        ckpt.update_table_status(project.name, "dbo.users", "completed", rows=100)

        orch = MigrationOrchestrator(project, source, sink, checkpoint=ckpt)
        result = orch.execute(resume=True)

        # Both tables should be in the result, but source should only be
        # read for the non-completed table(s)
        assert result.tables_completed >= 1
        # Verify the completed table was not re-read
        read_tables = [c["table_name"] for c in source.read_table_calls]
        assert "users" not in read_tables

    def test_resume_false_starts_fresh(self, tmp_path: Path) -> None:
        """execute(resume=False) should create a fresh checkpoint."""
        project = create_test_project()
        schema = create_test_schema()
        source = MockSourceConnector(schema)
        sink = MockSinkConnector()
        ckpt = CheckpointManager(base_dir=tmp_path)

        # Run once — succeeds, so checkpoint is cleared
        orch = MigrationOrchestrator(project, source, sink, checkpoint=ckpt)
        result = orch.execute(resume=False)

        assert result.tables_failed == 0
        assert ckpt.load(project.name) is None

    def test_resume_with_invalid_checkpoint_starts_fresh(self, tmp_path: Path) -> None:
        """resume=True with a stale checkpoint should start fresh."""
        project = create_test_project()
        schema = create_test_schema()
        source = MockSourceConnector(schema)
        sink = MockSinkConnector()
        ckpt = CheckpointManager(base_dir=tmp_path)

        # Create checkpoint with wrong hash
        ckpt.create(project.name, "wrong_hash", ("public.users",))
        ckpt.update_table_status(project.name, "public.users", "completed", rows=100)

        orch = MigrationOrchestrator(project, source, sink, checkpoint=ckpt)
        result = orch.execute(resume=True)

        # Should have processed the table (not skipped) because checkpoint
        # was invalidated
        assert result.tables_completed >= 1

    def test_checkpoint_cleared_after_full_success(self, tmp_path: Path) -> None:
        """Checkpoint should be cleared after a fully successful migration."""
        project = create_test_project()
        schema = create_test_schema()
        source = MockSourceConnector(schema)
        sink = MockSinkConnector()
        ckpt = CheckpointManager(base_dir=tmp_path)

        orch = MigrationOrchestrator(project, source, sink, checkpoint=ckpt)
        result = orch.execute()

        assert result.tables_failed == 0
        # Checkpoint file is cleaned up on full success
        assert ckpt.load(project.name) is None

    def test_checkpoint_after_table_failure_abort(self, tmp_path: Path) -> None:
        """Checkpoint should persist when migration has failures (ABORT mode)."""
        project = ProjectModel(
            name="test_migration",
            source=ConnectionConfig(dialect="postgresql"),
            target=ConnectionConfig(dialect="mssql"),
            options=ProjectOptions(
                batch_size=50_000,
                parallel_workers=1,
                on_error=ErrorHandlingStrategy.ABORT,
            ),
        )
        schema = create_test_schema()

        class FailingSource(MockSourceConnector):
            """Source that yields batches that cause write failures."""

            def read_table(
                self,
                table_name: str,
                schema_name: str,
                columns: list[str] | None = None,
                filter_sql: str | None = None,
                batch_size: int = 100_000,
            ) -> Iterator[pa.RecordBatch]:
                """Yield a batch."""
                yield pa.record_batch(
                    [pa.array([1]), pa.array(["test"])],
                    names=["id", "name"],
                )

        class FailingSink(MockSinkConnector):
            """Sink that fails to write."""

            def write_batch(
                self, table_name: str, schema_name: str, batch: pa.RecordBatch
            ) -> int:
                raise RuntimeError("write error")

        source = FailingSource(schema)
        sink = FailingSink()
        ckpt = CheckpointManager(base_dir=tmp_path)

        orch = MigrationOrchestrator(project, source, sink, checkpoint=ckpt)
        result = orch.execute()

        # With ABORT, table failure means tables_failed > 0,
        # so checkpoint is preserved for resume
        assert result.tables_failed > 0
        data = ckpt.load(project.name)
        assert data is not None


class TestQuarantineIntegration:
    """Tests for quarantine integration in the orchestrator."""

    def test_orchestrator_has_quarantine_manager(self) -> None:
        """Orchestrator should have a quarantine manager."""
        project = create_test_project()
        source = MockSourceConnector(create_test_schema())
        sink = MockSinkConnector()

        orch = MigrationOrchestrator(project, source, sink)
        assert isinstance(orch._quarantine, QuarantineManager)

    def test_orchestrator_accepts_custom_quarantine(self) -> None:
        """Orchestrator should accept a custom quarantine manager."""
        project = create_test_project()
        source = MockSourceConnector(create_test_schema())
        sink = MockSinkConnector()
        qm = QuarantineManager()

        orch = MigrationOrchestrator(project, source, sink, quarantine=qm)
        assert orch._quarantine is qm

    def test_failed_batch_quarantined_on_log_and_continue(self, tmp_path: Path) -> None:
        """Failed batches should be quarantined with LOG_AND_CONTINUE."""
        project = ProjectModel(
            name="test_quarantine",
            source=ConnectionConfig(dialect="postgresql"),
            target=ConnectionConfig(dialect="mssql"),
            options=ProjectOptions(
                batch_size=50_000,
                parallel_workers=1,
                on_error=ErrorHandlingStrategy.LOG_AND_CONTINUE,
            ),
        )
        schema = create_test_schema()

        class BatchSource(MockSourceConnector):
            """Source that yields one batch."""

            def read_table(
                self,
                table_name: str,
                schema_name: str,
                columns: list[str] | None = None,
                filter_sql: str | None = None,
                batch_size: int = 100_000,
            ) -> Iterator[pa.RecordBatch]:
                """Yield a test batch."""
                yield pa.record_batch(
                    [pa.array([1, 2]), pa.array(["a", "b"])],
                    names=["id", "name"],
                )

        class FailWriteSink(MockSinkConnector):
            """Sink where write_batch fails."""

            def write_batch(
                self, table_name: str, schema_name: str, batch: pa.RecordBatch
            ) -> int:
                raise RuntimeError("constraint violation")

        source = BatchSource(schema)
        sink = FailWriteSink()
        ckpt = CheckpointManager(base_dir=tmp_path)

        orch = MigrationOrchestrator(project, source, sink, checkpoint=ckpt)
        result = orch.execute()

        # Migration should complete (not abort)
        assert result.project_name == "test_quarantine"
        # The quarantine INSERT was attempted via execute_sql
        # (FailWriteSink inherits MockSinkConnector.execute_sql which records calls)
        quarantine_inserts = [
            sql for sql in sink.executed_sql if "INSERT INTO bani_quarantine" in sql
        ]
        assert len(quarantine_inserts) >= 1
