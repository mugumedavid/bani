"""Tests for the MigrationOrchestrator."""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from bani.application.orchestrator import MigrationOrchestrator
from bani.application.progress import ProgressTracker
from bani.connectors.base import SinkConnector, SourceConnector
from bani.domain.project import ConnectionConfig, ProjectModel, ProjectOptions
from bani.domain.schema import ColumnDefinition, DatabaseSchema, TableDefinition

if TYPE_CHECKING:
    import pyarrow as pa


class MockSourceConnector(SourceConnector):
    """Mock source connector for testing."""

    def __init__(self, schema: DatabaseSchema) -> None:
        """Initialize with a fixed schema."""
        self._schema = schema
        self._connected = False

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
        # For testing, we'll just not yield anything
        return iter([])

    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        """Return the row count estimate from the schema."""
        for table in self._schema.tables:
            if table.table_name == table_name and table.schema_name == schema_name:
                return table.row_count_estimate
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

    # Should have created the users table
    assert "public.users" in sink.created_tables
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
    assert "public.users" in sink.created_tables


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
