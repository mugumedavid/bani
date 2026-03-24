"""Migration orchestrator that coordinates schema and data transfer."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import TYPE_CHECKING

from bani.application.progress import ProgressTracker
from bani.connectors.base import SinkConnector, SourceConnector
from bani.domain.dependency import DependencyResolver
from bani.domain.errors import BaniError, WriteError
from bani.domain.project import ErrorHandlingStrategy, ProjectModel, ProjectOptions
from bani.domain.schema import ForeignKeyDefinition, TableDefinition

if TYPE_CHECKING:
    from bani.domain.schema import DatabaseSchema


@dataclass(frozen=True)
class MigrationResult:
    """Result summary of a migration execution."""

    project_name: str
    tables_completed: int
    tables_failed: int
    total_rows_read: int
    total_rows_written: int
    duration_seconds: float
    errors: tuple[str, ...] = ()


class MigrationOrchestrator:
    """Orchestrates a full database migration from source to target.

    Responsibilities:
    1. Use DependencyResolver to determine safe table ordering
    2. Create target schema (tables, columns, types)
    3. Transfer data in batches, respecting concurrency limits
    4. Create indexes and foreign keys after data transfer
    5. Emit progress events via ProgressTracker
    """

    def __init__(
        self,
        project: ProjectModel,
        source: SourceConnector,
        sink: SinkConnector,
        tracker: ProgressTracker | None = None,
    ) -> None:
        """Initialize the orchestrator.

        Args:
            project: The migration project model.
            source: Source database connector.
            sink: Target database connector.
            tracker: Optional progress tracker for event emission.
        """
        self.project = project
        self.source = source
        self.sink = sink
        self.tracker = tracker or ProgressTracker()
        self.options = project.options or ProjectOptions()

    def execute(self) -> MigrationResult:
        """Execute the full migration from schema creation through data transfer.

        Returns:
            MigrationResult with summary statistics.
        """
        start_time = time.time()
        errors: list[str] = []
        tables_completed = 0
        tables_failed = 0
        total_rows_read = 0
        total_rows_written = 0

        try:
            # Introspect source schema
            source_schema = self.source.introspect_schema()

            # Resolve table dependencies
            resolver = DependencyResolver()
            resolution = resolver.resolve(source_schema)
            ordered_tables = resolution.ordered_tables
            deferred_fks = resolution.deferred_fks

            # Emit migration started
            self.tracker.migration_started(
                project_name=self.project.name,
                source_dialect=source_schema.source_dialect,
                target_dialect=self.project.target.dialect
                if self.project.target
                else "unknown",
                table_count=len(ordered_tables),
            )

            # Create target schema if requested
            if self.options.create_target_schema:
                self._create_target_schema(source_schema)

            # Transfer table data in parallel
            transfer_results = self._transfer_tables_parallel(
                source_schema, ordered_tables
            )

            # Aggregate results
            for result in transfer_results:
                if result.success:
                    tables_completed += 1
                    total_rows_read += result.rows_read
                    total_rows_written += result.rows_written
                else:
                    tables_failed += 1
                    if result.error:
                        errors.append(result.error)

            # Create indexes and foreign keys after data transfer
            if self.options.transfer_indexes or self.options.transfer_foreign_keys:
                self._create_indexes_and_fks(source_schema, deferred_fks)

        except BaniError as e:
            errors.append(str(e))
            if self.options.on_error == ErrorHandlingStrategy.ABORT:
                raise

        duration = time.time() - start_time

        # Emit migration complete
        self.tracker.migration_complete(
            project_name=self.project.name,
            tables_completed=tables_completed,
            tables_failed=tables_failed,
            total_rows_read=total_rows_read,
            total_rows_written=total_rows_written,
            duration_seconds=duration,
        )

        return MigrationResult(
            project_name=self.project.name,
            tables_completed=tables_completed,
            tables_failed=tables_failed,
            total_rows_read=total_rows_read,
            total_rows_written=total_rows_written,
            duration_seconds=duration,
            errors=tuple(errors),
        )

    def _create_target_schema(self, source_schema: DatabaseSchema) -> None:
        """Create tables in the target database.

        Args:
            source_schema: The introspected source schema.
        """
        for table in source_schema.tables:
            # Drop if requested
            if self.options.drop_target_tables_first:
                try:
                    self.sink.execute_sql(
                        f"DROP TABLE IF EXISTS {table.fully_qualified_name}"
                    )
                except Exception:
                    pass  # Ignore errors if table doesn't exist

            # Create new table
            try:
                self.sink.create_table(table)
            except Exception:
                if self.options.on_error == ErrorHandlingStrategy.ABORT:
                    raise
                # Log and continue on LOG_AND_CONTINUE

    def _transfer_tables_parallel(
        self, source_schema: DatabaseSchema, ordered_tables: tuple[str, ...]
    ) -> list[_TableTransferResult]:
        """Transfer tables in parallel with concurrency limit.

        Args:
            source_schema: The source database schema.
            ordered_tables: Tables in dependency-safe order.

        Returns:
            List of transfer results for each table.
        """
        results: list[_TableTransferResult] = []
        max_workers = self.options.parallel_workers

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}

            for table_name in ordered_tables:
                table = source_schema.get_table("", table_name.split(".")[-1])
                if table is None:
                    # Fallback: search by fully qualified name
                    for t in source_schema.tables:
                        if t.fully_qualified_name == table_name:
                            table = t
                            break

                if table is None:
                    results.append(
                        _TableTransferResult(
                            table_name,
                            False,
                            0,
                            0,
                            f"Table not found in schema: {table_name}",
                        )
                    )
                    continue

                future = executor.submit(self._transfer_table, table)
                futures[future] = table_name

            # Collect results as they complete
            for future in as_completed(futures):
                result = future.result()
                results.append(result)

        return results

    def _transfer_table(self, table: TableDefinition) -> _TableTransferResult:
        """Transfer a single table from source to target.

        Args:
            table: The table definition.

        Returns:
            Transfer result for this table.
        """
        table_name = table.fully_qualified_name
        total_rows_read = 0
        total_rows_written = 0
        batch_number = 0

        try:
            self.tracker.table_started(
                table_name, estimated_rows=table.row_count_estimate
            )

            batch_size = self.options.batch_size
            for batch in self.source.read_table(
                table.table_name, table.schema_name, batch_size=batch_size
            ):
                rows_read = len(batch)
                total_rows_read += rows_read

                try:
                    rows_written = self.sink.write_batch(
                        table.table_name, table.schema_name, batch
                    )
                    total_rows_written += rows_written

                    self.tracker.batch_complete(
                        table_name,
                        batch_number,
                        rows_read,
                        rows_written,
                    )
                except Exception as e:
                    if self.options.on_error == ErrorHandlingStrategy.ABORT:
                        raise WriteError(
                            f"Failed to write batch {batch_number} for {table_name}",
                            table=table_name,
                            batch_number=batch_number,
                        ) from e
                    # Continue on log-and-continue

                batch_number += 1

            self.tracker.table_complete(
                table_name,
                total_rows_read,
                total_rows_written,
                batch_number,
            )

            return _TableTransferResult(
                table_name, True, total_rows_read, total_rows_written
            )

        except Exception as e:
            error_msg = f"Table transfer failed: {e!s}"
            return _TableTransferResult(
                table_name, False, total_rows_read, total_rows_written, error_msg
            )

    def _create_indexes_and_fks(
        self,
        source_schema: DatabaseSchema,
        deferred_fks: tuple[ForeignKeyDefinition, ...],
    ) -> None:
        """Create indexes and foreign keys in the target database.

        Args:
            source_schema: The source database schema.
            deferred_fks: Foreign keys that were deferred due to circular deps.
        """
        if self.options.transfer_indexes:
            for table in source_schema.tables:
                if table.indexes:
                    try:
                        self.sink.create_indexes(
                            table.table_name, table.schema_name, table.indexes
                        )
                    except Exception:
                        if self.options.on_error == ErrorHandlingStrategy.ABORT:
                            raise
                        # Continue on log-and-continue

        if self.options.transfer_foreign_keys:
            all_fks: list[ForeignKeyDefinition] = []
            for table in source_schema.tables:
                all_fks.extend(table.foreign_keys)

            if all_fks:
                try:
                    self.sink.create_foreign_keys(tuple(all_fks))
                except Exception:
                    if self.options.on_error == ErrorHandlingStrategy.ABORT:
                        raise
                    # Continue on log-and-continue

            # Also create deferred FKs
            if deferred_fks:
                try:
                    self.sink.create_foreign_keys(deferred_fks)
                except Exception:
                    if self.options.on_error == ErrorHandlingStrategy.ABORT:
                        raise


@dataclass(frozen=True)
class _TableTransferResult:
    """Internal result tracking for a single table transfer."""

    table_name: str
    success: bool
    rows_read: int
    rows_written: int
    error: str | None = None
