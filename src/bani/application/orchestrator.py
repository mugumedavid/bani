"""Migration orchestrator (Section 3, Application Service Layer)."""

from __future__ import annotations

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from bani.connectors.base import SinkConnector, SourceConnector
from bani.domain.dependency import DependencyResolver
from bani.domain.errors import DataTransferError
from bani.domain.pipeline import TableTransferResult, TransferStatus
from bani.domain.project import ErrorHandlingStrategy, ProjectModel, ProjectOptions

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class MigrationResult:
    """Result of a migration run."""

    tables_succeeded: int
    tables_failed: int
    total_rows: int
    total_duration_seconds: float
    table_results: tuple[TableTransferResult, ...]


class MigrationOrchestrator:
    """Orchestrates full-table-copy migrations."""

    def __init__(
        self,
        project: ProjectModel,
        source: SourceConnector,
        sink: SinkConnector,
        on_progress: Callable[[Any], None] | None = None,
    ) -> None:
        """Initialize the orchestrator.

        Args:
            project: The migration project model.
            source: The source connector instance.
            sink: The target sink connector instance.
            on_progress: Optional callback for progress updates.
        """
        self._project = project
        self._source = source
        self._sink = sink
        self._on_progress = on_progress

    def run(self) -> MigrationResult:
        """Execute the migration and return results.

        Returns:
            A MigrationResult with summary statistics and per-table results.

        Raises:
            DataTransferError: If error handling strategy is ABORT and a table fails.
        """
        start = time.monotonic()
        options = self._project.options or ProjectOptions()

        # Introspect source schema
        schema = self._source.introspect_schema()

        # Resolve dependencies
        resolver = DependencyResolver()
        resolution = resolver.resolve(schema)

        # Filter to mapped tables if any
        mapped_tables: set[str]
        if self._project.table_mappings:
            mapped_tables = {
                f"{m.source_schema}.{m.source_table}"
                for m in self._project.table_mappings
            }
        else:
            mapped_tables = set(resolution.ordered_tables)

        ordered = [t for t in resolution.ordered_tables if t in mapped_tables]

        # Create target schema if needed
        if options.create_target_schema:
            for table_name in ordered:
                parts = table_name.split(".", 1)
                table_def = None
                if len(parts) == 2:
                    table_def = schema.get_table(parts[0], parts[1])
                if table_def:
                    if options.drop_target_tables_first:
                        self._sink.drop_table(table_name)
                    self._sink.create_table(table_def)

        # Transfer data
        results: list[TableTransferResult] = []
        total_rows = 0

        def transfer_table(table_name: str) -> TableTransferResult:
            nonlocal total_rows
            parts = table_name.split(".", 1)
            table_def = None
            if len(parts) == 2:
                table_def = schema.get_table(parts[0], parts[1])
            if not table_def:
                return TableTransferResult(
                    table_name=table_name,
                    status=TransferStatus.SKIPPED,
                )
            rows_read = 0
            rows_written = 0
            batch_count = 0
            try:
                batch_size = options.batch_size
                for batch in self._source.read_batches(
                    table_def, batch_size=batch_size
                ):
                    written = self._sink.write_batch(table_name, batch)
                    rows_read += batch.num_rows
                    rows_written += written
                    batch_count += 1
                total_rows += rows_written
                return TableTransferResult(
                    table_name=table_name,
                    status=TransferStatus.COMPLETED,
                    total_rows_read=rows_read,
                    total_rows_written=rows_written,
                    batch_count=batch_count,
                )
            except Exception as exc:
                if options.on_error == ErrorHandlingStrategy.ABORT:
                    raise DataTransferError(str(exc)) from exc
                return TableTransferResult(
                    table_name=table_name,
                    status=TransferStatus.FAILED,
                    total_rows_read=rows_read,
                    total_rows_written=rows_written,
                    batch_count=batch_count,
                    error_message=str(exc),
                )

        if options.parallel_workers > 1 and len(ordered) > 1:
            with ThreadPoolExecutor(max_workers=options.parallel_workers) as pool:
                futures = {pool.submit(transfer_table, t): t for t in ordered}
                for future in futures:
                    results.append(future.result())
        else:
            for table_name in ordered:
                results.append(transfer_table(table_name))

        # Create indexes and FKs after transfer
        if options.transfer_indexes:
            for table_name in ordered:
                parts = table_name.split(".", 1)
                table_def = None
                if len(parts) == 2:
                    table_def = schema.get_table(parts[0], parts[1])
                if table_def:
                    for idx in table_def.indexes:
                        self._sink.create_index(table_name, idx)

        if options.transfer_foreign_keys:
            for table_name in ordered:
                parts = table_name.split(".", 1)
                table_def = None
                if len(parts) == 2:
                    table_def = schema.get_table(parts[0], parts[1])
                if table_def:
                    for fk in table_def.foreign_keys:
                        self._sink.create_foreign_key(fk)

        elapsed = time.monotonic() - start
        succeeded = sum(1 for r in results if r.status == TransferStatus.COMPLETED)
        failed = sum(1 for r in results if r.status == TransferStatus.FAILED)

        return MigrationResult(
            tables_succeeded=succeeded,
            tables_failed=failed,
            total_rows=total_rows,
            total_duration_seconds=elapsed,
            table_results=tuple(results),
        )
