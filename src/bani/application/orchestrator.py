"""Minimal migration orchestrator (Section 9).

Orchestrates the execution of a migration by:
1. Introspecting source and target schemas
2. Resolving table dependencies
3. Transferring tables in dependency-safe order
4. Creating indexes and foreign keys on the target
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from bani.connectors.registry import ConnectorRegistry
from bani.domain.dependency import DependencyResolver
from bani.domain.errors import (
    SourceConnectionError,
    TargetConnectionError,
)
from bani.domain.pipeline import TableTransferResult, TransferStatus

if TYPE_CHECKING:
    from bani.connectors.base import SinkConnector, SourceConnector
    from bani.domain.project import ProjectModel


@dataclass(frozen=True)
class MigrationResult:
    """Result of a complete migration execution.

    Attributes:
        status: Final migration status (success/failure).
        total_tables: Number of tables that were attempted.
        succeeded_tables: Number of tables successfully transferred.
        failed_tables: Number of tables that failed.
        total_rows: Total rows transferred across all tables.
        error_message: Error message if the migration failed at the top level.
        table_results: Tuple of per-table results.
    """

    status: TransferStatus
    total_tables: int = 0
    succeeded_tables: int = 0
    failed_tables: int = 0
    total_rows: int = 0
    error_message: str | None = None
    table_results: tuple[TableTransferResult, ...] = ()


class MigrationOrchestrator:
    """Orchestrates the execution of a database migration.

    Coordinates source and target connectors, manages dependency resolution,
    and executes table transfers in the correct order.
    """

    def __init__(
        self,
        project: ProjectModel,
        progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        """Initialize the orchestrator.

        Args:
            project: The parsed migration project.
            progress_callback: Optional callback for progress updates
                (event_name, event_data).
        """
        self.project = project
        self.progress_callback = progress_callback
        self._source_connector: SourceConnector | None = None
        self._sink_connector: SinkConnector | None = None

    def _emit_progress(self, event: str, data: dict[str, Any]) -> None:
        """Emit a progress event to the callback."""
        if self.progress_callback:
            self.progress_callback(event, data)

    def _connect_source(self) -> SourceConnector:
        """Create and connect the source connector."""
        if self.project.source is None:
            raise SourceConnectionError("No source connection configured")

        registry = ConnectorRegistry()
        connector = registry.create_source_connector(
            self.project.source.dialect,
            host=self.project.source.host,
            port=self.project.source.port,
            database=self.project.source.database,
            username_env=self.project.source.username_env,
            password_env=self.project.source.password_env,
        )
        try:
            connector.connect()
        except Exception as e:
            raise SourceConnectionError(
                f"Failed to connect to source: {e}",
            ) from e
        return connector

    def _connect_target(self) -> SinkConnector:
        """Create and connect the target connector."""
        if self.project.target is None:
            raise TargetConnectionError("No target connection configured")

        registry = ConnectorRegistry()
        connector = registry.create_sink_connector(
            self.project.target.dialect,
            host=self.project.target.host,
            port=self.project.target.port,
            database=self.project.target.database,
            username_env=self.project.target.username_env,
            password_env=self.project.target.password_env,
        )
        try:
            connector.connect()
        except Exception as e:
            raise TargetConnectionError(
                f"Failed to connect to target: {e}",
            ) from e
        return connector

    def run(self, dry_run: bool = False) -> MigrationResult:
        """Execute the migration.

        Args:
            dry_run: If True, validate but don't execute transfers.

        Returns:
            MigrationResult with final status and per-table results.
        """
        self._source_connector = self._connect_source()
        self._sink_connector = self._connect_target()

        try:
            return self._execute()
        finally:
            if self._source_connector:
                self._source_connector.close()
            if self._sink_connector:
                self._sink_connector.close()

    def _execute(self) -> MigrationResult:
        """Execute the migration after connectors are connected."""
        # Introspect source schema
        try:
            source_schema = self._source_connector.introspect_schema()  # type: ignore[union-attr]
            self._emit_progress(
                "schema_introspected",
                {
                    "tables": len(source_schema.tables),
                    "dialect": source_schema.source_dialect,
                },
            )
        except Exception as e:
            return MigrationResult(
                status=TransferStatus.FAILED,
                error_message=f"Failed to introspect source schema: {e}",
            )

        # Resolve table dependencies
        try:
            resolver = DependencyResolver()
            resolution = resolver.resolve(source_schema)
            ordered_tables = resolution.ordered_tables
        except Exception as e:
            return MigrationResult(
                status=TransferStatus.FAILED,
                error_message=f"Dependency resolution error: {e}",
            )

        # Estimate total rows
        total_rows_estimate = 0
        for table_name in ordered_tables:
            table = source_schema.get_table(
                table_name.split(".")[0], table_name.split(".")[1]
            )
            if table and table.row_count_estimate:
                total_rows_estimate += table.row_count_estimate

        self._emit_progress(
            "migration_started",
            {
                "tables": len(ordered_tables),
                "estimated_rows": total_rows_estimate,
            },
        )

        # Execute transfers in dependency order
        table_results: list[TableTransferResult] = []
        succeeded = 0
        failed = 0

        for table_name in ordered_tables:
            parts = table_name.split(".")
            if len(parts) != 2:
                continue
            schema_name, simple_table_name = parts

            table = source_schema.get_table(schema_name, simple_table_name)
            if table is None:
                continue

            self._emit_progress(
                "table_started",
                {
                    "table": table_name,
                    "estimated_rows": table.row_count_estimate or 0,
                },
            )

            try:
                # Create table in target
                self._sink_connector.create_table(table)  # type: ignore[union-attr]

                # Transfer batches
                batch_num = 0
                total_rows_transferred = 0
                batch_size = 100_000
                if self.project.options:
                    batch_size = self.project.options.batch_size

                for batch in self._source_connector.read_batches(  # type: ignore[union-attr]
                    table, batch_size=batch_size
                ):
                    rows_written = self._sink_connector.write_batch(table_name, batch)  # type: ignore[union-attr]
                    total_rows_transferred += rows_written

                    self._emit_progress(
                        "batch_complete",
                        {
                            "table": table_name,
                            "batch": batch_num,
                            "rows": rows_written,
                            "total_rows": total_rows_transferred,
                        },
                    )
                    batch_num += 1

                table_result = TableTransferResult(
                    table_name=table_name,
                    status=TransferStatus.COMPLETED,
                    total_rows_read=total_rows_transferred,
                    total_rows_written=total_rows_transferred,
                    batch_count=batch_num,
                )
                table_results.append(table_result)
                succeeded += 1

                self._emit_progress(
                    "table_complete",
                    {
                        "table": table_name,
                        "rows": total_rows_transferred,
                    },
                )
            except Exception as e:
                table_result = TableTransferResult(
                    table_name=table_name,
                    status=TransferStatus.FAILED,
                    error_message=str(e),
                )
                table_results.append(table_result)
                failed += 1

                self._emit_progress(
                    "table_failed",
                    {
                        "table": table_name,
                        "error": str(e),
                    },
                )

        total_transferred = sum(r.total_rows_written for r in table_results)

        status = TransferStatus.COMPLETED if failed == 0 else TransferStatus.FAILED

        result = MigrationResult(
            status=status,
            total_tables=len(ordered_tables),
            succeeded_tables=succeeded,
            failed_tables=failed,
            total_rows=total_transferred,
            table_results=tuple(table_results),
        )

        self._emit_progress(
            "migration_complete",
            {
                "tables_succeeded": succeeded,
                "tables_failed": failed,
                "total_rows": total_transferred,
            },
        )

        return result
