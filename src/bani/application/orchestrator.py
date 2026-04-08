"""Migration orchestrator that coordinates schema and data transfer."""

from __future__ import annotations

import gc
import logging
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone

import pyarrow as pa  # type: ignore[import-untyped]

from bani.application.active_migration import ActiveMigrationTracker
from bani.application.checkpoint import CheckpointManager
from bani.application.hook_runner import HookRunner
from bani.application.progress import ProgressTracker
from bani.application.quarantine import QuarantineManager
from bani.application.run_log import RunLog, RunLogEntry
from bani.application.schema_remap import SchemaRemapper
from bani.connectors.base import SinkConnector, SourceConnector
from bani.domain.dependency import DependencyResolver
from bani.domain.errors import BaniError, WriteError
from bani.domain.project import ErrorHandlingStrategy, ProjectModel, ProjectOptions
from bani.domain.schema import DatabaseSchema, ForeignKeyDefinition, TableDefinition

logger = logging.getLogger(__name__)

_CHUNK_ROW_THRESHOLD = 50_000
"""Minimum estimated row count before chunk-level parallelism is used."""

_NUMERIC_ARROW_PREFIXES = ("int", "uint")
"""Arrow type string prefixes that indicate a numeric integer type."""


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
    warnings: tuple[str, ...] = ()


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
        checkpoint: CheckpointManager | None = None,
        quarantine: QuarantineManager | None = None,
        projects_dir: str = "~/.bani/projects",
    ) -> None:
        """Initialize the orchestrator.

        Args:
            project: The migration project model.
            source: Source database connector.
            sink: Target database connector.
            tracker: Optional progress tracker for event emission.
            checkpoint: Optional checkpoint manager (created if not provided).
            quarantine: Optional quarantine manager (created if not provided).
            projects_dir: Path to BDL project files (for migrate hooks).
        """
        self.project = project
        self.source = source
        self.sink = sink
        self.tracker = tracker or ProgressTracker()
        self.options = project.options or ProjectOptions()
        self._checkpoint = checkpoint or CheckpointManager()
        self._quarantine = quarantine or QuarantineManager()
        self._cancel_event: threading.Event | None = None
        self._skipped_tables: list[str] = []
        self._hook_runner = HookRunner(
            source_executor=source,
            target_executor=sink,
            projects_dir=projects_dir,
        )

    def set_cancel_event(self, event: threading.Event) -> None:
        """Register a threading.Event that signals cancellation."""
        self._cancel_event = event

    @property
    def _cancelled(self) -> bool:
        return self._cancel_event is not None and self._cancel_event.is_set()

    def _hook_context(self, **extra: str) -> dict[str, str]:
        """Build variable context for hook substitution."""
        ctx: dict[str, str] = {
            "project_name": self.project.name,
            "source_dialect": self.project.source.dialect if self.project.source else "",
            "target_dialect": self.project.target.dialect if self.project.target else "",
        }
        ctx.update(extra)
        return ctx

    def _run_hooks(self, phase: str, **extra: str) -> None:
        """Execute hooks for a lifecycle phase, if any are defined."""
        if not self.project.hooks:
            return
        # Check if any hooks match this phase before emitting
        matching = [h for h in self.project.hooks if h.event == phase]
        if not matching:
            return
        self.tracker.phase_change(f"hooks:{phase}")
        try:
            self._hook_runner.execute_hooks(
                self.project.hooks, phase, context=self._hook_context(**extra)
            )
        except Exception:
            # HookExecutionError with on_failure="abort" propagates;
            # all others are logged and swallowed.
            raise

    def execute(self, resume: bool = False) -> MigrationResult:
        """Execute the full migration from schema creation through data transfer.

        Args:
            resume: If ``True``, attempt to resume from a previous checkpoint.
                Completed tables are skipped; in-progress tables resume from
                their last committed row offset.

        Returns:
            MigrationResult with summary statistics.
        """
        start_time = time.time()
        run_start = datetime.now(timezone.utc).isoformat()
        errors: list[str] = []
        tables_completed = 0
        tables_failed = 0
        total_rows_read = 0
        total_rows_written = 0

        # Mark migration as active so the UI dashboard can see it.
        active_tracker = ActiveMigrationTracker()
        active_tracker.start(
            self.project.name,
            self.project.source.dialect if self.project.source else "",
            self.project.target.dialect if self.project.target else "",
        )

        checkpoint = self._checkpoint
        quarantine = self._quarantine
        project_hash = checkpoint.compute_hash(self.project)

        # Determine whether we can actually resume
        if resume:
            existing = checkpoint.load(self.project.name)
            if existing and checkpoint.is_valid(self.project.name, project_hash):
                logger.info(
                    "Resuming migration for project '%s' from checkpoint",
                    self.project.name,
                )
            else:
                if existing:
                    logger.warning(
                        "Checkpoint for '%s' is invalid (config changed) — "
                        "starting fresh",
                        self.project.name,
                    )
                resume = False

        try:
            # Introspect source schema
            source_schema = self.source.introspect_schema()

            # Filter tables if table_mappings are specified
            if self.project.table_mappings:
                source_schema = self._filter_tables(source_schema)

            # Store original source schema names for read_table calls
            self._source_schema_map: dict[str, str] = {
                t.table_name: t.schema_name for t in source_schema.tables
            }

            # Apply cross-dialect schema remapping if needed
            if (
                self.project.source
                and self.project.target
                and self.project.source.dialect != self.project.target.dialect
            ):
                source_schema = SchemaRemapper.remap_schema(
                    source_schema,
                    self.project.source.dialect,
                    self.project.target.dialect,
                )

            # Resolve table dependencies
            resolver = DependencyResolver()
            resolution = resolver.resolve(source_schema)
            ordered_tables = resolution.ordered_tables
            deferred_fks = resolution.deferred_fks

            # Emit introspection results with post-remap names
            # so they match the table names used in transfer events
            self.tracker.introspection_complete(
                tables=tuple(
                    (t.fully_qualified_name, t.row_count_estimate)
                    for t in source_schema.tables
                ),
                source_dialect=source_schema.source_dialect,
            )

            # Create or validate checkpoint
            if not resume:
                checkpoint.create(self.project.name, project_hash, ordered_tables)

            # Emit migration started
            self.tracker.migration_started(
                project_name=self.project.name,
                source_dialect=source_schema.source_dialect,
                target_dialect=self.project.target.dialect
                if self.project.target
                else "unknown",
                table_count=len(ordered_tables),
            )

            # Execute before-migration hooks
            self._run_hooks(
                "before-migration",
                table_count=str(len(ordered_tables)),
            )

            # Create target schema
            schema_failures: dict[str, str] = {}
            if self.options.create_target_schema:
                if resume:
                    # On resume: only recreate tables that were NOT completed.
                    # This wipes partial data from interrupted tables.
                    completed = {
                        t for t in ordered_tables
                        if checkpoint.is_table_completed(
                            self.project.name, t
                        )
                    }
                    incomplete_tables = [
                        t for t in source_schema.tables
                        if t.fully_qualified_name not in completed
                    ]
                    incomplete_schema = DatabaseSchema(
                        tables=tuple(incomplete_tables),
                        source_dialect=source_schema.source_dialect,
                    )
                    schema_failures = self._create_target_schema(
                        incomplete_schema
                    )
                else:
                    schema_failures = self._create_target_schema(
                        source_schema
                    )

            # Transfer table data in parallel (skip tables that failed creation)
            transfer_results = self._transfer_tables_parallel(
                source_schema, ordered_tables, resume=resume,
                skip_tables=schema_failures,
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
            if (
                not self._cancelled
                and (self.options.transfer_indexes
                     or self.options.transfer_foreign_keys)
            ):
                self.tracker.phase_change("indexes")
                self._create_indexes_and_fks(source_schema, deferred_fks)

            if self._cancelled:
                errors.append("Migration cancelled by user")

            # Execute after-migration hooks
            if not self._cancelled:
                self._run_hooks(
                    "after-migration",
                    table_count=str(len(ordered_tables)),
                    tables_completed=str(tables_completed),
                    tables_failed=str(tables_failed),
                    total_rows=str(total_rows_written),
                )

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

        # Clear checkpoint on full success (no use keeping it)
        if tables_failed == 0:
            try:
                checkpoint.clear(self.project.name)
            except Exception:
                logger.debug("Failed to clear checkpoint", exc_info=True)

        # Append to run history log
        try:
            run_log = RunLog()
            run_log.append(
                RunLogEntry(
                    project_name=self.project.name,
                    started_at=run_start,
                    finished_at=datetime.now(timezone.utc).isoformat(),
                    status="completed" if tables_failed == 0 else "failed",
                    tables_completed=tables_completed,
                    tables_failed=tables_failed,
                    total_rows=total_rows_written,
                    duration_seconds=duration,
                    error="; ".join(errors) if errors else None,
                )
            )
        except Exception:
            logger.debug("Failed to write run log entry", exc_info=True)

        # Clear active migration marker.  Stale markers from crashed
        # processes are cleaned up by ActiveMigrationTracker.list_active()
        # via PID liveness checks.
        active_tracker.finish(self.project.name)

        # Collect warnings
        warnings: list[str] = []
        if self._skipped_tables:
            warnings.append(
                f"Skipped {len(self._skipped_tables)} table(s) with no columns:"
            )
            for t in self._skipped_tables:
                warnings.append(f"  {t}")

        name_map = getattr(self.sink, "_name_map", {})
        if name_map:
            warnings.append(
                f"Renamed {len(name_map)} identifier(s) to fit "
                f"target database limits:"
            )
            for original, shortened in sorted(name_map.items()):
                warnings.append(f"  {original} → {shortened}")

        return MigrationResult(
            project_name=self.project.name,
            tables_completed=tables_completed,
            tables_failed=tables_failed,
            total_rows_read=total_rows_read,
            total_rows_written=total_rows_written,
            duration_seconds=duration,
            errors=tuple(errors),
            warnings=tuple(warnings),
        )

    def _create_target_schema(
        self, source_schema: DatabaseSchema
    ) -> dict[str, str]:
        """Create tables in the target database.

        Args:
            source_schema: The introspected source schema.

        Returns:
            Dict mapping failed table FQN to a brief error reason.
        """
        failed: dict[str, str] = {}

        for table in source_schema.tables:
            # Skip tables with no columns (e.g. empty types or stubs)
            if not table.columns:
                logger.info(
                    "Skipping table %s (no columns)",
                    table.fully_qualified_name,
                )
                self._skipped_tables.append(table.fully_qualified_name)
                continue

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
            except Exception as exc:
                reason = str(exc).split("\n")[0][:200]
                failed[table.fully_qualified_name] = reason
                logger.warning(
                    "Failed to create table %s: %s",
                    table.fully_qualified_name,
                    reason,
                )
                self.tracker.table_create_failed(
                    table.fully_qualified_name, reason
                )

        return failed

    def _transfer_tables_parallel(
        self,
        source_schema: DatabaseSchema,
        ordered_tables: tuple[str, ...],
        resume: bool = False,
        skip_tables: dict[str, str] | None = None,
    ) -> list[_TableTransferResult]:
        """Transfer tables in parallel with concurrency limit.

        Args:
            source_schema: The source database schema.
            ordered_tables: Tables in dependency-safe order.
            resume: If ``True``, skip tables that are already completed
                in the checkpoint.
            skip_tables: Dict of table FQN → reason for tables that
                should be skipped (e.g. failed schema creation).

        Returns:
            List of transfer results for each table.
        """
        results: list[_TableTransferResult] = []
        max_workers = self.options.parallel_workers
        checkpoint = self._checkpoint
        project_name = self.project.name
        _skip = skip_tables or {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}

            for table_name in ordered_tables:
                # Skip tables whose schema creation failed
                if table_name in _skip:
                    reason = _skip[table_name]
                    logger.info(
                        "Skipping table '%s' — schema creation failed: %s",
                        table_name,
                        reason,
                    )
                    results.append(
                        _TableTransferResult(
                            table_name, False, 0, 0,
                            f"Schema creation failed: {reason}",
                        )
                    )
                    continue

                # Skip completed tables when resuming
                if resume and checkpoint.is_table_completed(project_name, table_name):
                    logger.info(
                        "Skipping completed table '%s' (resume mode)",
                        table_name,
                    )
                    results.append(_TableTransferResult(table_name, True, 0, 0))
                    continue

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

                # Check for cancellation before submitting more tables
                if self._cancelled:
                    break

                # Mark as in_progress in checkpoint
                checkpoint.update_table_status(project_name, table_name, "in_progress")

                future = executor.submit(self._transfer_table, table)
                futures[future] = table_name

            # Collect results as they complete
            for future in as_completed(futures):
                tbl_name = futures[future]
                result = future.result()
                results.append(result)

                # Update checkpoint with result
                if result.success:
                    checkpoint.update_table_status(
                        project_name,
                        tbl_name,
                        "completed",
                        rows=result.rows_written,
                    )
                else:
                    checkpoint.update_table_status(
                        project_name,
                        tbl_name,
                        "failed",
                        error=result.error,
                    )

        return results

    def _source_schema_for(self, table: TableDefinition) -> str:
        """Return the original source schema name for reading from source DB.

        After schema remapping, table.schema_name is the TARGET schema
        (e.g. 'dbo'). For reading from the source, we need the original
        name (e.g. 'public').
        """
        return getattr(self, '_source_schema_map', {}).get(
            table.table_name, table.schema_name
        )

    def _filter_tables(self, schema: DatabaseSchema) -> DatabaseSchema:
        """Filter introspected schema to only include tables from table_mappings.

        Foreign keys referencing tables outside the filtered set are
        stripped so that the dependency resolver doesn't choke on
        missing nodes.

        Raises:
            BaniError: If any requested table is not found in the source.
        """
        from dataclasses import replace as dc_replace

        # Build lookups: FQN → table, and name → list of tables (for ambiguity check)
        by_fqn: dict[str, TableDefinition] = {}
        by_name: dict[str, list[TableDefinition]] = {}
        for t in schema.tables:
            by_fqn[f"{t.schema_name}.{t.table_name}"] = t
            by_name.setdefault(t.table_name, []).append(t)

        selected: list[TableDefinition] = []
        missing: list[str] = []
        ambiguous: list[str] = []

        for mapping in self.project.table_mappings:
            if mapping.source_schema:
                fqn = f"{mapping.source_schema}.{mapping.source_table}"
                table = by_fqn.get(fqn)
            else:
                # Name-only lookup — check for ambiguity
                matches = by_name.get(mapping.source_table, [])
                if len(matches) > 1:
                    schemas = [t.schema_name for t in matches]
                    ambiguous.append(
                        f"{mapping.source_table} (exists in: "
                        f"{', '.join(schemas)})"
                    )
                    continue
                table = matches[0] if matches else None

            if table is None:
                missing.append(
                    f"{mapping.source_schema}.{mapping.source_table}"
                    if mapping.source_schema
                    else mapping.source_table
                )
            elif table not in selected:
                selected.append(table)

        if ambiguous:
            raise BaniError(
                "Ambiguous table names — specify the schema: "
                + "; ".join(ambiguous)
            )

        if missing:
            raise BaniError(
                f"Tables not found in source: {', '.join(missing)}"
            )

        # Strip FKs that reference tables not in the selected set
        selected_names = {t.table_name for t in selected}
        cleaned: list[TableDefinition] = []
        for t in selected:
            kept_fks = tuple(
                fk for fk in t.foreign_keys
                if fk.referenced_table.split(".")[-1] in selected_names
            )
            if kept_fks != t.foreign_keys:
                t = dc_replace(t, foreign_keys=kept_fks)
            cleaned.append(t)

        logger.info(
            "Table filter: %d of %d tables selected",
            len(cleaned),
            len(schema.tables),
        )
        return DatabaseSchema(
            tables=tuple(cleaned),
            source_dialect=schema.source_dialect,
        )

    def _should_chunk(self, table: TableDefinition) -> bool:
        """Determine whether a table qualifies for chunk-level parallelism.

        A table qualifies when it has a single-column integer primary key and
        an estimated row count above ``_CHUNK_ROW_THRESHOLD``.

        Args:
            table: The table definition to evaluate.

        Returns:
            True if the table should be transferred using range-based chunks.
        """
        if len(table.primary_key) != 1:
            return False
        if (
            table.row_count_estimate is None
            or table.row_count_estimate < _CHUNK_ROW_THRESHOLD
        ):
            return False
        pk_col_name = table.primary_key[0]
        for col in table.columns:
            if col.name == pk_col_name:
                arrow_type = col.arrow_type_str or ""
                return arrow_type.startswith(_NUMERIC_ARROW_PREFIXES)
        return False

    def _get_pk_range(
        self, table: TableDefinition, pk_col: str
    ) -> tuple[int, int] | None:
        """Get min and max values of the primary key column.

        Uses the source connector's pool to check out a connection and
        execute a ``SELECT MIN/MAX`` query.  Returns ``None`` on any
        failure so the caller can fall back to non-chunked transfer.

        Args:
            table: The table definition.
            pk_col: Name of the primary key column.

        Returns:
            ``(min_val, max_val)`` tuple, or ``None`` if the range
            cannot be determined.
        """
        try:
            # Standard SQL — works across PostgreSQL, MySQL, MSSQL, Oracle, SQLite.
            # We use the simplest identifier quoting (double quotes) which is
            # ANSI standard and supported by PG, Oracle, SQLite.  MySQL accepts
            # them when ANSI_QUOTES is on; for MySQL we also fall back on failure.
            schema = self._source_schema_for(table)
            tbl = table.table_name
            if schema:
                fqn = f'"{schema}"."{tbl}"'
            else:
                fqn = f'"{tbl}"'
            sql = f'SELECT MIN("{pk_col}"), MAX("{pk_col}") FROM {fqn}'

            with self.source.checkout() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(sql)
                    row = cur.fetchone()
                finally:
                    cur.close()

            if row is None or row[0] is None or row[1] is None:
                return None
            return (int(row[0]), int(row[1]))
        except Exception:
            logger.debug(
                "Failed to get PK range for %s.%s — falling back to sequential",
                table.schema_name,
                table.table_name,
                exc_info=True,
            )
            return None

    def _transfer_table_chunked(self, table: TableDefinition) -> _TableTransferResult:
        """Transfer a large table using range-based chunk parallelism.

        Splits the table into N range-based chunks on the integer primary
        key and transfers each chunk concurrently using a thread pool.
        Each chunk uses ``read_table`` with a ``filter_sql`` parameter
        to restrict the PK range.

        Falls back to ``_transfer_table_sequential`` if range detection
        fails.

        Args:
            table: The table definition.

        Returns:
            Transfer result for this table.
        """
        pk_col = table.primary_key[0]
        pk_range = self._get_pk_range(table, pk_col)
        if pk_range is None:
            return self._transfer_table_sequential(table)

        min_val, max_val = pk_range
        if max_val <= min_val:
            return self._transfer_table_sequential(table)

        num_chunks = min(self.options.parallel_workers, 4)
        chunk_size = (max_val - min_val + 1) // num_chunks
        if chunk_size < 1:
            return self._transfer_table_sequential(table)

        ranges: list[tuple[int, int]] = [
            (min_val + i * chunk_size, min_val + (i + 1) * chunk_size - 1)
            for i in range(num_chunks)
        ]
        # Last chunk absorbs the remainder
        ranges[-1] = (ranges[-1][0], max_val)

        table_name = table.fully_qualified_name
        batch_size = self.options.batch_size

        # Shared mutable counters protected by a lock
        lock = threading.Lock()
        total_rows_read = 0
        total_rows_written = 0
        total_batches = 0
        chunk_errors: list[str] = []

        logger.debug(
            "Chunked transfer for %s: %d chunks on PK '%s' [%d..%d]",
            table_name,
            num_chunks,
            pk_col,
            min_val,
            max_val,
        )

        # IDENTITY INSERT handling for MSSQL targets
        identity_insert = self._needs_identity_insert(table)

        try:
            self.tracker.table_started(
                table_name, estimated_rows=table.row_count_estimate
            )
        except Exception:
            pass  # Progress tracking is best-effort

        if identity_insert:
            self._set_identity_insert(table, on=True)

        def transfer_chunk(start: int, end: int) -> tuple[int, int, int]:
            """Transfer one PK range chunk.

            Returns:
                Tuple of (rows_read, rows_written, batches).
            """
            chunk_read = 0
            chunk_written = 0
            chunk_batches = 0
            filter_sql = f'"{pk_col}" >= {start} AND "{pk_col}" <= {end}'

            for batch in self.source.read_table(
                table.table_name,
                self._source_schema_for(table),
                filter_sql=filter_sql,
                batch_size=batch_size,
            ):
                rows_in_batch = len(batch)
                chunk_read += rows_in_batch

                try:
                    rows_written = self.sink.write_batch(
                        table.table_name,
                        table.schema_name,
                        batch,
                    )
                    chunk_written += rows_written

                    with lock:
                        nonlocal total_batches
                        batch_num = total_batches
                        total_batches += 1

                    try:
                        self.tracker.batch_complete(
                            table_name, batch_num, rows_in_batch, rows_written
                        )
                    except Exception:
                        pass  # Progress tracking is best-effort

                    chunk_batches += 1
                finally:
                    del batch  # Release Arrow buffer immediately

            return chunk_read, chunk_written, chunk_batches

        try:
            with ThreadPoolExecutor(max_workers=num_chunks) as pool:
                futures = {
                    pool.submit(transfer_chunk, start, end): (start, end)
                    for start, end in ranges
                }

                for future in as_completed(futures):
                    rng = futures[future]
                    try:
                        c_read, c_written, _ = future.result()
                        with lock:
                            total_rows_read += c_read
                            total_rows_written += c_written
                    except Exception as exc:
                        msg = (
                            f"Chunk [{rng[0]}..{rng[1]}] failed for "
                            f"{table_name}: {exc!s}"
                        )
                        chunk_errors.append(msg)
                        if self.options.on_error == ErrorHandlingStrategy.ABORT:
                            raise WriteError(
                                msg,
                                table=table_name,
                                batch_number=0,
                            ) from exc

            if identity_insert:
                self._set_identity_insert(table, on=False)

            if chunk_errors:
                error_msg = "; ".join(chunk_errors)
                return _TableTransferResult(
                    table_name, False, total_rows_read, total_rows_written, error_msg
                )

            self.tracker.table_complete(
                table_name,
                total_rows_read,
                total_rows_written,
                total_batches,
            )

            return _TableTransferResult(
                table_name, True, total_rows_read, total_rows_written
            )

        except Exception as e:
            if identity_insert:
                try:
                    self._set_identity_insert(table, on=False)
                except Exception:
                    pass  # Best-effort cleanup
            error_msg = f"Table transfer failed: {e!s}"
            return _TableTransferResult(
                table_name, False, total_rows_read, total_rows_written, error_msg
            )

    def _transfer_table(self, table: TableDefinition) -> _TableTransferResult:
        """Transfer a single table from source to target.

        Delegates to ``_transfer_table_chunked`` for large tables with a
        single-column integer primary key, otherwise uses the sequential
        producer/consumer path.

        After each table, forces garbage collection and releases unused
        Arrow memory back to the OS to prevent cumulative slowdown.

        Args:
            table: The table definition.

        Returns:
            Transfer result for this table.
        """
        try:
            if self._should_chunk(table):
                result = self._transfer_table_chunked(table)
            else:
                result = self._transfer_table_sequential(table)
        except Exception as exc:
            if self._is_connection_error(exc):
                logger.warning(
                    "Connection broke during transfer of %s, "
                    "reconnecting and retrying...",
                    table.fully_qualified_name,
                )
                self._reconnect_all()
                # Retry once on fresh connections
                try:
                    if self._should_chunk(table):
                        result = self._transfer_table_chunked(table)
                    else:
                        result = self._transfer_table_sequential(table)
                except Exception as retry_exc:
                    result = _TableTransferResult(
                        table.fully_qualified_name,
                        False,
                        0,
                        0,
                        f"Failed after reconnect: {retry_exc!s}",
                    )
            else:
                raise

        # Free Arrow buffers and Python objects between tables to
        # prevent cumulative memory pressure that slows later tables.
        gc.collect()
        pool = pa.default_memory_pool()
        if hasattr(pool, "release_unused"):
            pool.release_unused()

        return result

    @staticmethod
    def _is_connection_error(exc: BaseException) -> bool:
        """Check if an exception indicates a broken connection."""
        msg = str(exc).lower()
        markers = (
            "connection is broken",
            "connection reset",
            "connection refused",
            "communication link",
            "operation timed out",
            "server closed the connection",
            "could not receive data",
            "lost connection",
            "gone away",
            "imc06",
            "08s01",  # ODBC connection error SQLSTATE
            "08003",  # connection does not exist
        )
        return any(m in msg for m in markers)

    def _reconnect_all(self) -> None:
        """Reconnect both source and sink connectors."""
        try:
            self.source.reconnect()
            logger.info("Source connector reconnected")
        except Exception as exc:
            logger.error("Source reconnect failed: %s", exc)
            raise
        try:
            self.sink.reconnect()
            logger.info("Sink connector reconnected")
        except Exception as exc:
            logger.error("Sink reconnect failed: %s", exc)
            raise

    def _transfer_table_sequential(
        self, table: TableDefinition
    ) -> _TableTransferResult:
        """Transfer a single table from source to target sequentially.

        Uses a producer/consumer pattern: a reader thread fills a bounded queue
        while the writer (main thread) drains it. This overlaps source I/O with
        target I/O, roughly doubling throughput by hiding network latency.

        After each successful batch write, the checkpoint is updated with the
        current row offset. On write failure with ``LOG_AND_CONTINUE``, the
        failed batch is sent to the quarantine table.

        Args:
            table: The table definition.

        Returns:
            Transfer result for this table.
        """
        table_name = table.fully_qualified_name
        total_rows_read = 0
        total_rows_written = 0
        batch_number = 0

        checkpoint = self._checkpoint
        quarantine = self._quarantine
        project_name = self.project.name

        # IDENTITY INSERT handling for MSSQL targets
        identity_insert = self._needs_identity_insert(table)

        try:
            self.tracker.table_started(
                table_name, estimated_rows=table.row_count_estimate
            )

            # Execute before-table hooks
            self._run_hooks("before-table", table_name=table_name)

            if identity_insert:
                self._set_identity_insert(table, on=True)

            batch_size = self.options.batch_size
            batch_queue: queue.Queue[object] = queue.Queue(maxsize=2)
            reader_error: list[BaseException | None] = [None]

            def reader_worker() -> None:
                try:
                    for batch in self.source.read_table(
                        table.table_name,
                        self._source_schema_for(table),
                        batch_size=batch_size,
                    ):
                        batch_queue.put(batch)
                except Exception as exc:
                    reader_error[0] = exc
                finally:
                    batch_queue.put(None)  # Sentinel: no more batches

            reader_thread = threading.Thread(target=reader_worker, daemon=True)
            reader_thread.start()

            # Writer runs in the current thread
            while True:
                # Check for cancellation between batches
                if self._cancelled:
                    error_msg = "Migration cancelled by user"
                    return _TableTransferResult(
                        table_name, False, total_rows_read, total_rows_written,
                        error_msg,
                    )

                batch = batch_queue.get()
                if batch is None:
                    break
                if reader_error[0] is not None:
                    raise reader_error[0]

                rows_read = len(batch)  # type: ignore[arg-type]
                total_rows_read += rows_read

                try:
                    rows_written = self.sink.write_batch(
                        table.table_name,
                        table.schema_name,
                        batch,
                    )
                    total_rows_written += rows_written

                    # Update checkpoint with current row offset
                    checkpoint.update_row_offset(
                        project_name, table_name, total_rows_written
                    )

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
                    # LOG_AND_CONTINUE: quarantine the failed batch
                    logger.warning(
                        "Batch %d for %s failed, quarantining: %s",
                        batch_number,
                        table_name,
                        e,
                    )
                    try:
                        quarantine.quarantine_row(
                            self.sink,
                            project_name,
                            table_name,
                            total_rows_read,
                            f"batch {batch_number} ({rows_read} rows)",
                            str(e),
                        )
                    except Exception:
                        pass  # Quarantine is best-effort
                finally:
                    del batch  # Release Arrow buffer immediately

                batch_number += 1

            reader_thread.join()
            if reader_error[0] is not None:
                raise reader_error[0]

            if identity_insert:
                self._set_identity_insert(table, on=False)

            self.tracker.table_complete(
                table_name,
                total_rows_read,
                total_rows_written,
                batch_number,
            )

            # Execute after-table hooks
            self._run_hooks(
                "after-table",
                table_name=table_name,
                rows_written=str(total_rows_written),
            )

            return _TableTransferResult(
                table_name, True, total_rows_read, total_rows_written
            )

        except Exception as e:
            if identity_insert:
                try:
                    self._set_identity_insert(table, on=False)
                except Exception:
                    pass  # Best-effort cleanup
            error_msg = f"Table transfer failed: {e!s}"
            return _TableTransferResult(
                table_name, False, total_rows_read, total_rows_written, error_msg
            )

    def _needs_identity_insert(self, table: TableDefinition) -> bool:
        """Check whether IDENTITY_INSERT is needed for this table.

        Returns ``True`` when the target is MSSQL and the table has at
        least one auto-increment column.

        Args:
            table: The table definition.

        Returns:
            Whether IDENTITY_INSERT should be toggled.
        """
        if not (self.project.target and self.project.target.dialect == "mssql"):
            return False
        return any(c.is_auto_increment for c in table.columns)

    def _set_identity_insert(self, table: TableDefinition, *, on: bool) -> None:
        """Toggle IDENTITY_INSERT for a table on the MSSQL sink.

        Args:
            table: The table definition.
            on: ``True`` to enable, ``False`` to disable.
        """
        state = "ON" if on else "OFF"
        sql = f"SET IDENTITY_INSERT [{table.schema_name}].[{table.table_name}] {state}"
        try:
            self.sink.execute_sql(sql)
        except Exception:
            logger.debug(
                "IDENTITY_INSERT %s failed for %s (may not be needed)",
                state,
                table.fully_qualified_name,
                exc_info=True,
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
