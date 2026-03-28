"""Incremental sync engine (Section 13).

Implements three sync strategies (timestamp, rowversion, checksum) and manages
sync state in a ``_bani_sync_state`` table in the target database.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable

import pyarrow as pa  # type: ignore[import-untyped]

from bani.connectors.base import SinkConnector, SourceConnector
from bani.domain.errors import ConfigurationError
from bani.domain.project import SyncConfig, SyncStrategy

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sync result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SyncResult:
    """Result summary of an incremental sync operation.

    Attributes:
        table_name: Fully qualified table name that was synced.
        strategy: Strategy used for the sync.
        rows_inserted: Number of rows inserted.
        rows_updated: Number of rows updated.
        rows_deleted: Number of rows deleted.
        is_full_sync: Whether this was a full (first-time) sync.
        duration_seconds: Wall-clock duration in seconds.
    """

    table_name: str
    strategy: str
    rows_inserted: int
    rows_updated: int
    rows_deleted: int
    is_full_sync: bool
    duration_seconds: float


# ---------------------------------------------------------------------------
# Protocol for reading from the target database
# ---------------------------------------------------------------------------


@runtime_checkable
class TargetReader(Protocol):
    """Protocol for reading data from the target database.

    Concrete connectors implement both ``SourceConnector`` and
    ``SinkConnector``, so a single connector instance satisfies this protocol
    and the ``SinkConnector`` ABC simultaneously.
    """

    def read_table(
        self,
        table_name: str,
        schema_name: str,
        columns: list[str] | None = None,
        filter_sql: str | None = None,
        batch_size: int = 100_000,
    ) -> Iterator[pa.RecordBatch]: ...


# ---------------------------------------------------------------------------
# Sync-state manager
# ---------------------------------------------------------------------------

_SYNC_STATE_TABLE = "_bani_sync_state"
_SYNC_STATE_SCHEMA = "public"

_CREATE_STATE_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS _bani_sync_state (
    project_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    last_timestamp VARCHAR(64),
    last_rowversion BIGINT,
    last_sync_at VARCHAR(64),
    PRIMARY KEY (project_name, table_name)
)"""

_UPSERT_STATE_SQL_TEMPLATE = (
    "DELETE FROM _bani_sync_state"
    " WHERE project_name = '{project}' AND table_name = '{table}';"
    " INSERT INTO _bani_sync_state"
    " (project_name, table_name, last_timestamp, last_rowversion, last_sync_at)"
    " VALUES ('{project}', '{table}', {ts}, {rv}, '{sync_at}')"
)

_CLEAR_STATE_SQL_TEMPLATE = (
    "DELETE FROM _bani_sync_state WHERE project_name = '{project}'"
)


@dataclass(frozen=True)
class SyncStateRow:
    """A single row from the ``_bani_sync_state`` table.

    Attributes:
        project_name: BDL project name.
        table_name: Fully qualified table name.
        last_timestamp: Last sync timestamp as ISO string (or ``None``).
        last_rowversion: Last rowversion value (or ``None``).
        last_sync_at: When the last sync completed as ISO string.
    """

    project_name: str
    table_name: str
    last_timestamp: str | None = None
    last_rowversion: int | None = None
    last_sync_at: str | None = None


class SyncStateManager:
    """Manages sync state in the ``_bani_sync_state`` table.

    Uses the sink connector's ``execute_sql()`` for DDL and writes, and a
    ``TargetReader`` (typically the same connector) for reads.

    The state table is created on first access if it does not already exist.
    """

    def __init__(
        self,
        sink: SinkConnector,
        target_reader: TargetReader,
    ) -> None:
        self._sink = sink
        self._reader = target_reader
        self._table_ensured = False

    def _ensure_table(self) -> None:
        """Create the state table if it does not already exist."""
        if self._table_ensured:
            return
        try:
            self._sink.execute_sql(_CREATE_STATE_TABLE_SQL)
        except Exception:
            # Table may already exist; swallow and move on.
            logger.debug("_bani_sync_state table may already exist; ignoring DDL error")
        self._table_ensured = True

    def read_state(
        self, project: str, table: str
    ) -> SyncStateRow | None:
        """Read the sync state for a given project/table pair.

        Args:
            project: BDL project name.
            table: Fully qualified table name.

        Returns:
            A ``SyncStateRow`` if state exists, else ``None``.
        """
        self._ensure_table()

        filter_sql = (
            f"project_name = '{project}' AND table_name = '{table}'"
        )
        try:
            for batch in self._reader.read_table(
                table_name=_SYNC_STATE_TABLE,
                schema_name=_SYNC_STATE_SCHEMA,
                filter_sql=filter_sql,
                batch_size=1,
            ):
                if batch.num_rows == 0:
                    continue
                row = {
                    col: batch.column(col)[0].as_py()
                    for col in batch.schema.names
                }
                return SyncStateRow(
                    project_name=str(row.get("project_name", "")),
                    table_name=str(row.get("table_name", "")),
                    last_timestamp=_optional_str(row.get("last_timestamp")),
                    last_rowversion=_optional_int(row.get("last_rowversion")),
                    last_sync_at=_optional_str(row.get("last_sync_at")),
                )
        except Exception:
            # Table might not exist yet despite _ensure_table succeeding
            # (e.g. connector doesn't support IF NOT EXISTS).
            logger.debug("Could not read sync state; treating as first sync")
        return None

    def update_state(
        self,
        project: str,
        table: str,
        *,
        last_timestamp: str | None = None,
        last_rowversion: int | None = None,
    ) -> None:
        """Write or update sync state for a project/table pair.

        Args:
            project: BDL project name.
            table: Fully qualified table name.
            last_timestamp: ISO-formatted timestamp of the latest synced row.
            last_rowversion: Rowversion value of the latest synced row.
        """
        self._ensure_table()

        sync_at = datetime.now(timezone.utc).isoformat()
        ts_val = f"'{last_timestamp}'" if last_timestamp is not None else "NULL"
        rv_val = str(last_rowversion) if last_rowversion is not None else "NULL"

        sql = _UPSERT_STATE_SQL_TEMPLATE.format(
            project=_escape_single_quotes(project),
            table=_escape_single_quotes(table),
            ts=ts_val,
            rv=rv_val,
            sync_at=sync_at,
        )
        self._sink.execute_sql(sql)

    def clear_state(self, project: str) -> None:
        """Delete all sync state for a project.

        Args:
            project: BDL project name.
        """
        self._ensure_table()
        sql = _CLEAR_STATE_SQL_TEMPLATE.format(
            project=_escape_single_quotes(project),
        )
        self._sink.execute_sql(sql)


# ---------------------------------------------------------------------------
# Incremental sync engine
# ---------------------------------------------------------------------------

# Dialects that support native rowversion-based change detection.
_ROWVERSION_DIALECTS: dict[str, str] = {
    "postgresql": "xmin",
    "mssql": "rowversion",
    "oracle": "ORA_ROWSCN",
}


class IncrementalSyncEngine:
    """Coordinates incremental data sync between source and target.

    Accepts a ``SyncConfig`` (strategy, tracking_columns) and connector
    instances, determines which rows need syncing based on the strategy,
    transfers only changed rows, and updates sync state after success.

    Args:
        sync_config: Configuration describing the sync strategy.
        project_name: BDL project name (used as key in the state table).
        source: Source database connector.
        sink: Target database connector (writes).
        target_reader: Reader for the target database (reads for state and
            checksum comparisons). Typically the same object as *sink*.
        source_dialect: Source database dialect (e.g. ``"postgresql"``).
        batch_size: Rows per batch when reading source data.
    """

    def __init__(
        self,
        sync_config: SyncConfig,
        project_name: str,
        source: SourceConnector,
        sink: SinkConnector,
        target_reader: TargetReader,
        source_dialect: str = "",
        batch_size: int = 100_000,
    ) -> None:
        self._config = sync_config
        self._project = project_name
        self._source = source
        self._sink = sink
        self._target_reader = target_reader
        self._source_dialect = source_dialect
        self._batch_size = batch_size
        self._state_mgr = SyncStateManager(sink, target_reader)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def sync_table(
        self,
        table_name: str,
        schema_name: str,
        primary_key_columns: tuple[str, ...],
    ) -> SyncResult:
        """Sync a single table using the configured strategy.

        Args:
            table_name: Name of the table to sync.
            schema_name: Schema containing the table.
            primary_key_columns: PK columns (needed for upsert / checksum).

        Returns:
            A ``SyncResult`` summarising what was transferred.

        Raises:
            ConfigurationError: If the strategy is invalid for the dialect.
        """
        import time as _time

        fq_name = f"{schema_name}.{table_name}"
        start = _time.monotonic()

        strategy = self._config.strategy
        if strategy == SyncStrategy.FULL:
            result = self._full_sync(table_name, schema_name, fq_name)
        elif strategy == SyncStrategy.TIMESTAMP:
            result = self._timestamp_sync(
                table_name, schema_name, fq_name, primary_key_columns
            )
        elif strategy == SyncStrategy.ROWVERSION:
            result = self._rowversion_sync(
                table_name, schema_name, fq_name, primary_key_columns
            )
        elif strategy == SyncStrategy.CHECKSUM:
            result = self._checksum_sync(
                table_name, schema_name, fq_name, primary_key_columns
            )
        else:
            raise ConfigurationError(
                f"Unknown sync strategy: {strategy}",
                strategy=str(strategy),
            )

        elapsed = _time.monotonic() - start
        return SyncResult(
            table_name=fq_name,
            strategy=strategy.name.lower(),
            rows_inserted=result.inserted,
            rows_updated=result.updated,
            rows_deleted=result.deleted,
            is_full_sync=result.is_full,
            duration_seconds=round(elapsed, 3),
        )

    # ------------------------------------------------------------------
    # Internal: per-strategy implementations
    # ------------------------------------------------------------------

    def _full_sync(
        self,
        table_name: str,
        schema_name: str,
        fq_name: str,
    ) -> _StrategyResult:
        """Full table copy — truncate target and re-insert everything."""
        try:
            self._sink.execute_sql(
                f"DELETE FROM {_quote_ident(schema_name)}.{_quote_ident(table_name)}"
            )
        except Exception:
            logger.debug("DELETE from target failed (table may not have data)")

        inserted = 0
        for batch in self._source.read_table(
            table_name=table_name,
            schema_name=schema_name,
            batch_size=self._batch_size,
        ):
            inserted += self._sink.write_batch(table_name, schema_name, batch)

        self._state_mgr.update_state(self._project, fq_name)
        return _StrategyResult(inserted=inserted, is_full=True)

    def _timestamp_sync(
        self,
        table_name: str,
        schema_name: str,
        fq_name: str,
        primary_key_columns: tuple[str, ...],
    ) -> _StrategyResult:
        """Sync rows where tracking_column > last_sync_timestamp."""
        tracking_col = self._resolve_tracking_column(table_name)
        state = self._state_mgr.read_state(self._project, fq_name)

        is_full = state is None or state.last_timestamp is None
        filter_sql: str | None = None
        if not is_full and state is not None and state.last_timestamp is not None:
            filter_sql = f"{_quote_ident(tracking_col)} > '{state.last_timestamp}'"

        inserted = 0
        updated = 0
        max_ts: str | None = state.last_timestamp if state is not None else None

        for batch in self._source.read_table(
            table_name=table_name,
            schema_name=schema_name,
            filter_sql=filter_sql,
            batch_size=self._batch_size,
        ):
            if batch.num_rows == 0:
                continue

            # Track the maximum timestamp seen in this batch.
            if tracking_col in batch.schema.names:
                col_idx = batch.schema.get_field_index(tracking_col)
                col_data = batch.column(col_idx)
                for i in range(len(col_data)):
                    val = col_data[i].as_py()
                    if val is not None:
                        val_str = val.isoformat() if isinstance(val, datetime) else str(val)
                        if max_ts is None or val_str > max_ts:
                            max_ts = val_str

            if is_full:
                inserted += self._sink.write_batch(table_name, schema_name, batch)
            else:
                # For incremental, we need to handle upserts: delete existing
                # rows matching the PK, then insert.
                if primary_key_columns:
                    count = self._upsert_batch(
                        table_name, schema_name, batch, primary_key_columns
                    )
                    updated += count
                else:
                    inserted += self._sink.write_batch(
                        table_name, schema_name, batch
                    )

        self._state_mgr.update_state(
            self._project, fq_name, last_timestamp=max_ts
        )
        return _StrategyResult(inserted=inserted, updated=updated, is_full=is_full)

    def _rowversion_sync(
        self,
        table_name: str,
        schema_name: str,
        fq_name: str,
        primary_key_columns: tuple[str, ...],
    ) -> _StrategyResult:
        """Sync using database-native rowversion columns."""
        rv_column = _ROWVERSION_DIALECTS.get(self._source_dialect)
        if rv_column is None:
            raise ConfigurationError(
                f"Rowversion strategy is not supported for dialect "
                f"'{self._source_dialect}'. "
                f"Supported dialects: {', '.join(sorted(_ROWVERSION_DIALECTS))}.",
                dialect=self._source_dialect,
            )

        state = self._state_mgr.read_state(self._project, fq_name)
        is_full = state is None or state.last_rowversion is None

        filter_sql: str | None = None
        if not is_full and state is not None and state.last_rowversion is not None:
            # PostgreSQL xmin is compared as integer; MSSQL rowversion is binary
            # but we store as bigint. Oracle ORA_ROWSCN is numeric.
            filter_sql = f"{rv_column} > {state.last_rowversion}"

        inserted = 0
        updated = 0
        max_rv: int | None = state.last_rowversion if state is not None else None

        for batch in self._source.read_table(
            table_name=table_name,
            schema_name=schema_name,
            filter_sql=filter_sql,
            batch_size=self._batch_size,
        ):
            if batch.num_rows == 0:
                continue

            # Track the maximum rowversion seen.
            if rv_column in batch.schema.names:
                col_idx = batch.schema.get_field_index(rv_column)
                col_data = batch.column(col_idx)
                for i in range(len(col_data)):
                    val = col_data[i].as_py()
                    if val is not None:
                        val_int = int(val)
                        if max_rv is None or val_int > max_rv:
                            max_rv = val_int

            if is_full:
                inserted += self._sink.write_batch(table_name, schema_name, batch)
            else:
                if primary_key_columns:
                    count = self._upsert_batch(
                        table_name, schema_name, batch, primary_key_columns
                    )
                    updated += count
                else:
                    inserted += self._sink.write_batch(
                        table_name, schema_name, batch
                    )

        self._state_mgr.update_state(
            self._project, fq_name, last_rowversion=max_rv
        )
        return _StrategyResult(inserted=inserted, updated=updated, is_full=is_full)

    def _checksum_sync(
        self,
        table_name: str,
        schema_name: str,
        fq_name: str,
        primary_key_columns: tuple[str, ...],
    ) -> _StrategyResult:
        """Sync by comparing row checksums between source and target.

        This is the slowest strategy but works universally. It:
        1. Reads all rows from the source, computes a hash per PK.
        2. Reads all rows from the target, computes a hash per PK.
        3. Inserts rows present in source but not target.
        4. Updates rows whose hashes differ.
        5. Deletes rows present in target but not source.
        """
        if not primary_key_columns:
            raise ConfigurationError(
                f"Checksum strategy requires primary key columns for "
                f"table '{schema_name}.{table_name}'.",
                table=f"{schema_name}.{table_name}",
            )

        source_hashes = self._compute_table_hashes(
            self._source, table_name, schema_name, primary_key_columns
        )
        target_hashes = self._compute_table_hashes(
            self._target_reader, table_name, schema_name, primary_key_columns
        )

        source_keys = set(source_hashes.keys())
        target_keys = set(target_hashes.keys())

        to_insert = source_keys - target_keys
        to_delete = target_keys - source_keys
        to_check = source_keys & target_keys
        to_update = {k for k in to_check if source_hashes[k] != target_hashes[k]}

        inserted = 0
        updated = 0
        deleted = 0

        # Delete rows no longer in source.
        for pk_tuple in to_delete:
            where = _pk_where_clause(primary_key_columns, pk_tuple)
            self._sink.execute_sql(
                f"DELETE FROM {_quote_ident(schema_name)}.{_quote_ident(table_name)}"
                f" WHERE {where}"
            )
            deleted += 1

        # Insert and update: re-read source rows and filter.
        keys_needed = to_insert | to_update
        if keys_needed:
            for batch in self._source.read_table(
                table_name=table_name,
                schema_name=schema_name,
                batch_size=self._batch_size,
            ):
                filtered = _filter_batch_by_pk(
                    batch, primary_key_columns, keys_needed
                )
                if filtered is None or filtered.num_rows == 0:
                    continue

                # For rows that need updating, delete first.
                for row_idx in range(filtered.num_rows):
                    pk_val = _extract_pk_tuple(filtered, primary_key_columns, row_idx)
                    if pk_val in to_update:
                        where = _pk_where_clause(primary_key_columns, pk_val)
                        self._sink.execute_sql(
                            f"DELETE FROM {_quote_ident(schema_name)}"
                            f".{_quote_ident(table_name)} WHERE {where}"
                        )

                written = self._sink.write_batch(
                    table_name, schema_name, filtered
                )
                # Count inserts vs updates.
                for row_idx in range(filtered.num_rows):
                    pk_val = _extract_pk_tuple(filtered, primary_key_columns, row_idx)
                    if pk_val in to_insert:
                        inserted += 1
                    elif pk_val in to_update:
                        updated += 1

        self._state_mgr.update_state(self._project, fq_name)
        return _StrategyResult(
            inserted=inserted, updated=updated, deleted=deleted, is_full=False
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_tracking_column(self, table_name: str) -> str:
        """Find the tracking column for a table from SyncConfig."""
        for tbl, col in self._config.tracking_columns:
            if tbl == table_name or tbl == "*":
                return col
        raise ConfigurationError(
            f"No tracking column configured for table '{table_name}'. "
            f"Add a (table, column) entry to SyncConfig.tracking_columns.",
            table=table_name,
        )

    def _upsert_batch(
        self,
        table_name: str,
        schema_name: str,
        batch: pa.RecordBatch,
        primary_key_columns: tuple[str, ...],
    ) -> int:
        """Delete-then-insert upsert for a batch of rows.

        Returns the number of rows upserted.
        """
        # Delete existing rows matching the PKs in this batch.
        for row_idx in range(batch.num_rows):
            pk_val = _extract_pk_tuple(batch, primary_key_columns, row_idx)
            where = _pk_where_clause(primary_key_columns, pk_val)
            self._sink.execute_sql(
                f"DELETE FROM {_quote_ident(schema_name)}.{_quote_ident(table_name)}"
                f" WHERE {where}"
            )

        return self._sink.write_batch(table_name, schema_name, batch)

    @staticmethod
    def _compute_table_hashes(
        reader: SourceConnector | TargetReader,
        table_name: str,
        schema_name: str,
        primary_key_columns: tuple[str, ...],
    ) -> dict[tuple[object, ...], str]:
        """Compute a SHA-256 hash for each row, keyed by PK tuple."""
        hashes: dict[tuple[object, ...], str] = {}
        for batch in reader.read_table(
            table_name=table_name,
            schema_name=schema_name,
        ):
            col_names = batch.schema.names
            for row_idx in range(batch.num_rows):
                pk_val = _extract_pk_tuple(batch, primary_key_columns, row_idx)
                row_data = "|".join(
                    str(batch.column(c)[row_idx].as_py()) for c in col_names
                )
                hashes[pk_val] = hashlib.sha256(
                    row_data.encode("utf-8")
                ).hexdigest()
        return hashes


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


@dataclass
class _StrategyResult:
    """Internal result before timing is applied."""

    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    is_full: bool = False


def _quote_ident(identifier: str) -> str:
    """Double-quote a SQL identifier to prevent injection."""
    return f'"{identifier}"'


def _escape_single_quotes(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")


def _pk_where_clause(
    pk_columns: tuple[str, ...],
    pk_values: tuple[object, ...],
) -> str:
    """Build a WHERE clause for a composite primary key."""
    parts: list[str] = []
    for col, val in zip(pk_columns, pk_values):
        if val is None:
            parts.append(f"{_quote_ident(col)} IS NULL")
        elif isinstance(val, (int, float)):
            parts.append(f"{_quote_ident(col)} = {val}")
        else:
            parts.append(
                f"{_quote_ident(col)} = '{_escape_single_quotes(str(val))}'"
            )
    return " AND ".join(parts)


def _extract_pk_tuple(
    batch: pa.RecordBatch,
    pk_columns: tuple[str, ...],
    row_idx: int,
) -> tuple[object, ...]:
    """Extract the primary key values for a single row."""
    return tuple(batch.column(c)[row_idx].as_py() for c in pk_columns)


def _filter_batch_by_pk(
    batch: pa.RecordBatch,
    pk_columns: tuple[str, ...],
    allowed_keys: set[tuple[object, ...]],
) -> pa.RecordBatch | None:
    """Return only rows whose PK is in *allowed_keys*."""
    mask: list[bool] = []
    for row_idx in range(batch.num_rows):
        pk_val = _extract_pk_tuple(batch, pk_columns, row_idx)
        mask.append(pk_val in allowed_keys)

    if not any(mask):
        return None

    return pa.RecordBatch.from_pydict(
        {
            col: [batch.column(col)[i].as_py() for i, keep in enumerate(mask) if keep]
            for col in batch.schema.names
        },
        schema=batch.schema,
    )


def _optional_str(value: object) -> str | None:
    """Convert a value to str or None."""
    if value is None:
        return None
    return str(value)


def _optional_int(value: object) -> int | None:
    """Convert a value to int or None."""
    if value is None:
        return None
    return int(value)  # type: ignore[arg-type]
