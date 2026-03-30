"""Project model — represents a parsed BDL migration project (Section 7).

These dataclasses hold the in-memory representation of a BDL document after
parsing. The BDL parser (separate module) creates ``ProjectModel`` instances;
the orchestrator consumes them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto


class SyncStrategy(Enum):
    """Incremental sync strategy (Section 13.1)."""

    FULL = auto()
    TIMESTAMP = auto()
    ROWVERSION = auto()
    CHECKSUM = auto()


class WriteStrategy(Enum):
    """How data is written to the target."""

    INSERT = auto()
    UPSERT = auto()
    TRUNCATE_INSERT = auto()


class ErrorHandlingStrategy(Enum):
    """How errors are handled during migration."""

    ABORT = "fail-fast"
    LOG_AND_CONTINUE = "log-and-continue"


@dataclass(frozen=True)
class ConnectionConfig:
    """Connection configuration for a source or target database.

    Credential values are never stored directly — only ``${env:VAR}``
    references that are resolved at runtime.

    Attributes:
        dialect: Database dialect, e.g. ``"mysql"``, ``"postgresql"``.
        host: Database host.
        port: Database port.
        database: Database name.
        username_env: Environment variable name for the username.
        password_env: Environment variable name for the password.
        extra: Additional connector-specific configuration.
        encrypt: Whether to use TLS (default ``True``).
    """

    dialect: str
    host: str = ""
    port: int = 0
    database: str = ""
    username_env: str = ""
    password_env: str = ""
    extra: tuple[tuple[str, str], ...] = ()
    encrypt: bool = True


@dataclass(frozen=True)
class ColumnMapping:
    """Mapping for a single column in a table.

    Attributes:
        source_name: Source column name.
        target_name: Target column name.
        target_type: Optional type override for the target column.
    """

    source_name: str
    target_name: str
    target_type: str | None = None


@dataclass(frozen=True)
class TypeMappingOverride:
    """Type mapping override from source to target type.

    Attributes:
        source_type: Source data type.
        target_type: Target data type to map to.
    """

    source_type: str
    target_type: str


@dataclass(frozen=True)
class ProjectOptions:
    """Project-level configuration options.

    Attributes:
        batch_size: Number of rows per batch.
        parallel_workers: Number of parallel workers.
        memory_limit_mb: Memory limit in MB.
        on_error: Error handling strategy.
        create_target_schema: Whether to create target schema.
        drop_target_tables_first: Whether to drop target tables first.
        transfer_indexes: Whether to transfer indexes.
        transfer_foreign_keys: Whether to transfer foreign keys.
        transfer_defaults: Whether to transfer default values.
        transfer_check_constraints: Whether to transfer check constraints.
    """

    batch_size: int = 100_000
    parallel_workers: int = 4
    memory_limit_mb: int = 2048
    on_error: ErrorHandlingStrategy = ErrorHandlingStrategy.LOG_AND_CONTINUE
    create_target_schema: bool = True
    drop_target_tables_first: bool = False
    transfer_indexes: bool = True
    transfer_foreign_keys: bool = True
    transfer_defaults: bool = True
    transfer_check_constraints: bool = True


@dataclass(frozen=True)
class ScheduleConfig:
    """Configuration for scheduled migrations.

    Attributes:
        enabled: Whether the schedule is enabled.
        cron: Cron expression for scheduling (optional).
        timezone: Timezone for cron evaluation.
        max_retries: Maximum number of retries on failure.
        retry_delay_seconds: Delay in seconds between retries.
    """

    enabled: bool = False
    cron: str | None = None
    timezone: str = "UTC"
    max_retries: int = 0
    retry_delay_seconds: int = 0


@dataclass(frozen=True)
class SyncConfig:
    """Configuration for incremental sync.

    Attributes:
        enabled: Whether sync is enabled.
        strategy: Sync strategy to use.
        tracking_columns: Tuples of (table, column) for tracking changes.
    """

    enabled: bool = False
    strategy: SyncStrategy = SyncStrategy.FULL
    tracking_columns: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class TableMapping:
    """Mapping configuration for a single table.

    Attributes:
        source_schema: Source schema name.
        source_table: Source table name.
        target_schema: Target schema name (may differ from source).
        target_table: Target table name (may differ from source).
        column_mappings: Tuple of column mappings (empty = all).
        filter_sql: Optional WHERE clause to filter source rows.
        write_strategy: How to write data to the target.
        batch_size: Number of rows per batch (``None`` = use project default).
    """

    source_schema: str
    source_table: str
    target_schema: str = ""
    target_table: str = ""
    column_mappings: tuple[ColumnMapping, ...] = ()
    filter_sql: str | None = None
    write_strategy: WriteStrategy = WriteStrategy.INSERT
    batch_size: int | None = None


@dataclass(frozen=True)
class HookConfig:
    """Configuration for a pre- or post-migration hook.

    Attributes:
        name: Human-readable hook name.
        event: When the hook runs (e.g. ``"before-migration"``,
            ``"after-migration"``, ``"before-table"``, ``"after-table"``).
        command: Command text (SQL statement or shell command).
        hook_type: ``"sql"`` or ``"shell"``.
        target: For SQL hooks, which connection to run against:
            ``"source"`` or ``"target"``.
        table_name: For per-table hooks, the table this applies to.
        timeout_seconds: Maximum execution time before the hook is killed.
        on_failure: Action on failure: ``"abort"`` or ``"continue"``.
    """

    name: str
    event: str
    command: str
    hook_type: str = "shell"
    target: str = ""
    table_name: str = ""
    timeout_seconds: int = 300
    on_failure: str = "abort"


@dataclass(frozen=True)
class ProjectModel:
    """In-memory representation of a parsed BDL migration project.

    Attributes:
        name: Project name.
        source: Source database connection configuration.
        target: Target database connection configuration.
        description: Optional project description.
        author: Project author.
        created: Project creation timestamp.
        tags: Tuple of project tags.
        table_mappings: Tuple of per-table mapping configurations.
        type_overrides: Optional user-supplied type mapping overrides.
        options: Project-level configuration options.
        hooks: Tuple of pre/post migration hooks.
        schedule: Schedule configuration for migrations.
        sync: Sync configuration for incremental migrations.
    """

    name: str
    source: ConnectionConfig | None = None
    target: ConnectionConfig | None = None
    description: str = ""
    author: str = ""
    created: datetime | None = None
    tags: tuple[str, ...] = ()
    table_mappings: tuple[TableMapping, ...] = ()
    type_overrides: tuple[TypeMappingOverride, ...] = ()
    options: ProjectOptions | None = None
    hooks: tuple[HookConfig, ...] = ()
    schedule: ScheduleConfig | None = None
    sync: SyncConfig | None = None


@dataclass(frozen=True)
class MigrationPlan:
    """A resolved plan ready for execution.

    Created from a ``ProjectModel`` after schema introspection and dependency
    resolution. Contains the ordered list of tables and their resolved
    configurations.

    Attributes:
        project: The source project model.
        ordered_tables: Tables in dependency-safe execution order.
        deferred_fk_tables: Tables whose FK constraints are deferred.
        total_estimated_rows: Sum of row count estimates across all tables.
    """

    project: ProjectModel
    ordered_tables: tuple[str, ...]
    deferred_fk_tables: tuple[str, ...] = ()
    total_estimated_rows: int | None = None
