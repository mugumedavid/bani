"""Project model — represents a parsed BDL migration project (Section 7).

These dataclasses hold the in-memory representation of a BDL document after
parsing. The BDL parser (separate module) creates ``ProjectModel`` instances;
the orchestrator consumes them.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from bani.domain.type_mapping import MappingRuleSet


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
class TableMapping:
    """Mapping configuration for a single table.

    Attributes:
        source_schema: Source schema name.
        source_table: Source table name.
        target_schema: Target schema name (may differ from source).
        target_table: Target table name (may differ from source).
        columns: Tuple of column names to include (empty = all).
        filter_sql: Optional WHERE clause to filter source rows.
        write_strategy: How to write data to the target.
        batch_size: Number of rows per batch (``None`` = use project default).
    """

    source_schema: str
    source_table: str
    target_schema: str = ""
    target_table: str = ""
    columns: tuple[str, ...] = ()
    filter_sql: str | None = None
    write_strategy: WriteStrategy = WriteStrategy.INSERT
    batch_size: int | None = None


@dataclass(frozen=True)
class HookConfig:
    """Configuration for a pre- or post-migration hook.

    Attributes:
        name: Human-readable hook name.
        phase: When the hook runs (``"pre"`` or ``"post"``).
        command: Shell command to execute.
        timeout_seconds: Maximum execution time before the hook is killed.
        on_failure: Action on failure: ``"abort"`` or ``"warn"``.
    """

    name: str
    phase: str
    command: str
    timeout_seconds: int = 300
    on_failure: str = "abort"


@dataclass(frozen=True)
class ProjectModel:
    """In-memory representation of a parsed BDL migration project.

    Attributes:
        name: Project name.
        version: BDL schema version (e.g. ``"1.0"``).
        source: Source database connection configuration.
        target: Target database connection configuration.
        table_mappings: Tuple of per-table mapping configurations.
        type_overrides: Optional user-supplied type mapping overrides.
        sync_strategy: Sync strategy for incremental migrations.
        default_batch_size: Default batch size for data transfer.
        hooks: Tuple of pre/post migration hooks.
        description: Optional project description.
    """

    name: str
    version: str
    source: ConnectionConfig
    target: ConnectionConfig
    table_mappings: tuple[TableMapping, ...] = ()
    type_overrides: MappingRuleSet | None = None
    sync_strategy: SyncStrategy = SyncStrategy.FULL
    default_batch_size: int = 10_000
    hooks: tuple[HookConfig, ...] = ()
    description: str = ""


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
