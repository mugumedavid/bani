"""Domain models for BDL projects."""  # STUB

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class SyncStrategy(str, Enum):
    """Synchronization strategy."""

    FULL = "full"
    TIMESTAMP = "timestamp"
    ROWVERSION = "rowversion"
    CHECKSUM = "checksum"


class WriteStrategy(str, Enum):
    """Write strategy for target."""

    INSERT = "insert"
    UPSERT = "upsert"
    TRUNCATE_INSERT = "truncate_insert"


class ErrorHandlingStrategy(str, Enum):
    """Error handling strategy."""

    LOG_AND_CONTINUE = "log-and-continue"
    FAIL_FAST = "fail-fast"


@dataclass(frozen=True)
class ConnectionConfig:
    """Database connection configuration."""

    dialect: str
    host: str
    port: int
    database: str
    username_env: str
    password_env: str
    encrypt: bool = False
    extra: tuple[tuple[str, str], ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class ColumnMapping:
    """Column mapping between source and target."""

    source_name: str
    target_name: str
    target_type: str | None = None


@dataclass(frozen=True)
class TableMapping:
    """Table mapping between source and target."""

    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    columns: tuple[ColumnMapping, ...] = field(default_factory=tuple)
    filter_sql: str | None = None
    write_strategy: WriteStrategy = WriteStrategy.INSERT
    batch_size: int | None = None


@dataclass(frozen=True)
class TypeMappingOverride:
    """Type mapping override from source to target."""

    source_type: str
    target_type: str


@dataclass(frozen=True)
class HookConfig:
    """Hook configuration."""

    name: str
    phase: str
    command: str
    timeout_seconds: int = 300
    on_failure: str = "fail"


@dataclass(frozen=True)
class ScheduleConfig:
    """Schedule configuration."""

    enabled: bool = False
    cron: str | None = None
    timezone: str = "UTC"
    max_retries: int = 0
    retry_delay_seconds: int = 300


@dataclass(frozen=True)
class SyncConfig:
    """Synchronization configuration."""

    enabled: bool = False
    strategy: SyncStrategy = SyncStrategy.FULL
    tracking_columns: tuple[tuple[str, str], ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class ProjectOptions:
    """Project-wide options."""

    batch_size: int = 100000
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
class ProjectModel:
    """BDL project model."""

    name: str
    version: str = "1.0"
    description: str = ""
    author: str = ""
    created: datetime | None = None
    tags: tuple[str, ...] = field(default_factory=tuple)
    source: ConnectionConfig | None = None
    target: ConnectionConfig | None = None
    table_mappings: tuple[TableMapping, ...] = field(default_factory=tuple)
    type_overrides: tuple[TypeMappingOverride, ...] = field(default_factory=tuple)
    options: ProjectOptions = field(default_factory=ProjectOptions)
    hooks: tuple[HookConfig, ...] = field(default_factory=tuple)
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)
    sync: SyncConfig = field(default_factory=SyncConfig)
