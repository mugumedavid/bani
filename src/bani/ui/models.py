"""Pydantic request/response models for the Bani Web UI API."""

from __future__ import annotations

from pydantic import BaseModel, Field


# --- Projects ---


class ProjectCreate(BaseModel):
    """Request body for creating a new project."""

    name: str = Field(..., min_length=1, description="Project name (used as filename)")
    content: str = Field(..., description="BDL file content (XML or JSON)")


class ProjectUpdate(BaseModel):
    """Request body for updating an existing project."""

    content: str = Field(..., description="Updated BDL file content")


class ProjectSummary(BaseModel):
    """Brief project listing entry."""

    name: str
    path: str


class ProjectDetail(BaseModel):
    """Full project content."""

    name: str
    path: str
    content: str


# --- Migration ---


class MigrateRequest(BaseModel):
    """Request body for starting a migration."""

    project_name: str = Field(..., description="Name of the project (.bdl file)")
    resume: bool = Field(default=False, description="Resume from checkpoint")


class MigrateStatus(BaseModel):
    """Current migration status."""

    running: bool = False
    project_name: str | None = None
    tables_completed: int = 0
    tables_failed: int = 0
    total_rows_read: int = 0
    total_rows_written: int = 0
    error: str | None = None


class MigrateResult(BaseModel):
    """Migration execution result."""

    project_name: str
    tables_completed: int
    tables_failed: int
    total_rows_read: int
    total_rows_written: int
    duration_seconds: float
    errors: list[str] = []


# --- Schema ---


class SchemaInspectRequest(BaseModel):
    """Request body for schema inspection."""

    dialect: str = Field(..., description="Database dialect (e.g. postgresql, mysql)")
    host: str = Field(default="", description="Database host")
    port: int = Field(default=0, description="Database port")
    database: str = Field(default="", description="Database name")
    username_env: str = Field(default="", description="Env var for username")
    password_env: str = Field(default="", description="Env var for password")
    extra: dict[str, str] = Field(default_factory=dict, description="Extra params")


class ColumnInfo(BaseModel):
    """Column details for schema inspection response."""

    name: str
    data_type: str
    nullable: bool
    default_value: str | None = None
    is_auto_increment: bool = False
    arrow_type_str: str | None = None


class IndexInfo(BaseModel):
    """Index details for schema inspection response."""

    name: str
    columns: list[str]
    is_unique: bool = False


class ForeignKeyInfo(BaseModel):
    """Foreign key details for schema inspection response."""

    name: str
    source_table: str
    source_columns: list[str]
    referenced_table: str
    referenced_columns: list[str]


class TableInfo(BaseModel):
    """Table details for schema inspection response."""

    schema_name: str
    table_name: str
    columns: list[ColumnInfo]
    primary_key: list[str]
    indexes: list[IndexInfo]
    foreign_keys: list[ForeignKeyInfo]
    row_count_estimate: int | None = None


class SchemaInspectResponse(BaseModel):
    """Response for schema inspection."""

    source_dialect: str
    tables: list[TableInfo]


# --- Connectors ---


class ConnectorInfo(BaseModel):
    """Connector details."""

    name: str
    class_name: str
    module: str


# --- Settings ---


class SettingsModel(BaseModel):
    """Application settings."""

    projects_dir: str = "~/.bani/projects"
    default_host: str = "127.0.0.1"
    default_port: int = 8910
