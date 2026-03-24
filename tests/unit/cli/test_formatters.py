"""Tests for the Rich formatters."""

from __future__ import annotations

from io import StringIO

import pytest
from rich.console import Console

from bani.cli.formatters import (
    format_error,
    format_migration_progress,
    format_schema_table,
    format_table_details,
    format_validation_results,
)
from bani.domain.errors import BaniError
from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    TableDefinition,
)


@pytest.fixture
def console() -> Console:
    """Create a console for testing."""
    return Console(file=StringIO(), force_terminal=True)


@pytest.fixture
def sample_schema() -> DatabaseSchema:
    """Create a sample schema for testing."""
    col1 = ColumnDefinition(
        name="id", data_type="INTEGER", nullable=False, ordinal_position=0
    )
    col2 = ColumnDefinition(
        name="name", data_type="VARCHAR(255)", nullable=True, ordinal_position=1
    )
    table = TableDefinition(
        schema_name="public",
        table_name="users",
        columns=(col1, col2),
        primary_key=("id",),
        row_count_estimate=1000,
    )
    return DatabaseSchema(tables=(table,), source_dialect="postgresql")


def test_format_schema_table(console: Console, sample_schema: DatabaseSchema) -> None:
    """Test schema table formatting."""
    format_schema_table(console, sample_schema)
    output = console.file.getvalue()  # type: ignore[attr-defined]
    assert "public" in output
    assert "users" in output
    assert "postgresql" in output


def test_format_table_details(console: Console, sample_schema: DatabaseSchema) -> None:
    """Test table details formatting."""
    table = sample_schema.tables[0]
    format_table_details(console, table)
    output = console.file.getvalue()  # type: ignore[attr-defined]
    assert "id" in output
    assert "name" in output
    assert "INTEGER" in output


def test_format_validation_results_no_errors(console: Console) -> None:
    """Test validation results formatting with no errors."""
    format_validation_results(console, [], [])
    output = console.file.getvalue()  # type: ignore[attr-defined]
    assert "passed" in output.lower()


def test_format_validation_results_with_errors(console: Console) -> None:
    """Test validation results formatting with errors."""
    errors = ["Missing required field", "Invalid reference"]
    warnings = ["Deprecated option"]
    format_validation_results(console, errors, warnings)
    output = console.file.getvalue()  # type: ignore[attr-defined]
    assert "ERRORS" in output
    assert "WARNINGS" in output
    assert "Missing required field" in output
    assert "Deprecated option" in output


def test_format_migration_progress_started(console: Console) -> None:
    """Test migration started progress event."""
    format_migration_progress(
        console, "migration_started", {"tables": 5, "estimated_rows": 50000}
    )
    output = console.file.getvalue()  # type: ignore[attr-defined]
    assert "Starting migration" in output or "5 tables" in output


def test_format_migration_progress_table_started(console: Console) -> None:
    """Test table started progress event."""
    format_migration_progress(
        console, "table_started", {"table": "public.users", "estimated_rows": 10000}
    )
    output = console.file.getvalue()  # type: ignore[attr-defined]
    assert "public.users" in output


def test_format_migration_progress_batch_complete(console: Console) -> None:
    """Test batch complete progress event."""
    format_migration_progress(
        console,
        "batch_complete",
        {"table": "public.users", "batch": 0, "rows": 1000, "total_rows": 1000},
    )
    output = console.file.getvalue()  # type: ignore[attr-defined]
    # Rich formats numbers with commas, so check for the formatted version
    assert "1" in output and "000" in output


def test_format_migration_progress_complete(console: Console) -> None:
    """Test migration complete progress event."""
    format_migration_progress(
        console,
        "migration_complete",
        {
            "tables_succeeded": 5,
            "tables_failed": 0,
            "total_rows": 50000,
        },
    )
    output = console.file.getvalue()  # type: ignore[attr-defined]
    assert "Migration complete" in output or "5 succeeded" in output


def test_format_error_simple(console: Console) -> None:
    """Test error formatting."""
    error = ValueError("Test error message")
    format_error(console, error)
    output = console.file.getvalue()  # type: ignore[attr-defined]
    assert "ValueError" in output
    assert "Test error message" in output


def test_format_error_bani_error(console: Console) -> None:
    """Test BaniError formatting with context."""
    error = BaniError("Test error", table="users", schema="public")
    format_error(console, error)
    output = console.file.getvalue()  # type: ignore[attr-defined]
    assert "BaniError" in output
    assert "Test error" in output
