"""Rich console formatters for CLI output (Section 18).

Provides helper functions to render structured data (schema, validation
results, migration progress) as Rich tables and panels.
"""

from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.table import Table

from bani.domain.errors import BaniError
from bani.domain.schema import DatabaseSchema, TableDefinition


def format_schema_table(console: Console, schema: DatabaseSchema) -> None:
    """Render a DatabaseSchema as a Rich table.

    Args:
        console: The Rich console to write to.
        schema: The schema to display.
    """
    table = Table(title=f"Schema Introspection ({schema.source_dialect})")
    table.add_column("Schema", style="cyan")
    table.add_column("Table", style="magenta")
    table.add_column("Columns", justify="right", style="green")
    table.add_column("Rows (est.)", justify="right", style="blue")

    for table_def in schema.tables:
        if table_def.row_count_estimate:
            row_est = str(table_def.row_count_estimate)
        else:
            row_est = "—"
        table.add_row(
            table_def.schema_name,
            table_def.table_name,
            str(len(table_def.columns)),
            row_est,
        )

    console.print(table)


def format_table_details(console: Console, table: TableDefinition) -> None:
    """Render a TableDefinition with full details as a Rich table.

    Args:
        console: The Rich console to write to.
        table: The table to display.
    """
    # Column table
    col_table = Table(title=f"Columns in {table.fully_qualified_name}")
    col_table.add_column("Name", style="cyan")
    col_table.add_column("Type", style="magenta")
    col_table.add_column("Nullable", style="yellow")
    col_table.add_column("Default", style="blue")

    for col in table.columns:
        nullable_str = "YES" if col.nullable else "NO"
        default_str = col.default_value or "—"
        col_table.add_row(col.name, col.data_type, nullable_str, default_str)

    console.print(col_table)

    # Constraints summary
    if table.primary_key:
        console.print(f"[bold]Primary Key:[/bold] {', '.join(table.primary_key)}")
    if table.indexes:
        console.print(f"[bold]Indexes:[/bold] {len(table.indexes)}")
    if table.foreign_keys:
        console.print(f"[bold]Foreign Keys:[/bold] {len(table.foreign_keys)}")


def format_validation_results(
    console: Console, errors: list[str], warnings: list[str]
) -> None:
    """Render validation results as colored output.

    Args:
        console: The Rich console to write to.
        errors: List of validation errors.
        warnings: List of validation warnings.
    """
    if errors:
        console.print("[bold red]ERRORS:[/bold red]")
        for error in errors:
            console.print(f"  [red]✗[/red] {error}")
    if warnings:
        console.print("[bold yellow]WARNINGS:[/bold yellow]")
        for warning in warnings:
            console.print(f"  [yellow]⚠[/yellow] {warning}")
    if not errors and not warnings:
        console.print("[bold green]✓ Validation passed[/bold green]")


def format_migration_progress(
    console: Console, event: str, data: dict[str, Any]
) -> None:
    """Render a single migration progress event.

    Args:
        console: The Rich console to write to.
        event: The event name (e.g., "migration_started", "batch_complete").
        data: The event data dictionary.
    """
    if event == "migration_started":
        console.print(
            f"[bold cyan]Starting migration:[/bold cyan] "
            f"{data.get('tables', 0)} tables, "
            f"~{data.get('estimated_rows', 0):,} rows"
        )
    elif event == "table_started":
        table_name = data.get("table", "unknown")
        est_rows = data.get("estimated_rows", 0)
        console.print(f"[bold]Table:[/bold] {table_name} (~{est_rows:,} rows)")
    elif event == "batch_complete":
        table_name = data.get("table", "unknown")
        batch_num = data.get("batch", 0)
        rows = data.get("rows", 0)
        total = data.get("total_rows", 0)
        console.print(
            f"  [green]Batch {batch_num}:[/green] +{rows:,} rows (total: {total:,})"
        )
    elif event == "table_complete":
        table_name = data.get("table", "unknown")
        rows = data.get("rows", 0)
        console.print(f"[green]✓ {table_name}:[/green] {rows:,} rows transferred")
    elif event == "table_failed":
        table_name = data.get("table", "unknown")
        error = data.get("error", "unknown error")
        console.print(f"[red]✗ {table_name}:[/red] {error}")
    elif event == "migration_complete":
        succeeded = data.get("tables_succeeded", 0)
        failed = data.get("tables_failed", 0)
        total_rows = data.get("total_rows", 0)
        status_color = "green" if failed == 0 else "red"
        console.print(
            f"[bold {status_color}]Migration complete:[/bold {status_color}] "
            f"{succeeded} succeeded, {failed} failed, {total_rows:,} rows transferred"
        )


def format_error(console: Console, error: Exception) -> None:
    """Format and print an error message.

    Args:
        console: The Rich console to write to.
        error: The exception to format.
    """
    error_type = type(error).__name__
    message = str(error)

    if isinstance(error, BaniError) and error.context:
        context_str = ", ".join(f"{k}={v}" for k, v in error.context.items())
        console.print(
            f"[bold red]{error_type}:[/bold red] {message}\n"
            f"[dim]Context: {context_str}[/dim]"
        )
    else:
        console.print(f"[bold red]{error_type}:[/bold red] {message}")
