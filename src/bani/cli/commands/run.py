"""Run command — executes a migration (Section 10.1)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click
import typer
from rich.console import Console

from bani.application.orchestrator import MigrationOrchestrator
from bani.bdl.parser import parse
from bani.bdl.validator import validate_json, validate_xml
from bani.cli.formatters import format_error, format_migration_progress
from bani.domain.errors import BaniError


def run(
    project_file: str = typer.Argument(..., help="Path to BDL project file"),
    dry_run: bool = typer.Option(False, help="Validate but don't execute."),
    tables: str | None = typer.Option(
        None, help="Comma-separated table names to migrate (all if omitted)."
    ),
    parallel: int = typer.Option(1, help="Number of parallel workers."),
    batch_size: int = typer.Option(100_000, help="Rows per batch."),
) -> None:
    """Execute a database migration from a BDL project file.

    Loads the BDL project, validates it, and executes the migration using
    the source and target connectors specified in the project.

    Args:
        project_file: Path to the BDL XML or JSON project file.
        dry_run: If True, validate but don't execute.
        tables: Comma-separated table names (all if omitted).
        parallel: Number of parallel workers.
        batch_size: Rows per batch.
    """
    try:
        ctx_obj = click.get_current_context().obj
    except RuntimeError:
        ctx_obj = None

    if ctx_obj is None:
        ctx_obj = {"output": "human", "quiet": False, "console": Console()}

    output_format = ctx_obj.get("output", "human")
    quiet = ctx_obj.get("quiet", False)
    console: Console = ctx_obj.get("console", Console())

    # Validate BDL file first
    project_path = Path(project_file)
    if not project_path.exists():
        console.print(f"[red]Error:[/red] File not found: {project_file}")
        raise typer.Exit(1)

    try:
        content = project_path.read_text()
        if project_file.endswith(".json") or project_file.endswith(".bdl.json"):
            errors = validate_json(content)
        else:
            errors = validate_xml(content)

        if errors:
            console.print("[bold red]BDL Validation Errors:[/bold red]")
            for error in errors:
                console.print(f"  [red]✗[/red] {error}")
            raise typer.Exit(1)
    except BaniError as e:
        format_error(console, e)
        raise typer.Exit(1) from e

    # Parse BDL
    try:
        project = parse(project_path)
    except BaniError as e:
        format_error(console, e)
        raise typer.Exit(1) from e

    if dry_run:
        console.print("[bold green]✓ Dry run passed (validation only)[/bold green]")
        raise typer.Exit(0)

    # Set up progress callback
    def progress_callback(event: str, data: dict[str, Any]) -> None:
        """Emit progress to JSON or human output."""
        if output_format == "json":
            # JSON-lines streaming (Section 18.2)
            event_obj: dict[str, Any] = {
                "event": event,
                "timestamp": datetime.now(timezone.utc).isoformat(),  # noqa: UP017
                **data,
            }
            console.print(json.dumps(event_obj))
        elif not quiet:
            format_migration_progress(console, event, data)

    # Run migration
    try:
        orchestrator = MigrationOrchestrator(
            project, progress_callback=progress_callback
        )
        result = orchestrator.run(dry_run=False)

        if output_format == "json":
            # Final result as JSON
            result_obj = {
                "status": result.status.name,
                "total_tables": result.total_tables,
                "succeeded_tables": result.succeeded_tables,
                "failed_tables": result.failed_tables,
                "total_rows": result.total_rows,
                "error_message": result.error_message,
            }
            console.print(json.dumps(result_obj))

        exit_code = 0 if result.failed_tables == 0 else 1
        raise typer.Exit(exit_code)

    except BaniError as e:
        format_error(console, e)
        raise typer.Exit(1) from e
    except Exception as e:
        console.print(f"[bold red]Unexpected error:[/bold red] {e}")
        if not quiet:
            import traceback

            traceback.print_exc()
        raise typer.Exit(1) from e
