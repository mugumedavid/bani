"""Run command — executes a migration (Section 10.1)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import click
import typer
from rich.console import Console

from bani.application.orchestrator import MigrationOrchestrator
from bani.application.progress import ProgressTracker
from bani.bdl.parser import parse
from bani.bdl.validator import validate_json, validate_xml
from bani.cli.formatters import format_error
from bani.connectors.base import SinkConnector, SourceConnector
from bani.connectors.registry import ConnectorRegistry
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

    # Set up progress tracker
    tracker = ProgressTracker()

    # Run migration
    try:
        source_cfg = project.source
        target_cfg = project.target

        assert source_cfg is not None
        assert target_cfg is not None

        # Create source connector
        source_connector_class = ConnectorRegistry.get(source_cfg.dialect)
        source = cast(type[SourceConnector], source_connector_class)()
        source.connect(source_cfg)

        # Create sink connector
        sink_connector_class = ConnectorRegistry.get(target_cfg.dialect)
        sink = cast(type[SinkConnector], sink_connector_class)()
        sink.connect(target_cfg)

        try:
            orchestrator = MigrationOrchestrator(project, source, sink, tracker=tracker)
            result = orchestrator.execute()

            if output_format == "json":
                # Final result as JSON
                result_obj = {
                    "project_name": result.project_name,
                    "tables_completed": result.tables_completed,
                    "tables_failed": result.tables_failed,
                    "total_rows_read": result.total_rows_read,
                    "total_rows_written": result.total_rows_written,
                    "duration_seconds": result.duration_seconds,
                    "errors": result.errors,
                }
                console.print(json.dumps(result_obj))

            exit_code = 0 if result.tables_failed == 0 else 1
            raise typer.Exit(exit_code)
        finally:
            source.disconnect()
            sink.disconnect()

    except BaniError as e:
        format_error(console, e)
        raise typer.Exit(1) from e
    except Exception as e:
        console.print(f"[bold red]Unexpected error:[/bold red] {e}")
        if not quiet:
            import traceback

            traceback.print_exc()
        raise typer.Exit(1) from e
