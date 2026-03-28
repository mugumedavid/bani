"""Run command — executes a migration (Section 10.1, 18.2)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, cast

import click
import typer
from rich.console import Console

from bani.application.orchestrator import MigrationOrchestrator, MigrationResult
from bani.application.progress import (
    BatchComplete,
    MigrationComplete,
    MigrationStarted,
    ProgressEvent,
    ProgressTracker,
    TableComplete,
    TableStarted,
)
from bani.bdl.parser import parse
from bani.bdl.validator import validate_json, validate_xml
from bani.cli.formatters import format_error
from bani.connectors.base import SinkConnector, SourceConnector
from bani.connectors.registry import ConnectorRegistry
from bani.domain.errors import BaniError


def _get_ctx() -> dict[str, Any]:
    """Retrieve context from the Typer/Click context chain."""
    try:
        ctx_obj = click.get_current_context().obj
    except RuntimeError:
        ctx_obj = None

    if ctx_obj is None:
        ctx_obj = {"output": "human", "quiet": False, "console": Console()}
    return ctx_obj


def _emit_json_event(event: dict[str, Any]) -> None:
    """Write a single JSON-lines event to stdout (not via Rich)."""
    sys.stdout.write(json.dumps(event) + "\n")
    sys.stdout.flush()


def _make_json_callback(quiet: bool) -> Any:
    """Build a ProgressTracker callback that emits JSON-lines events."""

    def _on_event(event: ProgressEvent) -> None:
        if quiet:
            return

        ts = event.timestamp.isoformat()

        if isinstance(event, MigrationStarted):
            _emit_json_event(
                {
                    "event": "migration_started",
                    "timestamp": ts,
                    "tables": event.table_count,
                    "estimated_rows": 0,
                }
            )
        elif isinstance(event, TableStarted):
            _emit_json_event(
                {
                    "event": "table_started",
                    "timestamp": ts,
                    "table": event.table_name,
                    "estimated_rows": event.estimated_rows or 0,
                }
            )
        elif isinstance(event, BatchComplete):
            _emit_json_event(
                {
                    "event": "batch_complete",
                    "timestamp": ts,
                    "table": event.table_name,
                    "batch": event.batch_number,
                    "rows": event.rows_written,
                    "total_rows": event.rows_written,
                    "throughput_rps": 0,
                }
            )
        elif isinstance(event, TableComplete):
            _emit_json_event(
                {
                    "event": "table_complete",
                    "timestamp": ts,
                    "table": event.table_name,
                    "rows": event.total_rows_written,
                    "duration_sec": 0.0,
                }
            )
        elif isinstance(event, MigrationComplete):
            _emit_json_event(
                {
                    "event": "migration_complete",
                    "timestamp": ts,
                    "tables_succeeded": event.tables_completed,
                    "tables_failed": event.tables_failed,
                    "total_rows": event.total_rows_written,
                    "duration_sec": event.duration_seconds,
                }
            )

    return _on_event


def run(
    project_file: str = typer.Argument(..., help="Path to BDL project file"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate and plan but don't execute."),
    tables: str | None = typer.Option(
        None, help="Comma-separated table names to migrate (all if omitted)."
    ),
    parallel: int = typer.Option(1, help="Number of parallel workers."),
    batch_size: int = typer.Option(100_000, "--batch-size", help="Rows per batch."),
    resume: bool = typer.Option(False, "--resume", help="Resume a previously failed migration."),
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
        resume: If True, resume a previously failed migration.
    """
    ctx_obj = _get_ctx()
    output_format = ctx_obj.get("output", "human")
    quiet = ctx_obj.get("quiet", False)
    console: Console = ctx_obj.get("console", Console())

    # Validate BDL file first
    project_path = Path(project_file)
    if not project_path.exists():
        if output_format == "json":
            _emit_json_event(
                {
                    "command": "run",
                    "status": "failed",
                    "error": {
                        "type": "FileNotFoundError",
                        "message": f"File not found: {project_file}",
                    },
                }
            )
        else:
            console.print(f"[red]Error:[/red] File not found: {project_file}")
        raise typer.Exit(1)

    try:
        content = project_path.read_text()
        if project_file.endswith(".json") or project_file.endswith(".bdl.json"):
            errors = validate_json(content)
        else:
            errors = validate_xml(content)

        if errors:
            if output_format == "json":
                _emit_json_event(
                    {
                        "command": "run",
                        "status": "failed",
                        "error": {
                            "type": "BDLValidationError",
                            "message": "; ".join(errors),
                        },
                    }
                )
            else:
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
        if output_format == "json":
            _emit_json_event(
                {
                    "command": "run",
                    "status": "ok",
                    "dry_run": True,
                    "message": "Validation passed",
                }
            )
        else:
            console.print("[bold green]✓ Dry run passed (validation only)[/bold green]")
        raise typer.Exit(0)

    # Set up progress tracker
    tracker = ProgressTracker()

    if output_format == "json":
        tracker.add_listener(_make_json_callback(quiet))

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
            orchestrator = MigrationOrchestrator(
                project, source, sink, tracker=tracker
            )
            result = orchestrator.execute(resume=resume)

            if output_format == "human" and not quiet:
                _render_human_result(console, result)

            exit_code = 0 if result.tables_failed == 0 else 1
            raise typer.Exit(exit_code)
        finally:
            source.disconnect()
            sink.disconnect()

    except BaniError as e:
        if output_format == "json":
            _emit_json_event(
                {
                    "command": "run",
                    "status": "failed",
                    "error": {
                        "type": type(e).__name__,
                        "message": str(e),
                    },
                    "resumable": resume,
                }
            )
        else:
            format_error(console, e)
        raise typer.Exit(1) from e
    except Exception as e:
        if output_format == "json":
            _emit_json_event(
                {
                    "command": "run",
                    "status": "failed",
                    "error": {
                        "type": type(e).__name__,
                        "message": str(e),
                    },
                }
            )
        else:
            console.print(f"[bold red]Unexpected error:[/bold red] {e}")
            if not quiet:
                import traceback

                traceback.print_exc()
        raise typer.Exit(1) from e


def _render_human_result(console: Console, result: MigrationResult) -> None:
    """Render migration result in Rich format."""
    status_color = "green" if result.tables_failed == 0 else "red"
    console.print(
        f"\n[bold {status_color}]Migration complete:[/bold {status_color}] "
        f"{result.tables_completed} tables, "
        f"{result.total_rows_written:,} rows, "
        f"{result.duration_seconds:.1f}s"
    )
    if result.tables_failed > 0:
        console.print(f"[red]  {result.tables_failed} table(s) failed[/red]")
    if result.errors:
        for err in result.errors:
            console.print(f"  [red]✗[/red] {err}")
