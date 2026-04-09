"""Preview command — sample rows from source tables (Section 10.1, 18.2)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, cast

import click
import typer
from rich.console import Console
from rich.table import Table

from bani.application.preview import PreviewResult, preview_source
from bani.bdl.parser import parse
from bani.bdl.validator import validate_json, validate_xml
from bani.cli.formatters import format_error
from bani.connectors.base import SourceConnector
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


def preview(
    project_file: str = typer.Argument(..., help="Path to BDL project file"),
    sample_size: int = typer.Option(
        10,
        "--sample-size",
        help="Rows to sample per table (default: 10).",
    ),
) -> None:
    """Preview source data by sampling N rows per table.

    Connects to the source database defined in the BDL project and
    displays sample rows for each table.

    Args:
        project_file: Path to the BDL XML or JSON project file.
        sample_size: Number of rows to sample per table.
    """
    ctx_obj = _get_ctx()
    output_format = ctx_obj.get("output", "human")
    quiet = ctx_obj.get("quiet", False)
    console: Console = ctx_obj.get("console", Console())

    project_path = Path(project_file)
    if not project_path.exists():
        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "preview",
                        "status": "error",
                        "error": f"File not found: {project_file}",
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
        else:
            console.print(f"[red]Error:[/red] File not found: {project_file}")
        raise typer.Exit(1)

    # Validate
    try:
        content = project_path.read_text()
        if project_file.endswith(".json") or project_file.endswith(".bdl.json"):
            errors = validate_json(content)
        else:
            errors = validate_xml(content)

        if errors:
            if output_format == "json":
                sys.stdout.write(
                    json.dumps(
                        {
                            "command": "preview",
                            "status": "error",
                            "errors": errors,
                        }
                    )
                    + "\n"
                )
                sys.stdout.flush()
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

    # Connect to source and preview
    try:
        source_cfg = project.source
        assert source_cfg is not None

        source_connector_class = ConnectorRegistry.get(source_cfg.dialect)
        source = cast(type[SourceConnector], source_connector_class)()
        source.connect(source_cfg)

        try:
            result = preview_source(source, sample_size=sample_size)

            if output_format == "json":
                _render_json(result)
            elif not quiet:
                _render_human(console, result)
        finally:
            source.disconnect()

    except BaniError as e:
        format_error(console, e)
        raise typer.Exit(1) from e
    except Exception as e:
        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "preview",
                        "status": "error",
                        "error": str(e),
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
        else:
            console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1) from e


def _render_json(result: PreviewResult) -> None:
    """Render preview result as JSON to stdout."""
    tables_data = []
    for tp in result.tables:
        columns_data = [
            {
                "name": col.name,
                "type": col.data_type,
                "nullable": col.nullable,
                "arrow_type": col.arrow_type,
            }
            for col in tp.columns
        ]
        tables_data.append(
            {
                "table": tp.table_name,
                "schema": tp.schema_name,
                "row_count_estimate": tp.row_count_estimate,
                "columns": columns_data,
                "sample_rows": list(tp.sample_rows),
            }
        )
    output = {
        "command": "preview",
        "source_dialect": result.source_dialect,
        "tables": tables_data,
    }
    sys.stdout.write(json.dumps(output) + "\n")
    sys.stdout.flush()


def _render_human(console: Console, result: PreviewResult) -> None:
    """Render preview result as Rich tables."""
    console.print(
        f"\n[bold cyan]Preview from {result.source_dialect} source[/bold cyan]"
    )
    console.print(f"[dim]{len(result.tables)} table(s)[/dim]\n")

    for tp in result.tables:
        fqn = f"{tp.schema_name}.{tp.table_name}" if tp.schema_name else tp.table_name
        row_est = f" (~{tp.row_count_estimate:,} rows)" if tp.row_count_estimate else ""
        console.print(f"[bold]{fqn}[/bold]{row_est}")

        if not tp.sample_rows:
            console.print("  [dim](no rows)[/dim]\n")
            continue

        table = Table(show_header=True, header_style="bold magenta")
        for col in tp.columns:
            table.add_column(col.name, style="cyan")

        for row in tp.sample_rows:
            values = [str(row.get(col.name, "")) for col in tp.columns]
            table.add_row(*values)

        console.print(table)
        console.print()
