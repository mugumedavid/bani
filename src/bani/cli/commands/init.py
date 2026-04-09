"""Init command — interactive wizard to create a BDL project file (Section 10.1)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click
import typer
from rich.console import Console
from rich.panel import Panel

from bani.connectors.registry import ConnectorRegistry


def _get_ctx() -> dict[str, Any]:
    """Retrieve context from the Typer/Click context chain."""
    try:
        ctx_obj = click.get_current_context().obj
    except RuntimeError:
        ctx_obj = None

    if ctx_obj is None:
        ctx_obj = {"output": "human", "quiet": False, "console": Console()}
    return ctx_obj


_DEFAULT_PORTS: dict[str, int] = {
    "postgresql": 5432,
    "mysql": 3306,
    "mssql": 1433,
    "oracle": 1521,
    "sqlite": 0,
}


def init(
    source: str | None = typer.Option(None, "--source", help="Source connector name"),
    target: str | None = typer.Option(None, "--target", help="Target connector name"),
    output_file: str | None = typer.Option(
        None, "--out", "-f", help="Output BDL file path (default: migration.bdl)"
    ),
) -> None:
    """Create a new BDL project file via an interactive wizard.

    Prompts for source and target connection details and generates a
    BDL XML project file.

    Args:
        source: Source connector (e.g., postgresql, mysql).
        target: Target connector (e.g., postgresql, mysql).
        output_file: Path for the output BDL file.
    """
    ctx_obj = _get_ctx()
    output_format = ctx_obj.get("output", "human")
    console: Console = ctx_obj.get("console", Console())

    connectors = ConnectorRegistry.discover()
    available = list(connectors.keys())

    if not available:
        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "init",
                        "status": "error",
                        "error": "No connectors available",
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
        else:
            console.print("[red]Error:[/red] No connectors available")
        raise typer.Exit(1)

    # Source connector
    if not source:
        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "init",
                        "status": "error",
                        "error": "Missing --source flag "
                        "(required in non-interactive mode)",
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
            raise typer.Exit(1)
        console.print(Panel("[bold]Bani Project Wizard[/bold]", style="cyan"))
        console.print(f"[dim]Available connectors: {', '.join(available)}[/dim]\n")
        source = typer.prompt("Source connector", default=available[0])

    if source not in available:
        msg = f"Unknown source connector: {source}. Available: {', '.join(available)}"
        if output_format == "json":
            sys.stdout.write(
                json.dumps({"command": "init", "status": "error", "error": msg})
                + "\n"
            )
            sys.stdout.flush()
        else:
            console.print(f"[red]Error:[/red] {msg}")
        raise typer.Exit(1)

    # Target connector
    if not target:
        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "init",
                        "status": "error",
                        "error": "Missing --target flag "
                        "(required in non-interactive mode)",
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
            raise typer.Exit(1)
        target = typer.prompt("Target connector", default=available[0])

    if target not in available:
        msg = f"Unknown target connector: {target}. Available: {', '.join(available)}"
        if output_format == "json":
            sys.stdout.write(
                json.dumps({"command": "init", "status": "error", "error": msg})
                + "\n"
            )
            sys.stdout.flush()
        else:
            console.print(f"[red]Error:[/red] {msg}")
        raise typer.Exit(1)

    # Gather connection details interactively
    if output_format != "json":
        console.print(f"\n[bold]Source ({source}) connection details:[/bold]")
        src_host = typer.prompt("  Host", default="localhost")
        src_port = typer.prompt("  Port", default=str(_DEFAULT_PORTS.get(source, 5432)))
        src_database = typer.prompt("  Database", default="source_db")
        src_user_env = typer.prompt("  Username env var", default="SRC_DB_USER")
        src_pass_env = typer.prompt("  Password env var", default="SRC_DB_PASS")

        console.print(f"\n[bold]Target ({target}) connection details:[/bold]")
        tgt_host = typer.prompt("  Host", default="localhost")
        tgt_port = typer.prompt("  Port", default=str(_DEFAULT_PORTS.get(target, 5432)))
        tgt_database = typer.prompt("  Database", default="target_db")
        tgt_user_env = typer.prompt("  Username env var", default="TGT_DB_USER")
        tgt_pass_env = typer.prompt("  Password env var", default="TGT_DB_PASS")

        project_name = typer.prompt("\nProject name", default="my-migration")
    else:
        # JSON mode: use defaults
        src_host = "localhost"
        src_port = str(_DEFAULT_PORTS.get(source, 5432))
        src_database = "source_db"
        src_user_env = "SRC_DB_USER"
        src_pass_env = "SRC_DB_PASS"
        tgt_host = "localhost"
        tgt_port = str(_DEFAULT_PORTS.get(target, 5432))
        tgt_database = "target_db"
        tgt_user_env = "TGT_DB_USER"
        tgt_pass_env = "TGT_DB_PASS"
        project_name = "my-migration"

    # Generate BDL XML
    bdl_content = _generate_bdl(
        project_name=project_name,
        source_connector=source,
        source_host=src_host,
        source_port=src_port,
        source_database=src_database,
        source_user_env=src_user_env,
        source_pass_env=src_pass_env,
        target_connector=target,
        target_host=tgt_host,
        target_port=tgt_port,
        target_database=tgt_database,
        target_user_env=tgt_user_env,
        target_pass_env=tgt_pass_env,
    )

    # Write file
    out_path = Path(output_file) if output_file else Path("migration.bdl")
    out_path.write_text(bdl_content)

    if output_format == "json":
        sys.stdout.write(
            json.dumps(
                {
                    "command": "init",
                    "status": "ok",
                    "file": str(out_path),
                    "source": source,
                    "target": target,
                }
            )
            + "\n"
        )
        sys.stdout.flush()
    else:
        console.print(
            f"\n[bold green]✓ Created BDL project file:[/bold green] {out_path}"
        )
        console.print(
            "[dim]Edit the file to add table selections,"
            " type mappings, and hooks.[/dim]"
        )


def _generate_bdl(
    *,
    project_name: str,
    source_connector: str,
    source_host: str,
    source_port: str,
    source_database: str,
    source_user_env: str,
    source_pass_env: str,
    target_connector: str,
    target_host: str,
    target_port: str,
    target_database: str,
    target_user_env: str,
    target_pass_env: str,
) -> str:
    """Generate a BDL XML document from the wizard inputs."""
    src_user = f"${{env:{source_user_env}}}"
    src_pass = f"${{env:{source_pass_env}}}"
    tgt_user = f"${{env:{target_user_env}}}"
    tgt_pass = f"${{env:{target_pass_env}}}"
    return f"""\
<?xml version="1.0" encoding="UTF-8"?>
<bani schemaVersion="1.0">
  <project name="{project_name}"
           description="Generated by bani init"
           author="bani" />
  <source connector="{source_connector}">
    <connection host="{source_host}"
                port="{source_port}"
                database="{source_database}"
                username="{src_user}"
                password="{src_pass}" />
  </source>
  <target connector="{target_connector}">
    <connection host="{target_host}"
                port="{target_port}"
                database="{target_database}"
                username="{tgt_user}"
                password="{tgt_pass}" />
  </target>
  <options>
    <batchSize>100000</batchSize>
    <parallelWorkers>4</parallelWorkers>
  </options>
  <tables>
    <!-- Add table selections here, e.g.:
    <table sourceSchema="public" sourceName="users" targetName="users" />
    -->
  </tables>
</bani>
"""
