"""Connectors commands — list and inspect connectors (Section 10.1, 18.2)."""

from __future__ import annotations

import json
import sys
from typing import Any

import click
import typer
from rich.console import Console
from rich.table import Table

from bani.connectors.base import SinkConnector, SourceConnector
from bani.connectors.registry import ConnectorRegistry

connectors_app = typer.Typer(help="Manage and inspect connectors")


def _get_ctx() -> dict[str, Any]:
    """Retrieve context from the Typer/Click context chain."""
    try:
        ctx_obj = click.get_current_context().obj
    except RuntimeError:
        ctx_obj = None

    if ctx_obj is None:
        ctx_obj = {"output": "human", "quiet": False, "console": Console()}
    return ctx_obj


def _connector_type(cls: type[SourceConnector] | type[SinkConnector]) -> str:
    """Determine if a connector class is source, sink, or both."""
    is_source = issubclass(cls, SourceConnector) and not _is_abstract(
        cls, SourceConnector
    )
    is_sink = issubclass(cls, SinkConnector) and not _is_abstract(cls, SinkConnector)
    if is_source and is_sink:
        return "source+sink"
    if is_source:
        return "source"
    if is_sink:
        return "sink"
    return "unknown"


def _is_abstract(cls: type[Any], base: type[Any]) -> bool:
    """Check if cls is the abstract base itself."""
    return cls is base


def _get_connector_info(
    name: str, cls: type[SourceConnector] | type[SinkConnector]
) -> dict[str, Any]:
    """Build a connector info dict."""
    conn_type = _connector_type(cls)

    # Try to get version and driver info from the class
    version = getattr(cls, "VERSION", "1.0.0")
    driver_version = getattr(cls, "DRIVER_VERSION", "unknown")
    supported_versions = getattr(cls, "SUPPORTED_DB_VERSIONS", [])

    return {
        "name": name,
        "version": str(version),
        "type": conn_type,
        "default_driver_version": str(driver_version),
        "bundled_driver_versions": [str(driver_version)],
        "supported_db_versions": list(supported_versions) if supported_versions else [],
    }


@connectors_app.command("list")
def list_connectors() -> None:
    """Show all discovered connectors.

    Lists connectors registered via entry points or built-in fallback.
    """
    ctx_obj = _get_ctx()
    output_format = ctx_obj.get("output", "human")
    console: Console = ctx_obj.get("console", Console())

    discovered = ConnectorRegistry.discover()

    if output_format == "json":
        connectors_list = [
            _get_connector_info(name, cls) for name, cls in sorted(discovered.items())
        ]
        result = {
            "command": "connectors_list",
            "connectors": connectors_list,
        }
        sys.stdout.write(json.dumps(result) + "\n")
        sys.stdout.flush()
    else:
        if not discovered:
            console.print("[yellow]No connectors discovered.[/yellow]")
            return

        table = Table(title="Installed Connectors")
        table.add_column("Name", style="cyan")
        table.add_column("Type", style="magenta")
        table.add_column("Version", style="green")
        table.add_column("Driver", style="blue")

        for name, cls in sorted(discovered.items()):
            info = _get_connector_info(name, cls)
            table.add_row(
                info["name"],
                info["type"],
                info["version"],
                info["default_driver_version"],
            )

        console.print(table)


@connectors_app.command("info")
def connector_info(
    name: str = typer.Argument(..., help="Connector name (e.g., postgresql)"),
) -> None:
    """Show detailed information about a specific connector.

    Args:
        name: The connector name to inspect.
    """
    ctx_obj = _get_ctx()
    output_format = ctx_obj.get("output", "human")
    console: Console = ctx_obj.get("console", Console())

    try:
        cls = ConnectorRegistry.get(name)
    except ValueError as e:
        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "connectors_info",
                        "status": "error",
                        "error": str(e),
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
        else:
            console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from e

    info = _get_connector_info(name, cls)

    if output_format == "json":
        result = {
            "command": "connectors_info",
            "connector": info,
        }
        sys.stdout.write(json.dumps(result) + "\n")
        sys.stdout.flush()
    else:
        console.print(f"\n[bold cyan]Connector: {info['name']}[/bold cyan]")
        console.print(f"  [bold]Type:[/bold]    {info['type']}")
        console.print(f"  [bold]Version:[/bold] {info['version']}")
        console.print(f"  [bold]Driver:[/bold]  {info['default_driver_version']}")
        if info["supported_db_versions"]:
            console.print(
                "  [bold]Supported DB versions:[/bold] "
                f"{', '.join(info['supported_db_versions'])}"
            )
        console.print()
