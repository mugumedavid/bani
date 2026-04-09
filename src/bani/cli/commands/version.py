"""Version command — show Bani and connector versions (Section 10.1)."""

from __future__ import annotations

import json
import sys
from typing import Any

import click
from rich.console import Console
from rich.table import Table

from bani import __version__
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


def version() -> None:
    """Show Bani version and installed connector versions."""
    ctx_obj = _get_ctx()
    output_format = ctx_obj.get("output", "human")
    console: Console = ctx_obj.get("console", Console())

    connectors = ConnectorRegistry.discover()

    connector_versions: list[dict[str, str]] = []
    for name, cls in sorted(connectors.items()):
        ver = getattr(cls, "VERSION", "1.0.0")
        driver_ver = getattr(cls, "DRIVER_VERSION", "unknown")
        connector_versions.append(
            {
                "name": name,
                "version": str(ver),
                "driver_version": str(driver_ver),
            }
        )

    if output_format == "json":
        result = {
            "command": "version",
            "bani_version": __version__,
            "connectors": connector_versions,
        }
        sys.stdout.write(json.dumps(result) + "\n")
        sys.stdout.flush()
    else:
        console.print(f"[bold cyan]bani[/bold cyan] {__version__}\n")

        if connector_versions:
            table = Table(title="Installed Connectors")
            table.add_column("Connector", style="cyan")
            table.add_column("Version", style="green")
            table.add_column("Driver", style="blue")

            for conn in connector_versions:
                table.add_row(
                    conn["name"],
                    conn["version"],
                    conn["driver_version"],
                )

            console.print(table)
        else:
            console.print("[yellow]No connectors installed.[/yellow]")
