"""Bani CLI application (Section 10).

Main entry point for the Bani command-line interface. Uses Typer to
define commands and subcommands, with Rich for formatted output.
"""

from __future__ import annotations

import sys

import typer
from rich.console import Console

from bani import __version__
from bani.cli.commands.connectors import connectors_app
from bani.cli.commands.init import init as init_command
from bani.cli.commands.mcp_cmd import app as mcp_app
from bani.cli.commands.preview import preview as preview_command
from bani.cli.commands.run import run as run_command
from bani.cli.commands.schedule import schedule as schedule_command
from bani.cli.commands.schema import schema as schema_group
from bani.cli.commands.ui import ui as ui_command
from bani.cli.commands.validate import validate as validate_command
from bani.cli.commands.version import version as version_command

# Global console instance
console = Console()

app = typer.Typer(
    name="bani",
    help="An open-source database migration engine powered by Apache Arrow",
    no_args_is_help=True,
)


def _version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        console.print(f"bani {__version__}")
        raise typer.Exit()


@app.callback()
def _app_main(
    ctx: typer.Context,
    version: bool = typer.Option(
        False,
        "--version",
        help="Show version and exit.",
        callback=_version_callback,
        is_eager=True,
    ),
    output: str = typer.Option(
        "human",
        "--output",
        "-o",
        help="Output format: 'human' or 'json'.",
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet",
        "-q",
        help="Suppress progress output.",
    ),
    log_level: str = typer.Option(
        "info",
        "--log-level",
        help="Logging level: debug, info, warn, error.",
    ),
) -> None:
    """Main Bani CLI application.

    Global options are available to all commands.
    """
    # Store options in context state so subcommands can access them
    ctx.obj = {
        "output": output,
        "quiet": quiet,
        "log_level": log_level,
        "console": console,
    }


# Register subcommands
app.command()(run_command)
app.command()(validate_command)
app.command()(preview_command)
app.command(name="init")(init_command)
app.command(name="schedule")(schedule_command)
app.command(name="version")(version_command)
app.add_typer(schema_group, name="schema")
app.add_typer(connectors_app, name="connectors")
app.add_typer(mcp_app, name="mcp")
app.command(name="ui")(ui_command)


def main() -> None:
    """Entry point for the CLI."""
    try:
        app()
    except KeyboardInterrupt:
        console.print("[red]Interrupted by user[/red]")
        sys.exit(1)
    except Exception as e:
        # Uncaught exceptions are printed by Typer, but we can customize here
        console.print(f"[red]Fatal error: {e}[/red]")
        sys.exit(1)
