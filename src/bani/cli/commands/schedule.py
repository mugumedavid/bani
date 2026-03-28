"""Schedule command — register migrations with the OS scheduler (Section 10.1)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click
import typer
from rich.console import Console
from rich.table import Table

from bani.cli.formatters import format_error
from bani.domain.errors import SchedulerError
from bani.infra.os_scheduler import OSSchedulerBridge


def _get_ctx() -> dict[str, Any]:
    """Retrieve context from the Typer/Click context chain."""
    try:
        ctx_obj = click.get_current_context().obj
    except RuntimeError:
        ctx_obj = None

    if ctx_obj is None:
        ctx_obj = {"output": "human", "quiet": False, "console": Console()}
    return ctx_obj


schedule_app = typer.Typer(help="Manage scheduled migrations")


@schedule_app.command("register")
def schedule_register(
    project_file: str = typer.Argument(..., help="Path to BDL project file"),
    cron: str = typer.Option(..., "--cron", help='Cron expression (e.g., "0 2 * * *")'),
    timezone: str = typer.Option("UTC", "--timezone", help="IANA timezone (default: UTC)"),
) -> None:
    """Register a migration with the OS scheduler.

    Uses crontab on Linux/macOS to schedule recurring migration runs.

    Args:
        project_file: Path to the BDL project file.
        cron: Standard 5-field cron expression.
        timezone: IANA timezone.
    """
    ctx_obj = _get_ctx()
    output_format = ctx_obj.get("output", "human")
    console: Console = ctx_obj.get("console", Console())

    project_path = Path(project_file)
    if not project_path.exists():
        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "schedule",
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

    try:
        OSSchedulerBridge.register(
            project_path=str(project_path.resolve()),
            cron_expr=cron,
            timezone=timezone,
        )

        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "schedule",
                        "status": "ok",
                        "action": "register",
                        "project": str(project_path),
                        "cron": cron,
                        "timezone": timezone,
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
        else:
            console.print(
                f"[bold green]✓ Scheduled:[/bold green] {project_path}\n"
                f"  [bold]Cron:[/bold]     {cron}\n"
                f"  [bold]Timezone:[/bold] {timezone}"
            )
    except SchedulerError as e:
        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "schedule",
                        "status": "error",
                        "error": str(e),
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
        else:
            format_error(console, e)
        raise typer.Exit(1) from e


@schedule_app.command("unregister")
def schedule_unregister(
    project_name: str = typer.Argument(..., help="Project name to unregister"),
) -> None:
    """Unregister a migration from the OS scheduler.

    Args:
        project_name: The project name (file stem used during registration).
    """
    ctx_obj = _get_ctx()
    output_format = ctx_obj.get("output", "human")
    console: Console = ctx_obj.get("console", Console())

    try:
        OSSchedulerBridge.unregister(project_name)

        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "schedule",
                        "status": "ok",
                        "action": "unregister",
                        "project_name": project_name,
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
        else:
            console.print(
                f"[bold green]✓ Unregistered:[/bold green] {project_name}"
            )
    except SchedulerError as e:
        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "schedule",
                        "status": "error",
                        "error": str(e),
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
        else:
            format_error(console, e)
        raise typer.Exit(1) from e


@schedule_app.command("list")
def schedule_list() -> None:
    """List all Bani-managed scheduled migrations."""
    ctx_obj = _get_ctx()
    output_format = ctx_obj.get("output", "human")
    console: Console = ctx_obj.get("console", Console())

    try:
        entries = OSSchedulerBridge.list_registered()

        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "schedule_list",
                        "status": "ok",
                        "schedules": entries,
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
        else:
            if not entries:
                console.print("[yellow]No scheduled migrations found.[/yellow]")
                return

            table = Table(title="Scheduled Migrations")
            table.add_column("Project", style="cyan")
            table.add_column("Cron Entry", style="magenta")

            for entry in entries:
                table.add_row(
                    entry.get("project_name", ""),
                    entry.get("cron_entry", ""),
                )

            console.print(table)
    except SchedulerError as e:
        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "schedule_list",
                        "status": "error",
                        "error": str(e),
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
        else:
            format_error(console, e)
        raise typer.Exit(1) from e


def schedule(
    project_file: str = typer.Argument(..., help="Path to BDL project file"),
    cron: str = typer.Option(..., "--cron", help='Cron expression (e.g., "0 2 * * *")'),
    timezone: str = typer.Option("UTC", "--timezone", help="IANA timezone (default: UTC)"),
) -> None:
    """Register a migration with the OS scheduler.

    Uses crontab on Linux/macOS to schedule recurring migration runs.

    Args:
        project_file: Path to the BDL project file.
        cron: Standard 5-field cron expression.
        timezone: IANA timezone.
    """
    ctx_obj = _get_ctx()
    output_format = ctx_obj.get("output", "human")
    console: Console = ctx_obj.get("console", Console())

    project_path = Path(project_file)
    if not project_path.exists():
        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "schedule",
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

    try:
        OSSchedulerBridge.register(
            project_path=str(project_path.resolve()),
            cron_expr=cron,
            timezone=timezone,
        )

        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "schedule",
                        "status": "ok",
                        "project": str(project_path),
                        "cron": cron,
                        "timezone": timezone,
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
        else:
            console.print(
                f"[bold green]✓ Scheduled:[/bold green] {project_path}\n"
                f"  [bold]Cron:[/bold]     {cron}\n"
                f"  [bold]Timezone:[/bold] {timezone}"
            )
    except SchedulerError as e:
        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "schedule",
                        "status": "error",
                        "error": str(e),
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
        else:
            format_error(console, e)
        raise typer.Exit(1) from e
