"""Schema inspect command — introspects database schemas (Section 10.3)."""

from __future__ import annotations

import json
import os
from typing import Any

import click
import typer
from rich.console import Console

from bani.cli.formatters import format_error, format_schema_table
from bani.connectors.registry import ConnectorRegistry
from bani.domain.errors import BaniError

schema = typer.Typer(help="Schema inspection commands")


@schema.command()
def inspect(
    connector: str = typer.Option(..., help="Connector name (e.g., postgresql, mysql)"),
    host: str = typer.Option(..., help="Database host"),
    port: int = typer.Option(..., help="Database port"),
    database: str = typer.Option(..., help="Database name"),
    username_env: str = typer.Option(..., help="Environment variable for username"),
    password_env: str = typer.Option(..., help="Environment variable for password"),
    schema_filter: str | None = typer.Option(
        None, "--schema", help="Filter by schema name (optional)"
    ),
    table_filter: str | None = typer.Option(
        None, "--table", help="Filter by table name (optional)"
    ),
) -> None:
    """Introspect a database schema.

    Connects to a database, introspects the schema, and displays tables,
    columns, indexes, and constraints.

    Args:
        connector: Connector dialect name.
        host: Database host.
        port: Database port.
        database: Database name.
        username_env: Environment variable name for username.
        password_env: Environment variable name for password.
        schema_filter: Optional schema filter.
        table_filter: Optional table filter.
    """
    try:
        ctx_obj = click.get_current_context().obj
    except RuntimeError:
        ctx_obj = None

    if ctx_obj is None:
        ctx_obj = {"output": "human", "console": Console()}

    output_format = ctx_obj.get("output", "human")
    console: Console = ctx_obj.get("console", Console())

    # Get credentials from environment
    try:
        username = os.environ.get(username_env)
        password = os.environ.get(password_env)

        if not username:
            console.print(f"[red]Error:[/red] {username_env} not set")
            raise typer.Exit(1)
        if not password:
            console.print(f"[red]Error:[/red] {password_env} not set")
            raise typer.Exit(1)
    except Exception as e:
        format_error(console, e)
        raise typer.Exit(1) from e

    # Create and connect to source
    try:
        registry = ConnectorRegistry()
        source_connector = registry.create_source_connector(
            connector,
            host=host,
            port=port,
            database=database,
            username_env=username_env,
            password_env=password_env,
        )
        source_connector.connect()

        try:
            schema_obj = source_connector.introspect_schema()

            # Filter if requested
            if schema_filter or table_filter:
                filtered_tables = []
                for table in schema_obj.tables:
                    if schema_filter and table.schema_name != schema_filter:
                        continue
                    if table_filter and table.table_name != table_filter:
                        continue
                    filtered_tables.append(table)

                # Create a simple wrapper object with the filtered tables
                class FilteredSchema:
                    def __init__(self, tables: tuple[Any, ...], dialect: str) -> None:
                        self.tables = tables
                        self.source_dialect = dialect
                        self.get_table = schema_obj.get_table

                filtered_schema: Any = FilteredSchema(
                    tuple(filtered_tables), schema_obj.source_dialect
                )
                schema_obj = filtered_schema

            # Output
            if output_format == "json":
                tables_data = []
                for table in schema_obj.tables:
                    columns_data = [
                        {
                            "name": col.name,
                            "type": col.data_type,
                            "nullable": col.nullable,
                            "default": col.default_value,
                        }
                        for col in table.columns
                    ]
                    tables_data.append(
                        {
                            "schema": table.schema_name,
                            "table": table.table_name,
                            "columns": columns_data,
                            "row_count_estimate": table.row_count_estimate,
                            "indexes": len(table.indexes),
                            "foreign_keys": len(table.foreign_keys),
                        }
                    )
                result = {
                    "command": "schema_inspect",
                    "connector": connector,
                    "tables": tables_data,
                }
                console.print(json.dumps(result))
            else:
                # Cast for display purposes
                format_schema_table(console, schema_obj)  # type: ignore[arg-type,unused-ignore]

        finally:
            source_connector.close()

    except BaniError as e:
        format_error(console, e)
        raise typer.Exit(1) from e
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1) from e
