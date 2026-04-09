"""Schema commands — introspect database schemas (Section 10.1, 18.2)."""

from __future__ import annotations

import json
import os
import sys
from typing import Any, cast

import click
import typer
from rich.console import Console

from bani.cli.formatters import format_error, format_schema_table
from bani.connectors.base import SourceConnector
from bani.connectors.registry import ConnectorRegistry
from bani.domain.errors import BaniError
from bani.domain.project import ConnectionConfig

schema = typer.Typer(help="Schema inspection commands")


def _get_ctx() -> dict[str, Any]:
    """Retrieve context from the Typer/Click context chain."""
    try:
        ctx_obj = click.get_current_context().obj
    except RuntimeError:
        ctx_obj = None

    if ctx_obj is None:
        ctx_obj = {"output": "human", "console": Console()}
    return ctx_obj


@schema.command()
def inspect(
    connector: str = typer.Option(..., help="Connector name (e.g., postgresql, mysql)"),
    host: str = typer.Option("", help="Database host"),
    port: int = typer.Option(0, help="Database port"),
    database: str = typer.Option(..., help="Database name"),
    username_env: str = typer.Option(
        "", "--username-env",
        help="Environment variable for username",
    ),
    password_env: str = typer.Option(
        "", "--password-env",
        help="Environment variable for password",
    ),
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
    ctx_obj = _get_ctx()
    output_format = ctx_obj.get("output", "human")
    console: Console = ctx_obj.get("console", Console())

    # Get credentials from environment
    if username_env:
        username = os.environ.get(username_env)
        if not username:
            if output_format == "json":
                sys.stdout.write(
                    json.dumps(
                        {
                            "command": "schema_inspect",
                            "status": "error",
                            "error": f"Environment variable {username_env} not set",
                        }
                    )
                    + "\n"
                )
                sys.stdout.flush()
            else:
                console.print(f"[red]Error:[/red] {username_env} not set")
            raise typer.Exit(1)

    if password_env:
        password = os.environ.get(password_env)
        if not password:
            if output_format == "json":
                sys.stdout.write(
                    json.dumps(
                        {
                            "command": "schema_inspect",
                            "status": "error",
                            "error": f"Environment variable {password_env} not set",
                        }
                    )
                    + "\n"
                )
                sys.stdout.flush()
            else:
                console.print(f"[red]Error:[/red] {password_env} not set")
            raise typer.Exit(1)

    # Create and connect to source
    try:
        connector_class = ConnectorRegistry.get(connector)
        source_connector = cast(type[SourceConnector], connector_class)()
        config = ConnectionConfig(
            dialect=connector,
            host=host,
            port=port,
            database=database,
            username_env=username_env,
            password_env=password_env,
        )
        source_connector.connect(config)

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
                            "auto_increment": col.is_auto_increment,
                        }
                        for col in table.columns
                    ]
                    indexes_data = [
                        {
                            "name": idx.name,
                            "columns": list(idx.columns),
                            "unique": idx.is_unique,
                        }
                        for idx in table.indexes
                    ]
                    fk_data = [
                        {
                            "name": fk.name,
                            "columns": list(fk.source_columns),
                            "referenced_table": fk.referenced_table,
                            "referenced_columns": list(fk.referenced_columns),
                        }
                        for fk in table.foreign_keys
                    ]
                    tables_data.append(
                        {
                            "schema": table.schema_name,
                            "name": table.table_name,
                            "row_count_estimate": table.row_count_estimate,
                            "columns": columns_data,
                            "primary_key": list(table.primary_key),
                            "indexes": indexes_data,
                            "foreign_keys": fk_data,
                        }
                    )
                result = {
                    "command": "schema_inspect",
                    "connector": connector,
                    "tables": tables_data,
                }
                sys.stdout.write(json.dumps(result) + "\n")
                sys.stdout.flush()
            else:
                # Cast for display purposes
                format_schema_table(console, schema_obj)  # type: ignore[arg-type,unused-ignore]

        finally:
            source_connector.disconnect()

    except BaniError as e:
        format_error(console, e)
        raise typer.Exit(1) from e
    except Exception as e:
        if output_format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "command": "schema_inspect",
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
