#!/usr/bin/env python3
"""Generic cross-database migration script using Bani connectors.

Supports migration between any combination of supported database engines:
PostgreSQL, MySQL, MSSQL, Oracle, and SQLite.

Usage:
    python scripts/migrate.py \\
        --source-dialect postgresql \\
        --target-dialect mysql \\
        [--source-host localhost] \\
        [--source-port 5432] \\
        [--source-database mydb] \\
        [--target-host localhost] \\
        [--target-port 3306] \\
        [--target-database mydb] \\
        [--target-schema public]

Environment variables:
    Source:   {SOURCE_DIALECT}_HOST, {SOURCE_DIALECT}_PORT,
              {SOURCE_DIALECT}_USER, {SOURCE_DIALECT}_PASS
    Target:   {TARGET_DIALECT}_HOST, {TARGET_DIALECT}_PORT,
              {TARGET_DIALECT}_USER, {TARGET_DIALECT}_PASS
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import replace as dc_replace
from typing import TYPE_CHECKING

from bani.domain.project import ConnectionConfig
from bani.domain.schema import TableDefinition

if TYPE_CHECKING:
    from bani.connectors.base import SinkConnector, SourceConnector


def _get_connector(
    dialect: str, is_source: bool = True
) -> SourceConnector | SinkConnector:
    """Factory function to create a connector for the given dialect."""
    if dialect == "postgresql":
        from bani.connectors.postgresql.connector import PostgreSQLConnector

        return PostgreSQLConnector()
    elif dialect == "mysql":
        from bani.connectors.mysql.connector import MySQLConnector

        return MySQLConnector()
    elif dialect == "mssql":
        from bani.connectors.mssql.connector import MSSQLConnector

        return MSSQLConnector()
    elif dialect == "oracle":
        from bani.connectors.oracle.connector import OracleConnector

        return OracleConnector()
    elif dialect == "sqlite":
        from bani.connectors.sqlite.connector import SQLiteConnector

        return SQLiteConnector()
    else:
        raise ValueError(f"Unknown dialect: {dialect}")


def _build_config(
    dialect: str,
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    service_name: str | None = None,
) -> ConnectionConfig:
    """Build a ConnectionConfig from CLI arguments and environment variables."""
    dialect_upper = dialect.upper()

    # Get from env or use CLI arg
    final_host = host or os.environ.get(f"{dialect_upper}_HOST", "localhost")
    final_port = port or int(os.environ.get(f"{dialect_upper}_PORT", "0"))
    final_db = database or os.environ.get(f"{dialect_upper}_DATABASE")

    config_kwargs = {
        "dialect": dialect,
        "host": final_host,
        "username_env": f"{dialect_upper}_USER",
        "password_env": f"{dialect_upper}_PASS",
    }

    if dialect != "sqlite":
        if final_port <= 0:
            # Use default port for the dialect
            defaults = {
                "postgresql": 5432,
                "mysql": 3306,
                "mssql": 1433,
                "oracle": 1521,
            }
            final_port = defaults.get(dialect, 5432)

        config_kwargs["port"] = final_port

        if final_db:
            if dialect == "oracle":
                config_kwargs["service_name"] = final_db
            else:
                config_kwargs["database"] = final_db
    else:
        # SQLite needs a database path
        config_kwargs["database"] = final_db or "migration.db"

    return ConnectionConfig(**config_kwargs)  # type: ignore


def _remap_fqn(fqn: str, target_schema: str) -> str:
    """Replace the schema portion of a fully-qualified 'schema.table' name."""
    parts = fqn.split(".")
    if len(parts) > 1:
        return f"{target_schema}.{parts[-1]}"
    return f"{target_schema}.{fqn}"


def _remap_table(
    table: TableDefinition,
    target_schema: str,
) -> TableDefinition:
    """Remap a table and its FK FQNs to the target schema."""
    remapped_fks = tuple(
        dc_replace(
            fk,
            source_table=_remap_fqn(fk.source_table, target_schema),
            referenced_table=_remap_fqn(fk.referenced_table, target_schema),
        )
        for fk in table.foreign_keys
    )
    return dc_replace(
        table,
        schema_name=target_schema,
        foreign_keys=remapped_fks,
    )


def main() -> int:
    """Execute the generic cross-database migration."""
    parser = argparse.ArgumentParser(
        description="Migrate data between any two supported databases using Bani."
    )

    # Source arguments
    parser.add_argument(
        "--source-dialect",
        required=True,
        help="Source database dialect: postgresql, mysql, mssql, oracle, sqlite",
    )
    parser.add_argument("--source-host", help="Source database host")
    parser.add_argument("--source-port", type=int, help="Source database port")
    parser.add_argument("--source-database", help="Source database name")

    # Target arguments
    parser.add_argument(
        "--target-dialect",
        required=True,
        help="Target database dialect: postgresql, mysql, mssql, oracle, sqlite",
    )
    parser.add_argument("--target-host", help="Target database host")
    parser.add_argument("--target-port", type=int, help="Target database port")
    parser.add_argument("--target-database", help="Target database name")
    parser.add_argument(
        "--target-schema",
        default="public",
        help="Target schema name (default: public)",
    )

    # Migration options
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for data transfer (default: 1000)",
    )

    args = parser.parse_args()

    print(f"Bani Cross-Database Migration")
    print(f"{'=' * 50}")
    print(f"Source: {args.source_dialect}")
    print(f"Target: {args.target_dialect} (schema: {args.target_schema})")
    print()

    # Create connectors
    try:
        source = _get_connector(args.source_dialect, is_source=True)
        sink = _get_connector(args.target_dialect, is_source=False)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    # Build connection configs
    source_config = _build_config(
        args.source_dialect,
        host=args.source_host,
        port=args.source_port,
        database=args.source_database,
    )

    target_config = _build_config(
        args.target_dialect,
        host=args.target_host,
        port=args.target_port,
        database=args.target_database,
    )

    try:
        # Connect
        print("Connecting to source database...")
        source.connect(source_config)
        print("✓ Source connected")

        print("Connecting to target database...")
        sink.connect(target_config)
        print("✓ Target connected")

        # Introspect
        print("\nIntrospecting source schema...")
        schema = source.read_schema()
        if not schema:
            print("Error: Failed to introspect source schema")
            return 1

        print(f"Found {len(schema.tables)} tables:")
        for table in schema.tables:
            print(
                f"  {table.table_name}: {len(table.columns)} cols, "
                f"~{table.row_count_estimate} rows"
            )

        # Create tables
        print("\nCreating tables on target...")
        remapped_tables: list[TableDefinition] = []
        for table in schema.tables:
            remapped = _remap_table(table, args.target_schema)
            sink.create_table(remapped)
            remapped_tables.append(remapped)
            print(f"  ✓ {remapped.table_name}")

        # Transfer data
        print("\nTransferring data...")
        for table in remapped_tables:
            print(f"  {table.table_name}...", end=" ", flush=True)
            row_count = 0
            for batch in source.read_batches(
                table.table_name,
                batch_size=args.batch_size,
                schema_name=table.schema_name,
            ):
                sink.write_batch(
                    table.table_name,
                    batch,
                    schema_name=args.target_schema,
                )
                row_count += batch.num_rows
            print(f"{row_count} rows")

        # Create indexes
        print("\nCreating indexes...")
        for table in remapped_tables:
            if table.indexes:
                sink.create_indexes(
                    table.table_name,
                    args.target_schema,
                    table.indexes,
                )
                print(f"  ✓ {table.table_name}: {len(table.indexes)} indexes")

        # Create foreign keys
        print("\nCreating foreign keys...")
        for table in remapped_tables:
            if table.foreign_keys:
                sink.create_foreign_keys(table.foreign_keys)
                print(f"  ✓ {table.table_name}: {len(table.foreign_keys)} foreign keys")

        print("\n" + "=" * 50)
        print("Migration complete! ✓")
        return 0

    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        return 1

    finally:
        print("\nCleaning up...")
        source.disconnect()
        sink.disconnect()


if __name__ == "__main__":
    sys.exit(main())
