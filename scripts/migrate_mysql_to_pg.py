#!/usr/bin/env python3
"""Migrate a MySQL database to PostgreSQL using Bani connectors.

Usage:
    # Set credentials as env vars, then run:
    export MYSQL_USER=bani_test MYSQL_PASS=bani_test
    export PG_USER=bani_test PG_PASS=bani_test
    python scripts/migrate_mysql_to_pg.py

    # Or override host/port/database via env:
    export MYSQL_HOST=localhost MYSQL_PORT=3306 MYSQL_DB=bani_test
    export PG_HOST=localhost PG_PORT=5433 PG_DB=bani_test
"""

from __future__ import annotations

import os
from dataclasses import replace as dc_replace

from bani.connectors.mysql.connector import MySQLConnector
from bani.connectors.postgresql.connector import PostgreSQLConnector
from bani.domain.project import ConnectionConfig
from bani.domain.schema import TableDefinition


def _remap_fqn(fqn: str, target_schema: str) -> str:
    """Replace the schema portion of a fully-qualified 'schema.table' name."""
    parts = fqn.split(".")
    return f"{target_schema}.{parts[-1]}"


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


def main() -> None:
    """Run the MySQL → PostgreSQL migration."""
    # ---- Configuration (env-driven with sensible defaults) ----
    mysql_host = os.environ.get("MYSQL_HOST", "localhost")
    mysql_port = int(os.environ.get("MYSQL_PORT", "3306"))
    mysql_db = os.environ.get("MYSQL_DB", "bani_test")

    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = int(os.environ.get("PG_PORT", "5433"))
    pg_db = os.environ.get("PG_DB", "bani_test")

    target_schema = "public"

    # ---- Connect ----
    source = MySQLConnector()
    source.connect(
        ConnectionConfig(
            dialect="mysql",
            host=mysql_host,
            port=mysql_port,
            database=mysql_db,
            username_env="MYSQL_USER",
            password_env="MYSQL_PASS",
        )
    )

    sink = PostgreSQLConnector()
    sink.connect(
        ConnectionConfig(
            dialect="postgresql",
            host=pg_host,
            port=pg_port,
            database=pg_db,
            username_env="PG_USER",
            password_env="PG_PASS",
        )
    )

    try:
        # ---- Step 1: Introspect MySQL schema ----
        print("Introspecting MySQL schema...")
        schema = source.introspect_schema()
        for t in schema.tables:
            print(
                f"  {t.table_name}: {len(t.columns)} cols, ~{t.row_count_estimate} rows"
            )

        # ---- Step 2: Remap schema and create tables on PG ----
        print("\nCreating tables on PostgreSQL...")
        remapped_tables: list[TableDefinition] = []
        for table in schema.tables:
            remapped = _remap_table(table, target_schema)
            sink.create_table(remapped)
            remapped_tables.append(remapped)
            print(f"  Created: {remapped.table_name}")

        # ---- Step 3: Transfer data table by table ----
        print("\nTransferring data...")
        for table in remapped_tables:
            row_count = 0
            for batch in source.read_table(table.table_name, mysql_db):
                sink.write_batch(table.table_name, table.schema_name, batch)
                row_count += batch.num_rows
            print(f"  {table.table_name}: {row_count} rows")

        # ---- Step 4: Create indexes ----
        print("\nCreating indexes...")
        for table in remapped_tables:
            if table.indexes:
                sink.create_indexes(
                    table.table_name,
                    table.schema_name,
                    table.indexes,
                )
                print(f"  {table.table_name}: {len(table.indexes)} indexes")

        # ---- Step 5: Create foreign keys ----
        print("\nCreating foreign keys...")
        for table in remapped_tables:
            if table.foreign_keys:
                sink.create_foreign_keys(table.foreign_keys)
                print(f"  {table.table_name}: {len(table.foreign_keys)} FKs")

        # ---- Verify ----
        print("\n--- Verification ---")
        for table in remapped_tables:
            count = sink.estimate_row_count(table.table_name, table.schema_name)
            print(f"  {table.table_name}: {count} rows in PostgreSQL")

        print("\nDone! MySQL -> PostgreSQL migration complete.")

    finally:
        source.disconnect()
        sink.disconnect()


if __name__ == "__main__":
    main()
