#!/usr/bin/env python3
"""Migrate a SQLite database to MySQL using Bani connectors.

Usage:
    docker compose up -d mysql
    export MYSQL_USER=bani_test MYSQL_PASS='bani_test'
    python scripts/06_migrate_sqlite_to_mysql.py
"""
from __future__ import annotations

import os
import sys
from dataclasses import replace as dc_replace

from bani.connectors.sqlite.connector import SQLiteConnector
from bani.connectors.mysql.connector import MySQLConnector
from bani.domain.project import ConnectionConfig
from bani.domain.schema import TableDefinition


def _remap_fqn(fqn: str, target_schema: str) -> str:
    parts = fqn.split(".")
    return f"{target_schema}.{parts[-1]}"


def _remap_table(table: TableDefinition, target_schema: str) -> TableDefinition:
    remapped_fks = tuple(
        dc_replace(
            fk,
            source_table=_remap_fqn(fk.source_table, target_schema),
            referenced_table=_remap_fqn(fk.referenced_table, target_schema),
        )
        for fk in table.foreign_keys
    )
    return dc_replace(table, schema_name=target_schema, foreign_keys=remapped_fks)


def main() -> int:
    """SQLite -> MySQL migration."""
    source = SQLiteConnector()
    source.connect(
        ConnectionConfig(
            dialect="sqlite",
            host="",
            port=0,
            database=os.environ.get("SQLITE_DB", "target.db"),
            username_env="SQLITE_USER",
            password_env="SQLITE_PASS",
        )
    )

    sink = MySQLConnector()
    sink.connect(
        ConnectionConfig(
            dialect="mysql",
            host=os.environ.get("MYSQL_HOST", "localhost"),
            port=int(os.environ.get("MYSQL_PORT", "3306")),
            database=os.environ.get("MYSQL_DB", "bani_test"),
            username_env="MYSQL_USER",
            password_env="MYSQL_PASS",
        )
    )

    source_schema = "main"
    target_schema = os.environ.get("MYSQL_DB", "bani_test")

    try:
        print("SQLite -> MySQL migration")
        print("=" * 50)

        print("Introspecting SQLite schema...")
        schema = source.introspect_schema()

        print(f"Found {len(schema.tables)} tables")
        for tbl in schema.tables:
            print(
                f"  {tbl.table_name}: {len(tbl.columns)} cols, "
                f"~{tbl.row_count_estimate} rows"
            )

        print("\nCreating tables on MySQL...")
        remapped: list[TableDefinition] = []
        for table in schema.tables:
            r = _remap_table(table, target_schema)
            sink.create_table(r)
            remapped.append(r)
            print(f"  ✓ {r.table_name}")

        print("\nTransferring data...")
        # Disable FK checks during data transfer to avoid insert-order issues
        with sink.connection.cursor() as _cur:
            _cur.execute("SET FOREIGN_KEY_CHECKS=0")
        for table in remapped:
            row_count = 0
            for batch in source.read_table(
                table.table_name, source_schema, batch_size=1000
            ):
                sink.write_batch(table.table_name, target_schema, batch)
                row_count += batch.num_rows
            print(f"  {table.table_name}: {row_count} rows")

        with sink.connection.cursor() as _cur:
            _cur.execute("SET FOREIGN_KEY_CHECKS=1")

        print("\nCreating indexes...")
        for table in remapped:
            if table.indexes:
                sink.create_indexes(table.table_name, target_schema, table.indexes)
                print(f"  ✓ {table.table_name}: {len(table.indexes)} indexes")

        print("\nCreating foreign keys...")
        for table in remapped:
            if table.foreign_keys:
                sink.create_foreign_keys(table.foreign_keys)
                print(f"  ✓ {table.table_name}: {len(table.foreign_keys)} FKs")

        print("\n--- Verification ---")
        for table in remapped:
            count = sink.estimate_row_count(table.table_name, target_schema)
            print(f"  {table.table_name}: {count} rows in MySQL")

        print("\n" + "=" * 50)
        print("Migration complete! ✓")
        return 0

    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        return 1
    finally:
        source.disconnect()
        sink.disconnect()


if __name__ == "__main__":
    sys.exit(main())
