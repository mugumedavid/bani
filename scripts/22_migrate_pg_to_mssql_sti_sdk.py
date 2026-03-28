#!/usr/bin/env python3
"""Real-world migration: PostgreSQL (DHIS STI) -> MSSQL (STI_SOURCE).

Same migration as script 21, but uses Bani's SDK features:
  - ProjectBuilder for declarative project configuration
  - ConnectorRegistry for dialect-driven connector discovery
  - DependencyResolver for FK-safe table ordering
  - ProgressTracker for structured progress events

Source: 192.168.100.46:5434/dhis_sti (PostgreSQL)
Target: 192.168.100.46:1433/STI_SOURCE (MSSQL)

Usage:
    python scripts/22_migrate_pg_to_mssql_sti_sdk.py
"""
from __future__ import annotations

import os
import sys
import time
from dataclasses import replace as dc_replace
from typing import cast

from bani.application.progress import ProgressTracker
from bani.connectors.base import SinkConnector, SourceConnector
from bani.connectors.registry import ConnectorRegistry
from bani.domain.dependency import DependencyResolver
from bani.domain.schema import DatabaseSchema, TableDefinition
from bani.sdk.project_builder import ProjectBuilder


def _remap_for_mssql(schema: DatabaseSchema, target_schema: str) -> DatabaseSchema:
    """Remap a PG-introspected schema for MSSQL compatibility.

    - Rewrites schema_name (e.g. public -> dbo)
    - Strips PG-specific check constraints
    - Drops tables with no columns
    - Remaps FK references to the target schema
    """

    def _remap_fqn(fqn: str) -> str:
        return f"{target_schema}.{fqn.split('.')[-1]}"

    tables: list[TableDefinition] = []
    for t in schema.tables:
        if not t.columns:
            continue
        remapped_fks = tuple(
            dc_replace(
                fk,
                source_table=_remap_fqn(fk.source_table),
                referenced_table=_remap_fqn(fk.referenced_table),
            )
            for fk in t.foreign_keys
        )
        tables.append(
            dc_replace(
                t,
                schema_name=target_schema,
                foreign_keys=remapped_fks,
                check_constraints=(),
            )
        )

    return DatabaseSchema(tables=tuple(tables), source_dialect=schema.source_dialect)


def main() -> int:
    """Run the migration using Bani SDK features."""

    # -- 1. Declarative project via ProjectBuilder -----------------------
    os.environ["SOURCE_USER"] = "postgres"
    os.environ["SOURCE_PASS"] = "Password@123"
    os.environ["TARGET_USER"] = "sa"
    os.environ["TARGET_PASS"] = "Password@123"

    project = (
        ProjectBuilder("dhis_sti_to_mssql")
        .source(
            "postgresql",
            host="192.168.100.46",
            port=5434,
            database="dhis_sti",
            username_env="SOURCE_USER",
            password_env="SOURCE_PASS",
        )
        .target(
            "mssql",
            host="192.168.100.46",
            port=1433,
            database="STI_SOURCE",
            username_env="TARGET_USER",
            password_env="TARGET_PASS",
        )
        .batch_size(1000)
        .build()
    )

    # -- 2. Connector discovery via registry -----------------------------
    assert project.source is not None and project.target is not None

    source = cast(SourceConnector, ConnectorRegistry.get(project.source.dialect)())
    source.connect(project.source)

    sink = cast(SinkConnector, ConnectorRegistry.get(project.target.dialect)())
    sink.connect(project.target)

    # Show which MSSQL driver is active
    mssql_driver = getattr(sink, "_driver", "unknown")
    print(f"MSSQL driver: {mssql_driver}"
          f"{' (fast_executemany)' if mssql_driver == 'pyodbc' else ' (inline INSERT)'}")

    tracker = ProgressTracker()
    start = time.time()

    try:
        print(f"Project: {project.name}")
        print("=" * 55)

        # -- 3. Introspect & remap --------------------------------------
        print("Introspecting PostgreSQL schema...")
        raw_schema = source.introspect_schema()
        schema = _remap_for_mssql(raw_schema, target_schema="dbo")

        print(f"Found {len(schema.tables)} tables")
        for tbl in schema.tables:
            print(
                f"  {tbl.table_name}: {len(tbl.columns)} cols, "
                f"~{tbl.row_count_estimate} rows"
            )

        # -- 4. Dependency-safe ordering ---------------------------------
        resolver = DependencyResolver()
        resolution = resolver.resolve(schema)
        ordered = resolution.ordered_tables
        deferred_fks = resolution.deferred_fks
        print(f"\nTable order resolved ({len(ordered)} tables, "
              f"{len(deferred_fks)} deferred FKs)")

        tracker.migration_started(
            project_name=project.name,
            source_dialect="postgresql",
            target_dialect="mssql",
            table_count=len(ordered),
        )

        # Build a lookup from FQN -> TableDefinition
        table_map = {t.fully_qualified_name: t for t in schema.tables}

        # -- 5. Create tables in dependency order ------------------------
        print("\nCreating tables on MSSQL...")
        for fqn in ordered:
            table = table_map.get(fqn)
            if table is None:
                continue
            try:
                sink.create_table(table)
                print(f"  created {table.table_name}")
            except Exception as exc:
                print(f"  FAILED {table.table_name}: {exc}", file=sys.stderr)
                raise

        # -- 6. Transfer data (with IDENTITY INSERT handling) ------------
        print("\nTransferring data...")
        total_rows = 0
        source_schema = "public"

        for fqn in ordered:
            table = table_map.get(fqn)
            if table is None:
                continue

            try:
                has_identity = any(c.is_auto_increment for c in table.columns)
                if has_identity:
                    with sink.connection.cursor() as cur:  # type: ignore[union-attr]
                        cur.execute(
                            f"SET IDENTITY_INSERT [{table.schema_name}]"
                            f".[{table.table_name}] ON"
                        )

                row_count = 0
                batch_num = 0
                for batch in source.read_table(
                    table.table_name, source_schema, batch_size=1000
                ):
                    sink.write_batch(table.table_name, table.schema_name, batch)
                    row_count += batch.num_rows
                    batch_num += 1

                if has_identity:
                    with sink.connection.cursor() as cur:  # type: ignore[union-attr]
                        cur.execute(
                            f"SET IDENTITY_INSERT [{table.schema_name}]"
                            f".[{table.table_name}] OFF"
                        )

                tracker.table_complete(fqn, row_count, row_count, batch_num)
                total_rows += row_count
                print(f"  {table.table_name}: {row_count} rows")
            except Exception as exc:
                print(f"  {table.table_name}: SKIPPED ({exc})")
                continue

        # -- 7. Create indexes -------------------------------------------
        print("\nCreating indexes...")
        for table in schema.tables:
            if table.indexes:
                try:
                    sink.create_indexes(
                        table.table_name, table.schema_name, table.indexes
                    )
                    print(f"  {table.table_name}: {len(table.indexes)} indexes")
                except Exception as exc:
                    print(f"  {table.table_name}: FAILED ({exc})", file=sys.stderr)

        # -- 8. Create foreign keys (non-deferred first, then deferred) --
        deferred_names = {fk.name for fk in deferred_fks}
        print("\nCreating foreign keys...")
        for table in schema.tables:
            # Skip FKs that the resolver deferred (they go in the second pass)
            non_deferred = tuple(
                fk for fk in table.foreign_keys if fk.name not in deferred_names
            )
            if non_deferred:
                try:
                    sink.create_foreign_keys(non_deferred)
                    print(f"  {table.table_name}: {len(non_deferred)} FKs")
                except Exception as exc:
                    print(f"  {table.table_name}: FAILED ({exc})", file=sys.stderr)

        if deferred_fks:
            print(f"  Creating {len(deferred_fks)} deferred FKs...")
            sink.create_foreign_keys(deferred_fks)

        # -- 9. Verification ---------------------------------------------
        duration = time.time() - start
        tracker.migration_complete(
            project_name=project.name,
            tables_completed=len(ordered),
            tables_failed=0,
            total_rows_read=total_rows,
            total_rows_written=total_rows,
            duration_seconds=duration,
        )

        print(f"\n{'=' * 55}")
        print(f"Migration complete: {len(ordered)} tables, "
              f"{total_rows} rows in {duration:.1f}s")
        return 0

    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        return 1
    finally:
        source.disconnect()
        sink.disconnect()


if __name__ == "__main__":
    sys.exit(main())
