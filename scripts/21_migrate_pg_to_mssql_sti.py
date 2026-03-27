#!/usr/bin/env python3
"""Real-world migration: PostgreSQL (DHIS STI) -> MSSQL (STI_SOURCE).

Migrates all tables from a PostgreSQL DHIS instance to a MSSQL staging
database.  This script targets live infrastructure, not the local
docker-compose stack.

Source: 192.168.100.46:5434/dhis_sti (PostgreSQL)
Target: 192.168.100.46:1433/STI_SOURCE (MSSQL)

Usage:
    python scripts/21_migrate_pg_to_mssql_sti.py
"""
from __future__ import annotations

import os
import sys
from dataclasses import replace as dc_replace

from bani.connectors.postgresql.connector import PostgreSQLConnector
from bani.connectors.mssql.connector import MSSQLConnector
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
    # Drop PG-specific check constraints — they use syntax MSSQL can't parse
    return dc_replace(
        table,
        schema_name=target_schema,
        foreign_keys=remapped_fks,
        check_constraints=(),
    )


def main() -> int:
    """PostgreSQL (dhis_sti) -> MSSQL (STI_SOURCE) migration."""
    # Inject credentials into env so connectors can resolve them
    os.environ["SOURCE_USER"] = "postgres"
    os.environ["SOURCE_PASS"] = "Password@123"
    os.environ["TARGET_USER"] = "sa"
    os.environ["TARGET_PASS"] = "Password@123"

    source = PostgreSQLConnector()
    source.connect(
        ConnectionConfig(
            dialect="postgresql",
            host="192.168.100.46",
            port=5434,
            database="dhis_sti",
            username_env="SOURCE_USER",
            password_env="SOURCE_PASS",
        )
    )

    sink = MSSQLConnector()
    sink.connect(
        ConnectionConfig(
            dialect="mssql",
            host="192.168.100.46",
            port=1433,
            database="STI_SOURCE",
            username_env="TARGET_USER",
            password_env="TARGET_PASS",
        )
    )

    source_schema = "public"
    target_schema = "dbo"

    try:
        print("PostgreSQL (dhis_sti) -> MSSQL (STI_SOURCE) migration")
        print("=" * 55)

        print("Introspecting PostgreSQL schema...")
        schema = source.introspect_schema()

        print(f"Found {len(schema.tables)} tables")
        for tbl in schema.tables:
            print(
                f"  {tbl.table_name}: {len(tbl.columns)} cols, "
                f"~{tbl.row_count_estimate} rows"
            )

        print("\nCreating tables on MSSQL...")
        remapped: list[TableDefinition] = []
        for table in schema.tables:
            if not table.columns:
                print(f"  skipped {table.table_name} (no columns)")
                continue
            r = _remap_table(table, target_schema)
            try:
                sink.create_table(r)
            except Exception as exc:
                print(f"\n  FAILED on table: {r.table_name}", file=sys.stderr)
                for col in r.columns:
                    print(
                        f"    {col.name}: {col.data_type} "
                        f"(arrow={col.arrow_type_str}) "
                        f"default={col.default_value!r}",
                        file=sys.stderr,
                    )
                # Rebuild the SQL so we can see exactly what was sent
                from bani.connectors.mssql.type_mapper import MSSQLTypeMapper
                from bani.connectors.mssql.connector import _extract_char_length
                from bani.connectors.default_translation import translate_default
                parts = []
                for col in r.columns:
                    if col.arrow_type_str:
                        mt = MSSQLTypeMapper.from_arrow_type(col.arrow_type_str)
                        if mt == "NVARCHAR(MAX)" and col.data_type:
                            ln = _extract_char_length(col.data_type)
                            if ln is not None and ln <= 4000:
                                mt = f"NVARCHAR({ln})"
                    else:
                        mt = col.data_type
                    p = f"[{col.name}] {mt}"
                    if not col.nullable:
                        p += " NOT NULL"
                    if col.is_auto_increment:
                        p += " IDENTITY(1,1)"
                    elif col.default_value:
                        td = translate_default(col.default_value, "mssql", mt)
                        if td is not None:
                            p += f" DEFAULT {td}"
                    parts.append(p)
                if r.primary_key:
                    pk = ", ".join(f"[{c}]" for c in r.primary_key)
                    parts.append(f"PRIMARY KEY ({pk})")
                sql = (
                    f"CREATE TABLE [{r.schema_name}].[{r.table_name}] "
                    f"({', '.join(parts)})"
                )
                print(f"\n  Generated SQL:\n  {sql}\n", file=sys.stderr)
                raise exc
            remapped.append(r)
            print(f"  created {r.table_name}")

        print("\nTransferring data...")
        for table in remapped:
            has_identity = any(c.is_auto_increment for c in table.columns)
            if has_identity:
                with sink.connection.cursor() as _cur:
                    _cur.execute(
                        f"SET IDENTITY_INSERT [{target_schema}].[{table.table_name}] ON"
                    )
            row_count = 0
            for batch in source.read_table(
                table.table_name, source_schema, batch_size=1000
            ):
                sink.write_batch(table.table_name, target_schema, batch)
                row_count += batch.num_rows
            if has_identity:
                with sink.connection.cursor() as _cur:
                    _cur.execute(
                        f"SET IDENTITY_INSERT [{target_schema}].[{table.table_name}] OFF"
                    )
            print(f"  {table.table_name}: {row_count} rows")

        print("\nCreating indexes...")
        for table in remapped:
            if table.indexes:
                sink.create_indexes(table.table_name, target_schema, table.indexes)
                print(f"  {table.table_name}: {len(table.indexes)} indexes")

        print("\nCreating foreign keys...")
        for table in remapped:
            if table.foreign_keys:
                sink.create_foreign_keys(table.foreign_keys)
                print(f"  {table.table_name}: {len(table.foreign_keys)} FKs")

        print("\n--- Verification ---")
        for table in remapped:
            count = sink.estimate_row_count(table.table_name, target_schema)
            print(f"  {table.table_name}: {count} rows in MSSQL")

        print("\n" + "=" * 55)
        print("Migration complete!")
        return 0

    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        return 1
    finally:
        source.disconnect()
        sink.disconnect()


if __name__ == "__main__":
    sys.exit(main())
