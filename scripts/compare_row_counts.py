"""Compare actual row counts between pg17 source and Oracle target.

Usage:
    python scripts/compare_row_counts.py <oracle_connection_key>

Example:
    python scripts/compare_row_counts.py oracle23c
"""

import os
import sys

# Bootstrap env vars from connections.json
from bani.infra.connections import ConnectionRegistry

for key, conn in ConnectionRegistry.load().items():
    ConnectionRegistry.to_connection_config(conn)


def count_source() -> dict[str, int]:
    """Count all rows in pg17 source tables."""
    import psycopg

    conn = psycopg.connect(
        host="DAVIDMUGUMED4C3.local",
        port=5434,
        dbname="dhis_sti",
        user=os.environ["_BANI_CONN_pg17_USER"],
        password=os.environ["_BANI_CONN_pg17_PASS"],
        sslmode="prefer",
    )
    cursor = conn.cursor()

    # Get all user tables
    cursor.execute("""
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY tablename
    """)
    tables = cursor.fetchall()

    counts: dict[str, int] = {}
    for schema, table in tables:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            count = cursor.fetchone()[0]
            counts[table] = count
        except Exception as e:
            print(f"  [source] Error counting {schema}.{table}: {e}")
            conn.rollback()

    conn.close()
    return counts


def count_target(connection_key: str) -> dict[str, int]:
    """Count all rows in Oracle target tables."""
    import oracledb

    # Check if thick mode needed
    conn_entry = ConnectionRegistry.get(connection_key)
    config = ConnectionRegistry.to_connection_config(conn_entry)

    # Init thick mode if needed
    for k, v in config.extra:
        if k == "oracle_client_lib" and v:
            from bani.connectors.oracle.connector import _init_thick_mode
            _init_thick_mode(v)

    # Build connection kwargs
    connect_kwargs = {
        "host": config.host,
        "port": config.port or 1521,
        "user": os.environ.get(config.username_env, config.username_env),
        "password": os.environ.get(config.password_env, config.password_env),
    }

    # service_name vs SID
    service_name = None
    for k, v in config.extra:
        if k == "service_name":
            service_name = v
    if service_name:
        connect_kwargs["service_name"] = service_name
    elif config.database:
        connect_kwargs["sid"] = config.database

    conn = oracledb.connect(**connect_kwargs)
    cursor = conn.cursor()

    # Get owner (connected user)
    cursor.execute("SELECT USER FROM DUAL")
    owner = cursor.fetchone()[0]

    # Get all tables owned by user
    cursor.execute(
        "SELECT table_name FROM all_tables WHERE owner = :o ORDER BY table_name",
        {"o": owner},
    )
    tables = cursor.fetchall()

    counts: dict[str, int] = {}
    for (table,) in tables:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{owner}"."{table}"')
            count = cursor.fetchone()[0]
            counts[table] = count
        except Exception as e:
            print(f"  [target] Error counting {owner}.{table}: {e}")

    conn.close()
    return counts


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts/compare_row_counts.py <oracle_connection_key>")
        sys.exit(1)

    target_key = sys.argv[1]
    print(f"Counting source (pg17) rows...")
    src = count_source()
    print(f"  Found {len(src)} tables, {sum(src.values()):,} total rows\n")

    print(f"Counting target ({target_key}) rows...")
    tgt = count_target(target_key)
    print(f"  Found {len(tgt)} tables, {sum(tgt.values()):,} total rows\n")

    # Compare — match by lowercase name
    tgt_lower = {k.lower(): (k, v) for k, v in tgt.items()}

    mismatches = []
    missing_in_target = []
    total_src = 0
    total_tgt = 0

    for src_table, src_count in sorted(src.items()):
        total_src += src_count
        tgt_entry = tgt_lower.get(src_table.lower())
        if tgt_entry is None:
            if src_count > 0:
                missing_in_target.append((src_table, src_count))
        else:
            tgt_name, tgt_count = tgt_entry
            total_tgt += tgt_count
            if src_count != tgt_count:
                mismatches.append((src_table, src_count, tgt_count))

    print("=" * 70)
    print(f"TOTALS:  Source={total_src:,}  Target={total_tgt:,}  "
          f"Diff={total_src - total_tgt:,}")
    print("=" * 70)

    if mismatches:
        print(f"\nMISMATCHES ({len(mismatches)} tables):")
        print(f"{'Table':<50} {'Source':>10} {'Target':>10} {'Diff':>10}")
        print("-" * 80)
        for table, sc, tc in sorted(mismatches, key=lambda x: x[1] - x[2], reverse=True):
            print(f"{table:<50} {sc:>10,} {tc:>10,} {sc - tc:>10,}")
    else:
        print("\nNo mismatches — all row counts match!")

    if missing_in_target:
        print(f"\nMISSING FROM TARGET ({len(missing_in_target)} tables):")
        for table, count in missing_in_target:
            print(f"  {table} ({count:,} rows)")

    print()


if __name__ == "__main__":
    main()
