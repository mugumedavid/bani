"""Benchmark: PG→PG transfer throughput (Section 4.4).

Measures rows/second for:
  - Wide table: 50 columns, 1M rows
  - Narrow table: 5 columns, 5M rows

Run with:
  BENCH_PG_USER=x BENCH_PG_PASS=y pytest benchmarks/ -m benchmark -v
"""

from __future__ import annotations

import time
from typing import cast

import psycopg
import pytest

from bani.application.orchestrator import MigrationOrchestrator
from bani.connectors.base import SinkConnector, SourceConnector
from bani.connectors.registry import ConnectorRegistry
from bani.domain.project import ConnectionConfig, ProjectModel, ProjectOptions


def _pg_config(conninfo: str) -> ConnectionConfig:
    """Parse conninfo string into ConnectionConfig."""
    import os
    import re

    parts = dict(re.findall(r"(\w+)=(\S+)", conninfo))
    # Set env vars so the connector can resolve them
    os.environ["_BENCH_USER"] = parts.get("user", "")
    os.environ["_BENCH_PASS"] = parts.get("password", "")
    return ConnectionConfig(
        dialect="postgresql",
        host=parts.get("host", "localhost"),
        port=int(parts.get("port", "5432")),
        database=parts.get("dbname", "bani_test"),
        username_env="_BENCH_USER",
        password_env="_BENCH_PASS",
    )


def _seed_wide_table(conninfo: str, n_rows: int = 1_000_000) -> None:
    """Create and seed the wide benchmark table (50 columns)."""
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS bench_source.bench_wide CASCADE")
            cur.execute("CREATE SCHEMA IF NOT EXISTS bench_source")
            cur.execute("CREATE SCHEMA IF NOT EXISTS bench_target")

            cols = []
            for i in range(10):
                cols.append(f"int_col_{i} INTEGER")
            for i in range(10):
                cols.append(f"varchar_col_{i} VARCHAR(255)")
            for i in range(10):
                cols.append(f"decimal_col_{i} DECIMAL(12,4)")
            for i in range(10):
                cols.append(f"ts_col_{i} TIMESTAMP")
            for i in range(5):
                cols.append(f"text_col_{i} TEXT")
            for i in range(5):
                cols.append(f"bool_col_{i} BOOLEAN")

            cur.execute(
                f"CREATE TABLE bench_source.bench_wide ("
                f"id SERIAL PRIMARY KEY, {', '.join(cols)})"
            )

            # Bulk insert using generate_series
            set_clauses = []
            for i in range(10):
                set_clauses.append(f"i * {i + 1}")  # int
            for i in range(10):
                set_clauses.append(f"'value_' || i || '_{i}'")  # varchar
            for i in range(10):
                set_clauses.append(f"(i * 0.{i + 1}1)::decimal(12,4)")  # decimal
            for i in range(10):
                set_clauses.append(f"NOW() - (i || ' seconds')::interval")  # ts
            for i in range(5):
                set_clauses.append(
                    f"repeat('x', 50 + (i % 150))"
                )  # text ~200 chars avg
            for i in range(5):
                set_clauses.append(f"(i % {i + 2} = 0)")  # bool

            cur.execute(
                f"INSERT INTO bench_source.bench_wide "
                f"({', '.join(c.split()[0] for c in cols)}) "
                f"SELECT {', '.join(set_clauses)} "
                f"FROM generate_series(1, {n_rows}) AS i"
            )
            cur.execute("ANALYZE bench_source.bench_wide")


def _seed_narrow_table(conninfo: str, n_rows: int = 5_000_000) -> None:
    """Create and seed the narrow benchmark table (5 columns)."""
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS bench_source.bench_narrow CASCADE")
            cur.execute("CREATE SCHEMA IF NOT EXISTS bench_source")
            cur.execute("CREATE SCHEMA IF NOT EXISTS bench_target")

            cur.execute("""
                CREATE TABLE bench_source.bench_narrow (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    value DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT NOW(),
                    active BOOLEAN DEFAULT TRUE
                )
            """)
            cur.execute(
                f"INSERT INTO bench_source.bench_narrow (name, value, active) "
                f"SELECT 'row_' || i, random() * 1000, (i % 3 != 0) "
                f"FROM generate_series(1, {n_rows}) AS i"
            )
            cur.execute("ANALYZE bench_source.bench_narrow")


def _run_migration(
    config: ConnectionConfig,
    source_schema: str,
    target_schema: str,
    table_name: str,
    workers: int = 1,
) -> tuple[int, float]:
    """Run a PG→PG migration and return (rows, seconds)."""
    from bani.domain.project import TableMapping

    project = ProjectModel(
        name="benchmark",
        source=config,
        target=config,
        options=ProjectOptions(
            batch_size=100_000,
            parallel_workers=workers,
        ),
        table_mappings=(
            TableMapping(
                source_schema=source_schema,
                source_table=table_name,
            ),
        ),
    )

    pool_size = workers
    source = cast(SourceConnector, ConnectorRegistry.get("postgresql")())
    source.connect(config, pool_size=pool_size)
    sink = cast(SinkConnector, ConnectorRegistry.get("postgresql")())
    sink.connect(config, pool_size=pool_size)

    try:
        orchestrator = MigrationOrchestrator(project, source, sink)
        start = time.perf_counter()
        result = orchestrator.execute()
        elapsed = time.perf_counter() - start
        return result.total_rows_written, elapsed
    finally:
        source.disconnect()
        sink.disconnect()


@pytest.mark.benchmark()
class TestWideTableThroughput:
    """Benchmark: 50-column table, 1M rows."""

    @pytest.fixture(autouse=True)
    def _seed(self, pg_conninfo: str) -> None:
        _seed_wide_table(pg_conninfo, n_rows=100_000)  # 100k for CI speed

    def test_single_worker(self, pg_conninfo: str) -> None:
        config = _pg_config(pg_conninfo)
        rows, elapsed = _run_migration(
            config, "bench_source", "bench_target", "bench_wide", workers=1
        )
        throughput = rows / elapsed if elapsed > 0 else 0
        print(f"\n  Wide table (1 worker): {rows:,} rows in {elapsed:.2f}s "
              f"= {throughput:,.0f} rows/sec")
        assert rows > 0

    def test_multi_worker(self, pg_conninfo: str) -> None:
        config = _pg_config(pg_conninfo)
        rows, elapsed = _run_migration(
            config, "bench_source", "bench_target", "bench_wide", workers=4
        )
        throughput = rows / elapsed if elapsed > 0 else 0
        print(f"\n  Wide table (4 workers): {rows:,} rows in {elapsed:.2f}s "
              f"= {throughput:,.0f} rows/sec")
        assert rows > 0


@pytest.mark.benchmark()
class TestNarrowTableThroughput:
    """Benchmark: 5-column table, 5M rows."""

    @pytest.fixture(autouse=True)
    def _seed(self, pg_conninfo: str) -> None:
        _seed_narrow_table(pg_conninfo, n_rows=500_000)  # 500k for CI speed

    def test_single_worker(self, pg_conninfo: str) -> None:
        config = _pg_config(pg_conninfo)
        rows, elapsed = _run_migration(
            config, "bench_source", "bench_target", "bench_narrow", workers=1
        )
        throughput = rows / elapsed if elapsed > 0 else 0
        print(f"\n  Narrow table (1 worker): {rows:,} rows in {elapsed:.2f}s "
              f"= {throughput:,.0f} rows/sec")
        assert rows > 0

    def test_multi_worker(self, pg_conninfo: str) -> None:
        config = _pg_config(pg_conninfo)
        rows, elapsed = _run_migration(
            config, "bench_source", "bench_target", "bench_narrow", workers=4
        )
        throughput = rows / elapsed if elapsed > 0 else 0
        print(f"\n  Narrow table (4 workers): {rows:,} rows in {elapsed:.2f}s "
              f"= {throughput:,.0f} rows/sec")
        assert rows > 0
