"""Benchmark fixtures — shared setup for performance tests.

Requires environment variables:
  BENCH_PG_USER, BENCH_PG_PASS  — PostgreSQL credentials
  BENCH_PG_HOST (default: localhost), BENCH_PG_PORT (default: 5433)
  BENCH_PG_DB (default: bani_test)
"""

from __future__ import annotations

import os

import pytest


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        pytest.skip(f"{name} not set — skipping benchmark")
    return val


@pytest.fixture()
def pg_conninfo() -> str:
    """PostgreSQL connection string from environment."""
    user = _require_env("BENCH_PG_USER")
    password = _require_env("BENCH_PG_PASS")
    host = os.environ.get("BENCH_PG_HOST", "localhost")
    port = os.environ.get("BENCH_PG_PORT", "5433")
    database = os.environ.get("BENCH_PG_DB", "bani_test")
    return f"host={host} port={port} dbname={database} user={user} password={password}"
