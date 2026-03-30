#!/usr/bin/env python3
"""Real-world migration: PostgreSQL (DHIS STI) -> MSSQL (STI_SOURCE).

Uses the same code path as the UI/API: ProjectBuilder -> BaniProject.run()
which delegates to MigrationOrchestrator.execute().

Source: 172.20.10.4:5434/dhis_sti (PostgreSQL)
Target: 172.20.10.4:1433/STI_SOURCE (MSSQL)

Usage:
    python scripts/22_migrate_pg_to_mssql_sti_sdk.py
"""
from __future__ import annotations

import os
import sys
from typing import Any

from bani.application.progress import (
    BatchComplete,
    MigrationComplete,
    MigrationStarted,
    TableComplete,
    TableStarted,
)
from bani.sdk.bani import BaniProject
from bani.sdk.project_builder import ProjectBuilder


def _on_progress(event: Any) -> None:
    """Print progress events to stdout."""
    if isinstance(event, MigrationStarted):
        print(f"\n[migration] {event.source_dialect} -> {event.target_dialect}, "
              f"{event.table_count} tables")
    elif isinstance(event, TableStarted):
        est = f" (~{event.estimated_rows} rows)" if event.estimated_rows else ""
        print(f"  [table] {event.table_name} started{est}")
    elif isinstance(event, BatchComplete):
        print(f"    [batch] {event.table_name} batch {event.batch_number}: "
              f"{event.rows_written} rows written")
    elif isinstance(event, TableComplete):
        print(f"  [table] {event.table_name} done: "
              f"{event.total_rows_written} rows, {event.batch_count} batches")
    elif isinstance(event, MigrationComplete):
        print(f"\n[migration] done in {event.duration_seconds:.1f}s — "
              f"{event.tables_completed} ok, {event.tables_failed} failed, "
              f"{event.total_rows_written} rows")


def main() -> int:
    """Run the migration using the same core path as the API."""

    os.environ["SOURCE_USER"] = "postgres"
    os.environ["SOURCE_PASS"] = "Password@123"
    os.environ["TARGET_USER"] = "sa"
    os.environ["TARGET_PASS"] = "Password@123"

    project = (
        ProjectBuilder("dhis_sti_to_mssql")
        .source(
            "postgresql",
            host="172.20.10.4",
            port=5434,
            database="dhis_sti",
            username_env="SOURCE_USER",
            password_env="SOURCE_PASS",
        )
        .target(
            "mssql",
            host="172.20.10.4",
            port=1433,
            database="STI_SOURCE",
            username_env="TARGET_USER",
            password_env="TARGET_PASS",
        )
        .batch_size(1000)
        .build()
    )

    bp = BaniProject(project)

    is_valid, errors = bp.validate()
    if not is_valid:
        print(f"Validation failed: {'; '.join(errors)}", file=sys.stderr)
        return 1

    print(f"Project: {project.name}")
    print("=" * 55)

    result = bp.run(on_progress=_on_progress)

    print(f"\n{'=' * 55}")
    print(f"Tables completed: {result.tables_completed}")
    print(f"Tables failed:    {result.tables_failed}")
    print(f"Rows read:        {result.total_rows_read}")
    print(f"Rows written:     {result.total_rows_written}")
    print(f"Duration:         {result.duration_seconds:.1f}s")

    if result.errors:
        print(f"\nErrors ({len(result.errors)}):")
        for err in result.errors:
            print(f"  - {err}")

    return 0 if result.tables_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
