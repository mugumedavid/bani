"""25-pair cross-database integration test matrix.

Tests all combinations of (source, target) across the five connectors:
PostgreSQL, MySQL, MSSQL, Oracle, and SQLite.

Uses the MigrationOrchestrator to test the real migration path:
introspect source → create tables on sink → transfer data → verify
row counts. This exercises the same code path as a real migration.

Mark all tests with @pytest.mark.integration so they are skipped
by default.
"""

from __future__ import annotations

import pytest

from bani.application.orchestrator import MigrationOrchestrator, MigrationResult
from bani.connectors.base import SinkConnector, SourceConnector
from bani.domain.project import ConnectionConfig, ProjectModel, ProjectOptions

from .conftest import EXPECTED_ROW_COUNTS

# The 5 connectors: PostgreSQL, MySQL, MSSQL, Oracle, SQLite
CONNECTORS = ["postgresql", "mysql", "mssql", "oracle", "sqlite"]

# All 25 source-target combinations
PAIRS = [(src, tgt) for src in CONNECTORS for tgt in CONNECTORS]

# Dummy ConnectionConfig for building ProjectModel.
# The orchestrator uses the already-connected connectors passed
# directly, so these configs are only needed for project metadata.
_DUMMY_CFG = ConnectionConfig(dialect="dummy")


@pytest.mark.integration
@pytest.mark.parametrize("source,target", PAIRS)
def test_25pair_cross_database_matrix(
    request: pytest.FixtureRequest,
    source: str,
    target: str,
) -> None:
    """Test a (source, target) pair using the MigrationOrchestrator.

    Runs a full migration: introspect → create schema → transfer data.
    Verifies that all tables are created and row counts match.
    """
    source_fixture = f"{source}_source"
    target_fixture = f"{target}_sink"

    try:
        source_conn = request.getfixturevalue(source_fixture)
        target_conn = request.getfixturevalue(target_fixture)
    except pytest.FixtureLookupError as exc:
        pytest.skip(f"Fixture not available: {exc}")

    if not isinstance(source_conn, SourceConnector):
        pytest.skip(f"{source} is not a SourceConnector")
    if not isinstance(target_conn, SinkConnector):
        pytest.skip(f"{target} is not a SinkConnector")

    # Build a minimal project model for the orchestrator
    project = ProjectModel(
        name=f"test-{source}-to-{target}",
        description=f"Integration test: {source} → {target}",
        source=ConnectionConfig(dialect=source),
        target=ConnectionConfig(dialect=target),
        options=ProjectOptions(
            batch_size=100,
            parallel_workers=1,
            transfer_indexes=False,
            transfer_foreign_keys=False,
        ),
    )

    # Run migration through the orchestrator
    orchestrator = MigrationOrchestrator(
        project=project,
        source=source_conn,
        sink=target_conn,
    )
    result: MigrationResult = orchestrator.execute()

    # Verify: no table failures
    assert result.tables_failed == 0, (
        f"Migration {source}→{target} had "
        f"{result.tables_failed} table failures: "
        f"{result.errors}"
    )

    # Verify: tables were migrated
    assert result.tables_completed > 0, (
        f"Migration {source}→{target} completed 0 tables"
    )

    # Verify: row counts match expected
    assert result.total_rows_read == result.total_rows_written, (
        f"Row mismatch: read={result.total_rows_read}, "
        f"written={result.total_rows_written}"
    )

    # Verify: total rows match the fixture data
    expected_total = sum(EXPECTED_ROW_COUNTS.values())
    assert result.total_rows_written == expected_total, (
        f"Expected {expected_total} rows, "
        f"got {result.total_rows_written}"
    )
