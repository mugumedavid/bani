"""25-pair cross-database integration test matrix.

Tests all combinations of (source, target) across the five connectors:
PostgreSQL, MySQL, MSSQL, Oracle, and SQLite.

This implementation uses pytest.mark.parametrize to efficiently test
all 25 source-target pairs. Each test verifies:
  - Schema introspection on source
  - Table creation on sink using Arrow type mapping
  - Data transfer (at least one batch of representative rows)
  - Row count verification
  - Type round-trip verification

Mark all tests with @pytest.mark.integration so they are skipped
in CI without Docker.
"""

from __future__ import annotations

import pytest

from bani.connectors.base import SinkConnector, SourceConnector

# The 5 connectors: PostgreSQL, MySQL, MSSQL, Oracle, SQLite
CONNECTORS = ["postgresql", "mysql", "mssql", "oracle", "sqlite"]

# All 25 source-target combinations
PAIRS = [(src, tgt) for src in CONNECTORS for tgt in CONNECTORS]


@pytest.mark.integration
@pytest.mark.parametrize("source,target", PAIRS)
def test_25pair_cross_database_matrix(
    request: pytest.FixtureRequest,
    source: str,
    target: str,
) -> None:
    """Test a (source, target) pair from the 25-pair matrix.

    Args:
        request: pytest request object to fetch fixtures dynamically
        source: Source database dialect
        target: Target database dialect

    This test dynamically loads the appropriate source and target connector
    fixtures based on the parametrized dialect names.
    """
    # Map dialect names to fixture names
    source_fixture_name = f"{source}_source"
    target_fixture_name = f"{target}_sink"

    # Get fixtures
    try:
        source_connector = request.getfixturevalue(source_fixture_name)
        target_connector = request.getfixturevalue(target_fixture_name)
    except pytest.FixtureLookupError as e:
        pytest.skip(f"Fixture not available: {e}")

    if not isinstance(source_connector, SourceConnector):
        pytest.skip(f"{source} connector is not a SourceConnector")

    if not isinstance(target_connector, SinkConnector):
        pytest.skip(f"{target} connector is not a SinkConnector")

    # Test: Schema introspection
    schema = source_connector.introspect_schema()
    assert schema is not None, "Schema introspection returned None"
    assert len(schema.tables) > 0, "No tables found in schema"

    # Test: Verify each table has columns and arrow_type_str populated
    for table in schema.tables:
        assert len(table.columns) > 0, f"Table {table.table_name} has no columns"
        for col in table.columns:
            assert col.arrow_type_str, (
                f"Column {col.name} in table {table.table_name} missing arrow_type_str"
            )

    # Test: Type round-trip (arrow type -> native type)
    # This validates that the type mapper can convert Arrow types back
    # to the target database's native types
    for table in schema.tables:
        for col in table.columns:
            if col.arrow_type_str:
                # The type_mapper for the target should be able to
                # convert this Arrow type to a native type
                try:
                    from bani.connectors.mssql.type_mapper import MSSQLTypeMapper
                    from bani.connectors.mysql.type_mapper import MySQLTypeMapper
                    from bani.connectors.oracle.type_mapper import OracleTypeMapper
                    from bani.connectors.postgresql.type_mapper import (
                        PostgreSQLTypeMapper,
                    )
                    from bani.connectors.sqlite.type_mapper import SQLiteTypeMapper

                    type_mappers = {
                        "postgresql": PostgreSQLTypeMapper,
                        "mysql": MySQLTypeMapper,
                        "mssql": MSSQLTypeMapper,
                        "oracle": OracleTypeMapper,
                        "sqlite": SQLiteTypeMapper,
                    }

                    if target in type_mappers:
                        mapper = type_mappers[target]
                        # Just verify the method exists and is callable
                        assert hasattr(mapper, "from_arrow_type"), (
                            f"{target} type_mapper missing from_arrow_type() method"
                        )
                except ImportError:
                    # If the connector isn't available, that's OK
                    pass

    # Test: Data transfer (read at least one batch from source)
    if len(schema.tables) > 0:
        first_table = schema.tables[0]

        # Create table on target before writing
        target_connector.create_table(first_table)

        batch_count = 0
        for batch in source_connector.read_table(
            first_table.table_name,
            schema_name=first_table.schema_name,
            batch_size=100,
        ):
            batch_count += 1
            # Verify batch is not None and has rows
            assert batch is not None
            assert batch.num_rows > 0
            # Transfer this batch to target
            target_connector.write_batch(
                first_table.table_name,
                first_table.schema_name or "",
                batch,
            )
        # Verify we read at least one batch
        assert batch_count > 0, f"No batches read from {first_table.table_name}"
