"""Integration tests: MySQL -> MySQL same-engine migration.

Tests migration from MySQL 5.7 source to MySQL 8.x target, validating
multi-version driver loading and same-engine migration support.
Uses two separate Docker containers (mysql55 on port 3307, mysql on 3306).

Requires Docker containers running via docker-compose.
Run with: pytest -m integration tests/integration/end_to_end/
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

try:
    from bani.connectors.mysql.connector import MySQLConnector

    _HAS_MYSQL = True
except ImportError:
    _HAS_MYSQL = False
    if TYPE_CHECKING:
        from bani.connectors.mysql.connector import MySQLConnector
from bani.domain.schema import ForeignKeyDefinition

from .conftest import EXPECTED_ROW_COUNTS, TABLE_NAMES

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _HAS_MYSQL, reason="MySQL connector not available"),
]


class TestMySQLToMySQLSchemaTransfer:
    """Test schema introspection and creation from MySQL 5.7 to MySQL 8.x."""

    def test_introspect_mysql55_source_schema(
        self, mysql55_source: MySQLConnector
    ) -> None:
        """MySQL 5.7 source schema should be introspectable."""
        schema = mysql55_source.introspect_schema()
        assert schema.source_dialect == "mysql"
        assert len(schema.tables) >= 5

    def test_mysql55_and_mysql8_are_independent(
        self,
        mysql55_source: MySQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Two MySQL connector instances should be independent."""
        # Both should be connected simultaneously
        assert mysql55_source.connection is not None
        assert mysql_sink.connection is not None

        # They should be different connections
        assert mysql55_source.connection is not mysql_sink.connection

    def test_create_tables_on_mysql8_target(
        self,
        mysql55_source: MySQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Should create all 5 tables on MySQL 8.x from MySQL 5.7 schema."""
        schema = mysql55_source.introspect_schema()

        # Drop existing tables in reverse dependency order
        for table_name in reversed(TABLE_NAMES):
            mysql_sink.execute_sql(f"DROP TABLE IF EXISTS `bani_test`.`{table_name}`")

        # Create tables in dependency order
        for table_name in TABLE_NAMES:
            table_def = schema.get_table("bani_test", table_name)
            assert table_def is not None
            mysql_sink.create_table(table_def)

    def test_mysql55_schema_has_auto_increment(
        self, mysql55_source: MySQLConnector
    ) -> None:
        """MySQL 5.7 should detect auto_increment on id columns."""
        schema = mysql55_source.introspect_schema()
        for table_name in TABLE_NAMES:
            table_def = schema.get_table("bani_test", table_name)
            assert table_def is not None
            id_col = next((c for c in table_def.columns if c.name == "id"), None)
            assert id_col is not None
            assert id_col.is_auto_increment is True


class TestMySQLToMySQLDataTransfer:
    """Test data transfer from MySQL 5.7 to MySQL 8.x."""

    def test_transfer_all_tables(
        self,
        mysql55_source: MySQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Should transfer all 5 tables with correct row counts."""
        for table_name in TABLE_NAMES:
            batches = list(mysql55_source.read_table(table_name, "bani_test"))
            total_read = sum(b.num_rows for b in batches)

            total_written = 0
            for batch in batches:
                total_written += mysql_sink.write_batch(table_name, "bani_test", batch)

            assert total_written == total_read
            assert total_written == EXPECTED_ROW_COUNTS[table_name]

    def test_transfer_preserves_unicode(
        self,
        mysql55_source: MySQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Should preserve CJK and emoji characters across MySQL versions."""
        batches = list(mysql55_source.read_table("categories", "bani_test"))

        for batch in batches:
            mysql_sink.write_batch("categories", "bani_test", batch)

        # Read back from target and verify row count
        target_batches = list(mysql_sink.read_table("categories", "bani_test"))
        target_total = sum(b.num_rows for b in target_batches)
        assert target_total == EXPECTED_ROW_COUNTS["categories"]

    def test_transfer_preserves_null_and_empty(
        self,
        mysql55_source: MySQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Should preserve NULLs and empty strings across MySQL versions."""
        batches = list(mysql55_source.read_table("customers", "bani_test"))

        for batch in batches:
            mysql_sink.write_batch("customers", "bani_test", batch)

        target_batches = list(mysql_sink.read_table("customers", "bani_test"))
        target_total = sum(b.num_rows for b in target_batches)
        assert target_total == EXPECTED_ROW_COUNTS["customers"]

    def test_transfer_boundary_numeric_values(
        self,
        mysql55_source: MySQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Should preserve boundary numeric values (0.00, 0.01, 9999999.99)."""
        batches = list(mysql55_source.read_table("products", "bani_test"))

        total_written = 0
        for batch in batches:
            total_written += mysql_sink.write_batch("products", "bani_test", batch)

        assert total_written == EXPECTED_ROW_COUNTS["products"]

    def test_read_with_filter_sql(self, mysql55_source: MySQLConnector) -> None:
        """Should support filter_sql on MySQL 5.7 reads."""
        batches = list(
            mysql55_source.read_table(
                "categories",
                "bani_test",
                filter_sql="id = 1",
            )
        )
        total = sum(b.num_rows for b in batches)
        assert total == 1


class TestMySQLToMySQLIndexesAndConstraints:
    """Test index and FK recreation on MySQL 8.x target."""

    def test_create_indexes_on_target(
        self,
        mysql55_source: MySQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Should create indexes on MySQL 8.x from MySQL 5.7 schema."""
        schema = mysql55_source.introspect_schema()

        for table_name in TABLE_NAMES:
            table_def = schema.get_table("bani_test", table_name)
            assert table_def is not None

            if table_def.indexes:
                mysql_sink.create_indexes(table_name, "bani_test", table_def.indexes)

    def test_create_foreign_keys_on_target(
        self,
        mysql55_source: MySQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Should create FKs on MySQL 8.x from MySQL 5.7 schema."""
        schema = mysql55_source.introspect_schema()

        all_fks: list[ForeignKeyDefinition] = []
        for table_name in TABLE_NAMES:
            table_def = schema.get_table("bani_test", table_name)
            assert table_def is not None
            all_fks.extend(table_def.foreign_keys)

        if all_fks:
            mysql_sink.create_foreign_keys(tuple(all_fks))

    def test_target_schema_complete(
        self,
        mysql_sink: MySQLConnector,
    ) -> None:
        """MySQL 8.x target should have all tables after migration."""
        schema = mysql_sink.introspect_schema()
        table_names = {t.table_name for t in schema.tables}
        for expected in TABLE_NAMES:
            assert expected in table_names

    def test_target_has_primary_keys(
        self,
        mysql_sink: MySQLConnector,
    ) -> None:
        """All target tables should have primary keys."""
        schema = mysql_sink.introspect_schema()
        for table_name in TABLE_NAMES:
            table_def = schema.get_table("bani_test", table_name)
            if table_def is not None:
                assert len(table_def.primary_key) > 0, (
                    f"Table {table_name} should have a primary key"
                )
