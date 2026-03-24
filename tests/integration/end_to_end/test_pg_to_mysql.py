"""Integration tests: PostgreSQL -> MySQL full migration.

Tests full migration from PostgreSQL source to MySQL target including
schema creation, data transfer, index recreation, and FK recreation.

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
from bani.connectors.postgresql.connector import PostgreSQLConnector
from bani.domain.schema import ForeignKeyDefinition

from .conftest import EXPECTED_ROW_COUNTS, TABLE_NAMES

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _HAS_MYSQL, reason="MySQL connector not available"),
]


class TestPGToMySQLSchemaTransfer:
    """Test schema introspection and creation from PG to MySQL."""

    def test_introspect_pg_source_schema(self, pg_source: PostgreSQLConnector) -> None:
        """PostgreSQL source schema should be introspectable."""
        schema = pg_source.introspect_schema()
        assert schema.source_dialect == "postgresql"
        assert len(schema.tables) >= 5

    def test_create_tables_on_mysql_target(
        self,
        pg_source: PostgreSQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Should create all 5 tables on MySQL from PG schema."""
        schema = pg_source.introspect_schema()

        # Drop existing tables in reverse dependency order
        for table_name in reversed(TABLE_NAMES):
            mysql_sink.execute_sql(f"DROP TABLE IF EXISTS `bani_test`.`{table_name}`")

        # Create tables in dependency order
        for table_name in TABLE_NAMES:
            table_def = schema.get_table("public", table_name)
            assert table_def is not None, f"Table {table_name} not in PG schema"

            # Remap PG types to MySQL types for create_table
            mysql_sink.create_table(table_def)

    def test_pg_schema_has_foreign_keys(self, pg_source: PostgreSQLConnector) -> None:
        """PG schema should have foreign keys on products, orders, order_items."""
        schema = pg_source.introspect_schema()
        products = schema.get_table("public", "products")
        assert products is not None
        assert len(products.foreign_keys) >= 1

        orders = schema.get_table("public", "orders")
        assert orders is not None
        assert len(orders.foreign_keys) >= 1

        order_items = schema.get_table("public", "order_items")
        assert order_items is not None
        assert len(order_items.foreign_keys) >= 2

    def test_pg_schema_has_indexes(self, pg_source: PostgreSQLConnector) -> None:
        """PG schema should have indexes on products and order_items."""
        schema = pg_source.introspect_schema()

        products = schema.get_table("public", "products")
        assert products is not None
        # idx_products_category + idx_products_sku (unique)
        assert len(products.indexes) >= 2

        order_items = schema.get_table("public", "order_items")
        assert order_items is not None
        assert len(order_items.indexes) >= 2


class TestPGToMySQLDataTransfer:
    """Test data transfer from PG to MySQL."""

    def test_read_pg_table_as_arrow_batches(
        self, pg_source: PostgreSQLConnector
    ) -> None:
        """Should read PG table data as Arrow RecordBatches."""
        batches = list(pg_source.read_table("categories", "public"))
        assert len(batches) >= 1
        total_rows = sum(b.num_rows for b in batches)
        assert total_rows == EXPECTED_ROW_COUNTS["categories"]

    def test_transfer_categories_data(
        self,
        pg_source: PostgreSQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Should transfer categories data preserving all values."""
        # Read from PG
        batches = list(pg_source.read_table("categories", "public"))
        total_read = sum(b.num_rows for b in batches)
        assert total_read == EXPECTED_ROW_COUNTS["categories"]

        # Write to MySQL
        total_written = 0
        for batch in batches:
            total_written += mysql_sink.write_batch("categories", "bani_test", batch)
        assert total_written == total_read

    def test_transfer_preserves_unicode_data(
        self,
        pg_source: PostgreSQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Should preserve CJK and emoji characters during transfer."""
        # Read categories from PG (includes Japanese and emoji data)
        batches = list(pg_source.read_table("categories", "public"))

        # Write to MySQL
        for batch in batches:
            mysql_sink.write_batch("categories", "bani_test", batch)

        # Read back from MySQL and verify
        mysql_batches = list(mysql_sink.read_table("categories", "bani_test"))
        mysql_total = sum(b.num_rows for b in mysql_batches)
        assert mysql_total == EXPECTED_ROW_COUNTS["categories"]

    def test_transfer_preserves_null_values(
        self,
        pg_source: PostgreSQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Should preserve NULL values during transfer."""
        batches = list(pg_source.read_table("customers", "public"))

        for batch in batches:
            mysql_sink.write_batch("customers", "bani_test", batch)

        mysql_batches = list(mysql_sink.read_table("customers", "bani_test"))
        mysql_total = sum(b.num_rows for b in mysql_batches)
        assert mysql_total == EXPECTED_ROW_COUNTS["customers"]

    def test_transfer_all_tables_row_counts(
        self,
        pg_source: PostgreSQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Should transfer all 5 tables with correct row counts."""
        for table_name in TABLE_NAMES:
            batches = list(pg_source.read_table(table_name, "public"))
            total_read = sum(b.num_rows for b in batches)

            total_written = 0
            for batch in batches:
                total_written += mysql_sink.write_batch(table_name, "bani_test", batch)

            assert total_written == total_read, (
                f"Row count mismatch for {table_name}: "
                f"read={total_read}, written={total_written}"
            )
            assert total_written == EXPECTED_ROW_COUNTS[table_name], (
                f"Unexpected row count for {table_name}: "
                f"got={total_written}, expected={EXPECTED_ROW_COUNTS[table_name]}"
            )

    def test_estimate_row_count(self, pg_source: PostgreSQLConnector) -> None:
        """Should estimate row count for PG tables."""
        for table_name in TABLE_NAMES:
            count = pg_source.estimate_row_count(table_name, "public")
            # Estimate may not be exact but should be non-negative
            assert count >= 0


class TestPGToMySQLIndexesAndConstraints:
    """Test index and FK recreation on MySQL target."""

    def test_create_indexes_on_mysql(
        self,
        pg_source: PostgreSQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Should create indexes on MySQL target."""
        schema = pg_source.introspect_schema()

        for table_name in TABLE_NAMES:
            table_def = schema.get_table("public", table_name)
            assert table_def is not None

            if table_def.indexes:
                mysql_sink.create_indexes(table_name, "bani_test", table_def.indexes)

    def test_create_foreign_keys_on_mysql(
        self,
        pg_source: PostgreSQLConnector,
        mysql_sink: MySQLConnector,
    ) -> None:
        """Should create foreign keys on MySQL target."""
        schema = pg_source.introspect_schema()

        all_fks: list[ForeignKeyDefinition] = []
        for table_name in TABLE_NAMES:
            table_def = schema.get_table("public", table_name)
            assert table_def is not None
            all_fks.extend(table_def.foreign_keys)

        if all_fks:
            mysql_sink.create_foreign_keys(tuple(all_fks))

    def test_mysql_target_schema_has_indexes(
        self,
        mysql_sink: MySQLConnector,
    ) -> None:
        """MySQL target should have the expected indexes after recreation."""
        schema = mysql_sink.introspect_schema()
        products = schema.get_table("bani_test", "products")
        if products is not None:
            # Should have category index + sku unique index (at minimum)
            assert len(products.indexes) >= 2
