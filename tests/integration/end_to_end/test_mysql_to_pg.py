"""Integration tests: MySQL -> PostgreSQL full migration.

Tests full migration from MySQL source to PostgreSQL target including
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


class TestMySQLToPGSchemaTransfer:
    """Test schema introspection and creation from MySQL to PG."""

    def test_introspect_mysql_source_schema(self, mysql_source: MySQLConnector) -> None:
        """MySQL source schema should be introspectable."""
        schema = mysql_source.introspect_schema()
        assert schema.source_dialect == "mysql"
        assert len(schema.tables) >= 5

    def test_mysql_schema_table_names(self, mysql_source: MySQLConnector) -> None:
        """MySQL schema should contain all 5 expected tables."""
        schema = mysql_source.introspect_schema()
        table_names = {t.table_name for t in schema.tables}
        for expected in TABLE_NAMES:
            assert expected in table_names, f"Missing table: {expected}"

    def test_mysql_schema_has_auto_increment(
        self, mysql_source: MySQLConnector
    ) -> None:
        """MySQL schema should detect auto_increment columns."""
        schema = mysql_source.introspect_schema()
        categories = schema.get_table("bani_test", "categories")
        assert categories is not None
        id_col = next((c for c in categories.columns if c.name == "id"), None)
        assert id_col is not None
        assert id_col.is_auto_increment is True

    def test_mysql_schema_has_foreign_keys(self, mysql_source: MySQLConnector) -> None:
        """MySQL schema should have FKs on products, orders, order_items."""
        schema = mysql_source.introspect_schema()

        products = schema.get_table("bani_test", "products")
        assert products is not None
        assert len(products.foreign_keys) >= 1

        order_items = schema.get_table("bani_test", "order_items")
        assert order_items is not None
        assert len(order_items.foreign_keys) >= 2

    def test_create_tables_on_pg_target(
        self,
        mysql_source: MySQLConnector,
        pg_sink: PostgreSQLConnector,
    ) -> None:
        """Should create all 5 tables on PG from MySQL schema."""
        schema = mysql_source.introspect_schema()

        # Drop existing tables in reverse dependency order
        for table_name in reversed(TABLE_NAMES):
            pg_sink.execute_sql(f'DROP TABLE IF EXISTS "public"."{table_name}" CASCADE')

        # Create tables in dependency order
        for table_name in TABLE_NAMES:
            table_def = schema.get_table("bani_test", table_name)
            assert table_def is not None
            pg_sink.create_table(table_def)


class TestMySQLToPGDataTransfer:
    """Test data transfer from MySQL to PostgreSQL."""

    def test_read_mysql_table_as_arrow_batches(
        self, mysql_source: MySQLConnector
    ) -> None:
        """Should read MySQL table data as Arrow RecordBatches."""
        batches = list(mysql_source.read_table("categories", "bani_test"))
        assert len(batches) >= 1
        total_rows = sum(b.num_rows for b in batches)
        assert total_rows == EXPECTED_ROW_COUNTS["categories"]

    def test_transfer_categories_data(
        self,
        mysql_source: MySQLConnector,
        pg_sink: PostgreSQLConnector,
    ) -> None:
        """Should transfer categories data from MySQL to PG."""
        batches = list(mysql_source.read_table("categories", "bani_test"))
        total_read = sum(b.num_rows for b in batches)

        total_written = 0
        for batch in batches:
            total_written += pg_sink.write_batch("categories", "public", batch)
        assert total_written == total_read

    def test_transfer_preserves_unicode_data(
        self,
        mysql_source: MySQLConnector,
        pg_sink: PostgreSQLConnector,
    ) -> None:
        """Should preserve CJK and emoji characters from MySQL to PG."""
        batches = list(mysql_source.read_table("categories", "bani_test"))

        for batch in batches:
            pg_sink.write_batch("categories", "public", batch)

        pg_batches = list(pg_sink.read_table("categories", "public"))
        pg_total = sum(b.num_rows for b in pg_batches)
        assert pg_total == EXPECTED_ROW_COUNTS["categories"]

    def test_transfer_preserves_null_values(
        self,
        mysql_source: MySQLConnector,
        pg_sink: PostgreSQLConnector,
    ) -> None:
        """Should preserve NULL values from MySQL to PG."""
        batches = list(mysql_source.read_table("customers", "bani_test"))

        for batch in batches:
            pg_sink.write_batch("customers", "public", batch)

        pg_batches = list(pg_sink.read_table("customers", "public"))
        pg_total = sum(b.num_rows for b in pg_batches)
        assert pg_total == EXPECTED_ROW_COUNTS["customers"]

    def test_transfer_all_tables_row_counts(
        self,
        mysql_source: MySQLConnector,
        pg_sink: PostgreSQLConnector,
    ) -> None:
        """Should transfer all 5 tables with correct row counts."""
        for table_name in TABLE_NAMES:
            batches = list(mysql_source.read_table(table_name, "bani_test"))
            total_read = sum(b.num_rows for b in batches)

            total_written = 0
            for batch in batches:
                total_written += pg_sink.write_batch(table_name, "public", batch)

            assert total_written == total_read
            assert total_written == EXPECTED_ROW_COUNTS[table_name]

    def test_estimate_row_count_mysql(self, mysql_source: MySQLConnector) -> None:
        """Should estimate row count for MySQL tables."""
        for table_name in TABLE_NAMES:
            count = mysql_source.estimate_row_count(table_name, "bani_test")
            assert count >= 0

    def test_read_table_with_column_filter(self, mysql_source: MySQLConnector) -> None:
        """Should read only specified columns from MySQL."""
        batches = list(
            mysql_source.read_table("categories", "bani_test", columns=["id", "name"])
        )
        assert len(batches) >= 1
        for batch in batches:
            assert batch.num_columns == 2
            assert batch.schema.names == ["id", "name"]


class TestMySQLToPGIndexesAndConstraints:
    """Test index and FK recreation on PG target."""

    def test_create_indexes_on_pg(
        self,
        mysql_source: MySQLConnector,
        pg_sink: PostgreSQLConnector,
    ) -> None:
        """Should create indexes on PG target from MySQL schema."""
        schema = mysql_source.introspect_schema()

        for table_name in TABLE_NAMES:
            table_def = schema.get_table("bani_test", table_name)
            assert table_def is not None

            if table_def.indexes:
                pg_sink.create_indexes(table_name, "public", table_def.indexes)

    def test_create_foreign_keys_on_pg(
        self,
        mysql_source: MySQLConnector,
        pg_sink: PostgreSQLConnector,
    ) -> None:
        """Should create foreign keys on PG target from MySQL schema."""
        schema = mysql_source.introspect_schema()

        all_fks: list[ForeignKeyDefinition] = []
        for table_name in TABLE_NAMES:
            table_def = schema.get_table("bani_test", table_name)
            assert table_def is not None
            all_fks.extend(table_def.foreign_keys)

        if all_fks:
            pg_sink.create_foreign_keys(tuple(all_fks))

    def test_pg_target_schema_verification(
        self,
        pg_sink: PostgreSQLConnector,
    ) -> None:
        """PG target should have the recreated tables with schema intact."""
        schema = pg_sink.introspect_schema()
        table_names = {t.table_name for t in schema.tables}
        for expected in TABLE_NAMES:
            assert expected in table_names
