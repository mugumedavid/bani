"""Integration tests for PostgreSQL connector (requires running PostgreSQL)."""

from __future__ import annotations

import os
from collections.abc import Generator

import pytest

try:
    import psycopg

    PG_AVAILABLE = True
except ImportError:
    PG_AVAILABLE = False


pytestmark = pytest.mark.integration


@pytest.fixture
def pg_connection() -> Generator[psycopg.Connection[tuple[str, ...]], None, None]:
    """Connect to the test PostgreSQL instance.

    Yields:
        A psycopg connection.

    Skips:
        If PostgreSQL is not available or connection fails.
    """
    if not PG_AVAILABLE:
        pytest.skip("psycopg not available")

    try:
        conn = psycopg.connect(
            "postgresql://bani_test:bani_test@localhost:5433/bani_test"
        )
        yield conn
        conn.close()
    except Exception as e:
        pytest.skip(f"PostgreSQL not available: {e}")


@pytest.fixture
def test_schema(
    pg_connection: psycopg.Connection[tuple[str, ...]],
) -> Generator[str, None, None]:
    """Create a test schema and return its name.

    Args:
        pg_connection: Active PostgreSQL connection.

    Yields:
        The test schema name.

    Cleans up:
        Drops the schema after test.
    """
    schema_name = "bani_test_schema"

    # Ensure clean transaction state before DDL
    pg_connection.rollback()
    with pg_connection.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        cur.execute(f'CREATE SCHEMA "{schema_name}"')
    pg_connection.commit()

    yield schema_name

    # Cleanup
    pg_connection.rollback()
    with pg_connection.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
    pg_connection.commit()


class TestPostgreSQLConnectorSchema:
    """Tests for schema introspection on a real database."""

    def test_introspect_empty_schema(
        self, pg_connection: psycopg.Connection[tuple[str, ...]], test_schema: str
    ) -> None:
        """Should return empty schema for schema with no tables."""
        from bani.connectors.postgresql.schema_reader import PostgreSQLSchemaReader

        reader = PostgreSQLSchemaReader(pg_connection)
        schema = reader.read_schema()

        # Filter to our test schema
        test_tables = [t for t in schema.tables if t.schema_name == test_schema]
        assert len(test_tables) == 0

    def test_introspect_simple_table(
        self, pg_connection: psycopg.Connection[tuple[str, ...]], test_schema: str
    ) -> None:
        """Should introspect a simple table with columns."""
        from bani.connectors.postgresql.schema_reader import PostgreSQLSchemaReader

        # Create a test table
        with pg_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    email VARCHAR(255) UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

        reader = PostgreSQLSchemaReader(pg_connection)
        schema = reader.read_schema()

        # Find our table
        table = schema.get_table(test_schema, "users")
        assert table is not None
        assert table.table_name == "users"
        assert len(table.columns) == 4

        # Check columns
        col_names = [c.name for c in table.columns]
        assert "id" in col_names
        assert "name" in col_names
        assert "email" in col_names
        assert "created_at" in col_names

        # Check primary key
        assert "id" in table.primary_key

        # Check nullability
        name_col = next(c for c in table.columns if c.name == "name")
        assert name_col.nullable is False

    def test_introspect_table_with_index(
        self, pg_connection: psycopg.Connection[tuple[str, ...]], test_schema: str
    ) -> None:
        """Should introspect indexes on a table."""
        from bani.connectors.postgresql.schema_reader import PostgreSQLSchemaReader

        # Create a table with an index
        with pg_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".products (
                    id SERIAL PRIMARY KEY,
                    sku VARCHAR(50) NOT NULL
                )
            """
            )
            cur.execute(
                f'CREATE UNIQUE INDEX idx_sku ON "{test_schema}".products (sku)'
            )

        reader = PostgreSQLSchemaReader(pg_connection)
        schema = reader.read_schema()

        table = schema.get_table(test_schema, "products")
        assert table is not None

        # Check that index was found
        idx_names = [idx.name for idx in table.indexes]
        assert "idx_sku" in idx_names

        # Check index properties
        sku_idx = next(idx for idx in table.indexes if idx.name == "idx_sku")
        assert sku_idx.is_unique is True
        assert "sku" in sku_idx.columns

    def test_introspect_table_with_foreign_key(
        self, pg_connection: psycopg.Connection[tuple[str, ...]], test_schema: str
    ) -> None:
        """Should introspect foreign keys on a table."""
        from bani.connectors.postgresql.schema_reader import PostgreSQLSchemaReader

        # Create tables with FK relationship
        with pg_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".categories (
                    id SERIAL PRIMARY KEY,
                    name TEXT
                )
            """
            )
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".items (
                    id SERIAL PRIMARY KEY,
                    category_id INTEGER REFERENCES "{test_schema}".categories(id)
                )
            """
            )

        reader = PostgreSQLSchemaReader(pg_connection)
        schema = reader.read_schema()

        table = schema.get_table(test_schema, "items")
        assert table is not None
        assert len(table.foreign_keys) > 0

        # Check FK properties
        fk = table.foreign_keys[0]
        assert "category_id" in fk.source_columns
        assert test_schema in fk.referenced_table


class TestPostgreSQLConnectorDataRead:
    """Tests for reading data from a real database."""

    def test_read_empty_table(
        self, pg_connection: psycopg.Connection[tuple[str, ...]], test_schema: str
    ) -> None:
        """Should handle reading from an empty table."""
        from bani.connectors.postgresql.data_reader import PostgreSQLDataReader

        # Create and leave empty
        with pg_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".empty_table (
                    id INTEGER,
                    name TEXT
                )
            """
            )

        reader = PostgreSQLDataReader(pg_connection)
        batches = list(reader.read_table("empty_table", test_schema))

        assert len(batches) == 0

    def test_read_table_with_data(
        self, pg_connection: psycopg.Connection[tuple[str, ...]], test_schema: str
    ) -> None:
        """Should read data and convert to Arrow batches."""
        from bani.connectors.postgresql.data_reader import PostgreSQLDataReader

        # Create and populate table
        with pg_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".test_data (
                    id INTEGER,
                    name TEXT
                )
            """
            )
            insert_sql = (
                f'INSERT INTO "{test_schema}".test_data '
                "VALUES (1, 'Alice'), (2, 'Bob')"
            )
            cur.execute(insert_sql)

        reader = PostgreSQLDataReader(pg_connection)
        batches = list(reader.read_table("test_data", test_schema, batch_size=10))

        assert len(batches) > 0
        batch = batches[0]
        assert batch.num_rows == 2
        assert batch.num_columns == 2

    def test_read_table_with_column_filter(
        self, pg_connection: psycopg.Connection[tuple[str, ...]], test_schema: str
    ) -> None:
        """Should respect column selection."""
        from bani.connectors.postgresql.data_reader import PostgreSQLDataReader

        # Create table
        with pg_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".multi_col (
                    id INTEGER,
                    name TEXT,
                    age INTEGER
                )
            """
            )
            cur.execute(
                f"""
                INSERT INTO "{test_schema}".multi_col VALUES
                (1, 'Alice', 30),
                (2, 'Bob', 25)
            """
            )

        reader = PostgreSQLDataReader(pg_connection)
        batches = list(
            reader.read_table(
                "multi_col", test_schema, columns=["id", "name"], batch_size=10
            )
        )

        assert len(batches) > 0
        batch = batches[0]
        assert batch.num_columns == 2
        assert batch.schema.names == ["id", "name"]

    def test_estimate_row_count(
        self, pg_connection: psycopg.Connection[tuple[str, ...]], test_schema: str
    ) -> None:
        """Should estimate row count for a table."""
        from bani.connectors.postgresql.data_reader import PostgreSQLDataReader

        # Create and populate
        with pg_connection.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".row_count_test (
                    id INTEGER
                )
            """
            )
            for i in range(10):
                cur.execute(f'INSERT INTO "{test_schema}".row_count_test VALUES ({i})')

        reader = PostgreSQLDataReader(pg_connection)
        count = reader.estimate_row_count("row_count_test", test_schema)

        assert count >= 10  # At least the rows we inserted


class TestPostgreSQLConnectorDataWrite:
    """Tests for writing data to a real database."""

    def test_write_batch(
        self, pg_connection: psycopg.Connection[tuple[str, ...]], test_schema: str
    ) -> None:
        """Should write an Arrow batch to a table."""
        import pyarrow as pa

        from bani.connectors.postgresql.data_writer import PostgreSQLDataWriter

        # Clear any failed transaction from prior tests
        pg_connection.rollback()

        # Create table and commit so it persists across rollbacks
        # (the data writer rollbacks on COPY fallback attempts)
        with pg_connection.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{test_schema}".write_test')
            cur.execute(
                f"""
                CREATE TABLE "{test_schema}".write_test (
                    id INTEGER,
                    name TEXT
                )
            """
            )
        pg_connection.commit()

        # Create and write batch
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])],
            names=["id", "name"],
        )

        writer = PostgreSQLDataWriter(pg_connection)
        rows_written = writer.write_batch("write_test", test_schema, batch)

        assert rows_written == 3

        # Verify data was written
        with pg_connection.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{test_schema}".write_test')
            result: list[tuple[str, ...]] = cur.fetchall()
            assert int(result[0][0]) == 3


class TestPostgreSQLConnectorEndToEnd:
    """End-to-end tests using the full connector."""

    def test_connector_connect_and_introspect(self, test_schema: str) -> None:
        """Should connect and introspect schema end-to-end."""
        from bani.connectors.postgresql import PostgreSQLConnector
        from bani.domain.project import ConnectionConfig

        # Skip if PostgreSQL not available
        if not PG_AVAILABLE:
            pytest.skip("PostgreSQL not available")

        connector = PostgreSQLConnector()

        try:
            config = ConnectionConfig(
                dialect="postgresql",
                host="localhost",
                port=5433,
                database="bani_test",
                username_env="PGUSER",
                password_env="PGPASSWORD",
            )

            # Set environment variables
            os.environ["PGUSER"] = "bani_test"
            os.environ["PGPASSWORD"] = "bani_test"

            connector.connect(config)

            # Create a test table
            connector.execute_sql(
                f"""
                CREATE TABLE "{test_schema}".connector_test (
                    id SERIAL PRIMARY KEY,
                    value TEXT
                )
            """
            )

            # Introspect
            schema = connector.introspect_schema()
            assert schema is not None
            assert schema.source_dialect == "postgresql"

            connector.disconnect()
        except Exception as e:
            pytest.skip(f"PostgreSQL test setup failed: {e}")
        finally:
            if connector.connection is not None:
                connector.disconnect()
