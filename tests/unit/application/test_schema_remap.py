"""Tests for cross-dialect schema remapping."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from bani.application.orchestrator import MigrationOrchestrator
from bani.application.schema_remap import SchemaRemapper
from bani.connectors.base import SinkConnector, SourceConnector
from bani.domain.project import ConnectionConfig, ProjectModel, ProjectOptions
from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    ForeignKeyDefinition,
    TableDefinition,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_table(
    schema_name: str,
    table_name: str,
    columns: tuple[ColumnDefinition, ...] | None = None,
    foreign_keys: tuple[ForeignKeyDefinition, ...] = (),
    check_constraints: tuple[str, ...] = (),
) -> TableDefinition:
    """Build a minimal TableDefinition for testing."""
    if columns is None:
        columns = (
            ColumnDefinition(name="id", data_type="int4", nullable=False),
            ColumnDefinition(name="name", data_type="varchar(100)"),
        )
    return TableDefinition(
        schema_name=schema_name,
        table_name=table_name,
        columns=columns,
        primary_key=("id",),
        foreign_keys=foreign_keys,
        check_constraints=check_constraints,
    )


def _make_schema(
    tables: tuple[TableDefinition, ...],
    dialect: str = "postgresql",
) -> DatabaseSchema:
    return DatabaseSchema(tables=tables, source_dialect=dialect)


# ---------------------------------------------------------------------------
# SchemaRemapper unit tests
# ---------------------------------------------------------------------------


class TestPgToMssqlRemapping:
    """PG -> MSSQL remapping (public -> dbo)."""

    def test_schema_name_remapped(self) -> None:
        table = _make_table("public", "users")
        schema = _make_schema((table,), "postgresql")

        result = SchemaRemapper.remap_schema(schema, "postgresql", "mssql")

        assert len(result.tables) == 1
        assert result.tables[0].schema_name == "dbo"

    def test_fully_qualified_name_uses_target_schema(self) -> None:
        table = _make_table("public", "orders")
        schema = _make_schema((table,), "postgresql")

        result = SchemaRemapper.remap_schema(schema, "postgresql", "mssql")

        assert result.tables[0].fully_qualified_name == "dbo.orders"

    def test_source_dialect_preserved(self) -> None:
        table = _make_table("public", "t")
        schema = _make_schema((table,), "postgresql")

        result = SchemaRemapper.remap_schema(schema, "postgresql", "mssql")

        assert result.source_dialect == "postgresql"


class TestMysqlToPgRemapping:
    """MySQL -> PG remapping (empty default -> public)."""

    def test_schema_name_remapped_to_public(self) -> None:
        table = _make_table("", "products")
        schema = _make_schema((table,), "mysql")

        result = SchemaRemapper.remap_schema(schema, "mysql", "postgresql")

        assert result.tables[0].schema_name == "public"


class TestMssqlToPgRemapping:
    """MSSQL -> PG remapping (dbo -> public)."""

    def test_schema_name_remapped_to_public(self) -> None:
        table = _make_table("dbo", "accounts")
        schema = _make_schema((table,), "mssql")

        result = SchemaRemapper.remap_schema(schema, "mssql", "postgresql")

        assert result.tables[0].schema_name == "public"


class TestSameDialectNoRemapping:
    """Same dialect: no remapping should occur."""

    def test_pg_to_pg_no_change(self) -> None:
        table = _make_table("public", "users", check_constraints=("len(name) > 0",))
        schema = _make_schema((table,), "postgresql")

        result = SchemaRemapper.remap_schema(schema, "postgresql", "postgresql")

        assert result.tables[0].schema_name == "public"
        # Check constraints preserved for same dialect
        assert result.tables[0].check_constraints == ("len(name) > 0",)

    def test_mssql_to_mssql_no_change(self) -> None:
        table = _make_table("dbo", "orders")
        schema = _make_schema((table,), "mssql")

        result = SchemaRemapper.remap_schema(schema, "mssql", "mssql")

        assert result.tables[0].schema_name == "dbo"


class TestFkReferenceRemapping:
    """FK source_table and referenced_table are updated."""

    def test_fk_tables_remapped_pg_to_mssql(self) -> None:
        fk = ForeignKeyDefinition(
            name="fk_orders_users",
            source_table="public.orders",
            source_columns=("user_id",),
            referenced_table="public.users",
            referenced_columns=("id",),
        )
        table = _make_table("public", "orders", foreign_keys=(fk,))
        schema = _make_schema((table,), "postgresql")

        result = SchemaRemapper.remap_schema(schema, "postgresql", "mssql")

        remapped_fk = result.tables[0].foreign_keys[0]
        assert remapped_fk.source_table == "dbo.orders"
        assert remapped_fk.referenced_table == "dbo.users"

    def test_fk_columns_preserved(self) -> None:
        fk = ForeignKeyDefinition(
            name="fk_test",
            source_table="public.child",
            source_columns=("parent_id",),
            referenced_table="public.parent",
            referenced_columns=("id",),
            on_delete="CASCADE",
        )
        table = _make_table("public", "child", foreign_keys=(fk,))
        schema = _make_schema((table,), "postgresql")

        result = SchemaRemapper.remap_schema(schema, "postgresql", "mssql")

        remapped_fk = result.tables[0].foreign_keys[0]
        assert remapped_fk.source_columns == ("parent_id",)
        assert remapped_fk.referenced_columns == ("id",)
        assert remapped_fk.on_delete == "CASCADE"
        assert remapped_fk.name == "fk_test"


class TestCheckConstraintStripping:
    """All check constraints removed for cross-dialect migrations."""

    def test_check_constraints_stripped_cross_dialect(self) -> None:
        table = _make_table(
            "public",
            "events",
            check_constraints=(
                "value::text ~ '^[0-9]+'",
                "status IN ('active', 'inactive')",
            ),
        )
        schema = _make_schema((table,), "postgresql")

        result = SchemaRemapper.remap_schema(schema, "postgresql", "mssql")

        assert result.tables[0].check_constraints == ()

    def test_check_constraints_preserved_same_dialect(self) -> None:
        table = _make_table(
            "public",
            "events",
            check_constraints=("status IN ('active', 'inactive')",),
        )
        schema = _make_schema((table,), "postgresql")

        result = SchemaRemapper.remap_schema(schema, "postgresql", "postgresql")

        assert result.tables[0].check_constraints == (
            "status IN ('active', 'inactive')",
        )


class TestEmptyTableFiltering:
    """Tables with no columns are removed."""

    def test_empty_tables_filtered(self) -> None:
        good_table = _make_table("public", "users")
        empty_table = TableDefinition(
            schema_name="public",
            table_name="empty",
            columns=(),
        )
        schema = _make_schema((good_table, empty_table), "postgresql")

        result = SchemaRemapper.remap_schema(schema, "postgresql", "mssql")

        assert len(result.tables) == 1
        assert result.tables[0].table_name == "users"

    def test_all_empty_returns_empty(self) -> None:
        empty1 = TableDefinition(schema_name="public", table_name="a", columns=())
        empty2 = TableDefinition(schema_name="public", table_name="b", columns=())
        schema = _make_schema((empty1, empty2), "postgresql")

        result = SchemaRemapper.remap_schema(schema, "postgresql", "mssql")

        assert len(result.tables) == 0


class TestExplicitTargetSchema:
    """Explicit target_schema overrides the default."""

    def test_custom_schema_name(self) -> None:
        table = _make_table("public", "reports")
        schema = _make_schema((table,), "postgresql")

        result = SchemaRemapper.remap_schema(
            schema, "postgresql", "mssql", target_schema="staging"
        )

        assert result.tables[0].schema_name == "staging"
        assert result.tables[0].fully_qualified_name == "staging.reports"

    def test_custom_schema_applied_to_fks(self) -> None:
        fk = ForeignKeyDefinition(
            name="fk_ref",
            source_table="public.child",
            source_columns=("pid",),
            referenced_table="public.parent",
            referenced_columns=("id",),
        )
        table = _make_table("public", "child", foreign_keys=(fk,))
        schema = _make_schema((table,), "postgresql")

        result = SchemaRemapper.remap_schema(
            schema, "postgresql", "mssql", target_schema="custom"
        )

        remapped_fk = result.tables[0].foreign_keys[0]
        assert remapped_fk.source_table == "custom.child"
        assert remapped_fk.referenced_table == "custom.parent"


class TestMultipleTablesRemapping:
    """Ensure all tables in a schema are remapped."""

    def test_all_tables_remapped(self) -> None:
        t1 = _make_table("public", "users")
        t2 = _make_table("public", "orders")
        t3 = _make_table("public", "products")
        schema = _make_schema((t1, t2, t3), "postgresql")

        result = SchemaRemapper.remap_schema(schema, "postgresql", "mssql")

        assert len(result.tables) == 3
        for table in result.tables:
            assert table.schema_name == "dbo"


# ---------------------------------------------------------------------------
# IDENTITY INSERT orchestrator integration tests
# ---------------------------------------------------------------------------


class MockSourceConnector(SourceConnector):
    """Minimal source connector for orchestrator tests."""

    def __init__(self, schema: DatabaseSchema) -> None:
        self._schema = schema

    def connect(self, config: ConnectionConfig) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def introspect_schema(self) -> DatabaseSchema:
        return self._schema

    def read_table(
        self,
        table_name: str,
        schema_name: str,
        columns: list[str] | None = None,
        filter_sql: str | None = None,
        batch_size: int = 100_000,
    ) -> Iterator[pa.RecordBatch]:
        # Yield a single small batch so we exercise the write path
        batch = pa.record_batch(
            [pa.array([1, 2]), pa.array(["a", "b"])],
            names=["id", "name"],
        )
        yield batch

    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        return 2


class MockSinkConnector(SinkConnector):
    """Minimal sink connector that records execute_sql calls."""

    def __init__(self) -> None:
        self.sql_calls: list[str] = []
        self.write_batch_calls: list[tuple[str, str]] = []

    def connect(self, config: ConnectionConfig) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def create_table(self, table_def: TableDefinition) -> None:
        pass

    def write_batch(self, table_name: str, schema_name: str, batch: Any) -> int:
        self.write_batch_calls.append((table_name, schema_name))
        return len(batch)

    def create_indexes(
        self,
        table_name: str,
        schema_name: str,
        indexes: Any,
    ) -> None:
        pass

    def create_foreign_keys(self, fks: Any) -> None:
        pass

    def execute_sql(self, sql: str) -> None:
        self.sql_calls.append(sql)


class TestIdentityInsertOrchestrator:
    """Verify IDENTITY INSERT is toggled for MSSQL targets."""

    def _make_project(
        self, source_dialect: str = "postgresql", target_dialect: str = "mssql"
    ) -> ProjectModel:
        return ProjectModel(
            name="test_project",
            source=ConnectionConfig(
                dialect=source_dialect,
                host="localhost",
                port=5432,
                database="src_db",
                username_env="USER",
                password_env="PASS",
            ),
            target=ConnectionConfig(
                dialect=target_dialect,
                host="localhost",
                port=1433,
                database="tgt_db",
                username_env="USER",
                password_env="PASS",
            ),
            options=ProjectOptions(
                batch_size=1000,
                parallel_workers=1,
                create_target_schema=False,
                transfer_indexes=False,
                transfer_foreign_keys=False,
            ),
        )

    def test_identity_insert_on_off_for_auto_increment(self) -> None:
        """MSSQL target + auto-increment column -> SET IDENTITY_INSERT ON/OFF."""
        cols = (
            ColumnDefinition(
                name="id", data_type="int4", nullable=False, is_auto_increment=True
            ),
            ColumnDefinition(name="name", data_type="varchar(100)"),
        )
        table = _make_table("dbo", "users", columns=cols)
        schema = _make_schema((table,), "postgresql")

        source = MockSourceConnector(schema)
        sink = MockSinkConnector()
        project = self._make_project()

        orch = MigrationOrchestrator(project, source, sink)
        result = orch._transfer_table_sequential(table)

        assert result.success
        # Expect ON then OFF
        on_calls = [s for s in sink.sql_calls if "ON" in s]
        off_calls = [s for s in sink.sql_calls if "OFF" in s]
        assert len(on_calls) == 1
        assert len(off_calls) == 1
        assert "IDENTITY_INSERT" in on_calls[0]
        assert "[dbo].[users]" in on_calls[0]
        assert "IDENTITY_INSERT" in off_calls[0]
        assert "[dbo].[users]" in off_calls[0]

    def test_no_identity_insert_for_non_auto_increment(self) -> None:
        """No auto-increment columns -> no IDENTITY_INSERT calls."""
        cols = (
            ColumnDefinition(name="id", data_type="int4", nullable=False),
            ColumnDefinition(name="name", data_type="varchar(100)"),
        )
        table = _make_table("dbo", "users", columns=cols)
        schema = _make_schema((table,), "postgresql")

        source = MockSourceConnector(schema)
        sink = MockSinkConnector()
        project = self._make_project()

        orch = MigrationOrchestrator(project, source, sink)
        result = orch._transfer_table_sequential(table)

        assert result.success
        identity_calls = [s for s in sink.sql_calls if "IDENTITY_INSERT" in s]
        assert len(identity_calls) == 0

    def test_no_identity_insert_for_non_mssql_target(self) -> None:
        """Non-MSSQL target -> no IDENTITY_INSERT calls even with auto-increment."""
        cols = (
            ColumnDefinition(
                name="id", data_type="int4", nullable=False, is_auto_increment=True
            ),
            ColumnDefinition(name="name", data_type="varchar(100)"),
        )
        table = _make_table("public", "users", columns=cols)
        schema = _make_schema((table,), "postgresql")

        source = MockSourceConnector(schema)
        sink = MockSinkConnector()
        project = self._make_project(target_dialect="postgresql")

        orch = MigrationOrchestrator(project, source, sink)
        result = orch._transfer_table_sequential(table)

        assert result.success
        identity_calls = [s for s in sink.sql_calls if "IDENTITY_INSERT" in s]
        assert len(identity_calls) == 0

    def test_identity_insert_cleanup_on_error(self) -> None:
        """IDENTITY_INSERT OFF is called even when transfer fails."""
        cols = (
            ColumnDefinition(
                name="id", data_type="int4", nullable=False, is_auto_increment=True
            ),
            ColumnDefinition(name="name", data_type="varchar(100)"),
        )
        table = _make_table("dbo", "items", columns=cols)
        schema = _make_schema((table,), "postgresql")

        source = MockSourceConnector(schema)
        sink = MockSinkConnector()
        project = self._make_project()

        # Make write_batch raise an error
        def failing_write(*args: Any, **kwargs: Any) -> int:
            raise RuntimeError("write failed")

        sink.write_batch = failing_write  # type: ignore[assignment]

        orch = MigrationOrchestrator(project, source, sink)
        _result = orch._transfer_table_sequential(table)

        # Even when writes fail, IDENTITY_INSERT OFF should be attempted
        off_calls = [s for s in sink.sql_calls if "OFF" in s]
        assert len(off_calls) == 1


class TestOrchestratorSchemaRemapping:
    """Verify orchestrator applies schema remapping in execute()."""

    def test_cross_dialect_remapping_applied(self) -> None:
        """PG->MSSQL execute() remaps public->dbo before creating tables."""
        table = _make_table(
            "public",
            "users",
            check_constraints=("name::text ~ '^[A-Z]'",),
        )
        schema = _make_schema((table,), "postgresql")

        source = MockSourceConnector(schema)
        sink = MockSinkConnector()

        project = ProjectModel(
            name="remap_test",
            source=ConnectionConfig(
                dialect="postgresql",
                host="localhost",
                port=5432,
                database="src",
                username_env="U",
                password_env="P",
            ),
            target=ConnectionConfig(
                dialect="mssql",
                host="localhost",
                port=1433,
                database="tgt",
                username_env="U",
                password_env="P",
            ),
            options=ProjectOptions(
                batch_size=1000,
                parallel_workers=1,
                create_target_schema=True,
                transfer_indexes=False,
                transfer_foreign_keys=False,
            ),
        )

        orch = MigrationOrchestrator(project, source, sink)

        # We override create_table to capture what schema was used
        created_tables: list[TableDefinition] = []
        original_create = sink.create_table

        def capturing_create(table_def: TableDefinition) -> None:
            created_tables.append(table_def)
            original_create(table_def)

        sink.create_table = capturing_create  # type: ignore[assignment]

        result = orch.execute()

        assert result.tables_completed == 1
        assert len(created_tables) == 1
        assert created_tables[0].schema_name == "dbo"
        assert created_tables[0].check_constraints == ()

    def test_same_dialect_no_remapping(self) -> None:
        """PG->PG execute() does NOT remap."""
        table = _make_table(
            "public",
            "users",
            check_constraints=("name IS NOT NULL",),
        )
        schema = _make_schema((table,), "postgresql")

        source = MockSourceConnector(schema)
        sink = MockSinkConnector()

        project = ProjectModel(
            name="same_dialect",
            source=ConnectionConfig(
                dialect="postgresql",
                host="localhost",
                port=5432,
                database="src",
                username_env="U",
                password_env="P",
            ),
            target=ConnectionConfig(
                dialect="postgresql",
                host="localhost",
                port=5433,
                database="tgt",
                username_env="U",
                password_env="P",
            ),
            options=ProjectOptions(
                batch_size=1000,
                parallel_workers=1,
                create_target_schema=True,
                transfer_indexes=False,
                transfer_foreign_keys=False,
            ),
        )

        orch = MigrationOrchestrator(project, source, sink)

        created_tables: list[TableDefinition] = []

        def capturing_create(table_def: TableDefinition) -> None:
            created_tables.append(table_def)

        sink.create_table = capturing_create  # type: ignore[assignment]

        result = orch.execute()

        assert result.tables_completed == 1
        assert len(created_tables) == 1
        assert created_tables[0].schema_name == "public"
        assert created_tables[0].check_constraints == ("name IS NOT NULL",)
