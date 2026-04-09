"""Tests for project model dataclasses."""

from __future__ import annotations

from bani.domain.project import (
    ColumnMapping,
    ConnectionConfig,
    HookConfig,
    MigrationPlan,
    ProjectModel,
    SyncStrategy,
    TableMapping,
    TypeMappingOverride,
    WriteStrategy,
)


class TestConnectionConfig:
    """Tests for ConnectionConfig."""

    def test_defaults(self) -> None:
        cfg = ConnectionConfig(dialect="postgresql")
        assert cfg.host == ""
        assert cfg.port == 0
        assert cfg.encrypt is False
        assert cfg.extra == ()

    def test_all_fields(self) -> None:
        cfg = ConnectionConfig(
            dialect="mysql",
            host="db.example.com",
            port=3306,
            database="mydb",
            username_env="MYSQL_USER",
            password_env="MYSQL_PASS",
            extra=(("charset", "utf8mb4"),),
            encrypt=True,
        )
        assert cfg.dialect == "mysql"
        assert cfg.password_env == "MYSQL_PASS"
        assert cfg.extra == (("charset", "utf8mb4"),)

    def test_frozen(self) -> None:
        cfg = ConnectionConfig(dialect="mysql")
        try:
            cfg.dialect = "pg"  # type: ignore[misc]
            raise AssertionError("Should be frozen")
        except AttributeError:
            pass


class TestTableMapping:
    """Tests for TableMapping."""

    def test_defaults(self) -> None:
        tm = TableMapping(source_schema="dbo", source_table="users")
        assert tm.target_schema == ""
        assert tm.target_table == ""
        assert tm.column_mappings == ()
        assert tm.filter_sql is None
        assert tm.write_strategy == WriteStrategy.INSERT
        assert tm.batch_size is None

    def test_full(self) -> None:
        tm = TableMapping(
            source_schema="dbo",
            source_table="users",
            target_schema="public",
            target_table="users",
            column_mappings=(
                ColumnMapping(source_name="id", target_name="id"),
                ColumnMapping(source_name="name", target_name="name"),
                ColumnMapping(source_name="email", target_name="email"),
            ),
            filter_sql="is_active = 1",
            write_strategy=WriteStrategy.UPSERT,
            batch_size=5000,
        )
        assert tm.write_strategy == WriteStrategy.UPSERT
        assert tm.batch_size == 5000


class TestHookConfig:
    """Tests for HookConfig."""

    def test_defaults(self) -> None:
        hook = HookConfig(
            name="backup", event="before-migration",
            command="pg_dump mydb",
        )
        assert hook.timeout_seconds == 300
        assert hook.on_failure == "abort"
        assert hook.hook_type == "shell"
        assert hook.target == ""
        assert hook.table_name == ""


class TestProjectModel:
    """Tests for ProjectModel."""

    def test_minimal(self) -> None:
        project = ProjectModel(
            name="test-migration",
            source=ConnectionConfig(dialect="mysql"),
            target=ConnectionConfig(dialect="postgresql"),
        )
        assert project.table_mappings == ()
        assert project.type_overrides == ()
        assert project.hooks == ()
        assert project.description == ""
        assert project.author == ""

    def test_with_overrides(self) -> None:
        overrides = (TypeMappingOverride(source_type="INT", target_type="INTEGER"),)
        project = ProjectModel(
            name="test",
            source=ConnectionConfig(dialect="mysql"),
            target=ConnectionConfig(dialect="postgresql"),
            type_overrides=overrides,
        )
        assert project.type_overrides == overrides
        assert len(project.type_overrides) == 1


class TestMigrationPlan:
    """Tests for MigrationPlan."""

    def test_basic(self) -> None:
        project = ProjectModel(
            name="test",
            source=ConnectionConfig(dialect="mysql"),
            target=ConnectionConfig(dialect="postgresql"),
        )
        plan = MigrationPlan(
            project=project,
            ordered_tables=("public.users", "public.orders"),
            total_estimated_rows=100_000,
        )
        assert len(plan.ordered_tables) == 2
        assert plan.deferred_fk_tables == ()
        assert plan.total_estimated_rows == 100_000


class TestEnums:
    """Tests for SyncStrategy and WriteStrategy enums."""

    def test_sync_strategies(self) -> None:
        assert SyncStrategy.FULL is not None
        assert SyncStrategy.TIMESTAMP is not None
        assert SyncStrategy.ROWVERSION is not None
        assert SyncStrategy.CHECKSUM is not None

    def test_write_strategies(self) -> None:
        assert WriteStrategy.INSERT is not None
        assert WriteStrategy.UPSERT is not None
        assert WriteStrategy.TRUNCATE_INSERT is not None
