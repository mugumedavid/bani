"""Tests for project model dataclasses."""

from __future__ import annotations

from bani.domain.project import (
    ConnectionConfig,
    HookConfig,
    MigrationPlan,
    ProjectModel,
    SyncStrategy,
    TableMapping,
    WriteStrategy,
)
from bani.domain.type_mapping import MappingRuleSet


class TestConnectionConfig:
    """Tests for ConnectionConfig."""

    def test_defaults(self) -> None:
        cfg = ConnectionConfig(dialect="postgresql")
        assert cfg.host == ""
        assert cfg.port == 0
        assert cfg.encrypt is True
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
        assert tm.columns == ()
        assert tm.filter_sql is None
        assert tm.write_strategy == WriteStrategy.INSERT
        assert tm.batch_size is None

    def test_full(self) -> None:
        tm = TableMapping(
            source_schema="dbo",
            source_table="users",
            target_schema="public",
            target_table="users",
            columns=("id", "name", "email"),
            filter_sql="is_active = 1",
            write_strategy=WriteStrategy.UPSERT,
            batch_size=5000,
        )
        assert tm.write_strategy == WriteStrategy.UPSERT
        assert tm.batch_size == 5000


class TestHookConfig:
    """Tests for HookConfig."""

    def test_defaults(self) -> None:
        hook = HookConfig(name="backup", phase="pre", command="pg_dump mydb")
        assert hook.timeout_seconds == 300
        assert hook.on_failure == "abort"


class TestProjectModel:
    """Tests for ProjectModel."""

    def test_minimal(self) -> None:
        project = ProjectModel(
            name="test-migration",
            version="1.0",
            source=ConnectionConfig(dialect="mysql"),
            target=ConnectionConfig(dialect="postgresql"),
        )
        assert project.table_mappings == ()
        assert project.type_overrides is None
        assert project.sync_strategy == SyncStrategy.FULL
        assert project.default_batch_size == 10_000
        assert project.hooks == ()
        assert project.description == ""

    def test_with_overrides(self) -> None:
        overrides = MappingRuleSet(rules=(), name="user-overrides")
        project = ProjectModel(
            name="test",
            version="1.0",
            source=ConnectionConfig(dialect="mysql"),
            target=ConnectionConfig(dialect="postgresql"),
            type_overrides=overrides,
            sync_strategy=SyncStrategy.TIMESTAMP,
        )
        assert project.type_overrides is not None
        assert project.sync_strategy == SyncStrategy.TIMESTAMP


class TestMigrationPlan:
    """Tests for MigrationPlan."""

    def test_basic(self) -> None:
        project = ProjectModel(
            name="test",
            version="1.0",
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
