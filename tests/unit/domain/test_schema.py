"""Tests for schema introspection models."""

from __future__ import annotations

from bani.domain.schema import (
    ColumnDefinition,
    ConstraintType,
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


class TestColumnDefinition:
    """Tests for ColumnDefinition."""

    def test_defaults(self) -> None:
        col = ColumnDefinition(name="id", data_type="INT")
        assert col.nullable is True
        assert col.default_value is None
        assert col.is_auto_increment is False
        assert col.ordinal_position == 0

    def test_all_fields(self) -> None:
        col = ColumnDefinition(
            name="created_at",
            data_type="TIMESTAMP",
            nullable=False,
            default_value="CURRENT_TIMESTAMP",
            is_auto_increment=False,
            ordinal_position=5,
        )
        assert col.name == "created_at"
        assert col.data_type == "TIMESTAMP"
        assert col.nullable is False
        assert col.default_value == "CURRENT_TIMESTAMP"

    def test_frozen(self) -> None:
        col = ColumnDefinition(name="id", data_type="INT")
        try:
            col.name = "other"  # type: ignore[misc]
            raise AssertionError("Should not be able to mutate frozen dataclass")
        except AttributeError:
            pass


class TestIndexDefinition:
    """Tests for IndexDefinition."""

    def test_basic(self) -> None:
        idx = IndexDefinition(name="idx_email", columns=("email",))
        assert idx.is_unique is False
        assert idx.is_clustered is False
        assert idx.filter_expression is None

    def test_unique_filtered(self) -> None:
        idx = IndexDefinition(
            name="idx_active_email",
            columns=("email",),
            is_unique=True,
            filter_expression="is_active = 1",
        )
        assert idx.is_unique is True
        assert idx.filter_expression == "is_active = 1"


class TestForeignKeyDefinition:
    """Tests for ForeignKeyDefinition."""

    def test_defaults(self) -> None:
        fk = ForeignKeyDefinition(
            name="fk_order_user",
            source_table="orders",
            source_columns=("user_id",),
            referenced_table="users",
            referenced_columns=("id",),
        )
        assert fk.on_delete == "NO ACTION"
        assert fk.on_update == "NO ACTION"

    def test_cascade(self) -> None:
        fk = ForeignKeyDefinition(
            name="fk_order_user",
            source_table="orders",
            source_columns=("user_id",),
            referenced_table="users",
            referenced_columns=("id",),
            on_delete="CASCADE",
            on_update="CASCADE",
        )
        assert fk.on_delete == "CASCADE"


class TestTableDefinition:
    """Tests for TableDefinition."""

    def test_minimal(self) -> None:
        table = TableDefinition(
            schema_name="public",
            table_name="users",
            columns=(ColumnDefinition(name="id", data_type="INT"),),
        )
        assert table.primary_key == ()
        assert table.indexes == ()
        assert table.foreign_keys == ()
        assert table.check_constraints == ()
        assert table.row_count_estimate is None

    def test_fully_qualified_name(self) -> None:
        table = TableDefinition(
            schema_name="dbo",
            table_name="orders",
            columns=(),
        )
        assert table.fully_qualified_name == "dbo.orders"

    def test_with_row_count(self) -> None:
        table = TableDefinition(
            schema_name="public",
            table_name="users",
            columns=(),
            row_count_estimate=1_000_000,
        )
        assert table.row_count_estimate == 1_000_000


class TestDatabaseSchema:
    """Tests for DatabaseSchema."""

    def test_get_table_found(self) -> None:
        users = TableDefinition(
            schema_name="public",
            table_name="users",
            columns=(ColumnDefinition(name="id", data_type="INT"),),
        )
        orders = TableDefinition(
            schema_name="public",
            table_name="orders",
            columns=(ColumnDefinition(name="id", data_type="INT"),),
        )
        schema = DatabaseSchema(
            tables=(users, orders),
            source_dialect="postgresql",
        )

        result = schema.get_table("public", "users")
        assert result is users

    def test_get_table_not_found(self) -> None:
        schema = DatabaseSchema(tables=(), source_dialect="mysql")
        assert schema.get_table("public", "missing") is None

    def test_frozen(self) -> None:
        schema = DatabaseSchema(tables=(), source_dialect="mysql")
        try:
            schema.source_dialect = "other"  # type: ignore[misc]
            raise AssertionError("Should not be able to mutate frozen dataclass")
        except AttributeError:
            pass


class TestConstraintType:
    """Tests for ConstraintType enum."""

    def test_members(self) -> None:
        assert ConstraintType.PRIMARY_KEY is not None
        assert ConstraintType.UNIQUE is not None
        assert ConstraintType.CHECK is not None
        assert ConstraintType.FOREIGN_KEY is not None
