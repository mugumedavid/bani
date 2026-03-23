"""Tests for dependency resolution (topological sort)."""

from __future__ import annotations

from bani.domain.dependency import DependencyResolver, ResolutionResult
from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    ForeignKeyDefinition,
    TableDefinition,
)


def _make_table(
    name: str,
    schema: str = "public",
    fks: tuple[ForeignKeyDefinition, ...] = (),
) -> TableDefinition:
    """Helper to create a minimal TableDefinition."""
    return TableDefinition(
        schema_name=schema,
        table_name=name,
        columns=(ColumnDefinition(name="id", data_type="INT"),),
        foreign_keys=fks,
    )


class TestDependencyResolver:
    """Tests for DependencyResolver."""

    def test_no_dependencies(self) -> None:
        schema = DatabaseSchema(
            tables=(
                _make_table("a"),
                _make_table("b"),
                _make_table("c"),
            ),
            source_dialect="postgresql",
        )
        resolver = DependencyResolver()
        result = resolver.resolve(schema)

        assert isinstance(result, ResolutionResult)
        assert set(result.ordered_tables) == {
            "public.a",
            "public.b",
            "public.c",
        }
        assert result.deferred_fks == ()

    def test_linear_chain(self) -> None:
        """A -> B -> C: C must come before B, B before A."""
        fk_a_b = ForeignKeyDefinition(
            name="fk_a_b",
            source_table="public.a",
            source_columns=("b_id",),
            referenced_table="b",
            referenced_columns=("id",),
        )
        fk_b_c = ForeignKeyDefinition(
            name="fk_b_c",
            source_table="public.b",
            source_columns=("c_id",),
            referenced_table="c",
            referenced_columns=("id",),
        )
        schema = DatabaseSchema(
            tables=(
                _make_table("a", fks=(fk_a_b,)),
                _make_table("b", fks=(fk_b_c,)),
                _make_table("c"),
            ),
            source_dialect="postgresql",
        )
        resolver = DependencyResolver()
        result = resolver.resolve(schema)

        ordered = list(result.ordered_tables)
        assert ordered.index("public.c") < ordered.index("public.b")
        assert ordered.index("public.b") < ordered.index("public.a")
        assert result.deferred_fks == ()

    def test_circular_dependency_defers_fks(self) -> None:
        """A -> B -> A: should detect cycle and defer at least one FK."""
        fk_a_b = ForeignKeyDefinition(
            name="fk_a_b",
            source_table="public.a",
            source_columns=("b_id",),
            referenced_table="b",
            referenced_columns=("id",),
        )
        fk_b_a = ForeignKeyDefinition(
            name="fk_b_a",
            source_table="public.b",
            source_columns=("a_id",),
            referenced_table="a",
            referenced_columns=("id",),
        )
        schema = DatabaseSchema(
            tables=(
                _make_table("a", fks=(fk_a_b,)),
                _make_table("b", fks=(fk_b_a,)),
            ),
            source_dialect="postgresql",
        )
        resolver = DependencyResolver()
        result = resolver.resolve(schema)

        assert len(result.ordered_tables) == 2
        assert len(result.deferred_fks) > 0

    def test_self_referencing_fk(self) -> None:
        """A table referencing itself should not cause issues."""
        fk_self = ForeignKeyDefinition(
            name="fk_self",
            source_table="public.tree",
            source_columns=("parent_id",),
            referenced_table="tree",
            referenced_columns=("id",),
        )
        schema = DatabaseSchema(
            tables=(_make_table("tree", fks=(fk_self,)),),
            source_dialect="postgresql",
        )
        resolver = DependencyResolver()
        result = resolver.resolve(schema)

        assert result.ordered_tables == ("public.tree",)
        assert result.deferred_fks == ()

    def test_empty_schema(self) -> None:
        schema = DatabaseSchema(tables=(), source_dialect="mysql")
        resolver = DependencyResolver()
        result = resolver.resolve(schema)
        assert result.ordered_tables == ()
        assert result.deferred_fks == ()

    def test_fk_to_table_outside_schema(self) -> None:
        """FK referencing a table not in the schema is ignored."""
        fk_ext = ForeignKeyDefinition(
            name="fk_ext",
            source_table="public.a",
            source_columns=("ext_id",),
            referenced_table="external_table",
            referenced_columns=("id",),
        )
        schema = DatabaseSchema(
            tables=(_make_table("a", fks=(fk_ext,)),),
            source_dialect="postgresql",
        )
        resolver = DependencyResolver()
        result = resolver.resolve(schema)
        assert result.ordered_tables == ("public.a",)
