"""Schema introspection models (Section 11.1).

All dataclasses are frozen and use tuples (not lists) for collection fields,
ensuring immutability and thread-safety as required by Section 11.2.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto


class ConstraintType(Enum):
    """Types of constraints that can be applied to a table."""

    PRIMARY_KEY = auto()
    UNIQUE = auto()
    CHECK = auto()
    FOREIGN_KEY = auto()


@dataclass(frozen=True)
class ColumnDefinition:
    """A single column within a table.

    Attributes:
        name: Column name.
        data_type: Raw source type string, e.g. ``"VARCHAR(255)"``.
        nullable: Whether the column allows NULL values.
        default_value: Default value expression, if any.
        is_auto_increment: Whether the column auto-increments.
        ordinal_position: 0-based position of the column in the table.
        arrow_type_str: Canonical Arrow type string (e.g. ``"int32"``,
            ``"timestamp[us]"``).  Populated during schema introspection
            and used by sink connectors to generate target-native DDL
            without needing NxN source→target translation tables.
            ``None`` when the column has not been through introspection
            (e.g. hand-built ``TableDefinition`` in tests).
    """

    name: str
    data_type: str
    nullable: bool = True
    default_value: str | None = None
    is_auto_increment: bool = False
    ordinal_position: int = 0
    arrow_type_str: str | None = None


@dataclass(frozen=True)
class IndexDefinition:
    """An index on a table.

    Attributes:
        name: Index name.
        columns: Ordered tuple of column names in the index.
        is_unique: Whether the index enforces uniqueness.
        is_clustered: Whether the index is clustered.
        filter_expression: Optional partial-index filter expression.
    """

    name: str
    columns: tuple[str, ...]
    is_unique: bool = False
    is_clustered: bool = False
    filter_expression: str | None = None


@dataclass(frozen=True)
class ForeignKeyDefinition:
    """A foreign key relationship between two tables.

    Attributes:
        name: Constraint name.
        source_table: Fully qualified name of the referencing table.
        source_columns: Columns in the referencing table.
        referenced_table: Fully qualified name of the referenced table.
        referenced_columns: Columns in the referenced table.
        on_delete: Referential action on delete.
        on_update: Referential action on update.
    """

    name: str
    source_table: str
    source_columns: tuple[str, ...]
    referenced_table: str
    referenced_columns: tuple[str, ...]
    on_delete: str = "NO ACTION"
    on_update: str = "NO ACTION"


@dataclass(frozen=True)
class TableDefinition:
    """A table within a database schema.

    Attributes:
        schema_name: Database schema (namespace) the table belongs to.
        table_name: Name of the table.
        columns: Ordered tuple of column definitions.
        primary_key: Tuple of column names forming the primary key.
        indexes: Tuple of index definitions on this table.
        foreign_keys: Tuple of foreign key definitions on this table.
        check_constraints: Tuple of CHECK constraint expressions.
        row_count_estimate: Estimated row count from schema introspection,
            or ``None`` if unavailable.
    """

    schema_name: str
    table_name: str
    columns: tuple[ColumnDefinition, ...]
    primary_key: tuple[str, ...] = ()
    indexes: tuple[IndexDefinition, ...] = ()
    foreign_keys: tuple[ForeignKeyDefinition, ...] = ()
    check_constraints: tuple[str, ...] = ()
    row_count_estimate: int | None = None

    @property
    def fully_qualified_name(self) -> str:
        """Return ``schema_name.table_name``."""
        return f"{self.schema_name}.{self.table_name}"


@dataclass(frozen=True)
class DatabaseSchema:
    """The full schema of a database as returned by introspection.

    Attributes:
        tables: Tuple of table definitions.
        source_dialect: Dialect identifier, e.g. ``"postgresql"``, ``"mssql"``.
    """

    tables: tuple[TableDefinition, ...]
    source_dialect: str

    def get_table(self, schema_name: str, table_name: str) -> TableDefinition | None:
        """Look up a table by schema and name.

        Args:
            schema_name: The schema (namespace) to search in.
            table_name: The table name to find.

        Returns:
            The matching ``TableDefinition``, or ``None`` if not found.
        """
        for table in self.tables:
            if table.schema_name == schema_name and table.table_name == table_name:
                return table
        return None
