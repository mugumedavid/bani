"""SQLite schema introspection reader.

Queries SQLite PRAGMAs and sqlite_master to build a complete picture of the
database schema, including tables, columns, indexes, constraints, and
foreign keys.

SQLite-specific notes:
- Uses PRAGMA table_info, PRAGMA index_list, PRAGMA index_info,
  PRAGMA foreign_key_list, and sqlite_master for introspection.
- INTEGER PRIMARY KEY is the ROWID alias and acts as auto-increment.
- 'main' is used as the default schema name.
- Foreign keys must be enabled with PRAGMA foreign_keys = ON.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from bani.connectors.sqlite.type_mapper import SQLiteTypeMapper
from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


class SQLiteSchemaReader:
    """Introspects SQLite schema using PRAGMAs and sqlite_master.

    Handles SQLite-specific features like type affinity, INTEGER PRIMARY KEY
    auto-increment, and the absence of native schema namespaces.
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        """Initialize the schema reader.

        Args:
            connection: An active sqlite3 connection.
        """
        self.connection = connection
        self._type_mapper = SQLiteTypeMapper()

    def read_schema(self) -> DatabaseSchema:
        """Introspect the complete schema and return a DatabaseSchema.

        Returns:
            A DatabaseSchema with all tables and their metadata.

        Raises:
            Exception: If any query fails.
        """
        tables = self._read_tables()
        return DatabaseSchema(tables=tuple(tables), source_dialect="sqlite")

    def _read_tables(self) -> list[TableDefinition]:
        """Read all user tables and their metadata.

        Returns:
            List of TableDefinition objects.
        """
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        )
        table_names: list[str] = [row[0] for row in cursor.fetchall()]

        tables = []
        for table_name in table_names:
            columns = self._read_columns(table_name)
            primary_key = self._read_primary_key(table_name)
            indexes = self._read_indexes(table_name)
            foreign_keys = self._read_foreign_keys(table_name)
            row_estimate = self._estimate_row_count(table_name)

            # Detect autoincrement: INTEGER PRIMARY KEY with exactly one PK
            # column whose type is INTEGER is the ROWID alias
            if len(primary_key) == 1:
                pk_col_name = primary_key[0]
                updated_columns: list[ColumnDefinition] = []
                for col in columns:
                    if col.name == pk_col_name and col.data_type.upper() == "INTEGER":
                        # INTEGER PRIMARY KEY is always the ROWID alias,
                        # so mark it as auto-increment
                        col = ColumnDefinition(
                            name=col.name,
                            data_type=col.data_type,
                            nullable=col.nullable,
                            default_value=col.default_value,
                            is_auto_increment=True,
                            ordinal_position=col.ordinal_position,
                            arrow_type_str=col.arrow_type_str,
                        )
                    updated_columns.append(col)
                columns = updated_columns

            table_def = TableDefinition(
                schema_name="main",
                table_name=table_name,
                columns=tuple(columns),
                primary_key=tuple(primary_key),
                indexes=tuple(indexes),
                foreign_keys=tuple(foreign_keys),
                check_constraints=(),
                row_count_estimate=row_estimate,
            )
            tables.append(table_def)

        return tables

    def _read_columns(self, table_name: str) -> list[ColumnDefinition]:
        """Read all columns for a table using PRAGMA table_info.

        Returns:
            List of ColumnDefinition objects.
        """
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        rows: list[tuple[Any, ...]] = cursor.fetchall()

        columns = []
        for cid, name, col_type, notnull, dflt_value, _pk in rows:
            # Map declared type to Arrow type
            arrow_type = self._type_mapper.map_sqlite_type_name(col_type)
            arrow_type_str = str(arrow_type)

            col_def = ColumnDefinition(
                name=name,
                data_type=col_type if col_type else "",
                nullable=(int(notnull) == 0),
                default_value=str(dflt_value) if dflt_value is not None else None,
                is_auto_increment=False,  # Updated later for INTEGER PK
                ordinal_position=int(cid),
                arrow_type_str=arrow_type_str,
            )
            columns.append(col_def)

        return columns

    def _read_primary_key(self, table_name: str) -> list[str]:
        """Read the primary key columns for a table.

        Uses PRAGMA table_info; columns with pk > 0 are part of the PK.

        Returns:
            List of column names in primary key order.
        """
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        rows: list[tuple[Any, ...]] = cursor.fetchall()

        pk_columns: list[tuple[int, str]] = []
        for _cid, name, _col_type, _notnull, _dflt_value, pk in rows:
            if int(pk) > 0:
                pk_columns.append((int(pk), name))

        # Sort by pk ordinal
        pk_columns.sort(key=lambda x: x[0])
        return [name for _, name in pk_columns]

    def _read_indexes(self, table_name: str) -> list[IndexDefinition]:
        """Read all indexes on a table (excluding auto-created indexes).

        Returns:
            List of IndexDefinition objects.
        """
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA index_list('{table_name}')")
        index_rows: list[tuple[Any, ...]] = cursor.fetchall()

        indexes = []
        for _seq, idx_name, is_unique, origin, _partial in index_rows:
            # Skip auto-created indexes for PRIMARY KEY (origin='pk')
            if origin == "pk":
                continue

            # Get columns for this index
            cursor.execute(f"PRAGMA index_info('{idx_name}')")
            col_rows: list[tuple[Any, ...]] = cursor.fetchall()

            col_names = [str(row[2]) for row in col_rows]

            idx_def = IndexDefinition(
                name=idx_name,
                columns=tuple(col_names),
                is_unique=(int(is_unique) == 1),
                is_clustered=False,
                filter_expression=None,
            )
            indexes.append(idx_def)

        return indexes

    def _read_foreign_keys(self, table_name: str) -> list[ForeignKeyDefinition]:
        """Read all foreign keys for a table using PRAGMA foreign_key_list.

        Returns:
            List of ForeignKeyDefinition objects.
        """
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA foreign_key_list('{table_name}')")
        rows: list[tuple[Any, ...]] = cursor.fetchall()

        # Group by FK id
        fk_groups: dict[int, dict[str, Any]] = {}
        for row in rows:
            fk_id, seq, ref_table = row[0], row[1], row[2]
            from_col, to_col = row[3], row[4]
            on_update, on_delete, _match = row[5], row[6], row[7]
            fk_id_int = int(fk_id)
            if fk_id_int not in fk_groups:
                fk_groups[fk_id_int] = {
                    "ref_table": ref_table,
                    "from_cols": [],
                    "to_cols": [],
                    "on_update": on_update,
                    "on_delete": on_delete,
                }
            fk_groups[fk_id_int]["from_cols"].append((int(seq), from_col))
            fk_groups[fk_id_int]["to_cols"].append((int(seq), to_col))

        fks = []
        for fk_id_int, fk_info in sorted(fk_groups.items()):
            # Sort columns by sequence
            from_cols = [
                col for _, col in sorted(fk_info["from_cols"], key=lambda x: x[0])
            ]
            to_cols = [col for _, col in sorted(fk_info["to_cols"], key=lambda x: x[0])]

            fk_name = f"fk_{table_name}_{fk_id_int}"

            fk_def = ForeignKeyDefinition(
                name=fk_name,
                source_table=f"main.{table_name}",
                source_columns=tuple(from_cols),
                referenced_table=f"main.{fk_info['ref_table']}",
                referenced_columns=tuple(to_cols),
                on_delete=fk_info["on_delete"] or "NO ACTION",
                on_update=fk_info["on_update"] or "NO ACTION",
            )
            fks.append(fk_def)

        return fks

    def _estimate_row_count(self, table_name: str) -> int | None:
        """Estimate the row count for a table.

        Tries sqlite_stat1 first for a fast estimate, falls back to
        COUNT(*).

        Returns:
            Estimated row count, or None if unavailable.
        """
        cursor = self.connection.cursor()

        # Try sqlite_stat1 (available after ANALYZE)
        try:
            cursor.execute(
                "SELECT stat FROM sqlite_stat1 WHERE tbl = ? AND idx IS NULL",
                (table_name,),
            )
            row = cursor.fetchone()
            if row and row[0]:
                # stat column format: "nrow ..." — first token is row count
                parts = str(row[0]).split()
                if parts:
                    return int(parts[0])
        except sqlite3.OperationalError:
            # sqlite_stat1 doesn't exist (ANALYZE never run)
            pass

        # Fallback: COUNT(*)
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        result = cursor.fetchone()
        return int(result[0]) if result else None

    def _has_autoincrement(self, table_name: str) -> bool:
        """Check if a table uses the AUTOINCREMENT keyword.

        Returns:
            True if AUTOINCREMENT is in the CREATE TABLE statement.
        """
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        row = cursor.fetchone()
        if row and row[0]:
            return "AUTOINCREMENT" in str(row[0]).upper()
        return False
