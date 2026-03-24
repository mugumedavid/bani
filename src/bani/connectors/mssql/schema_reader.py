"""MSSQL schema introspection reader.

Queries INFORMATION_SCHEMA and sys.* catalog views to build a complete picture
of the database schema, including tables, columns, types, PKs, indexes, FKs,
check constraints, identity columns, and row count estimates.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass  # pymssql typing not needed since we use Any in __init__

from bani.connectors.mssql.type_mapper import MSSQLTypeMapper
from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


class MSSQLSchemaReader:
    """Introspects MSSQL schema using INFORMATION_SCHEMA and sys views.

    Handles MSSQL-specific features like identity columns, nvarchar vs varchar,
    datetime2 vs datetime, SCOPE_IDENTITY, and schema-qualified names.
    """

    def __init__(self, connection: Any, database: str) -> None:
        """Initialize the schema reader.

        Args:
            connection: An active pymssql connection.
            database: The database name to introspect.
        """
        self.connection = connection
        self.database = database
        self._type_mapper = MSSQLTypeMapper()

    def read_schema(self) -> DatabaseSchema:
        """Introspect the complete schema and return a DatabaseSchema.

        Returns:
            A DatabaseSchema with all tables and their metadata.

        Raises:
            Exception: If any query fails.
        """
        tables = self._read_tables()
        return DatabaseSchema(tables=tuple(tables), source_dialect="mssql")

    def _read_tables(self) -> list[TableDefinition]:
        """Read all user tables and their metadata.

        Returns:
            List of TableDefinition objects.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT
                    t.table_schema,
                    t.table_name
                FROM information_schema.tables t
                WHERE t.table_catalog = DB_NAME()
                AND t.table_type = 'BASE TABLE'
                ORDER BY t.table_schema, t.table_name
            """
            cur.execute(query)
            tables_list: list[tuple[Any, ...]] = list(cur.fetchall())

        tables = []
        for schema_name, table_name in tables_list:
            columns = self._read_columns(schema_name, table_name)
            primary_key = self._read_primary_key(schema_name, table_name)
            indexes = self._read_indexes(schema_name, table_name)
            foreign_keys = self._read_foreign_keys(schema_name, table_name)
            check_constraints = self._read_check_constraints(schema_name, table_name)
            row_estimate = self._estimate_row_count(schema_name, table_name)

            table_def = TableDefinition(
                schema_name=schema_name,
                table_name=table_name,
                columns=tuple(columns),
                primary_key=tuple(primary_key),
                indexes=tuple(indexes),
                foreign_keys=tuple(foreign_keys),
                check_constraints=tuple(check_constraints),
                row_count_estimate=row_estimate,
            )
            tables.append(table_def)

        return tables

    def _read_columns(
        self, schema_name: str, table_name: str
    ) -> list[ColumnDefinition]:
        """Read all columns for a table.

        Handles MSSQL-specific column types and identity columns.

        Returns:
            List of ColumnDefinition objects.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    c.ordinal_position
                FROM information_schema.columns c
                WHERE c.table_schema = %s AND c.table_name = %s
                ORDER BY c.ordinal_position
            """
            cur.execute(query, (schema_name, table_name))
            columns_list: list[tuple[Any, ...]] = list(cur.fetchall())

        identity_cols = self._get_identity_columns(schema_name, table_name)

        columns = []
        for (
            col_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_pos,
        ) in columns_list:
            arrow_type = self._type_mapper.map_mssql_type_name(data_type)
            arrow_type_str = str(arrow_type)

            is_auto = col_name in identity_cols

            col_def = ColumnDefinition(
                name=col_name,
                data_type=data_type,
                nullable=(is_nullable == "YES"),
                default_value=column_default,
                is_auto_increment=is_auto,
                ordinal_position=int(ordinal_pos) - 1,
                arrow_type_str=arrow_type_str,
            )
            columns.append(col_def)

        return columns

    def _get_identity_columns(self, schema_name: str, table_name: str) -> set[str]:
        """Get the set of identity columns for a table.

        Returns:
            Set of column names that are IDENTITY columns.
        """
        try:
            with self.connection.cursor() as cur:
                query = """
                    SELECT c.name
                    FROM sys.columns c
                    JOIN sys.tables t ON c.object_id = t.object_id
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = %s AND t.name = %s AND c.is_identity = 1
                """
                cur.execute(query, (schema_name, table_name))
                rows: list[tuple[Any, ...]] = list(cur.fetchall())
                return {row[0] for row in rows}
        except Exception:
            return set()

    def _read_primary_key(self, schema_name: str, table_name: str) -> list[str]:
        """Read the primary key columns for a table.

        Returns:
            List of column names in primary key order.
        """
        try:
            with self.connection.cursor() as cur:
                query = """
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    WHERE tc.table_schema = %s
                    AND tc.table_name = %s
                    AND tc.constraint_type = 'PRIMARY KEY'
                    ORDER BY kcu.ordinal_position
                """
                cur.execute(query, (schema_name, table_name))
                rows: list[tuple[Any, ...]] = list(cur.fetchall())
                return [row[0] for row in rows]
        except Exception:
            return []

    def _read_indexes(self, schema_name: str, table_name: str) -> list[IndexDefinition]:
        """Read all indexes on a table (excluding primary key).

        Returns:
            List of IndexDefinition objects.
        """
        try:
            with self.connection.cursor() as cur:
                query = """
                    SELECT
                        i.name,
                        is_unique,
                        is_primary_key
                    FROM sys.indexes i
                    JOIN sys.tables t ON i.object_id = t.object_id
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = %s AND t.name = %s AND is_primary_key = 0
                    ORDER BY i.name
                """
                cur.execute(query, (schema_name, table_name))
                indexes_list: list[tuple[Any, ...]] = list(cur.fetchall())

            indexes = []
            for idx_name, is_unique, _ in indexes_list:
                col_names = self._get_index_columns(schema_name, table_name, idx_name)
                if col_names:
                    idx_def = IndexDefinition(
                        name=idx_name,
                        columns=tuple(col_names),
                        is_unique=bool(is_unique),
                        is_clustered=False,
                        filter_expression=None,
                    )
                    indexes.append(idx_def)

            return indexes
        except Exception:
            return []

    def _get_index_columns(
        self, schema_name: str, table_name: str, index_name: str
    ) -> list[str]:
        """Get the columns that make up an index.

        Returns:
            List of column names in index order.
        """
        try:
            with self.connection.cursor() as cur:
                query = """
                    SELECT c.name
                    FROM sys.index_columns ic
                    JOIN sys.columns c ON ic.object_id = c.object_id
                        AND ic.column_id = c.column_id
                    JOIN sys.indexes i ON ic.object_id = i.object_id
                        AND ic.index_id = i.index_id
                    JOIN sys.tables t ON i.object_id = t.object_id
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = %s AND t.name = %s AND i.name = %s
                    ORDER BY ic.key_ordinal
                """
                cur.execute(query, (schema_name, table_name, index_name))
                rows: list[tuple[Any, ...]] = list(cur.fetchall())
                return [row[0] for row in rows]
        except Exception:
            return []

    def _read_foreign_keys(
        self, schema_name: str, table_name: str
    ) -> list[ForeignKeyDefinition]:
        """Read all foreign keys for a table.

        Returns:
            List of ForeignKeyDefinition objects.
        """
        try:
            with self.connection.cursor() as cur:
                query = """
                    SELECT
                        rc.constraint_name,
                        kcu1.table_schema,
                        kcu1.table_name,
                        kcu1.column_name,
                        kcu2.table_schema,
                        kcu2.table_name,
                        kcu2.column_name,
                        rc.update_rule,
                        rc.delete_rule
                    FROM information_schema.referential_constraints rc
                    JOIN information_schema.key_column_usage kcu1
                        ON rc.constraint_name = kcu1.constraint_name
                        AND rc.constraint_schema = kcu1.constraint_schema
                    JOIN information_schema.key_column_usage kcu2
                        ON rc.unique_constraint_name = kcu2.constraint_name
                        AND rc.unique_constraint_schema = kcu2.constraint_schema
                        AND kcu1.ordinal_position = kcu2.ordinal_position
                    WHERE rc.constraint_schema = %s
                    AND kcu1.table_name = %s
                    ORDER BY rc.constraint_name, kcu1.ordinal_position
                """
                cur.execute(query, (schema_name, table_name))
                rows: list[tuple[Any, ...]] = list(cur.fetchall())

            fks_dict: dict[str, dict[str, Any]] = {}
            for (
                fk_name,
                src_schema,
                src_table,
                src_col,
                ref_schema,
                ref_table,
                ref_col,
                update_rule,
                delete_rule,
            ) in rows:
                if fk_name not in fks_dict:
                    fks_dict[fk_name] = {
                        "src_schema": src_schema,
                        "src_table": src_table,
                        "src_cols": [],
                        "ref_schema": ref_schema,
                        "ref_table": ref_table,
                        "ref_cols": [],
                        "update_rule": update_rule,
                        "delete_rule": delete_rule,
                    }
                fks_dict[fk_name]["src_cols"].append(src_col)
                fks_dict[fk_name]["ref_cols"].append(ref_col)

            fks = []
            for fk_name, fk_info in fks_dict.items():
                fk_def = ForeignKeyDefinition(
                    name=fk_name,
                    source_table=f"{fk_info['src_schema']}.{fk_info['src_table']}",
                    source_columns=tuple(fk_info["src_cols"]),
                    referenced_table=f"{fk_info['ref_schema']}.{fk_info['ref_table']}",
                    referenced_columns=tuple(fk_info["ref_cols"]),
                    on_delete=fk_info["delete_rule"],
                    on_update=fk_info["update_rule"],
                )
                fks.append(fk_def)

            return fks
        except Exception:
            return []

    def _read_check_constraints(self, schema_name: str, table_name: str) -> list[str]:
        """Read all CHECK constraints for a table.

        Returns:
            List of constraint expressions.
        """
        try:
            with self.connection.cursor() as cur:
                query = """
                    SELECT check_clause
                    FROM information_schema.check_constraints
                    WHERE constraint_schema = %s
                    AND table_name = %s
                """
                cur.execute(query, (schema_name, table_name))
                rows: list[tuple[Any, ...]] = list(cur.fetchall())
                return [row[0] for row in rows]
        except Exception:
            return []

    def _estimate_row_count(self, schema_name: str, table_name: str) -> int | None:
        """Get an estimated row count for a table.

        Uses sys.dm_db_partition_stats for a fast estimate.

        Returns:
            Estimated row count, or None if unavailable.
        """
        try:
            with self.connection.cursor() as cur:
                query = """
                    SELECT SUM(row_count)
                    FROM sys.dm_db_partition_stats
                    WHERE object_id = OBJECT_ID(%s + '.' + %s)
                    AND index_id <= 1
                """
                cur.execute(query, (schema_name, table_name))
                result: tuple[Any, ...] | None = cur.fetchone()
                if result and result[0] is not None:
                    return int(result[0])
        except Exception:
            pass

        try:
            with self.connection.cursor() as cur:
                query = f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]"
                cur.execute(query)
                result = cur.fetchone()
                if result:
                    return int(result[0])
        except Exception:
            pass

        return None
