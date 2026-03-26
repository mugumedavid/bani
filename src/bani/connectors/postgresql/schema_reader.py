"""PostgreSQL schema introspection reader.

Queries information_schema and pg_catalog to build a complete picture
of the database schema, including tables, columns, indexes, constraints,
and foreign keys.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import psycopg

from bani.connectors.postgresql.type_mapper import PostgreSQLTypeMapper
from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


class PostgreSQLSchemaReader:
    """Introspects PostgreSQL schema using information_schema and pg_catalog.

    All queries use standard information_schema where possible for portability.
    Queries return frozen dataclass instances matching the Bani domain model.
    """

    def __init__(self, connection: psycopg.Connection[tuple[str, ...]]) -> None:
        """Initialize the schema reader.

        Args:
            connection: An active psycopg connection.
        """
        self.connection = connection
        self._type_mapper = PostgreSQLTypeMapper()

    def read_schema(self) -> DatabaseSchema:
        """Introspect the complete schema and return a DatabaseSchema.

        Returns:
            A DatabaseSchema with all tables and their metadata.

        Raises:
            Exception: If any query fails.
        """
        tables = self._read_tables()
        return DatabaseSchema(tables=tuple(tables), source_dialect="postgresql")

    def _read_tables(self) -> list[TableDefinition]:
        """Read all user tables and their metadata.

        Returns:
            List of TableDefinition objects.
        """
        with self.connection.cursor() as cur:
            # Query all user tables (excluding system schemas)
            query = """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                AND table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
            """
            cur.execute(query)
            tables_list: list[tuple[str, ...]] = cur.fetchall()

        tables = []
        for schema_name, table_name in tables_list:
            columns = self._read_columns(schema_name, table_name)
            primary_key = self._read_primary_key(schema_name, table_name)
            indexes = self._read_indexes(schema_name, table_name)
            foreign_keys = self._read_foreign_keys(schema_name, table_name)
            check_constraints = self._read_check_constraints(schema_name, table_name)
            row_count = self._estimate_row_count(schema_name, table_name)

            table_def = TableDefinition(
                schema_name=schema_name,
                table_name=table_name,
                columns=tuple(columns),
                primary_key=tuple(primary_key),
                indexes=tuple(indexes),
                foreign_keys=tuple(foreign_keys),
                check_constraints=tuple(check_constraints),
                row_count_estimate=row_count,
            )
            tables.append(table_def)

        return tables

    def _read_columns(
        self, schema_name: str, table_name: str
    ) -> list[ColumnDefinition]:
        """Read all columns for a table.

        Returns:
            List of ColumnDefinition objects.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
            cur.execute(query, (schema_name, table_name))
            rows: list[tuple[str, ...]] = cur.fetchall()

        columns = []
        for col_name, data_type, is_nullable, column_default, ordinal_pos in rows:
            # Handle type parameters (e.g., varchar(255))
            full_type = self._get_full_column_type(
                schema_name, table_name, col_name, data_type
            )

            # Check if column is auto-increment (serial/bigserial or nextval default)
            is_auto = self._is_auto_increment(full_type) or (
                column_default is not None and "nextval(" in str(column_default)
            )

            # Resolve canonical Arrow type for cross-database portability
            arrow_type = self._type_mapper.map_pg_type_name(full_type)
            arrow_type_str = str(arrow_type)

            # Strip source-specific defaults that won't translate
            # (e.g., nextval(...) for auto-increment columns)
            clean_default = column_default
            if is_auto and clean_default and "nextval(" in str(clean_default):
                clean_default = None

            col_def = ColumnDefinition(
                name=col_name,
                data_type=full_type,
                nullable=(is_nullable == "YES"),
                default_value=clean_default,
                is_auto_increment=is_auto,
                ordinal_position=int(ordinal_pos) - 1,  # Convert to 0-based
                arrow_type_str=arrow_type_str,
            )
            columns.append(col_def)

        return columns

    def _get_full_column_type(
        self, schema_name: str, table_name: str, column_name: str, base_type: str
    ) -> str:
        """Get the full column type including parameters (e.g., varchar(255)).

        Args:
            schema_name: Schema name.
            table_name: Table name.
            column_name: Column name.
            base_type: Base data type from information_schema.

        Returns:
            Full type string with parameters if applicable.
        """
        with self.connection.cursor() as cur:
            # Use pg_catalog for complete type information
            query = """
                SELECT format_type(atttypid, atttypmod)
                FROM pg_attribute
                WHERE attname = %s
                AND attrelid = (
                    SELECT oid FROM pg_class
                    WHERE relname = %s
                    AND relnamespace = (
                        SELECT oid FROM pg_namespace WHERE nspname = %s
                    )
                )
            """
            cur.execute(query, (column_name, table_name, schema_name))
            result: list[tuple[str, ...]] = cur.fetchall()

        if result:
            return result[0][0]
        return base_type

    def _is_auto_increment(self, column_type: str) -> bool:
        """Check if a column type is auto-increment (serial/bigserial).

        Args:
            column_type: The column type string.

        Returns:
            True if the column is serial or bigserial.
        """
        col_lower = column_type.lower()
        return col_lower in (
            "smallserial",
            "serial",
            "bigserial",
        ) or col_lower.startswith(("smallserial", "serial", "bigserial"))

    @staticmethod
    def _parse_pg_array(value: object) -> tuple[str, ...]:
        """Convert a PostgreSQL array result to a tuple of strings.

        Handles both cases:
        - psycopg returns a Python list (e.g. ['col1', 'col2'])
        - psycopg returns a PG array literal string (e.g. '{col1,col2}')
        """
        if isinstance(value, (list, tuple)):
            return tuple(str(v) for v in value)
        # It's a string like '{col1,col2}'
        s = str(value).strip()
        if s.startswith("{") and s.endswith("}"):
            s = s[1:-1]
        if not s:
            return ()
        return tuple(c.strip().strip('"') for c in s.split(","))

    def _read_primary_key(self, schema_name: str, table_name: str) -> list[str]:
        """Read the primary key columns for a table.

        Returns:
            List of column names in primary key order.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                WHERE i.indisprimary
                AND i.indrelid = (
                    SELECT oid FROM pg_class
                    WHERE relname = %s
                    AND relnamespace = (
                        SELECT oid FROM pg_namespace WHERE nspname = %s
                    )
                )
                ORDER BY a.attnum
            """
            cur.execute(query, (table_name, schema_name))
            rows: list[tuple[str, ...]] = cur.fetchall()

        return [row[0] for row in rows]

    def _read_indexes(self, schema_name: str, table_name: str) -> list[IndexDefinition]:
        """Read all indexes on a table (excluding primary key).

        Returns:
            List of IndexDefinition objects.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT
                    i.relname,
                    ix.indisunique,
                    ix.indisclustered,
                    pg_get_indexdef(ix.indexrelid),
                    array_agg(a.attname ORDER BY a.attnum)
                FROM pg_index ix
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_class t ON t.oid = ix.indrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                WHERE t.relname = %s
                AND n.nspname = %s
                AND NOT ix.indisprimary
                GROUP BY i.relname, ix.indisunique, ix.indisclustered, ix.indexrelid
                ORDER BY i.relname
            """
            cur.execute(query, (table_name, schema_name))
            rows: list[tuple[str, ...]] = cur.fetchall()

        indexes = []
        for idx_name, is_unique, is_clustered, _, col_names in rows:
            # Extract WHERE clause if present
            filter_expr = self._extract_filter_expression(idx_name, schema_name)

            idx_def = IndexDefinition(
                name=idx_name,
                columns=tuple(col_names),
                is_unique=bool(is_unique),
                is_clustered=bool(is_clustered),
                filter_expression=filter_expr,
            )
            indexes.append(idx_def)

        return indexes

    def _extract_filter_expression(
        self, index_name: str, schema_name: str
    ) -> str | None:
        """Extract the WHERE clause from a partial index definition.

        Args:
            index_name: Index name.
            schema_name: Schema name.

        Returns:
            The WHERE clause without the WHERE keyword, or None.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT pg_get_expr(indpred, indrelid)
                FROM pg_index
                WHERE indexrelid = (
                    SELECT oid FROM pg_class
                    WHERE relname = %s
                    AND relnamespace = (
                        SELECT oid FROM pg_namespace WHERE nspname = %s
                    )
                )
                AND indpred IS NOT NULL
            """
            cur.execute(query, (index_name, schema_name))
            result: list[tuple[str, ...]] = cur.fetchall()

        return result[0][0] if result else None

    def _read_foreign_keys(
        self, schema_name: str, table_name: str
    ) -> list[ForeignKeyDefinition]:
        """Read all foreign keys for a table.

        Returns:
            List of ForeignKeyDefinition objects.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT
                    c.constraint_name,
                    kcu1.table_schema,
                    kcu1.table_name,
                    array_agg(kcu1.column_name ORDER BY kcu1.ordinal_position),
                    kcu2.table_schema,
                    kcu2.table_name,
                    array_agg(kcu2.column_name ORDER BY kcu2.ordinal_position),
                    rc.update_rule,
                    rc.delete_rule
                FROM information_schema.table_constraints c
                JOIN information_schema.key_column_usage kcu1
                    ON kcu1.constraint_name = c.constraint_name
                    AND kcu1.table_schema = c.table_schema
                    AND kcu1.table_name = c.table_name
                JOIN information_schema.referential_constraints rc
                    ON rc.constraint_name = c.constraint_name
                    AND rc.constraint_schema = c.constraint_schema
                JOIN information_schema.key_column_usage kcu2
                    ON kcu2.constraint_name = rc.unique_constraint_name
                    AND kcu2.constraint_schema = rc.unique_constraint_schema
                WHERE c.constraint_type = 'FOREIGN KEY'
                AND kcu1.table_schema = %s
                AND kcu1.table_name = %s
                AND kcu2.ordinal_position = kcu1.ordinal_position
                GROUP BY
                    c.constraint_name,
                    kcu1.table_schema,
                    kcu1.table_name,
                    kcu2.table_schema,
                    kcu2.table_name,
                    rc.update_rule,
                    rc.delete_rule
                ORDER BY c.constraint_name
            """
            cur.execute(query, (schema_name, table_name))
            rows: list[tuple[str, ...]] = cur.fetchall()

        fks = []
        for (
            fk_name,
            src_schema,
            src_table,
            src_cols,
            ref_schema,
            ref_table,
            ref_cols,
            update_rule,
            delete_rule,
        ) in rows:
            fk_def = ForeignKeyDefinition(
                name=fk_name,
                source_table=f"{src_schema}.{src_table}",
                source_columns=self._parse_pg_array(src_cols),
                referenced_table=f"{ref_schema}.{ref_table}",
                referenced_columns=self._parse_pg_array(ref_cols),
                on_delete=delete_rule,
                on_update=update_rule,
            )
            fks.append(fk_def)

        return fks

    def _read_check_constraints(self, schema_name: str, table_name: str) -> list[str]:
        """Read all CHECK constraints for a table.

        Returns:
            List of constraint expressions.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE contype = 'c'
                AND conrelid = (
                    SELECT oid FROM pg_class
                    WHERE relname = %s
                    AND relnamespace = (
                        SELECT oid FROM pg_namespace WHERE nspname = %s
                    )
                )
                ORDER BY conname
            """
            cur.execute(query, (table_name, schema_name))
            rows: list[tuple[str, ...]] = cur.fetchall()

        # Extract just the condition part (after "CHECK")
        constraints = []
        for constraint_def in rows:
            # pg_get_constraintdef returns "CHECK (condition)"
            condition = constraint_def[0]
            if condition.startswith("CHECK "):
                condition = condition[6:]  # Remove "CHECK "
            constraints.append(condition)

        return constraints

    def _estimate_row_count(self, schema_name: str, table_name: str) -> int | None:
        """Estimate the row count using pg_stat_user_tables.

        Args:
            schema_name: Schema name.
            table_name: Table name.

        Returns:
            Estimated row count, or None if unavailable.
        """
        try:
            with self.connection.cursor() as cur:
                query = """
                    SELECT n_live_tup
                    FROM pg_stat_user_tables
                    WHERE schemaname = %s AND relname = %s
                """
                cur.execute(query, (schema_name, table_name))
                result: list[tuple[str, ...]] = cur.fetchall()

            return int(result[0][0]) if result else None
        except Exception:
            # If stats are unavailable, fall back to None
            return None
