"""MySQL schema introspection reader.

Queries information_schema to build a complete picture of the database
schema, including tables, columns, indexes, constraints, foreign keys,
and MySQL-specific features like ENUM/SET types and auto_increment.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pymysql

from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


class MySQLSchemaReader:
    """Introspects MySQL schema using information_schema.

    Handles MySQL-specific features like unsigned integers, ENUM/SET types,
    auto_increment, and charset handling (utf8mb4).
    """

    def __init__(
        self, connection: pymysql.connections.Connection[Any], database: str
    ) -> None:
        """Initialize the schema reader.

        Args:
            connection: An active PyMySQL connection.
            database: The database name to introspect.
        """
        self.connection = connection
        self.database = database

    def read_schema(self) -> DatabaseSchema:
        """Introspect the complete schema and return a DatabaseSchema.

        Returns:
            A DatabaseSchema with all tables and their metadata.

        Raises:
            Exception: If any query fails.
        """
        tables = self._read_tables()
        return DatabaseSchema(tables=tuple(tables), source_dialect="mysql")

    def _read_tables(self) -> list[TableDefinition]:
        """Read all user tables and their metadata.

        Returns:
            List of TableDefinition objects.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT table_schema, table_name, table_rows
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
            """
            cur.execute(query, (self.database,))
            tables_list: list[tuple[Any, ...]] = list(cur.fetchall())

        tables = []
        for schema_name, table_name, table_rows in tables_list:
            columns = self._read_columns(schema_name, table_name)
            primary_key = self._read_primary_key(schema_name, table_name)
            indexes = self._read_indexes(schema_name, table_name)
            foreign_keys = self._read_foreign_keys(schema_name, table_name)
            check_constraints = self._read_check_constraints(schema_name, table_name)

            row_estimate = int(table_rows) if table_rows is not None else None

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

        Handles MySQL-specific column types including ENUM, SET, unsigned
        integers, and auto_increment detection.

        Returns:
            List of ColumnDefinition objects.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT
                    column_name,
                    column_type,
                    is_nullable,
                    column_default,
                    ordinal_position,
                    extra
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
            cur.execute(query, (schema_name, table_name))
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        columns = []
        for (
            col_name,
            column_type,
            is_nullable,
            column_default,
            ordinal_pos,
            extra,
        ) in rows:
            # Detect auto_increment from EXTRA field
            is_auto = "auto_increment" in str(extra).lower()

            col_def = ColumnDefinition(
                name=col_name,
                data_type=column_type,
                nullable=(is_nullable == "YES"),
                default_value=column_default,
                is_auto_increment=is_auto,
                ordinal_position=int(ordinal_pos) - 1,  # Convert to 0-based
            )
            columns.append(col_def)

        return columns

    def _read_primary_key(self, schema_name: str, table_name: str) -> list[str]:
        """Read the primary key columns for a table.

        Returns:
            List of column names in primary key order.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = %s
                AND table_name = %s
                AND constraint_name = 'PRIMARY'
                ORDER BY ordinal_position
            """
            cur.execute(query, (schema_name, table_name))
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        return [row[0] for row in rows]

    def _read_indexes(self, schema_name: str, table_name: str) -> list[IndexDefinition]:
        """Read all indexes on a table (excluding primary key).

        Returns:
            List of IndexDefinition objects.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT
                    s.index_name,
                    s.non_unique,
                    GROUP_CONCAT(s.column_name ORDER BY s.seq_in_index)
                FROM information_schema.statistics s
                WHERE s.table_schema = %s
                AND s.table_name = %s
                AND s.index_name != 'PRIMARY'
                GROUP BY s.index_name, s.non_unique
                ORDER BY s.index_name
            """
            cur.execute(query, (schema_name, table_name))
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        indexes = []
        for idx_name, non_unique, col_names_csv in rows:
            col_names = str(col_names_csv).split(",")

            idx_def = IndexDefinition(
                name=idx_name,
                columns=tuple(col_names),
                is_unique=(int(non_unique) == 0),
                is_clustered=False,
                filter_expression=None,
            )
            indexes.append(idx_def)

        return indexes

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
                    rc.constraint_name,
                    kcu.table_schema,
                    kcu.table_name,
                    GROUP_CONCAT(
                        kcu.column_name ORDER BY kcu.ordinal_position
                    ),
                    kcu.referenced_table_schema,
                    kcu.referenced_table_name,
                    GROUP_CONCAT(
                        kcu.referenced_column_name
                        ORDER BY kcu.ordinal_position
                    ),
                    rc.update_rule,
                    rc.delete_rule
                FROM information_schema.referential_constraints rc
                JOIN information_schema.key_column_usage kcu
                    ON rc.constraint_schema = kcu.constraint_schema
                    AND rc.constraint_name = kcu.constraint_name
                WHERE rc.constraint_schema = %s
                AND kcu.table_name = %s
                AND kcu.referenced_table_name IS NOT NULL
                GROUP BY
                    rc.constraint_name,
                    kcu.table_schema,
                    kcu.table_name,
                    kcu.referenced_table_schema,
                    kcu.referenced_table_name,
                    rc.update_rule,
                    rc.delete_rule
                ORDER BY rc.constraint_name
            """
            cur.execute(query, (schema_name, table_name))
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        fks = []
        for (
            fk_name,
            src_schema,
            src_table,
            src_cols_csv,
            ref_schema,
            ref_table,
            ref_cols_csv,
            update_rule,
            delete_rule,
        ) in rows:
            src_cols = str(src_cols_csv).split(",")
            ref_cols = str(ref_cols_csv).split(",")

            fk_def = ForeignKeyDefinition(
                name=fk_name,
                source_table=f"{src_schema}.{src_table}",
                source_columns=tuple(src_cols),
                referenced_table=f"{ref_schema}.{ref_table}",
                referenced_columns=tuple(ref_cols),
                on_delete=delete_rule,
                on_update=update_rule,
            )
            fks.append(fk_def)

        return fks

    def _read_check_constraints(self, schema_name: str, table_name: str) -> list[str]:
        """Read all CHECK constraints for a table.

        Note: CHECK constraints are enforced in MySQL 8.0.16+.
        Earlier versions parse but ignore them.

        Returns:
            List of constraint expressions.
        """
        try:
            with self.connection.cursor() as cur:
                # MySQL 8.0.16+ supports information_schema.check_constraints
                query = """
                    SELECT cc.check_clause
                    FROM information_schema.check_constraints cc
                    JOIN information_schema.table_constraints tc
                        ON cc.constraint_schema = tc.constraint_schema
                        AND cc.constraint_name = tc.constraint_name
                    WHERE tc.table_schema = %s
                    AND tc.table_name = %s
                    AND tc.constraint_type = 'CHECK'
                    ORDER BY cc.constraint_name
                """
                cur.execute(query, (schema_name, table_name))
                rows: list[tuple[Any, ...]] = list(cur.fetchall())

            return [str(row[0]) for row in rows]
        except Exception:
            # MySQL < 8.0.16 does not have check_constraints table
            return []
