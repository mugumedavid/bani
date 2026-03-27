"""MySQL schema introspection reader.

Queries information_schema to build a complete picture of the database
schema, including tables, columns, indexes, constraints, foreign keys,
and MySQL-specific features like ENUM/SET types and auto_increment.

All metadata is fetched in bulk (6 queries total, regardless of table
count) and then assembled in Python — eliminating the N+1 round-trips
that made the previous implementation slow on large databases.
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pymysql

from bani.connectors.mysql.type_mapper import MySQLTypeMapper
from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)

# Type alias for the (schema, table) key used to group rows in Python.
_TableKey = tuple[str, str]


class MySQLSchemaReader:
    """Introspects MySQL schema using information_schema.

    Handles MySQL-specific features like unsigned integers, ENUM/SET types,
    auto_increment, and charset handling (utf8mb4).

    All metadata is fetched in bulk queries and assembled in Python.
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
        self._type_mapper = MySQLTypeMapper()

    def read_schema(self) -> DatabaseSchema:
        """Introspect the complete schema and return a DatabaseSchema.

        Returns:
            A DatabaseSchema with all tables and their metadata.

        Raises:
            Exception: If any query fails.
        """
        tables = self._read_tables()
        return DatabaseSchema(tables=tuple(tables), source_dialect="mysql")

    # ------------------------------------------------------------------
    # Bulk readers — one query each, across *all* user tables
    # ------------------------------------------------------------------

    def _read_tables(self) -> list[TableDefinition]:
        """Read all user tables and their metadata using bulk queries.

        Returns:
            List of TableDefinition objects.
        """
        table_keys, rowcount_map = self._fetch_table_list()
        if not table_keys:
            return []

        columns_map = self._fetch_all_columns()
        pk_map = self._fetch_all_primary_keys()
        idx_map = self._fetch_all_indexes()
        fk_map = self._fetch_all_foreign_keys()
        chk_map = self._fetch_all_check_constraints()

        tables: list[TableDefinition] = []
        for key in table_keys:
            schema_name, table_name = key
            tables.append(
                TableDefinition(
                    schema_name=schema_name,
                    table_name=table_name,
                    columns=tuple(columns_map.get(key, [])),
                    primary_key=tuple(pk_map.get(key, [])),
                    indexes=tuple(idx_map.get(key, [])),
                    foreign_keys=tuple(fk_map.get(key, [])),
                    check_constraints=tuple(chk_map.get(key, [])),
                    row_count_estimate=rowcount_map.get(key),
                )
            )
        return tables

    # 1. Table list (includes row count estimates) -------------------------

    def _fetch_table_list(
        self,
    ) -> tuple[list[_TableKey], dict[_TableKey, int | None]]:
        """Fetch all base tables and their estimated row counts.

        Returns:
            A tuple of (ordered table keys, row-count mapping).
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
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        table_keys: list[_TableKey] = []
        rowcount_map: dict[_TableKey, int | None] = {}
        for schema_name, table_name, table_rows in rows:
            key: _TableKey = (schema_name, table_name)
            table_keys.append(key)
            rowcount_map[key] = int(table_rows) if table_rows is not None else None

        return table_keys, rowcount_map

    # 2. Columns (all tables in one query) ---------------------------------

    def _fetch_all_columns(self) -> dict[_TableKey, list[ColumnDefinition]]:
        """Fetch all columns across all tables in one query.

        Returns:
            Mapping of (schema, table) to list of ColumnDefinition objects.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT
                    table_schema,
                    table_name,
                    column_name,
                    column_type,
                    is_nullable,
                    column_default,
                    ordinal_position,
                    extra
                FROM information_schema.columns
                WHERE table_schema = %s
                ORDER BY table_schema, table_name, ordinal_position
            """
            cur.execute(query, (self.database,))
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        result: dict[_TableKey, list[ColumnDefinition]] = defaultdict(list)
        for (
            schema_name,
            table_name,
            col_name,
            column_type,
            is_nullable,
            column_default,
            ordinal_pos,
            extra,
        ) in rows:
            # Detect auto_increment from EXTRA field
            is_auto = "auto_increment" in str(extra).lower()

            # Resolve canonical Arrow type for cross-database portability
            arrow_type = self._type_mapper.map_mysql_type_name(column_type)
            arrow_type_str = str(arrow_type)

            result[(schema_name, table_name)].append(
                ColumnDefinition(
                    name=col_name,
                    data_type=column_type,
                    nullable=(is_nullable == "YES"),
                    default_value=column_default,
                    is_auto_increment=is_auto,
                    ordinal_position=int(ordinal_pos) - 1,  # Convert to 0-based
                    arrow_type_str=arrow_type_str,
                )
            )
        return dict(result)

    # 3. Primary keys (all tables in one query) ----------------------------

    def _fetch_all_primary_keys(self) -> dict[_TableKey, list[str]]:
        """Fetch all primary key columns across all tables in one query.

        Returns:
            Mapping of (schema, table) to list of PK column names in order.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT table_schema, table_name, column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = %s
                AND constraint_name = 'PRIMARY'
                ORDER BY table_schema, table_name, ordinal_position
            """
            cur.execute(query, (self.database,))
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        result: dict[_TableKey, list[str]] = defaultdict(list)
        for schema_name, table_name, col_name in rows:
            result[(schema_name, table_name)].append(col_name)
        return dict(result)

    # 4. Indexes (all tables in one query, excluding PRIMARY) --------------

    def _fetch_all_indexes(self) -> dict[_TableKey, list[IndexDefinition]]:
        """Fetch all non-primary indexes across all tables in one query.

        Returns:
            Mapping of (schema, table) to list of IndexDefinition objects.
        """
        with self.connection.cursor() as cur:
            query = """
                SELECT
                    s.table_schema,
                    s.table_name,
                    s.index_name,
                    s.non_unique,
                    GROUP_CONCAT(s.column_name ORDER BY s.seq_in_index)
                FROM information_schema.statistics s
                WHERE s.table_schema = %s
                AND s.index_name != 'PRIMARY'
                GROUP BY s.table_schema, s.table_name,
                         s.index_name, s.non_unique
                ORDER BY s.table_schema, s.table_name, s.index_name
            """
            cur.execute(query, (self.database,))
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        result: dict[_TableKey, list[IndexDefinition]] = defaultdict(list)
        for schema_name, table_name, idx_name, non_unique, col_names_csv in rows:
            col_names = str(col_names_csv).split(",")
            result[(schema_name, table_name)].append(
                IndexDefinition(
                    name=idx_name,
                    columns=tuple(col_names),
                    is_unique=(int(non_unique) == 0),
                    is_clustered=False,
                    filter_expression=None,
                )
            )
        return dict(result)

    # 5. Foreign keys (all tables in one query) ----------------------------

    def _fetch_all_foreign_keys(
        self,
    ) -> dict[_TableKey, list[ForeignKeyDefinition]]:
        """Fetch all foreign keys across all tables in one query.

        Returns:
            Mapping of (schema, table) to list of ForeignKeyDefinition objects.
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
                AND kcu.referenced_table_name IS NOT NULL
                GROUP BY
                    rc.constraint_name,
                    kcu.table_schema,
                    kcu.table_name,
                    kcu.referenced_table_schema,
                    kcu.referenced_table_name,
                    rc.update_rule,
                    rc.delete_rule
                ORDER BY kcu.table_schema, kcu.table_name, rc.constraint_name
            """
            cur.execute(query, (self.database,))
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        result: dict[_TableKey, list[ForeignKeyDefinition]] = defaultdict(list)
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

            result[(src_schema, src_table)].append(
                ForeignKeyDefinition(
                    name=fk_name,
                    source_table=f"{src_schema}.{src_table}",
                    source_columns=tuple(src_cols),
                    referenced_table=f"{ref_schema}.{ref_table}",
                    referenced_columns=tuple(ref_cols),
                    on_delete=delete_rule,
                    on_update=update_rule,
                )
            )
        return dict(result)

    # 6. Check constraints (all tables in one query) -----------------------

    def _fetch_all_check_constraints(self) -> dict[_TableKey, list[str]]:
        """Fetch all CHECK constraints across all tables in one query.

        Note: CHECK constraints are enforced in MySQL 8.0.16+.
        Earlier versions parse but ignore them.

        Returns:
            Mapping of (schema, table) to list of constraint expressions.
        """
        try:
            with self.connection.cursor() as cur:
                # MySQL 8.0.16+ supports information_schema.check_constraints
                query = """
                    SELECT
                        tc.table_schema,
                        tc.table_name,
                        cc.check_clause
                    FROM information_schema.check_constraints cc
                    JOIN information_schema.table_constraints tc
                        ON cc.constraint_schema = tc.constraint_schema
                        AND cc.constraint_name = tc.constraint_name
                    WHERE tc.table_schema = %s
                    AND tc.constraint_type = 'CHECK'
                    ORDER BY tc.table_schema, tc.table_name, cc.constraint_name
                """
                cur.execute(query, (self.database,))
                rows: list[tuple[Any, ...]] = list(cur.fetchall())

            result: dict[_TableKey, list[str]] = defaultdict(list)
            for schema_name, table_name, check_clause in rows:
                result[(schema_name, table_name)].append(str(check_clause))
            return dict(result)
        except Exception:
            # MySQL < 8.0.16 does not have check_constraints table
            return {}
